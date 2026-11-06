/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "fake_server.hxx"

#include <asio.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

namespace couchbase::test::bench
{
namespace
{
// Wire constants, from the memcached binary protocol. Spelled out here rather than taken from
// core/protocol so that the server is independent of the SDK it serves.
namespace magic
{
constexpr std::uint8_t request = 0x80;
constexpr std::uint8_t alt_request = 0x08; // carries framing extras; byte 3 is the key length
constexpr std::uint8_t response = 0x81;
} // namespace magic

namespace opcode
{
constexpr std::uint8_t get = 0x00;
constexpr std::uint8_t set = 0x01;
constexpr std::uint8_t noop = 0x0a;
constexpr std::uint8_t hello = 0x1f;
constexpr std::uint8_t sasl_list_mechs = 0x20;
constexpr std::uint8_t sasl_auth = 0x21;
constexpr std::uint8_t sasl_step = 0x22;
constexpr std::uint8_t select_bucket = 0x89;
constexpr std::uint8_t get_cluster_config = 0xb5;
constexpr std::uint8_t get_error_map = 0xfe;
} // namespace opcode

namespace status
{
constexpr std::uint16_t success = 0x00;
constexpr std::uint16_t not_found = 0x01;
constexpr std::uint16_t no_access = 0x24;
constexpr std::uint16_t unknown_command = 0x81;
} // namespace status

namespace feature
{
constexpr std::uint16_t mutation_seqno = 0x04;
constexpr std::uint16_t xerror = 0x07;
constexpr std::uint16_t select_bucket = 0x08;
constexpr std::uint16_t snappy = 0x0a;
constexpr std::uint16_t json = 0x0b;
constexpr std::uint16_t collections = 0x12;
} // namespace feature

constexpr std::uint8_t datatype_json = 0x01;
constexpr std::uint8_t datatype_snappy = 0x02;
constexpr std::size_t header_size = 24;
// The largest body a frame may declare: the server's default item size limit.
constexpr std::size_t max_body_size = 20 * 1024 * 1024;
constexpr std::uint64_t partition_uuid = 0x0123456789abcdefULL;

auto
read16(const unsigned char* p) -> std::uint16_t
{
  return static_cast<std::uint16_t>((p[0] << 8U) | p[1]);
}

auto
read32(const unsigned char* p) -> std::uint32_t
{
  return (static_cast<std::uint32_t>(p[0]) << 24U) | (static_cast<std::uint32_t>(p[1]) << 16U) |
         (static_cast<std::uint32_t>(p[2]) << 8U) | static_cast<std::uint32_t>(p[3]);
}

template<typename T>
void
put_be(std::string& out, T value)
{
  for (auto shift = static_cast<int>(sizeof(T) * 8) - 8; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xffU));
  }
}

struct document {
  std::string flags; // the four extras bytes of the SET, returned verbatim by GET
  std::uint8_t datatype{};
  std::string value;
  std::uint64_t cas{};
};

struct request {
  const char* header;
  std::uint8_t opcode;
  std::uint8_t datatype;
  std::string_view extras;
  std::string_view key; // with the collection-id prefix, so collections are distinct keys
  std::string_view value;
};
} // namespace

struct fake_server::impl {
  class connection;

  static constexpr std::uint16_t vbuckets{ 1024 };

  std::string bucket{ "default" };
  std::string bucket_config;
  std::string cluster_config;
  asio::io_context ctx{ 1 };
  asio::ip::tcp::acceptor acceptor{ ctx, { asio::ip::make_address("127.0.0.1"), 0 } };
  std::uint16_t port{ acceptor.local_endpoint().port() };
  std::set<std::shared_ptr<connection>> connections{};
  std::unordered_map<std::string, document> documents{};
  std::uint64_t last_cas{ 0x1000 };
  std::uint64_t last_seqno{ 0 };
  std::thread io_thread{};

  impl();
  void accept();
};

class fake_server::impl::connection : public std::enable_shared_from_this<connection>
{
public:
  connection(impl& server, asio::ip::tcp::socket socket)
    : server_{ server }
    , socket_{ std::move(socket) }
  {
  }

  void start()
  {
    read();
  }

  void close()
  {
    asio::error_code ignored;
    socket_.close(ignored);
  }

private:
  void read()
  {
    socket_.async_read_some(asio::buffer(chunk_),
                            [self = shared_from_this()](asio::error_code ec, std::size_t n) {
                              if (ec) {
                                return self->drop();
                              }
                              self->input_.append(self->chunk_.data(), n);
                              if (!self->dispatch()) {
                                return self->drop();
                              }
                              self->flush();
                              self->read();
                            });
  }

  void drop()
  {
    close();
    server_.connections.erase(shared_from_this());
  }

  // Answers every complete frame in the input and keeps a trailing partial one for the next read.
  // False on a frame that is not a request, whose body exceeds max_body_size, or whose sections
  // overrun its body. The first two are decided from the header, so no body is buffered for them.
  auto dispatch() -> bool
  {
    std::size_t offset = 0;
    while (input_.size() - offset >= header_size) {
      const char* header = input_.data() + offset;
      const auto* h = reinterpret_cast<const unsigned char*>(header);
      const std::size_t body_size = read32(h + 8);
      if (h[0] != magic::request && h[0] != magic::alt_request) {
        return false;
      }
      if (body_size > max_body_size) {
        return false;
      }
      if (input_.size() - offset - header_size < body_size) {
        break;
      }
      const bool alt = h[0] == magic::alt_request;
      const std::size_t framing_size = alt ? h[2] : 0;
      const std::size_t key_size = alt ? h[3] : read16(h + 2);
      const std::size_t extras_size = h[4];
      if (framing_size + extras_size + key_size > body_size) {
        return false;
      }
      const char* body = header + header_size + framing_size;
      handle(
        { header,
          h[1],
          h[5],
          { body, extras_size },
          { body + extras_size, key_size },
          { body + extras_size + key_size, body_size - framing_size - extras_size - key_size } });
      offset += header_size + body_size;
    }
    input_.erase(0, offset);
    return true;
  }

  void respond(const request& req,
               std::uint16_t status,
               std::string_view extras = {},
               std::string_view value = {},
               std::uint8_t datatype = 0,
               std::uint64_t cas = 0)
  {
    output_.push_back(static_cast<char>(magic::response));
    output_.push_back(static_cast<char>(req.opcode));
    put_be<std::uint16_t>(output_, 0);
    output_.push_back(static_cast<char>(extras.size()));
    output_.push_back(static_cast<char>(datatype));
    put_be(output_, status);
    put_be(output_, static_cast<std::uint32_t>(extras.size() + value.size()));
    output_.append(req.header + 12, 4); // opaque, echoed as is
    put_be(output_, cas);
    output_.append(extras);
    output_.append(value);
  }

  void handle(const request& req)
  {
    switch (req.opcode) {
      case opcode::hello: {
        std::string accepted;
        for (std::size_t i = 0; i + 1 < req.value.size(); i += 2) {
          const auto f = read16(reinterpret_cast<const unsigned char*>(req.value.data() + i));
          if (f == feature::xerror || f == feature::select_bucket || f == feature::snappy ||
              f == feature::json || f == feature::collections || f == feature::mutation_seqno) {
            put_be(accepted, f);
            mutation_seqno_ = mutation_seqno_ || f == feature::mutation_seqno;
          }
        }
        return respond(req, status::success, {}, accepted);
      }
      case opcode::sasl_list_mechs:
        return respond(req, status::success, {}, "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN");
      case opcode::sasl_auth:
      case opcode::sasl_step:
        // Any mechanism and any credentials succeed at the first step. The SDK does not check a
        // SCRAM server signature, so none is sent.
        return respond(req, status::success);
      case opcode::get_error_map:
        return respond(req, status::success, {}, R"({"version":2,"revision":1,"errors":{}})");
      case opcode::select_bucket:
        bucket_selected_ = req.key == server_.bucket;
        return respond(req, bucket_selected_ ? status::success : status::no_access);
      case opcode::get_cluster_config:
        return respond(req,
                       status::success,
                       {},
                       bucket_selected_ ? server_.bucket_config : server_.cluster_config,
                       datatype_json);
      case opcode::get: {
        const auto it = server_.documents.find(std::string{ req.key });
        if (it == server_.documents.end()) {
          return respond(req, status::not_found);
        }
        const auto& doc = it->second;
        return respond(req, status::success, doc.flags, doc.value, doc.datatype, doc.cas);
      }
      case opcode::set: {
        // CAS and expiry are ignored.
        auto& doc = server_.documents[std::string{ req.key }];
        doc.flags.assign(req.extras.substr(0, 4));
        doc.datatype = req.datatype;
        doc.value.assign(req.value);
        doc.cas = ++server_.last_cas;
        std::string extras;
        if (mutation_seqno_) {
          put_be(extras, partition_uuid);
          put_be(extras, ++server_.last_seqno);
        }
        return respond(req, status::success, extras, {}, 0, doc.cas);
      }
      case opcode::noop:
        return respond(req, status::success);
      default:
        return respond(req, status::unknown_command);
    }
  }

  // One write in flight; responses produced while it runs are sent together after it.
  void flush()
  {
    if (writing_ || output_.empty()) {
      return;
    }
    writing_ = true;
    std::swap(sending_, output_);
    output_.clear();
    asio::async_write(socket_,
                      asio::buffer(sending_),
                      [self = shared_from_this()](asio::error_code ec, std::size_t) {
                        self->writing_ = false;
                        if (ec) {
                          return self->drop();
                        }
                        self->flush();
                      });
  }

  impl& server_;
  asio::ip::tcp::socket socket_;
  std::array<char, 16 * 1024> chunk_{};
  std::string input_{};
  std::string output_{};
  std::string sending_{};
  bool writing_{ false };
  bool bucket_selected_{ false };
  bool mutation_seqno_{ false };
};

fake_server::impl::impl()
{
  const auto node = "127.0.0.1:" + std::to_string(port);
  const auto nodes_ext =
    R"("nodesExt":[{"thisNode":true,"hostname":"127.0.0.1","services":{"kv":)" +
    std::to_string(port) + "}}]";
  cluster_config = R"({"rev":1,)" + nodes_ext + "}";
  std::string vbucket_map;
  for (std::uint16_t i = 0; i < vbuckets; ++i) {
    vbucket_map += i == 0 ? "[0]" : ",[0]";
  }
  bucket_config =
    R"({"rev":1,"name":")" + bucket +
    R"(","nodeLocator":"vbucket","bucketCapabilities":["collections","xattr","cccp"],)" +
    nodes_ext + R"(,"vBucketServerMap":{"hashAlgorithm":"CRC","numReplicas":0,)" +
    R"("serverList":[")" + node + R"("],"vBucketMap":[)" + vbucket_map + "]}}";
}

void
fake_server::impl::accept()
{
  acceptor.async_accept([this](asio::error_code ec, asio::ip::tcp::socket socket) {
    // An accept that completed before shutdown closed the acceptor runs after it; its socket is
    // dropped, since shutdown has already closed every connection it knows of.
    if (ec || !acceptor.is_open()) {
      return;
    }
    asio::error_code ignored;
    socket.set_option(asio::ip::tcp::no_delay{ true }, ignored);
    auto conn = std::make_shared<connection>(*this, std::move(socket));
    connections.insert(conn);
    conn->start();
    accept();
  });
}

fake_server::fake_server()
  : impl_{ std::make_unique<impl>() }
{
  impl_->accept();
  impl_->io_thread = std::thread([this] {
    impl_->ctx.run();
  });
}

fake_server::~fake_server()
{
  // Closing the acceptor and every socket completes each pending operation with an error, after
  // which nothing re-arms and run() returns.
  asio::post(impl_->ctx, [this] {
    asio::error_code ignored;
    impl_->acceptor.close(ignored);
    for (const auto& conn : impl_->connections) {
      conn->close();
    }
  });
  impl_->io_thread.join();
}

auto
fake_server::connection_string() const -> std::string
{
  return "couchbase://127.0.0.1:" + std::to_string(impl_->port) + "=kv";
}

auto
fake_server::username() const -> std::string
{
  return "Administrator";
}

auto
fake_server::password() const -> std::string
{
  return "password";
}

auto
fake_server::snappy_documents() const -> std::size_t
{
  std::promise<std::size_t> count;
  asio::post(impl_->ctx, [this, &count] {
    std::size_t n = 0;
    for (const auto& [key, doc] : impl_->documents) {
      if ((doc.datatype & datatype_snappy) != 0) {
        ++n;
      }
    }
    count.set_value(n);
  });
  return count.get_future().get();
}
} // namespace couchbase::test::bench
