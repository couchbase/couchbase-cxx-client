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
constexpr std::uint8_t remove = 0x04;
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
constexpr std::uint16_t einval = 0x04;
constexpr std::uint16_t auth_continue = 0x21;
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

auto
rotr64(std::uint64_t x, unsigned n) -> std::uint64_t
{
  return (x >> n) | (x << (64U - n));
}

auto
sha512(std::string_view message) -> std::string
{
  static constexpr std::array<std::uint64_t, 80> k{
    0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL,
    0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL,
    0xd807aa98a3030242ULL, 0x12835b0145706fbeULL, 0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
    0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL, 0xc19bf174cf692694ULL,
    0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
    0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
    0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL,
    0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL, 0x06ca6351e003826fULL, 0x142929670a0e6e70ULL,
    0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
    0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
    0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL,
    0xd192e819d6ef5218ULL, 0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
    0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL, 0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL,
    0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL,
    0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
    0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL,
    0xca273eceea26619cULL, 0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL,
    0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL, 0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
    0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL, 0x431d67c49c100d4cULL,
    0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL,
  };

  std::array<std::uint64_t, 8> h{
    0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL, 0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL, 0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL,
  };

  std::string padded(message);
  padded.push_back(static_cast<char>(0x80));
  while ((padded.size() % 128) != 112) {
    padded.push_back('\0');
  }
  put_be<std::uint64_t>(padded, 0);
  put_be<std::uint64_t>(padded, std::uint64_t{ message.size() } * 8U);

  std::array<std::uint64_t, 80> w{};
  for (std::size_t chunk = 0; chunk < padded.size(); chunk += 128) {
    const auto* p = reinterpret_cast<const unsigned char*>(padded.data() + chunk);
    for (std::size_t i = 0; i < 16; ++i) {
      w[i] = (static_cast<std::uint64_t>(read32(p + i * 8)) << 32U) | read32(p + i * 8 + 4);
    }
    for (std::size_t i = 16; i < 80; ++i) {
      const auto s0 = rotr64(w[i - 15], 1) ^ rotr64(w[i - 15], 8) ^ (w[i - 15] >> 7U);
      const auto s1 = rotr64(w[i - 2], 19) ^ rotr64(w[i - 2], 61) ^ (w[i - 2] >> 6U);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    auto [a, b, c, d, e, f, g, hh] = h;
    for (std::size_t i = 0; i < 80; ++i) {
      const auto s1 = rotr64(e, 14) ^ rotr64(e, 18) ^ rotr64(e, 41);
      const auto ch = (e & f) ^ (~e & g);
      const auto temp1 = hh + s1 + ch + k[i] + w[i];
      const auto s0 = rotr64(a, 28) ^ rotr64(a, 34) ^ rotr64(a, 39);
      const auto maj = (a & b) ^ (a & c) ^ (b & c);
      const auto temp2 = s0 + maj;

      hh = g;
      g = f;
      f = e;
      e = d + temp1;
      d = c;
      c = b;
      b = a;
      a = temp1 + temp2;
    }
    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
    h[4] += e;
    h[5] += f;
    h[6] += g;
    h[7] += hh;
  }

  std::string digest;
  digest.reserve(64);
  for (const auto word : h) {
    put_be(digest, word);
  }
  return digest;
}

auto
hmac_sha512(std::string_view key, std::string_view data) -> std::string
{
  std::string key_block(128, '\0');
  if (key.size() > 128) {
    const auto hashed = sha512(key);
    key_block.replace(0, hashed.size(), hashed);
  } else {
    key_block.replace(0, key.size(), key);
  }

  std::string ipad(128, '\0');
  std::string opad(128, '\0');
  for (std::size_t i = 0; i < 128; ++i) {
    ipad[i] = static_cast<char>(static_cast<unsigned char>(key_block[i]) ^ 0x36U);
    opad[i] = static_cast<char>(static_cast<unsigned char>(key_block[i]) ^ 0x5cU);
  }
  ipad.append(data);
  opad.append(sha512(ipad));
  return sha512(opad);
}

auto
base64_encode(std::string_view input) -> std::string
{
  static constexpr std::string_view alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((input.size() + 2) / 3) * 4);
  std::size_t i = 0;
  for (; i + 2 < input.size(); i += 3) {
    const auto b0 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i]));
    const auto b1 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 1]));
    const auto b2 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 2]));
    const auto triple = (b0 << 16U) | (b1 << 8U) | b2;
    out.push_back(alphabet[(triple >> 18U) & 0x3fU]);
    out.push_back(alphabet[(triple >> 12U) & 0x3fU]);
    out.push_back(alphabet[(triple >> 6U) & 0x3fU]);
    out.push_back(alphabet[triple & 0x3fU]);
  }
  const auto rem = input.size() - i;
  if (rem == 1) {
    const auto b0 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i]));
    const auto triple = b0 << 16U;
    out.push_back(alphabet[(triple >> 18U) & 0x3fU]);
    out.push_back(alphabet[(triple >> 12U) & 0x3fU]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    const auto b0 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i]));
    const auto b1 = static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 1]));
    const auto triple = (b0 << 16U) | (b1 << 8U);
    out.push_back(alphabet[(triple >> 18U) & 0x3fU]);
    out.push_back(alphabet[(triple >> 12U) & 0x3fU]);
    out.push_back(alphabet[(triple >> 6U) & 0x3fU]);
    out.push_back('=');
  }
  return out;
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
        return respond(req, status::success, {}, "SCRAM-SHA512 PLAIN");
      case opcode::sasl_auth: {
        if (req.key == "PLAIN") {
          return respond(req, status::success);
        }
        if (req.key != "SCRAM-SHA512") {
          return respond(req, status::einval);
        }
        if (req.value.size() <= 3 || req.value.substr(0, 3) != "n,,") {
          return respond(req, status::no_access);
        }
        scram_client_first_bare_.assign(req.value.substr(3));
        const auto nonce_pos = scram_client_first_bare_.find(",r=");
        if (nonce_pos == std::string::npos || nonce_pos + 3 >= scram_client_first_bare_.size()) {
          return respond(req, status::no_access);
        }
        const auto client_nonce = scram_client_first_bare_.substr(nonce_pos + 3);
        scram_server_first_ = "r=" + client_nonce + "srv,s=c2FsdA==,i=1";
        return respond(req, status::auth_continue, {}, scram_server_first_);
      }
      case opcode::sasl_step: {
        const auto proof_pos = req.value.rfind(",p=");
        if (proof_pos == std::string_view::npos || scram_client_first_bare_.empty() ||
            scram_server_first_.empty()) {
          return respond(req, status::no_access);
        }
        const auto client_final_without_proof = req.value.substr(0, proof_pos);
        const std::string auth_message = scram_client_first_bare_ + "," + scram_server_first_ +
                                         "," + std::string{ client_final_without_proof };
        // PBKDF2-HMAC-SHA512("password", "salt", i=1, dkLen=64) reduces to a single HMAC-SHA512.
        const auto salted_password =
          hmac_sha512("password", std::string_view{ "salt\x00\x00\x00\x01", 8 });
        const auto server_key = hmac_sha512(salted_password, "Server Key");
        const auto server_signature = hmac_sha512(server_key, auth_message);
        return respond(req, status::success, {}, "v=" + base64_encode(server_signature));
      }
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
      case opcode::remove: {
        // CAS is ignored.
        if (server_.documents.erase(std::string{ req.key }) == 0) {
          return respond(req, status::not_found);
        }
        std::string extras;
        if (mutation_seqno_) {
          put_be(extras, partition_uuid);
          put_be(extras, ++server_.last_seqno);
        }
        return respond(req, status::success, extras, {}, 0, ++server_.last_cas);
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
  std::string scram_client_first_bare_{};
  std::string scram_server_first_{};
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
