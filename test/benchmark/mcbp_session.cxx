/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "bench_registry.hxx"

#include "core/document_id.hxx"
#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_message.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/origin.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::test::bench
{
namespace
{
using core::protocol::hello_feature;

// The session never bootstraps, so none of these is called.
class no_bootstrap_listener : public core::impl::bootstrap_state_listener
{
public:
  void report_bootstrap_error(const std::string& /*endpoint*/, std::error_code /*ec*/) override
  {
  }

  void report_bootstrap_success(const std::vector<std::string>& /*endpoints*/) override
  {
  }

  void register_config_listener(std::shared_ptr<core::config_listener> /*listener*/) override
  {
  }

  void unregister_config_listener(std::shared_ptr<core::config_listener> /*listener*/) override
  {
  }
};

// The features a server negotiates when it supports every one the session requests with the
// default cluster options. A server answers in the order of the request, and supports_feature()
// searches that order.
auto
negotiated_features() -> std::vector<hello_feature>
{
  core::protocol::hello_request_body hello{};
  hello.enable_unordered_execution();
  hello.enable_clustermap_change_notification();
  hello.enable_deduplicate_not_my_vbucket_clustermap();
  hello.enable_compression();
  hello.enable_mutation_tokens();
  return hello.features();
}

// Stops the session and drains io_context on exit; destroying an unstopped session calls
// shared_from_this() from its destructor and terminates.
class stop_on_exit
{
public:
  stop_on_exit(core::io::mcbp_session& session, asio::io_context& io)
    : session_{ session }
    , io_{ io }
  {
  }

  stop_on_exit(const stop_on_exit&) = delete;
  stop_on_exit(stop_on_exit&&) = delete;
  auto operator=(const stop_on_exit&) -> stop_on_exit& = delete;
  auto operator=(stop_on_exit&&) -> stop_on_exit& = delete;

  ~stop_on_exit()
  {
    try {
      session_.stop(retry_reason::do_not_retry);
      io_.run();
    } catch (const std::exception& e) {
      std::cerr << "stopping the session failed: " << e.what() << '\n';
    }
  }

private:
  core::io::mcbp_session& session_;
  asio::io_context& io_;
};

// Runs the four session lookups of mcbp_command::send() on 1 and 8 threads sharing one
// unconnected session: supports_feature(collections), default-collection get_collection_uid(),
// context(), and supports_feature(snappy).
void
mcbp_session_dispatch_lookups(state& st)
{
  asio::io_context io{};
  core::origin origin(core::cluster_credentials{ "user", "pass" },
                      core::utils::parse_connection_string("couchbase://127.0.0.1"));
  core::io::mcbp_session session{ "bench-client-id",
                                  "bench-node-uuid",
                                  io,
                                  std::move(origin),
                                  std::make_shared<no_bootstrap_listener>(),
                                  std::nullopt,
                                  negotiated_features() };
  const stop_on_exit stop{ session, io };
  if (!session.supports_feature(hello_feature::collections) ||
      !session.supports_feature(hello_feature::snappy) ||
      !session.get_collection_uid("_default._default").has_value()) {
    throw std::runtime_error("the session does not report the features it was given");
  }
  const std::string path{ "_default._default" };
  for (const std::size_t threads : { std::size_t{ 1 }, std::size_t{ 8 } }) {
    st.batched("t" + std::to_string(threads), threads, [&] {
      const bool collections = session.supports_feature(hello_feature::collections);
      const auto uid = session.get_collection_uid(path);
      const auto context = session.context();
      const bool snappy = session.supports_feature(hello_feature::snappy);
      do_not_optimize(collections);
      do_not_optimize(uid);
      do_not_optimize(context);
      do_not_optimize(snappy);
    });
  }
}

void
mcbp_protocol_key(state& st)
{
  st.batched("default_collection", 1, [&] {
    core::document_id id{ "default", "_default", "_default", "bench::document::000001" };
    id.collection_uid(0);
    do_not_optimize(core::make_protocol_key(id));
  });
  st.batched("named_collection", 1, [&] {
    core::document_id id{ "default", "inventory", "airline", "bench::document::000001" };
    id.collection_uid(42);
    do_not_optimize(core::make_protocol_key(id));
  });
}

// Constructing an mcbp_command instantiates its deadline handlers, which reach every member below.
// The benchmark never starts a command, so none of them is called.
struct stub_command_manager {
  [[noreturn]] static void not_started()
  {
    throw std::logic_error("the benchmark never starts a command");
  }

  [[nodiscard]] auto deadlines() -> core::io::deadline_queue&
  {
    not_started();
  }
  [[nodiscard]] auto name() const -> const std::string&
  {
    not_started();
  }
  [[nodiscard]] auto log_prefix() const -> const std::string&
  {
    not_started();
  }
  [[nodiscard]] auto orphan_reporter() const -> std::shared_ptr<core::orphan_reporter>
  {
    not_started();
  }
  [[nodiscard]] auto app_telemetry_meter() const -> std::shared_ptr<core::app_telemetry_meter>
  {
    not_started();
  }
};

void
mcbp_command_upsert(state& st)
{
  asio::io_context io{};
  auto manager = std::make_shared<stub_command_manager>();
  const core::document_id id{ "default", "_default", "_default", "bench::document::000001" };
  for (const auto& [variant, size] : {
         std::pair<std::string_view, std::size_t>{ "kib_1", 1024 },
         std::pair<std::string_view, std::size_t>{ "kib_64", 65536 },
       }) {
    std::vector<std::byte> value(size, std::byte{ 'x' });
    st.batched(variant, 1, [&] {
      core::operations::upsert_request req{ id, std::move(value) };
      core::operations::mcbp_command<stub_command_manager, core::operations::upsert_request> cmd{
        io, manager, std::move(req), std::chrono::milliseconds{ 2500 }
      };
      do_not_optimize(cmd);
      value = std::move(cmd.request.value);
    });
  }
}

void
mcbp_get_make_response(state& st)
{
  const core::operations::get_request req{ core::document_id{
    "default", "_default", "_default", "bench::document::000001" } };
  for (const auto& [variant, value_size] : {
         std::pair<std::string_view, std::size_t>{ "kib_1", 1024 },
         std::pair<std::string_view, std::size_t>{ "kib_64", 65536 },
       }) {
    const std::size_t size = value_size;
    std::vector<std::byte> body(4 + size, std::byte{ 'x' });
    st.batched(variant, 1, [&] {
      core::io::mcbp_message msg{};
      msg.header.magic = static_cast<std::uint8_t>(core::protocol::magic::client_response);
      msg.header.opcode = static_cast<std::uint8_t>(core::protocol::client_opcode::get);
      msg.header.extlen = 4;
      msg.header.bodylen = core::utils::host_to_network(static_cast<std::uint32_t>(4 + size));
      msg.body = std::move(body);
      core::operations::get_request::encoded_response_type encoded{ std::move(msg) };
      auto response = req.make_response({}, std::move(encoded));
      if (response.value.size() != size) {
        throw std::runtime_error("unexpected get_response value size");
      }
      do_not_optimize(response);
      body = std::move(encoded.data());
    });
  }
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return {
    suite_name,
    {
      { CASE(mcbp_session_dispatch_lookups),
        {},
        "Performs the four session lookups of one KV dispatch per iteration on 1 and 8 "
        "threads." },
      { CASE(mcbp_protocol_key),
        {},
        "Constructs a document_id and encodes its MCBP protocol key for the default and a named "
        "collection." },
      { CASE(mcbp_command_upsert),
        {},
        "Constructs an mcbp_command for an upsert_request carrying a 1 KiB or 64 KiB value." },
      { CASE(mcbp_get_make_response),
        {},
        "Decodes a 1 KiB or 64 KiB GET response frame and builds get_response with "
        "get_request::make_response." },
    },
  };
}
} // namespace couchbase::test::bench
