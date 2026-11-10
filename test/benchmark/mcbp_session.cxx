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

#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/origin.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>

#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
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
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name,
           { { CASE(mcbp_session_dispatch_lookups),
               {},
               "Performs the four session lookups of one KV dispatch per iteration on 1 and 8 "
               "threads." } } };
}
} // namespace couchbase::test::bench
