/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/io/http_session_manager.hxx"

#include "test_helper_integration.hxx"

#include "test/utils/logger.hxx"
#include "utils/move_only_context.hxx"

#include "couchbase/cluster.hxx"

#include "core/analytics_stream.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_any_replica.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/query_stream.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/connection_string.hxx"
#include "core/utils/json.hxx"

#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <asio/post.hpp>

#include <spdlog/fmt/bundled/format.h>

#include <atomic>
#include <cstddef>
#include <future>
#include <string>
#include <thread>
#include <tuple>

TEST_CASE("integration: connecting with empty bootstrap nodes list", "[integration]")
{
  asio::io_context io{};
  auto connstr = couchbase::core::utils::parse_connection_string("couchbase://");
  REQUIRE(connstr.bootstrap_nodes.empty());
  auto origin = couchbase::core::origin({}, connstr);
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  auto f = barrier->get_future();
  cluster.open(origin, [barrier](std::error_code ec) mutable {
    barrier->set_value(ec);
  });
  auto rc = f.get();
  REQUIRE(rc == couchbase::errc::common::invalid_argument);
  test::utils::close_cluster(cluster);
  io_thread.join();
}

TEST_CASE("integration: a guard whose cluster cannot open reports it instead of aborting",
          "[integration]")
{
  auto ctx = test::utils::test_context::load_from_environment();
  // No bootstrap nodes, so open() reports invalid_argument without touching the network.
  ctx.connection_string = "couchbase://";

  REQUIRE_THROWS_AS(test::utils::integration_test_guard{ ctx }, std::system_error);

  // Reaching this line is the assertion. The guard starts its io threads before opening the
  // cluster, and a constructor that throws leaves the destructor unrun: unless the constructor
  // joins them itself, the threads are still joinable when the members are destroyed and
  // ~thread() takes the whole binary down with std::terminate.
  SUCCEED();
}

TEST_CASE("integration: connecting with unresponsive first node in bootstrap nodes list",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};
  auto ctx = test::utils::test_context::load_from_environment();
  if (ctx.deployment == test::utils::deployment_type::capella ||
      ctx.deployment == test::utils::deployment_type::elixir) {
    // This breaks SRV assumptions (only one host in connection string)
    SKIP("capella deployment uses single host in the connection string, which assumed to be "
         "reachable");
  }
  auto connstr = couchbase::core::utils::parse_connection_string(ctx.connection_string);
  REQUIRE_FALSE(connstr.bootstrap_nodes.empty());
  connstr.bootstrap_nodes.insert(connstr.bootstrap_nodes.begin(),
                                 couchbase::core::utils::connection_string::node{
                                   "example.com",
                                   11210,
                                   couchbase::core::utils::connection_string::address_type::dns,
                                   couchbase::core::utils::connection_string::bootstrap_mode::gcccp,
                                 });
  auto origin = couchbase::core::origin(ctx.build_auth(), connstr);
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  auto f = barrier->get_future();
  cluster.open(origin, [barrier](std::error_code ec) mutable {
    barrier->set_value(ec);
  });
  auto rc = f.get();
  REQUIRE_SUCCESS(rc);
  test::utils::close_cluster(cluster);
  io_thread.join();
}

TEST_CASE("integration: can connect with handler capturing non-copyable object", "[integration]")
{
  test::utils::integration_test_guard integration;

  couchbase::core::cluster cluster(integration.io);

  // test connecting
  {
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    test::utils::move_only_context ctx("foobar");
    std::string output;
    cluster.open(integration.origin, [barrier, ctx = std::move(ctx), &output](std::error_code ec) {
      output = ctx.payload();
      barrier->set_value(ec);
    });
    auto rc = f.get();
    REQUIRE(!rc);
    CHECK(output == "foobar");
  }

  // test opening a bucket
  {
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    test::utils::move_only_context ctx("foobar");
    std::string output;
    cluster.open_bucket(integration.ctx.bucket,
                        [barrier, ctx = std::move(ctx), &output](std::error_code ec) {
                          output = ctx.payload();
                          barrier->set_value(ec);
                        });
    auto rc = f.get();
    REQUIRE(!rc);
    CHECK(output == "foobar");
  }

  // test disconnecting
  {
    auto barrier = std::make_shared<std::promise<bool>>();
    auto f = barrier->get_future();
    test::utils::move_only_context ctx("foobar");
    std::string output;
    cluster.close([barrier, ctx = std::move(ctx), &output]() mutable {
      output = ctx.payload();
      barrier->set_value(false);
    });

    auto rc = f.get();
    REQUIRE(!rc);
    CHECK(output == "foobar");
  }
}

TEST_CASE("integration: destroy cluster without waiting for close completion", "[integration]")
{
  test::utils::init_logger();
  auto ctx = test::utils::test_context::load_from_environment();

  if (ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment is incompatible with parts of this test, but it is probably bug in "
         "SDK. FIXME");
  }

  asio::io_context io{};

  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  auto origin = couchbase::core::origin(
    ctx.build_auth(), couchbase::core::utils::parse_connection_string(ctx.connection_string));
  if (ctx.use_wan_development_profile) {
    origin.options().apply_profile("wan_development");
  }

  test::utils::open_cluster(cluster, origin);
  test::utils::open_bucket(cluster, ctx.bucket);

  // hit KV
  {
    couchbase::core::document_id id{
      ctx.bucket, "_default", "_default", test::utils::uniq_id("foo")
    };
    couchbase::core::operations::upsert_request req{ id,
                                                     couchbase::core::utils::to_binary("{{}}") };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  // hit Query
  if (ctx.version.supports_query()) {
    couchbase::core::operations::query_request req{ R"(SELECT 42 AS the_answer)" };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  // close but do not explicitly wait for callback
  std::atomic_bool closed{ false };
  cluster.close([&closed]() {
    closed = true;
  });
  io_thread.join();
  REQUIRE(closed);
}

TEST_CASE("integration: connecting with a custom transactions metadata collection that is in a "
          "bucket that does not exist - Public API",
          "[integration]")
{
  test::utils::init_logger();
  auto ctx = test::utils::test_context::load_from_environment();

  auto opts = couchbase::cluster_options(ctx.username, ctx.password);
  if (ctx.use_wan_development_profile) {
    opts.apply_profile("wan_development");
  }

  opts.transactions().metadata_collection(couchbase::transactions::transaction_keyspace{
    "nonexistent",
    "_default",
    "_default",
  });

  auto [err, cluster] = couchbase::cluster::connect(ctx.connection_string, opts).get();

  REQUIRE(err.ec() == couchbase::errc::common::bucket_not_found);
}

TEST_CASE("integration: reopen bucket after changing to incorrect credentials", "[integration]")
{
  SKIP("Remove once CXXCBC-749 is fixed");

  test::utils::integration_test_guard integration;
  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  {
    auto err = integration.cluster.update_credentials(
      couchbase::core::cluster_credentials{ "invalid-username", "invalid-password" });
    REQUIRE_SUCCESS(err.ec);
  }

  test::utils::close_bucket(integration.cluster, integration.ctx.bucket);

  auto barrier = std::make_shared<std::promise<std::error_code>>();
  auto f = barrier->get_future();
  integration.cluster.open_bucket(integration.ctx.bucket, [barrier](std::error_code ec) {
    barrier->set_value(ec);
  });
  auto rc = f.get();

  REQUIRE(rc == couchbase::errc::common::authentication_failure);
}

TEST_CASE("integration: query after changing to incorrect credentials", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().is_7_6()) {
    SKIP(
      "Query versions without MB-39484 do not authenticate if RBAC is not required in the query");
  }

  couchbase::core::operations::query_request req{ R"(SELECT 1=1)" };
  {
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }
  {
    auto err = integration.cluster.update_credentials(
      couchbase::core::cluster_credentials{ "invalid-username", "invalid-password" });
    REQUIRE_SUCCESS(err.ec);
  }
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();

  session_mgr->close();
  auto resp = test::utils::execute(integration.cluster, req);

  REQUIRE(resp.ctx.ec == couchbase::errc::common::authentication_failure);
}

TEST_CASE("integration: cluster::find_bucket_by_name returns nullptr for unknown bucket",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  // No bucket has been opened — the bucket map must be empty.
  auto bucket = integration.cluster.find_bucket_by_name("no_such_bucket");
  CHECK(bucket == nullptr);
}

TEST_CASE("integration: cluster::find_bucket_by_name returns non-null after open_bucket",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  auto bucket = integration.cluster.find_bucket_by_name(integration.ctx.bucket);
  CHECK(bucket != nullptr);

  // A bucket that was never opened must still return nullptr.
  auto missing = integration.cluster.find_bucket_by_name("no_such_bucket");
  CHECK(missing == nullptr);
}

TEST_CASE("integration: cluster::tracer and meter return non-null shared_ptr by value",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  // tracer() and meter() return by value (std::shared_ptr) since CXXCBC-794;
  // verify they are non-null and that receiving them by value compiles.
  std::shared_ptr<couchbase::core::tracing::tracer_wrapper> tracer = integration.cluster.tracer();
  std::shared_ptr<couchbase::core::metrics::meter_wrapper> meter = integration.cluster.meter();
  CHECK(tracer != nullptr);
  CHECK(meter != nullptr);
}

TEST_CASE("integration: a request whose deadline passes while the open handler runs times out",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};
  auto ctx = test::utils::test_context::load_from_environment();
  auto origin = couchbase::core::origin(
    ctx.build_auth(), couchbase::core::utils::parse_connection_string(ctx.connection_string));
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  // The open handler runs on the IO thread before held requests resume, so no timer callback can
  // run while it is blocked. A request issued then is held, and its deadline passes in the hold.
  auto entered = std::make_shared<std::promise<std::error_code>>();
  auto entered_future = entered->get_future();
  auto release = std::make_shared<std::promise<void>>();
  cluster.open(origin, [entered, released = release->get_future().share()](std::error_code ec) {
    entered->set_value(ec);
    released.wait();
  });
  const auto open_ec = entered_future.get();

  // A dispatched request for a missing bucket fails with bucket_not_found, so that code is what
  // a request resumed past its deadline would report.
  couchbase::core::operations::get_request request{ couchbase::core::document_id{
    "no_such_bucket", "_default", "_default", "held-past-deadline" } };
  request.timeout = std::chrono::milliseconds{ 100 };
  auto response = std::make_shared<std::promise<std::error_code>>();
  auto response_future = response->get_future();
  cluster.execute(request, [response](couchbase::core::operations::get_response&& resp) {
    response->set_value(resp.ctx.ec());
  });
  std::this_thread::sleep_for(5 * request.timeout.value());
  release->set_value();

  const auto response_ec = response_future.get();
  test::utils::close_cluster(cluster);
  io_thread.join();

  REQUIRE_SUCCESS(open_ec);
  REQUIRE(response_ec == couchbase::errc::common::unambiguous_timeout);
}

TEST_CASE("integration: requests held by the bootstrap report their client_context_id",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};

  // Completes the TCP handshake from its backlog and never answers, so the bootstrap runs until
  // bootstrap_timeout and every request times out while held.
  asio::ip::tcp::acceptor silent_seed(io, { asio::ip::make_address("127.0.0.1"), 0 });
  auto origin = couchbase::core::origin(
    {},
    couchbase::core::utils::parse_connection_string(fmt::format(
      "couchbase://127.0.0.1:{}?bootstrap_timeout=20000", silent_seed.local_endpoint().port())));
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });
  cluster.open(origin, [](std::error_code /* the bootstrap times out */) {
  });

  couchbase::core::operations::query_request query{ "SELECT 1" };
  query.client_context_id = "held-query-stream";
  query.timeout = std::chrono::milliseconds{ 200 };
  auto query_ctx = std::make_shared<std::promise<couchbase::core::error_context::query>>();
  auto query_future = query_ctx->get_future();
  cluster.query_stream(std::move(query),
                       [query_ctx](couchbase::core::query_stream /* never opened */,
                                   couchbase::core::error_context::query ctx) {
                         query_ctx->set_value(std::move(ctx));
                       });

  couchbase::core::operations::analytics_request analytics{};
  analytics.statement = "SELECT 1";
  analytics.client_context_id = "held-analytics-stream";
  analytics.timeout = std::chrono::milliseconds{ 200 };
  auto analytics_ctx = std::make_shared<std::promise<couchbase::core::error_context::analytics>>();
  auto analytics_future = analytics_ctx->get_future();
  cluster.analytics_query_stream(
    std::move(analytics),
    [analytics_ctx](couchbase::core::analytics_stream /* never opened */,
                    couchbase::core::error_context::analytics ctx) {
      analytics_ctx->set_value(std::move(ctx));
    });

  couchbase::core::operations::query_request plain_query{ "SELECT 1" };
  plain_query.client_context_id = "held-query";
  plain_query.timeout = std::chrono::milliseconds{ 200 };
  auto plain_query_ctx = std::make_shared<std::promise<couchbase::core::error_context::query>>();
  auto plain_query_future = plain_query_ctx->get_future();
  cluster.execute(std::move(plain_query),
                  [plain_query_ctx](couchbase::core::operations::query_response&& resp) {
                    plain_query_ctx->set_value(std::move(resp.ctx));
                  });

  couchbase::core::operations::analytics_request plain_analytics{};
  plain_analytics.statement = "SELECT 1";
  plain_analytics.client_context_id = "held-analytics";
  plain_analytics.timeout = std::chrono::milliseconds{ 200 };
  auto plain_analytics_ctx =
    std::make_shared<std::promise<couchbase::core::error_context::analytics>>();
  auto plain_analytics_future = plain_analytics_ctx->get_future();
  cluster.execute(std::move(plain_analytics),
                  [plain_analytics_ctx](couchbase::core::operations::analytics_response&& resp) {
                    plain_analytics_ctx->set_value(std::move(resp.ctx));
                  });

  const auto query_result = query_future.get();
  const auto analytics_result = analytics_future.get();
  const auto plain_query_result = plain_query_future.get();
  const auto plain_analytics_result = plain_analytics_future.get();
  test::utils::close_cluster(cluster);
  io.stop();
  io_thread.join();

  REQUIRE(query_result.ec == couchbase::errc::common::unambiguous_timeout);
  REQUIRE(query_result.client_context_id == "held-query-stream");
  REQUIRE(analytics_result.ec == couchbase::errc::common::unambiguous_timeout);
  REQUIRE(analytics_result.client_context_id == "held-analytics-stream");
  REQUIRE(plain_query_result.ec == couchbase::errc::common::unambiguous_timeout);
  REQUIRE(plain_query_result.client_context_id == "held-query");
  REQUIRE(plain_analytics_result.ec == couchbase::errc::common::unambiguous_timeout);
  REQUIRE(plain_analytics_result.client_context_id == "held-analytics");
}

TEST_CASE("integration: a capability-checked request held by the bootstrap times out on schedule",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};

  // Completes the TCP handshake from its backlog and never answers, so the bootstrap runs until
  // bootstrap_timeout.
  asio::ip::tcp::acceptor silent_seed(io, { asio::ip::make_address("127.0.0.1"), 0 });
  const auto bootstrap_timeout = std::chrono::seconds{ 20 };
  auto origin = couchbase::core::origin({},
                                        couchbase::core::utils::parse_connection_string(fmt::format(
                                          "couchbase://127.0.0.1:{}?bootstrap_timeout={}",
                                          silent_seed.local_endpoint().port(),
                                          std::chrono::milliseconds{ bootstrap_timeout }.count())));
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });
  cluster.open(origin, [](std::error_code /* the bootstrap times out */) {
  });

  // `history` routes it through the bucket capability check, which opens the bucket first.
  couchbase::core::operations::management::collection_create_request request{ "default",
                                                                              "_default",
                                                                              "held-by-bootstrap" };
  request.history = true;
  request.timeout = std::chrono::seconds{ 1 };
  auto response = std::make_shared<std::promise<std::error_code>>();
  auto response_future = response->get_future();
  const auto start = std::chrono::steady_clock::now();
  cluster.execute(
    request,
    [response](couchbase::core::operations::management::collection_create_response&& resp) {
      response->set_value(resp.ctx.ec);
    });

  const auto response_ec = response_future.get();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  test::utils::close_cluster(cluster);
  io.stop();
  io_thread.join();

  REQUIRE(response_ec == couchbase::errc::common::unambiguous_timeout);
  REQUIRE(elapsed < bootstrap_timeout / 2);
}

TEST_CASE("integration: closing a cluster during its DNS SRV lookup completes the open",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};

  // A nameserver that never answers. The lookup falls back from UDP to TCP on the same port, so
  // both are held: the UDP socket is never read and the TCP listener never accepts.
  asio::ip::udp::socket silent_udp(io, { asio::ip::make_address("127.0.0.1"), 0 });
  const auto dns_port = silent_udp.local_endpoint().port();
  asio::ip::tcp::acceptor silent_tcp(io, { asio::ip::make_address("127.0.0.1"), dns_port });
  const auto dns_timeout = std::chrono::seconds{ 60 };

  auto connstr = couchbase::core::utils::parse_connection_string("couchbase://srv.example.invalid");
  REQUIRE(connstr.options.enable_dns_srv);
  connstr.options.dns_config =
    couchbase::core::io::dns::dns_config{ "127.0.0.1", dns_port, dns_timeout };
  auto origin = couchbase::core::origin({}, connstr);
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  auto opened = std::make_shared<std::promise<std::error_code>>();
  auto opened_future = opened->get_future();
  const auto start = std::chrono::steady_clock::now();
  cluster.open(origin, [opened](std::error_code ec) {
    opened->set_value(ec);
  });
  test::utils::close_cluster(cluster);

  const auto status = opened_future.wait_for(dns_timeout / 2);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  std::error_code open_ec{};
  if (status == std::future_status::ready) {
    open_ec = opened_future.get();
  }
  io.stop();
  io_thread.join();

  REQUIRE(status == std::future_status::ready);
  REQUIRE(open_ec == couchbase::errc::network::cluster_closed);
  REQUIRE(elapsed < dns_timeout / 2);
}

TEST_CASE("integration: updating credentials while open() configures TLS does not race",
          "[integration]")
{
  test::utils::init_logger();
  asio::io_context io{};

  // The seed never answers, so open() stays in its bootstrap while the credentials change.
  asio::ip::tcp::acceptor silent_seed(io, { asio::ip::make_address("127.0.0.1"), 0 });
  auto origin = couchbase::core::origin(
    couchbase::core::cluster_credentials{ "user", std::string(64, 'a') },
    couchbase::core::utils::parse_connection_string(fmt::format(
      "couchbases://127.0.0.1:{}?tls_verify=none", silent_seed.local_endpoint().port())));
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  // Catches update_credentials() rewriting origin_ while do_open(), inline in open(), configures
  // TLS. ThreadSanitizer reports it as a data race.
  std::atomic_bool stop{ false };
  std::promise<void> updating;
  auto updater = std::thread([&cluster, &stop, &updating]() {
    for (std::size_t i = 0; !stop; ++i) {
      std::ignore = cluster.update_credentials(
        couchbase::core::cluster_credentials{ "user", std::string(64 + (i % 64), 'b') });
      if (i == 0) {
        updating.set_value();
      }
    }
  });
  updating.get_future().wait();
  cluster.open(origin, [](std::error_code /* the bootstrap does not finish */) {
  });
  std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });
  stop = true;
  updater.join();
  test::utils::close_cluster(cluster);
  io.stop();
  io_thread.join();
  SUCCEED("open() configured TLS while the credentials changed");
}

namespace
{
// Opens a core cluster that has opened no bucket, blocks its IO thread, issues `request`, and
// releases the thread after `blocked`. While the thread is blocked no bucket bootstrap and no timer
// can run, so `blocked` is spent waiting for the bucket from the request's point of view.
template<typename Request>
auto
issue_while_io_blocked(Request request,
                       std::chrono::milliseconds blocked,
                       bool open_bucket_first = false,
                       std::chrono::steady_clock::duration* issued_to_completed = nullptr) ->
  typename Request::response_type
{
  using response_type = typename Request::response_type;
  asio::io_context io{};
  auto ctx = test::utils::test_context::load_from_environment();
  auto origin = couchbase::core::origin(
    ctx.build_auth(), couchbase::core::utils::parse_connection_string(ctx.connection_string));
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });
  auto opened = std::make_shared<std::promise<std::error_code>>();
  auto opened_future = opened->get_future();
  cluster.open(origin, [opened](std::error_code ec) {
    opened->set_value(ec);
  });
  const auto open_ec = opened_future.get();
  std::error_code bucket_ec{};
  if (!open_ec && open_bucket_first) {
    auto bucket_opened = std::make_shared<std::promise<std::error_code>>();
    auto bucket_future = bucket_opened->get_future();
    cluster.open_bucket(ctx.bucket, [bucket_opened](std::error_code ec) {
      bucket_opened->set_value(ec);
    });
    bucket_ec = bucket_future.get();
  }
  if (open_ec || bucket_ec) {
    test::utils::close_cluster(cluster);
    io_thread.join();
    REQUIRE_SUCCESS(open_ec);
    REQUIRE_SUCCESS(bucket_ec);
  }

  std::promise<void> entered;
  std::promise<void> release;
  asio::post(io, [&entered, released = release.get_future().share()] {
    entered.set_value();
    released.wait();
  });
  entered.get_future().wait();

  auto response = std::make_shared<std::promise<response_type>>();
  auto response_future = response->get_future();
  const auto issued = std::chrono::steady_clock::now();
  auto completed = std::make_shared<std::chrono::steady_clock::time_point>();
  cluster.execute(std::move(request), [response, completed](response_type&& resp) {
    *completed = std::chrono::steady_clock::now();
    response->set_value(std::move(resp));
  });
  std::this_thread::sleep_for(blocked);
  release.set_value();

  auto resp = response_future.get();
  if (issued_to_completed != nullptr) {
    *issued_to_completed = *completed - issued;
  }
  test::utils::close_cluster(cluster);
  io_thread.join();
  return resp;
}

auto
document_in_default_bucket(const std::string& prefix) -> couchbase::core::document_id
{
  return { test::utils::test_context::load_from_environment().bucket,
           "_default",
           "_default",
           test::utils::uniq_id(prefix) };
}
} // namespace

TEST_CASE("integration: a key-value request's timeout counts the wait for its bucket to open",
          "[integration]")
{
  test::utils::init_logger();
  couchbase::core::operations::get_request request{ document_in_default_bucket("bucket-wait") };
  request.timeout = std::chrono::milliseconds{ 200 };

  auto resp = issue_while_io_blocked(request, std::chrono::seconds{ 1 });

  REQUIRE(resp.ctx.ec() == couchbase::errc::common::unambiguous_timeout);
}

TEST_CASE("integration: a durable mutation's floor counts from when it is issued", "[integration]")
{
  {
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_enhanced_durability()) {
      SKIP("cluster does not support enhanced durability");
    }
  }
  test::utils::init_logger();
  const tao::json::value value{ { "a", 1 } };
  couchbase::core::operations::upsert_request request{
    document_in_default_bucket("durable-floor"),
    couchbase::core::utils::json::generate_binary(value)
  };
  request.durability_level = couchbase::durability_level::majority;
  request.timeout = std::chrono::milliseconds{ 100 };

  SECTION("the floor replaces a shorter timeout")
  {
    // Blocked past the request's own 100 ms. A slow server may still time the write out, but
    // not before the 1.5 s floor, counted from when the request was issued.
    std::chrono::steady_clock::duration elapsed{};
    auto resp = issue_while_io_blocked(request, std::chrono::milliseconds{ 200 }, true, &elapsed);
    // An upsert that times out after it was sent reports ambiguous_timeout.
    if (resp.ctx.ec() == couchbase::errc::common::unambiguous_timeout ||
        resp.ctx.ec() == couchbase::errc::common::ambiguous_timeout) {
      REQUIRE(elapsed >= std::chrono::milliseconds{ 1'400 });
    } else {
      REQUIRE_SUCCESS(resp.ctx.ec());
    }
  }

  SECTION("waiting for the bucket spends the floor")
  {
    auto resp = issue_while_io_blocked(request, std::chrono::seconds{ 2 });
    REQUIRE(resp.ctx.ec() == couchbase::errc::common::unambiguous_timeout);
  }
}

TEST_CASE("integration: a replica read's timeout counts the wait for its bucket", "[integration]")
{
  test::utils::init_logger();
  couchbase::core::operations::get_any_replica_request request{ document_in_default_bucket(
    "replica-wait") };
  request.timeout = std::chrono::milliseconds{ 200 };

  auto resp = issue_while_io_blocked(request, std::chrono::seconds{ 1 });

  REQUIRE(resp.ctx.ec() == couchbase::errc::common::unambiguous_timeout);
}

TEST_CASE("integration: a capability-checked request's timeout counts the wait for its bucket",
          "[integration]")
{
  test::utils::init_logger();
  couchbase::core::operations::management::collection_create_request request{
    test::utils::test_context::load_from_environment().bucket,
    "_default",
    test::utils::uniq_id("capability-wait")
  };
  request.history = true;
  request.timeout = std::chrono::milliseconds{ 200 };

  auto resp = issue_while_io_blocked(request, std::chrono::seconds{ 1 });

  REQUIRE(resp.ctx.ec == couchbase::errc::common::unambiguous_timeout);
}
