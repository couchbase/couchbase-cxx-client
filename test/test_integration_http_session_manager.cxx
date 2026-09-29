/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "test_helper_integration.hxx"

#include "core/diagnostics.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/management/cluster_describe.hxx"
#include "core/operations/management/search_index_get_all.hxx"
#include "core/query_stream.hxx"
#include "core/service_type_fmt.hxx"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

TEST_CASE("integration: random node selection with analytics service", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_analytics_service()) {
    SKIP("Requires analytics service");
  }

  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

  auto [origin_ec, origin] = integration.cluster.origin();
  REQUIRE_SUCCESS(origin_ec);

  auto [session_ec, session] = session_mgr->check_out(couchbase::core::service_type::analytics, "");
  REQUIRE_SUCCESS(session_ec);

  auto last_addr = fmt::format("{}:{}", session->hostname(), session->port());

  session_mgr->check_in(couchbase::core::service_type::analytics, session);
  auto [session2_ec, session2] =
    session_mgr->check_out(couchbase::core::service_type::analytics, "", last_addr);
  REQUIRE_SUCCESS(session2_ec);

  auto new_addr = fmt::format("{}:{}", session2->hostname(), session2->port());

  if (integration.number_of_analytics_nodes() > 1) {
    REQUIRE(new_addr != last_addr);
  } else {
    REQUIRE(new_addr == last_addr);
  }
}

namespace
{
using couchbase::core::service_type;

// Endpoint ids the session manager lists for `type`, busy and idle, as diagnostics reports them.
auto
listed_endpoints(const couchbase::core::cluster& cluster, service_type type)
  -> std::set<std::string>
{
  auto barrier = std::make_shared<std::promise<couchbase::core::diag::diagnostics_result>>();
  auto f = barrier->get_future();
  cluster.diagnostics({}, [barrier](couchbase::core::diag::diagnostics_result&& res) {
    barrier->set_value(std::move(res));
  });
  auto res = f.get();
  std::set<std::string> ids;
  for (const auto& endpoint : res.services[type]) {
    ids.insert(endpoint.id);
  }
  return ids;
}

// One request to `type` that needs no data: a query, a cluster describe, a search index listing or
// an analytics query.
auto
round_trip(const couchbase::core::cluster& cluster, service_type type) -> std::error_code
{
  switch (type) {
    case service_type::query:
      return test::utils::execute(cluster,
                                  couchbase::core::operations::query_request{ "SELECT 1=1" })
        .ctx.ec;
    case service_type::analytics: {
      couchbase::core::operations::analytics_request request{};
      request.statement = "SELECT 1=1";
      return test::utils::execute(cluster, request).ctx.ec;
    }
    case service_type::search:
      return test::utils::execute(
               cluster, couchbase::core::operations::management::search_index_get_all_request{})
        .ctx.ec;
    case service_type::management:
      return test::utils::execute(
               cluster, couchbase::core::operations::management::cluster_describe_request{})
        .ctx.ec;
    default:
      return couchbase::errc::common::feature_not_available;
  }
}

auto
available_http_services(test::utils::integration_test_guard& integration)
  -> std::vector<service_type>
{
  std::vector<service_type> services{ service_type::management };
  if (integration.has_service(service_type::query)) {
    services.push_back(service_type::query);
  }
  if (integration.has_service(service_type::search)) {
    services.push_back(service_type::search);
  }
  if (integration.has_analytics_service()) {
    services.push_back(service_type::analytics);
  }
  return services;
}
} // namespace

TEST_CASE("integration: every HTTP service serves requests after the session manager is closed",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

  for (const auto type : available_http_services(integration)) {
    INFO("service: " << fmt::format("{}", type));
    REQUIRE_SUCCESS(round_trip(integration.cluster, type));
    session_mgr->close();
    REQUIRE_SUCCESS(round_trip(integration.cluster, type));
  }
}

// Query requests send "connection: keep-alive", so their connections are pooled. The management,
// search and analytics requests in these cases do not, so check_in stops their connections.
TEST_CASE("integration: a query after the session manager is closed pools a new connection",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_service(service_type::query)) {
    SKIP("Requires query service");
  }
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

  REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
  const auto before = listed_endpoints(integration.cluster, service_type::query);
  REQUIRE(before.size() == 1);

  session_mgr->close();
  REQUIRE(listed_endpoints(integration.cluster, service_type::query).empty());

  REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
  const auto after = listed_endpoints(integration.cluster, service_type::query);
  REQUIRE(after.size() == 1);
  REQUIRE(after != before);

  // The connection opened after close() is pooled: the next query reuses it.
  REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
  REQUIRE(listed_endpoints(integration.cluster, service_type::query) == after);
}

TEST_CASE("integration: sequential queries reuse one pooled connection", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_service(service_type::query)) {
    SKIP("Requires query service");
  }

  REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
  const auto first = listed_endpoints(integration.cluster, service_type::query);
  REQUIRE(first.size() == 1);
  for (int i = 0; i < 10; ++i) {
    REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
    REQUIRE(listed_endpoints(integration.cluster, service_type::query) == first);
  }
}

TEST_CASE("integration: queries racing a session manager close each complete", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_service(service_type::query)) {
    SKIP("Requires query service");
  }
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

  constexpr int threads = 4;
  constexpr int per_thread = 25;
  // A request that outlives this has been stranded: close() either cancels it or leaves it to
  // complete on its connection.
  constexpr auto bound = std::chrono::seconds{ 30 };
  // Guarded by mutex; progress is notified whenever one changes.
  std::mutex mutex;
  std::condition_variable progress;
  int completed = 0;
  int finished = 0;
  int stranded = 0;
  std::vector<std::error_code> unexpected;

  std::vector<std::thread> clients;
  for (int t = 0; t < threads; ++t) {
    clients.emplace_back([&]() {
      for (int i = 0; i < per_thread; ++i) {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        integration.cluster.execute(couchbase::core::operations::query_request{ "SELECT 1=1" },
                                    [barrier](couchbase::core::operations::query_response&& resp) {
                                      barrier->set_value(resp.ctx.ec);
                                    });
        if (f.wait_for(bound) != std::future_status::ready) {
          const std::scoped_lock lock(mutex);
          ++stranded;
          break;
        }
        {
          const std::scoped_lock lock(mutex);
          if (const auto ec = f.get(); ec && ec != couchbase::errc::common::request_canceled) {
            unexpected.push_back(ec);
          }
          ++completed;
        }
        progress.notify_all();
      }
      {
        const std::scoped_lock lock(mutex);
        ++finished;
      }
      progress.notify_all();
    });
  }
  // Each completed query triggers a close(), while the other threads' queries are in flight. The
  // bound only ends the loop when nothing completes; a stranded client then reports it.
  {
    std::unique_lock lock(mutex);
    int seen = 0;
    while (finished < threads) {
      if (!progress.wait_for(lock, bound, [&]() {
            return completed != seen || finished == threads;
          })) {
        break;
      }
      seen = completed;
      lock.unlock();
      session_mgr->close();
      lock.lock();
    }
  }
  for (auto& client : clients) {
    client.join();
  }

  REQUIRE(stranded == 0);
  for (const auto& ec : unexpected) {
    INFO("error: " << ec.message());
    CHECK_FALSE(ec);
  }
  REQUIRE(unexpected.empty());
  REQUIRE(completed == threads * per_thread);
  REQUIRE_SUCCESS(round_trip(integration.cluster, service_type::query));
}

TEST_CASE("integration: a streaming query open across a session manager close ends",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_service(service_type::query)) {
    SKIP("Requires query service");
  }
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

  using pulled = std::pair<std::optional<std::string>, std::error_code>;
  // Bounded waits: a stream that close() strands never delivers its terminal.
  constexpr auto bound = std::chrono::seconds{ 30 };
  // 7.1 and earlier servers refuse an ARRAY_RANGE longer than a few tens of thousands, so a large
  // result is the product of two short ranges.
  const auto open = [&](std::size_t rows) -> couchbase::core::query_stream {
    REQUIRE((rows > 0 && (rows <= 1000 || rows % 1000 == 0)));
    const std::size_t inner = std::min<std::size_t>(rows, 1000);
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::query_stream, std::error_code>>>();
    auto f = barrier->get_future();
    integration.cluster.query_stream(
      couchbase::core::operations::query_request{ fmt::format(
        "SELECT RAW a * {} + b FROM ARRAY_RANGE(0, {}) AS a UNNEST ARRAY_RANGE(0, {}) AS b",
        inner,
        rows / inner,
        inner) },
      [barrier](couchbase::core::query_stream stream, couchbase::core::error_context::query ctx) {
        barrier->set_value({ std::move(stream), ctx.ec });
      });
    REQUIRE(f.wait_for(bound) == std::future_status::ready);
    auto [stream, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    return stream;
  };
  const auto pull = [&](couchbase::core::query_stream& stream) -> pulled {
    auto barrier = std::make_shared<std::promise<pulled>>();
    auto f = barrier->get_future();
    stream.next_row([barrier](std::optional<std::string> row, std::error_code ec) {
      barrier->set_value({ std::move(row), ec });
    });
    REQUIRE(f.wait_for(bound) == std::future_status::ready);
    return f.get();
  };

  // Larger than the stream buffers, so the response is still arriving when close() runs.
  constexpr std::size_t total_rows = 200000;
  auto stream = open(total_rows);
  const auto [first_row, first_ec] = pull(stream);
  REQUIRE_SUCCESS(first_ec);
  REQUIRE(first_row.has_value());

  session_mgr->close();

  std::size_t rows = 1;
  std::error_code terminal{};
  while (true) {
    auto [row, ec] = pull(stream);
    if (!row.has_value()) {
      terminal = ec;
      break;
    }
    ++rows;
  }
  INFO("rows: " << rows << ", terminal: " << terminal.message());
  // close() stops the connection the stream reads from. The stream ends with request_canceled,
  // unless the whole response had already arrived.
  REQUIRE(
    (terminal == couchbase::errc::common::request_canceled || (!terminal && rows == total_rows)));

  // A stream opened after close() connects through the free-form path and reads to its end.
  auto next = open(3);
  std::size_t next_rows = 0;
  while (true) {
    auto [row, ec] = pull(next);
    REQUIRE_SUCCESS(ec);
    if (!row.has_value()) {
      break;
    }
    ++next_rows;
  }
  REQUIRE(next_rows == 3);
}

TEST_CASE("integration: ping reaches every HTTP service after the session manager is closed",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);
  const auto services = available_http_services(integration);
  const std::set<service_type> requested(services.begin(), services.end());

  const auto ping = [&]() {
    auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
    auto f = barrier->get_future();
    integration.cluster.ping(
      {}, {}, requested, {}, [barrier](couchbase::core::diag::ping_result&& res) {
        barrier->set_value(std::move(res));
      });
    return f.get();
  };

  ping();
  session_mgr->close();
  auto res = ping();
  for (const auto type : services) {
    INFO("service: " << fmt::format("{}", type));
    REQUIRE_FALSE(res.services[type].empty());
    for (const auto& endpoint : res.services[type]) {
      INFO("endpoint: " << endpoint.remote << ", error: " << endpoint.error.value_or(""));
      REQUIRE(endpoint.state == couchbase::core::diag::ping_state::ok);
    }
  }
}
