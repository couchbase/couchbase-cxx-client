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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/analytics_stream.hxx"
#include "core/analytics_stream_component.hxx"
#include "core/app_telemetry_meter.hxx"
#include "core/cluster.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/cluster_options.hxx"
#include "core/core_sdk_shim.hxx"
#include "core/http_component.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/management/search_index_drop.hxx"
#include "core/query_stream.hxx"
#include "core/query_stream_component.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;

// A loopback endpoint that answers one request with HTTP 400 and the given JSON body.
class rejecting_endpoint
{
public:
  rejecting_endpoint(asio::io_context& io, const std::string& body)
    : acceptor_{ io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 } }
    , socket_{ io }
    , response_{ "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " +
                 std::to_string(body.size()) + "\r\n\r\n" + body }
  {
    acceptor_.async_accept(socket_, [this](std::error_code accept_ec) {
      if (accept_ec) {
        return;
      }
      socket_.async_read_some(asio::buffer(request_), [this](std::error_code read_ec, std::size_t) {
        if (!read_ec) {
          std::error_code ignored;
          asio::write(socket_, asio::buffer(response_), ignored);
        }
      });
    });
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return acceptor_.local_endpoint().port();
  }

private:
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket socket_;
  std::string response_;
  std::array<char, 4096> request_{};
};

// Points the session manager of a cluster that is never opened at the endpoint for every HTTP
// service, so a request runs through http_command or a stream component against it.
void
route_to(couchbase::core::cluster& cluster, std::uint16_t port)
{
  auto [manager_ec, manager] = cluster.http_session_manager();
  assert_success(manager_ec, "an unopened cluster has a session manager");
  auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
  manager->set_tracer(couchbase::core::tracing::tracer_wrapper::create(
    std::make_shared<couchbase::core::tracing::noop_tracer>(), labels));
  manager->set_meter(couchbase::core::metrics::meter_wrapper::create(
    std::make_shared<couchbase::core::metrics::noop_meter>(), labels));
  manager->set_app_telemetry_meter(std::make_shared<couchbase::core::app_telemetry_meter>());
  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.hostname = "127.0.0.1";
  node.services_plain.search = port;
  node.services_plain.query = port;
  node.services_plain.analytics = port;
  config.nodes.push_back(node);
  manager->set_configuration(config, couchbase::core::cluster_options{});
}

void
run_bounded(asio::io_context& io)
{
  asio::steady_timer give_up{ io };
  give_up.expires_after(4s);
  give_up.async_wait([&io](std::error_code ec) {
    if (ec != asio::error::operation_aborted) {
      io.stop();
    }
  });
  io.run();
}

// Closes the session manager and drains the loop, so the sessions it pooled are released before
// the cluster and the loop go out of scope.
void
shut_down(couchbase::core::cluster& cluster, asio::io_context& io)
{
  auto [manager_ec, manager] = cluster.http_session_manager();
  if (!manager_ec) {
    manager->close();
  }
  io.restart();
  io.run_for(2s);
}

void
a_buffered_request_with_an_undecodable_400_completes_with_invalid_argument(
  [[maybe_unused]] context& ctx)
{
  asio::io_context io;
  rejecting_endpoint server{ io, "{}" };
  couchbase::core::cluster cluster{ io };
  route_to(cluster, server.port());
  auto [manager_ec, manager] = cluster.http_session_manager();

  couchbase::core::operations::management::search_index_drop_request request{};
  request.index_name = "idx";
  int completions = 0;
  std::error_code completed_ec{};
  manager->execute(request, [&](auto response) {
    ++completions;
    completed_ec = response.ctx.ec;
    io.stop();
  });
  run_bounded(io);
  shut_down(cluster, io);

  assert_eq(completions, 1, "the request completes once instead of escaping as an exception");
  assert_eq(completed_ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "a 400 whose body lacks the members the response reads");
}

template<typename Component, typename Request, typename Stream>
auto
stream_dispatch_error(const std::string& body) -> std::optional<std::error_code>
{
  asio::io_context io;
  rejecting_endpoint server{ io, body };
  couchbase::core::cluster cluster{ io };
  route_to(cluster, server.port());

  const Component component{
    io, couchbase::core::http_component{ io, couchbase::core::core_sdk_shim{ cluster } }, 10s
  };
  Request request{};
  request.statement = "SELECT 1";
  std::optional<std::error_code> dispatched{};
  component.execute(std::move(request), [&](Stream, auto error_ctx) {
    dispatched = error_ctx.ec;
    io.stop();
  });
  run_bounded(io);
  shut_down(cluster, io);
  return dispatched;
}

void
a_stream_opened_by_a_400_reports_invalid_argument([[maybe_unused]] context& ctx)
{
  const auto query = stream_dispatch_error<couchbase::core::query_stream_component,
                                           couchbase::core::operations::query_request,
                                           couchbase::core::query_stream>(
    R"({"requestID":"r","errors":[{"code":5000,"msg":"boom"}],"results":[],"status":"fatal"})");
  assert_true(query.has_value(), "query: the component reports the response");
  assert_eq(query.value_or(std::error_code{}),
            std::error_code{ couchbase::errc::common::invalid_argument },
            "query: the component hands the response status to the stream");

  const auto analytics = stream_dispatch_error<couchbase::core::analytics_stream_component,
                                               couchbase::core::operations::analytics_request,
                                               couchbase::core::analytics_stream>(
    R"({"requestID":"r","errors":[{"code":20000,"msg":"boom"}],"results":[],"status":"fatal"})");
  assert_true(analytics.has_value(), "analytics: the component reports the response");
  assert_eq(analytics.value_or(std::error_code{}),
            std::error_code{ couchbase::errc::common::invalid_argument },
            "analytics: the component hands the response status to the stream");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_buffered_request_with_an_undecodable_400_completes_with_invalid_argument) },
      { CASE(a_stream_opened_by_a_400_reports_invalid_argument) },
    },
  };
}

} // namespace couchbase::test
