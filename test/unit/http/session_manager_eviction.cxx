/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/cluster_options.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/origin.hxx"
#include "core/ping_collector.hxx"
#include "core/ping_reporter.hxx"
#include "core/service_type.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/ssl.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::test
{
namespace
{
// The ping is the only way this case learns that the pooled session exists, so the reporter is
// what tells it the session is there.
class notifying_ping_reporter : public couchbase::core::diag::ping_reporter
{
public:
  explicit notifying_ping_reporter(std::function<void()> on_report)
    : on_report_{ std::move(on_report) }
  {
  }

  void report(couchbase::core::diag::endpoint_ping_info&& /* info */) override
  {
    on_report_();
  }

private:
  std::function<void()> on_report_;
};

class notifying_ping_collector : public couchbase::core::diag::ping_collector
{
public:
  explicit notifying_ping_collector(std::function<void()> on_report)
    : reporter_{ std::make_shared<notifying_ping_reporter>(std::move(on_report)) }
  {
  }

  auto build_reporter() -> std::shared_ptr<couchbase::core::diag::ping_reporter> override
  {
    return reporter_;
  }

private:
  std::shared_ptr<notifying_ping_reporter> reporter_;
};

// When a node leaves the cluster configuration, the session manager must close its pooled idle
// HTTP connections to that node promptly, rather than leaving them open until the idle timer fires
// (idle_http_connection_timeout). The timeout here is set well beyond the case's window so the
// only thing that can close the connection in time is update_config() tearing it down.
void
idle_connections_to_a_removed_node_are_closed([[maybe_unused]] context& ctx)
{
  asio::io_context io;

  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();

  // Minimal management endpoint: accept the connection, answer the noop ping with a keep-alive 200
  // (so the SDK parks the session as idle), then read again -- the next completion is the SDK
  // closing the connection.
  std::atomic_bool server_saw_close{ false };
  asio::ip::tcp::socket server{ io };
  auto rbuf = std::make_shared<std::array<char, 4096>>();
  acceptor.async_accept(server, [&, rbuf](std::error_code accept_ec) {
    assert_success(accept_ec, "the loopback endpoint accepts the manager's connection");
    server.async_read_some(asio::buffer(*rbuf), [&, rbuf](std::error_code read_ec, std::size_t) {
      if (read_ec) {
        return;
      }
      auto resp = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
      asio::async_write(
        server, asio::buffer(*resp), [&, rbuf, resp](std::error_code w_ec, std::size_t) {
          if (w_ec) {
            return;
          }
          server.async_read_some(asio::buffer(*rbuf),
                                 [&, rbuf](std::error_code r2_ec, std::size_t) {
                                   if (r2_ec) { // EOF/reset => the SDK closed its end
                                     server_saw_close = true;
                                     io.stop();
                                   }
                                 });
        });
    });
  });

  auto client_ssl_ctx = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client);
  couchbase::core::tls_context_provider tls{ client_ssl_ctx };

  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "pass";
  couchbase::core::cluster_options options{};
  options.enable_tls = false;
  options.network = "default";
  options.idle_http_connection_timeout = std::chrono::seconds(30);
  couchbase::core::origin origin{ creds, "127.0.0.1", port, options };

  auto manager =
    std::make_shared<couchbase::core::io::http_session_manager>("client-id", io, tls, origin);
  auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
  manager->set_tracer(couchbase::core::tracing::tracer_wrapper::create(
    std::make_shared<couchbase::core::tracing::noop_tracer>(), labels));
  manager->set_meter(couchbase::core::metrics::meter_wrapper::create(
    std::make_shared<couchbase::core::metrics::noop_meter>(), labels));
  manager->set_app_telemetry_meter(std::make_shared<couchbase::core::app_telemetry_meter>());

  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.hostname = "127.0.0.1";
  node.services_plain.management = port;
  config.nodes.push_back(node);
  manager->set_configuration(config, options);

  // The ping's completion handler reports to the collector and only then checks the session back
  // into the pool, so the node is dropped from a posted handler rather than after a wait long
  // enough to cover both steps: the post runs once that handler has returned and the session is
  // idle.
  auto collector = std::make_shared<notifying_ping_collector>([&]() {
    asio::post(io, [&]() {
      manager->update_config(couchbase::core::topology::configuration{});
    });
  });

  // Open a pooled management session to the node (the noop response parks it idle).
  manager->ping(
    std::set<couchbase::core::service_type>{ couchbase::core::service_type::management },
    std::chrono::seconds(10),
    collector);

  // Bound the case so a connection left open fails here rather than running to the harness budget.
  asio::steady_timer deadline{ io };
  deadline.expires_after(std::chrono::seconds(2));
  deadline.async_wait([&](std::error_code) {
    io.stop();
  });

  io.run();

  assert_true(server_saw_close.load(), "the manager closes the connection to the evicted node");
  manager->close();
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      // A loopback connect, one HTTP round trip and one close, bounded at two seconds inside the
      // case.
      { CASE(idle_connections_to_a_removed_node_are_closed), {}, timeout::network },
    },
  };
}

} // namespace couchbase::test
