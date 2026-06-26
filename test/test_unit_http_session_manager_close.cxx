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

#include "test_helper.hxx"

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
#include <asio/ssl.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>

namespace
{
class noop_ping_reporter : public couchbase::core::diag::ping_reporter
{
public:
  void report(couchbase::core::diag::endpoint_ping_info&& /* info */) override
  {
  }
};

class noop_ping_collector : public couchbase::core::diag::ping_collector
{
public:
  auto build_reporter() -> std::shared_ptr<couchbase::core::diag::ping_reporter> override
  {
    return reporter_;
  }

private:
  std::shared_ptr<noop_ping_reporter> reporter_{ std::make_shared<noop_ping_reporter>() };
};
} // namespace

// A pooled idle HTTP session keeps a read armed so a peer-initiated close is
// noticed promptly (see http_session::set_idle / do_read).  That armed read
// holds a shared_ptr to the session and counts as outstanding io_context work,
// so http_session_manager::close() must tear the session down (stop()), not
// merely cancel its idle timer (reset_idle()).  If it only cancels the timer the
// read stays pending forever and io_context::run() never returns -- which is how
// "destroy cluster without waiting for close completion" hangs on shutdown.
//
// The loopback server below answers the ping with a keep-alive 200 so the SDK
// parks the session idle, then deliberately keeps the connection open without
// arming a further read of its own.  After close(), the only thing that could
// keep the io_context alive is the leaked idle read, so io.run() returning on
// its own is exactly the property under test.
TEST_CASE("unit: http_session_manager close lets the io_context drain", "[unit]")
{
  asio::io_context io;

  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();

  std::atomic_bool server_responded{ false };
  asio::ip::tcp::socket server{ io };
  auto rbuf = std::make_shared<std::array<char, 4096>>();
  acceptor.async_accept(server, [&, rbuf](std::error_code accept_ec) {
    REQUIRE_FALSE(accept_ec);
    server.async_read_some(asio::buffer(*rbuf), [&, rbuf](std::error_code read_ec, std::size_t) {
      if (read_ec) {
        return;
      }
      auto resp = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
      asio::async_write(server, asio::buffer(*resp), [&, resp](std::error_code w_ec, std::size_t) {
        if (w_ec) {
          return;
        }
        // Intentionally do NOT arm another read here: the connection stays
        // open but contributes no outstanding work to the io_context, so the
        // only pending operation left is the SDK's idle liveness read.
        server_responded = true;
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
  // Well beyond the test window: the idle timer must not be what closes the
  // connection, otherwise the leaked read would be papered over.
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

  // Open a pooled management session to the node (the noop response parks it idle).
  manager->ping(
    std::set<couchbase::core::service_type>{ couchbase::core::service_type::management },
    std::chrono::seconds(10),
    std::make_shared<noop_ping_collector>());

  // Phase 1: pump the loop long enough for the session to be parked idle, then
  // stop so we can drive the close path deterministically.
  asio::steady_timer settle{ io };
  settle.expires_after(std::chrono::seconds(1));
  settle.async_wait([&](std::error_code) {
    io.stop();
  });
  io.run();
  REQUIRE(server_responded.load());

  // Phase 2: close the manager and assert the io_context drains on its own.  We
  // run io.run() on a worker and bound the wait with a future so a regression
  // (leaked idle read) surfaces as a failed assertion instead of a hung suite.
  io.restart();
  manager->close();

  std::packaged_task<void()> run_task([&]() {
    io.run();
  });
  auto drained_future = run_task.get_future();
  std::thread runner(std::move(run_task));

  const bool drained =
    drained_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  if (!drained) {
    // Unblock the hung run() so the worker can be joined and the suite proceeds.
    io.stop();
  }
  runner.join();

  CHECK(drained);
}
