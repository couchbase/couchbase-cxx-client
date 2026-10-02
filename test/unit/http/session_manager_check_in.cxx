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

#include "framework/context.hxx"
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

#include <algorithm>
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
// A ping is the only way this case obtains a connected, pooled session; the state it reports is
// what distinguishes one parked idle from one that failed and closed itself.
class notifying_ping_reporter : public couchbase::core::diag::ping_reporter
{
public:
  explicit notifying_ping_reporter(std::function<void(couchbase::core::diag::ping_state)> on_report)
    : on_report_{ std::move(on_report) }
  {
  }

  void report(couchbase::core::diag::endpoint_ping_info&& info) override
  {
    on_report_(info.state);
  }

private:
  std::function<void(couchbase::core::diag::ping_state)> on_report_;
};

class notifying_ping_collector : public couchbase::core::diag::ping_collector
{
public:
  explicit notifying_ping_collector(
    std::function<void(couchbase::core::diag::ping_state)> on_report)
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

// http_streaming_response_body::close_impl decides on a stop while holding its own mutex and
// executes it after releasing that mutex. A clean end landing in that window clears the recorded
// close and hands the connection to http_session_manager::check_in, which would publish it to the
// idle pool; the next request checks that connection out and the stop then runs underneath it.
// mark_stopping() records the decision at the point it is made, and check_in refuses a connection
// carrying one.
//
// The control establishes that this pool reuses a session at all, so a claimed session failing to
// come back is attributable to the claim rather than to the pool never reusing anything. check_in's
// refusal is probed before the next checkout, because check_out skips a claimed session too.
void
a_session_claimed_for_teardown_is_not_returned_to_the_pool([[maybe_unused]] context& ctx)
{
  asio::io_context io;

  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();

  // Minimal management endpoint: accept, answer the noop ping with a keep-alive 200 so the manager
  // parks the session as idle, and leave the socket open.
  asio::ip::tcp::socket server{ io };
  auto rbuf = std::make_shared<std::array<char, 4096>>();
  // Recorded in the handlers and asserted after io.run(): a failure thrown from a handler unwinds
  // out of run() past a live acceptor and server socket.
  std::error_code accept_error{};
  acceptor.async_accept(server, [&, rbuf](std::error_code accept_ec) {
    if (accept_ec) {
      accept_error = accept_ec;
      return;
    }
    server.async_read_some(asio::buffer(*rbuf), [&, rbuf](std::error_code read_ec, std::size_t) {
      if (read_ec) {
        return;
      }
      auto resp = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
      asio::async_write(server, asio::buffer(*resp), [resp](std::error_code, std::size_t) {
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

  std::string pooled_id;
  std::string reused_id;
  std::string after_claim_id;
  bool claimed_published = true;
  std::error_code checkout_error{};
  std::error_code reuse_error{};
  std::error_code after_claim_error{};

  // The ping's completion handler reports to the collector and only then checks the session back
  // in, so the sequence runs from a posted handler: by the time the post executes, the session is
  // idle and check_out can take it.
  auto reported_state = couchbase::core::diag::ping_state::error;
  auto collector =
    std::make_shared<notifying_ping_collector>([&](couchbase::core::diag::ping_state state) {
      reported_state = state;
      asio::post(io, [&]() {
        const auto type = couchbase::core::service_type::management;

        auto [checkout_ec, pooled] = manager->check_out(type, {});
        checkout_error = checkout_ec;
        if (checkout_ec) {
          return io.stop();
        }
        pooled_id = pooled->id();

        // Control: an unclaimed session goes back to the pool and comes out again.
        manager->check_in(type, pooled);
        auto [reuse_ec, reused] = manager->check_out(type, {});
        reuse_error = reuse_ec;
        if (reuse_ec) {
          return io.stop();
        }
        reused_id = reused->id();

        // A body that stops the connection claims it before releasing its own lock.
        reused->mark_stopping();
        manager->check_in(type, reused);
        // check_in arms the idle timer of a session it publishes, and the checkout above cancelled
        // the one armed before. Probed before check_out, which also skips a claimed session.
        claimed_published = reused->reset_idle();
        auto [after_ec, after_claim] = manager->check_out(type, {});
        after_claim_error = after_ec;
        if (!after_ec) {
          after_claim_id = after_claim->id();
        }

        io.stop();
      });
    });

  manager->ping(
    std::set<couchbase::core::service_type>{ couchbase::core::service_type::management },
    std::chrono::seconds(10),
    collector);

  // Bound the case so a connection left open fails here rather than running to the harness budget.
  asio::steady_timer deadline{ io };
  // Clamped: expires_after adds to now(), which overflows for a bound saturated at
  // milliseconds::max().
  deadline.expires_after(std::min<std::chrono::milliseconds>(scaled_budget(std::chrono::seconds(2)),
                                                             std::chrono::hours(1)));
  deadline.async_wait([&](std::error_code) {
    io.stop();
  });

  io.run();

  assert_success(accept_error, "the loopback endpoint accepts the manager's connection");
  assert_true(reported_state == couchbase::core::diag::ping_state::ok,
              "the ping succeeds, so the session it opened is checked back in as idle");
  assert_success(checkout_error, "the pooled session is available for checkout");
  assert_true(!pooled_id.empty(), "the ping leaves a session in the pool");
  assert_success(reuse_error, "the unclaimed session is available again");
  assert_eq(reused_id, pooled_id, "an unclaimed session is republished and checked out again");
  assert_false(claimed_published,
               "check_in does not publish a session claimed with mark_stopping()");
  assert_success(after_claim_error, "a checkout still succeeds once the pool is empty");
  assert_true(after_claim_id != reused_id,
              "a session claimed with mark_stopping() is not republished, so the next checkout "
              "opens a new one");

  manager->close();
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      // A loopback connect, one HTTP round trip and three checkouts, bounded at two scaled
      // seconds inside the case.
      { CASE(a_session_claimed_for_teardown_is_not_returned_to_the_pool), {}, timeout::network },
    },
  };
}

} // namespace couchbase::test
