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

#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

#include "utils/drop_guard.hxx"
#include "utils/integration_shortcuts.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/cluster.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/cluster_options.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/management/eventing_drop_function.hxx"
#include "core/operations/management/scope_drop.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/write.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <cerrno>

#include <unistd.h>
#endif

namespace couchbase::test
{
template<>
struct operand_printer<std::vector<std::string>> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const std::vector<std::string>& calls) -> std::string
  {
    std::string text{ "[" };
    for (const auto& call : calls) {
      text += (text.size() > 1 ? ", \"" : "\"") + call + "\"";
    }
    return text + "]";
  }
};

namespace
{
using couchbase::core::operations::management::eventing_drop_function_request;
using couchbase::core::operations::management::scope_drop_request;
using ::test::utils::drop_guard;

struct reply {
  unsigned status;
  std::string body;
  // Read the request and never answer it.
  bool hang{ false };
};

// HTTP/1.1 on loopback. Request n gets script[n], and the last entry repeats.
class scripted_server
{
public:
  explicit scripted_server(std::vector<reply> script)
    : script_{ std::move(script) }
  {
    assert_false(script_.empty(), "the server has a reply to give");
    accept();
    thread_ = std::thread([this]() {
      io_.run();
    });
  }

  scripted_server(const scripted_server&) = delete;
  scripted_server(scripted_server&&) = delete;
  auto operator=(const scripted_server&) -> scripted_server& = delete;
  auto operator=(scripted_server&&) -> scripted_server& = delete;

  ~scripted_server()
  {
    io_.stop();
    thread_.join();
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return port_;
  }

  // The request line of every request received, without the HTTP version.
  [[nodiscard]] auto calls() const -> std::vector<std::string>
  {
    const std::scoped_lock lock(calls_mutex_);
    return calls_;
  }

private:
  struct connection {
    explicit connection(asio::io_context& io)
      : socket{ io }
    {
    }
    asio::ip::tcp::socket socket;
    std::string buffer{};
    std::array<char, 4096> chunk{};
    std::string out{};
  };

  void accept()
  {
    auto conn = std::make_shared<connection>(io_);
    acceptor_.async_accept(conn->socket, [this, conn](std::error_code ec) {
      if (ec) {
        return;
      }
      // Held until the server stops, so a hung request keeps its connection open.
      connections_.push_back(conn);
      read(conn);
      accept();
    });
  }

  void read(const std::shared_ptr<connection>& conn)
  {
    // The management requests under test carry no body, so the headers end the request.
    if (const auto end = conn->buffer.find("\r\n\r\n"); end != std::string::npos) {
      auto line = conn->buffer.substr(0, conn->buffer.find("\r\n"));
      line.erase(line.rfind(' '));
      conn->buffer.erase(0, end + 4);
      return respond(conn, std::move(line));
    }
    conn->socket.async_read_some(asio::buffer(conn->chunk),
                                 [this, conn](std::error_code ec, std::size_t n) {
                                   if (ec) {
                                     return;
                                   }
                                   conn->buffer.append(conn->chunk.data(), n);
                                   read(conn);
                                 });
  }

  void respond(const std::shared_ptr<connection>& conn, std::string line)
  {
    std::size_t index = 0;
    {
      const std::scoped_lock lock(calls_mutex_);
      index = calls_.size();
      calls_.push_back(std::move(line));
    }
    const auto& r = script_[std::min(index, script_.size() - 1)];
    if (r.hang) {
      return;
    }
    conn->out =
      "HTTP/1.1 " + std::to_string(r.status) +
      " Scripted\r\nConnection: keep-alive\r\nContent-Length: " + std::to_string(r.body.size()) +
      "\r\n\r\n" + r.body;
    asio::async_write(
      conn->socket, asio::buffer(conn->out), [this, conn](std::error_code ec, std::size_t) {
        if (!ec) {
          read(conn);
        }
      });
  }

  std::vector<reply> script_;
  asio::io_context io_{};
  asio::ip::tcp::acceptor acceptor_{ io_,
                                     asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"),
                                                              0 } };
  std::uint16_t port_{ acceptor_.local_endpoint().port() };
  std::vector<std::shared_ptr<connection>> connections_{};
  mutable std::mutex calls_mutex_{};
  std::vector<std::string> calls_{};
  std::thread thread_{};
};

// A cluster that is never opened: its HTTP session manager is pointed at the scripted server, so
// test::utils::execute() reaches that server through the same path a real drop takes.
class loopback_cluster
{
public:
  explicit loopback_cluster(std::uint16_t port)
  {
    auto [ec, manager] = cluster_.http_session_manager();
    assert_success(ec, "an unopened cluster hands out its session manager");
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
    node.services_plain.eventing = port;
    config.nodes.push_back(node);
    couchbase::core::cluster_options options{};
    options.enable_tls = false;
    options.network = "default";
    manager->set_configuration(config, options);

    thread_ = std::thread([this]() {
      io_.run();
    });
  }

  loopback_cluster(const loopback_cluster&) = delete;
  loopback_cluster(loopback_cluster&&) = delete;
  auto operator=(const loopback_cluster&) -> loopback_cluster& = delete;
  auto operator=(loopback_cluster&&) -> loopback_cluster& = delete;

  ~loopback_cluster()
  {
    ::test::utils::close_cluster(cluster_);
    io_.stop();
    thread_.join();
  }

  [[nodiscard]] auto get() const -> const couchbase::core::cluster&
  {
    return cluster_;
  }

private:
  asio::io_context io_{};
  couchbase::core::cluster cluster_{ io_ };
  std::thread thread_{};
};

#ifndef _WIN32
// Redirects fd 2 into a pipe for its lifetime. A reader thread drains the pipe, so a writer never
// blocks on a full one.
class stderr_capture
{
public:
  stderr_capture()
  {
    std::array<int, 2> fds{};
    if (::pipe(fds.data()) != 0) {
      fail("a pipe for stderr is created");
    }
    std::fflush(stderr);
    saved_ = ::dup(STDERR_FILENO);
    if (saved_ < 0 || ::dup2(fds[1], STDERR_FILENO) < 0) {
      if (saved_ >= 0) {
        ::close(saved_);
      }
      ::close(fds[0]);
      ::close(fds[1]);
      fail("stderr is redirected into the pipe");
    }
    ::close(fds[1]);
    read_fd_ = fds[0];
    reader_ = std::thread([this]() {
      std::array<char, 1024> chunk{};
      while (true) {
        const auto n = ::read(read_fd_, chunk.data(), chunk.size());
        if (n < 0 && errno == EINTR) {
          continue;
        }
        if (n <= 0) {
          break;
        }
        printed_.append(chunk.data(), static_cast<std::size_t>(n));
      }
    });
  }

  stderr_capture(const stderr_capture&) = delete;
  stderr_capture(stderr_capture&&) = delete;
  auto operator=(const stderr_capture&) -> stderr_capture& = delete;
  auto operator=(stderr_capture&&) -> stderr_capture& = delete;

  ~stderr_capture()
  {
    restore();
  }

  // Restores fd 2 and returns everything written to it since construction.
  auto take() -> std::string
  {
    restore();
    return std::move(printed_);
  }

private:
  // Closing the last write end is what ends the reader's loop.
  void restore()
  {
    if (saved_ < 0) {
      return;
    }
    std::fflush(stderr);
    ::dup2(saved_, STDERR_FILENO);
    ::close(saved_);
    saved_ = -1;
    reader_.join();
    ::close(read_fd_);
  }

  int saved_{ -1 };
  int read_fd_{ -1 };
  std::string printed_{};
  std::thread reader_{};
};
#endif

// Destroys a guard for `request` and returns what it wrote to stderr. Off POSIX nothing is
// captured and the result is empty, so stderr assertions are POSIX-only.
template<typename Request>
auto
drop(const couchbase::core::cluster& cluster, Request request, std::chrono::seconds budget)
  -> std::string
{
#ifndef _WIN32
  stderr_capture capture;
#endif
  {
    const drop_guard<Request> guard{ cluster, "the resource", std::move(request), budget };
  }
#ifndef _WIN32
  return capture.take();
#else
  return {};
#endif
}

// Below the timeout::network case budget, so a guard that retries forever fails the case's own
// assertion rather than the runner's timeout.
constexpr std::chrono::seconds budget{ 3 };

// The guard clamps its final attempt to its deadline, so time past the budget is scheduling only.
constexpr std::chrono::milliseconds scheduling_margin{ 250 };

const std::string scope_drop_call{ "DELETE /pools/default/buckets/bucket/scopes/scope" };
const std::string eventing_drop_call{ "DELETE /api/v1/functions/function" };

auto
scope_drop() -> scope_drop_request
{
  scope_drop_request request{};
  request.bucket_name = "bucket";
  request.scope_name = "scope";
  return request;
}

auto
eventing_drop() -> eventing_drop_function_request
{
  eventing_drop_function_request request{};
  request.name = "function";
  return request;
}

void
a_drop_answered_503_is_retried_until_it_succeeds([[maybe_unused]] context& ctx)
{
  const scripted_server server{ {
    { 503, "" },
    { 503, "" },
    { 200, R"({"uid":"1"})" },
  } };
  std::string printed;
  {
    const loopback_cluster cluster{ server.port() };
    printed = drop(cluster.get(), scope_drop(), budget);
  }
  assert_eq(server.calls(),
            std::vector<std::string>{ scope_drop_call, scope_drop_call, scope_drop_call },
            "both 503s are retried with the same drop and the 200 ends it");
#ifndef _WIN32
  assert_eq(printed, std::string{}, "a drop that succeeded reports nothing");
#endif
}

void
a_drop_that_stays_unavailable_gives_up_within_its_budget([[maybe_unused]] context& ctx)
{
  const scripted_server server{ { { 503, "" } } };
  std::string printed;
  std::chrono::steady_clock::duration elapsed{};
  {
    const loopback_cluster cluster{ server.port() };
    const auto start = std::chrono::steady_clock::now();
    printed = drop(cluster.get(), eventing_drop(), budget);
    elapsed = std::chrono::steady_clock::now() - start;
  }
  assert_true(elapsed < budget + scheduling_margin,
              "the guard stops retrying when its budget runs out");
  const auto calls = server.calls();
  assert_true(calls.size() >= 2, "the 503 is retried while budget remains");
  assert_true(std::all_of(calls.begin(),
                          calls.end(),
                          [](const auto& call) {
                            return call == eventing_drop_call;
                          }),
              "every attempt is the eventing drop");
#ifndef _WIN32
  assert_contains(printed, std::string{ "service_not_available" }, "the report names the 503");
#endif
}

void
an_empty_503_from_eventing_is_retried_not_taken_as_success([[maybe_unused]] context& ctx)
{
  // The eventing decoder leaves a 503 with an empty body as success; the guard must not.
  const scripted_server server{ {
    { 503, "" },
    { 200, "" },
  } };
  std::string printed;
  {
    const loopback_cluster cluster{ server.port() };
    printed = drop(cluster.get(), eventing_drop(), budget);
  }
  assert_eq(server.calls(),
            std::vector<std::string>{ eventing_drop_call, eventing_drop_call },
            "the empty 503 is retried with the same drop");
#ifndef _WIN32
  assert_eq(printed, std::string{}, "a drop that succeeded reports nothing");
#endif
}

void
a_resource_already_gone_counts_as_dropped([[maybe_unused]] context& ctx)
{
  const scripted_server server{ { { 404, "Scope with name `scope` is not found" } } };
  std::string printed;
  {
    const loopback_cluster cluster{ server.port() };
    printed = drop(cluster.get(), scope_drop(), budget);
  }
  assert_eq(
    server.calls(), std::vector<std::string>{ scope_drop_call }, "scope_not_found is not retried");
#ifndef _WIN32
  assert_eq(printed, std::string{}, "a scope that is already gone reports nothing");
#endif
}

void
a_non_transient_failure_is_reported_without_a_retry([[maybe_unused]] context& ctx)
{
  const scripted_server server{ { { 500, "boom" } } };
  std::string printed;
  {
    const loopback_cluster cluster{ server.port() };
    printed = drop(cluster.get(), scope_drop(), budget);
  }
  assert_eq(server.calls(),
            std::vector<std::string>{ scope_drop_call },
            "internal_server_failure is not retried");
#ifndef _WIN32
  assert_contains(printed, std::string{ "internal_server_failure" }, "the failure is reported");
#endif
}

void
the_report_names_the_last_server_error_not_the_timeout_after_it([[maybe_unused]] context& ctx)
{
  const scripted_server server{ {
    { 406, R"({"code":20,"name":"ERR_APP_NOT_UNDEPLOYED","description":"still deployed"})" },
    { 0, "", true },
  } };
  std::string printed;
  std::chrono::steady_clock::duration elapsed{};
  {
    const loopback_cluster cluster{ server.port() };
    const auto start = std::chrono::steady_clock::now();
    printed = drop(cluster.get(), eventing_drop(), budget);
    elapsed = std::chrono::steady_clock::now() - start;
  }
  assert_true(elapsed < budget + scheduling_margin, "a hung request does not extend the budget");
  assert_eq(server.calls(),
            std::vector<std::string>{ eventing_drop_call, eventing_drop_call },
            "the deployed function is retried once");
#ifndef _WIN32
  assert_contains(
    printed, std::string{ "eventing_function_deployed" }, "the report names the server's error");
  assert_false(printed.find("timeout") != std::string::npos,
               "the timeout of the final attempt is not what is reported");
#endif
}

void
a_dismissed_guard_sends_nothing([[maybe_unused]] context& ctx)
{
  const scripted_server server{ { { 200, R"({"uid":"1"})" } } };
  {
    const loopback_cluster cluster{ server.port() };
    drop_guard<scope_drop_request> guard{ cluster.get(), "the resource", scope_drop(), budget };
    guard.dismiss();
  }
  assert_eq(server.calls(), std::vector<std::string>{}, "a dismissed guard sends no request");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_drop_answered_503_is_retried_until_it_succeeds), {}, timeout::network },
      { CASE(a_drop_that_stays_unavailable_gives_up_within_its_budget), {}, timeout::network },
      { CASE(an_empty_503_from_eventing_is_retried_not_taken_as_success), {}, timeout::network },
      { CASE(a_resource_already_gone_counts_as_dropped), {}, timeout::network },
      { CASE(a_non_transient_failure_is_reported_without_a_retry), {}, timeout::network },
      { CASE(the_report_names_the_last_server_error_not_the_timeout_after_it),
        {},
        timeout::network },
      { CASE(a_dismissed_guard_sends_nothing), {}, timeout::network },
    },
  };
}

} // namespace couchbase::test
