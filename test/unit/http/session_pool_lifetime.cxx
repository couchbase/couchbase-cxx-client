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

#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

#include "core/analytics_stream.hxx"
#include "core/analytics_stream_component.hxx"
#include "core/app_telemetry_meter.hxx"
#include "core/cluster.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/cluster_options.hxx"
#include "core/core_sdk_shim.hxx"
#include "core/diagnostics.hxx"
#include "core/error_context/query.hxx"
#include "core/free_form_http_request.hxx"
#include "core/http_component.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/io/http_session.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/io/http_streaming_response.hxx"
#include "core/io/query_cache.hxx"
#include "core/logger/logger.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/origin.hxx"
#include "core/pending_operation.hxx"
#include "core/pending_operation_connection_info.hxx"
#include "core/query_stream.hxx"
#include "core/query_stream_component.hxx"
#include "core/row_streamer_options.hxx"
#include "core/service_type.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/movable_function.hxx"

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/ssl.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#if defined(__linux__)
#include <linux/sockios.h>
#include <sys/ioctl.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::http_session;
using couchbase::core::io::http_session_manager;
using namespace std::chrono_literals;

constexpr auto service = couchbase::core::service_type::management;

// Bounds a single wait. Scaled by CB_TEST_TIMEOUT_MULTIPLIER, as the case budgets are, so a slow
// instrumented run does not fail a step that still has budget left, and a stall in a wait still
// fails on this bound before the case budget. run_on waits without bound for a step already
// running, so a deadlock inside a step surfaces as the case timeout.
auto
patience() -> std::chrono::milliseconds
{
  static const auto scaled = scaled_budget(2s);
  return scaled;
}

// Elapsed time against a scaled bound, in milliseconds. now() + bound, or a comparison at the
// clock's resolution, overflows for a bound saturated at milliseconds::max().
auto
within(std::chrono::steady_clock::time_point start, std::chrono::milliseconds bound) -> bool
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                               start) < bound;
}

// A query response of query_rows rows: the preamble with the first row, the other rows, then the
// trailer.
constexpr std::string_view query_first_row =
  R"({"requestID":"r1","signature":{"n":"number"},"results":[{"n":0})";
constexpr std::string_view query_later_rows = R"(,{"n":1},{"n":2})";
constexpr std::string_view query_trailer = R"(],"status":"success"})";
constexpr std::size_t query_rows = 3;

// Keep-alive HTTP endpoint on its own io_context and thread, so the client io_context under test
// carries only the SDK's work. The request path selects the response; a request to /query/service
// or /analytics/service is answered as the path its client-context-id header names:
//   /complete    200 with an empty body, in one write
//   /split       200 with a five-byte body written after a pause, so it arrives in a later read
//   /pieces      200 with a nine-byte body written in three parts, each after a pause
//   /close       as /complete, with Connection: close
//   /close-split as /split, with Connection: close
//   /incomplete  200 announcing five bytes and sending two; the rest never comes
//   /rows        200 with the whole query response, in one write
//   /one-row     200 with the query preamble and first row; the rest is held until release_held()
//   /many-rows   200 with the query preamble and eight rows in one write; the trailer is held until
//                release_held()
//   /malformed   200 with the query preamble, the first row and a malformed second; the rest of the
//                announced body never comes
//   /trailing    200 with the whole query response and a newline after it; the newline is held
//                until release_held()
//   /held/<path> nothing until release_held(), then the response to <path>
//   any other    no response; the connection is held open until the client closes it
class loopback_http_server
{
public:
  loopback_http_server()
    : acceptor_{ io_, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 } }
    , port_{ acceptor_.local_endpoint().port() }
  {
    accept();
    thread_ = std::thread([this]() {
      io_.run();
    });
  }

  loopback_http_server(const loopback_http_server&) = delete;
  loopback_http_server(loopback_http_server&&) = delete;
  auto operator=(const loopback_http_server&) -> loopback_http_server& = delete;
  auto operator=(loopback_http_server&&) -> loopback_http_server& = delete;

  ~loopback_http_server()
  {
    io_.stop();
    thread_.join();
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return port_;
  }

  [[nodiscard]] auto accepted() const -> std::size_t
  {
    return accepted_.load();
  }

  // Response heads whose write has completed.
  [[nodiscard]] auto heads_written() const -> std::size_t
  {
    return heads_written_.load();
  }

  // Response heads written by respond() that the client has acknowledged, so they are in its
  // receive queue. A completed write only means the kernel accepted the bytes.
  [[nodiscard]] auto heads_delivered() const -> std::size_t
  {
    return heads_delivered_.load();
  }

  // Requests to "/held/<path>", "/one-row", "/many-rows" and "/trailing" received so far.
  [[nodiscard]] auto held() const -> std::size_t
  {
    return held_count_.load();
  }

  // Answers every request held so far.
  void release_held()
  {
    asio::post(io_, [this]() {
      auto held = std::move(held_);
      held_.clear();
      for (const auto& [conn, path] : held) {
        answer(conn, path);
      }
    });
  }

  // Connections the client closed while the endpoint waited for a request.
  [[nodiscard]] auto released() const -> std::size_t
  {
    return released_.load();
  }

private:
  struct connection {
    connection(loopback_http_server& server,
               std::atomic<std::size_t>& heads,
               std::atomic<std::size_t>& delivered,
               std::atomic<std::size_t>& released_by_client)
      : socket{ server.io_ }
      , pause{ server.io_ }
      , owner{ server }
      , heads_written{ heads }
      , heads_delivered{ delivered }
      , released{ released_by_client }
    {
    }

    asio::ip::tcp::socket socket;
    asio::steady_timer pause;
    std::array<char, 4096> buffer{};
    std::string received{};
    loopback_http_server& owner;
    std::atomic<std::size_t>& heads_written;
    std::atomic<std::size_t>& heads_delivered;
    std::atomic<std::size_t>& released;
  };

  void accept()
  {
    auto conn = std::make_shared<connection>(*this, heads_written_, heads_delivered_, released_);
    acceptor_.async_accept(conn->socket, [this, conn](std::error_code ec) {
      if (ec) {
        return;
      }
      ++accepted_;
      // Without it Nagle holds a tail written after a pause until the client acknowledges the
      // head, which a delayed ACK postpones by tens of milliseconds, and the pause stops being
      // what separates the two reads.
      std::error_code ignored;
      conn->socket.set_option(asio::ip::tcp::no_delay{ true }, ignored);
      serve(conn);
      accept();
    });
  }

  // The value of header `name`, given in lower case, in `head`; empty when absent.
  static auto header_value(const std::string& head, const std::string& name) -> std::string
  {
    std::string lowered{ head };
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    const auto at = lowered.find("\r\n" + name + ":");
    if (at == std::string::npos) {
      return {};
    }
    const auto begin = head.find_first_not_of(' ', at + name.size() + 3);
    return head.substr(begin, head.find("\r\n", begin) - begin);
  }

  static void serve(const std::shared_ptr<connection>& conn)
  {
    const auto end = conn->received.find("\r\n\r\n");
    const auto head = conn->received.substr(0, end == std::string::npos ? 0 : end + 2);
    const auto length = header_value(head, "content-length");
    const auto body_size = length.empty() ? std::size_t{ 0 } : std::stoul(length);
    if (end == std::string::npos || conn->received.size() < end + 4 + body_size) {
      conn->socket.async_read_some(asio::buffer(conn->buffer),
                                   [conn](std::error_code ec, std::size_t bytes) {
                                     if (ec) {
                                       ++conn->released;
                                       return;
                                     }
                                     conn->received.append(conn->buffer.data(), bytes);
                                     serve(conn);
                                   });
      return;
    }
    const auto path_begin = head.find(' ') + 1;
    auto path = head.substr(path_begin, head.find(' ', path_begin) - path_begin);
    if (path == "/query/service" || path == "/analytics/service") {
      path = header_value(head, "client-context-id");
    }
    conn->received.erase(0, end + 4 + body_size);
    answer(conn, path);
  }

  static void answer(const std::shared_ptr<connection>& conn, const std::string& path)
  {
    if (const std::string held_prefix{ "/held" }; path.rfind(held_prefix + "/", 0) == 0) {
      conn->owner.held_.emplace_back(conn, path.substr(held_prefix.size()));
      ++conn->owner.held_count_;
      return;
    }
    if (path == "/close") {
      return respond(conn, "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n", {});
    }
    if (path == "/close-split") {
      return respond(
        conn, "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\n", { "abcde" });
    }
    if (path == "/split") {
      return respond(conn, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n", { "abcde" });
    }
    if (path == "/pieces") {
      return respond(conn, "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n", { "abc", "def", "ghi" });
    }
    if (path == "/incomplete") {
      return respond(conn, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab", {});
    }
    if (path == "/complete") {
      return respond(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", {});
    }
    if (path == "/rows" || path == "/one-row" || path == "/trailing") {
      const auto rest = std::string{ query_later_rows } + std::string{ query_trailer };
      const std::size_t newline = path == "/trailing" ? 1 : 0;
      auto head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
                  std::to_string(query_first_row.size() + rest.size() + newline) + "\r\n\r\n" +
                  std::string{ query_first_row };
      if (path == "/rows") {
        return respond(conn, head + rest, {});
      }
      if (path == "/trailing") {
        conn->owner.held_.emplace_back(conn, "/newline");
        ++conn->owner.held_count_;
        return respond(conn, head + rest, {});
      }
      conn->owner.held_.emplace_back(conn, "/rows-tail");
      ++conn->owner.held_count_;
      return respond(conn, std::move(head), {});
    }
    if (path == "/many-rows") {
      std::string body{ query_first_row };
      for (std::size_t n = 1; n < 8; ++n) {
        body += R"(,{"n":)" + std::to_string(n) + "}";
      }
      auto head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
                  std::to_string(body.size() + query_trailer.size()) + "\r\n\r\n" + body;
      conn->owner.held_.emplace_back(conn, "/trailer");
      ++conn->owner.held_count_;
      return respond(conn, std::move(head), {});
    }
    if (path == "/malformed") {
      const auto body = std::string{ query_first_row } + ",}";
      return respond(conn,
                     "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
                       std::to_string(body.size() + 1024 * 1024) + "\r\n\r\n" + body,
                     {});
    }
    if (path == "/rows-tail" || path == "/trailer" || path == "/newline") {
      // Not a response: the rest of a "/one-row", "/many-rows" or "/trailing" body. respond() for
      // its head already armed the read of the next request.
      auto tail = std::make_shared<std::string>("\n");
      if (path == "/rows-tail") {
        *tail = std::string{ query_later_rows } + std::string{ query_trailer };
      } else if (path == "/trailer") {
        *tail = std::string{ query_trailer };
      }
      return asio::async_write(
        conn->socket, asio::buffer(*tail), [conn, tail](std::error_code, std::size_t) {
        });
    }
    if (path == "/reset") {
      // Part of the body, then the connection closes after the pause, so a read parked for the
      // rest fails with an IO error rather than a stop.
      auto head = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab");
      return asio::async_write(
        conn->socket, asio::buffer(*head), [conn, head](std::error_code ec, std::size_t) {
          if (ec) {
            return;
          }
          ++conn->heads_written;
          conn->pause.expires_after(1ms);
          conn->pause.async_wait([conn](std::error_code) {
            std::error_code ignored;
            conn->socket.close(ignored);
          });
        });
    }
    // The pending read owns the connection; without it the socket would close and the client
    // would see EOF rather than silence.
    conn->socket.async_read_some(asio::buffer(conn->buffer), [conn](std::error_code, std::size_t) {
    });
  }

  // Writes `head`, then each of `tails` after a pause of its own, so each arrives in a later read.
  static void respond(const std::shared_ptr<connection>& conn,
                      std::string head,
                      std::vector<std::string> tails,
                      bool is_head = true)
  {
    auto data = std::make_shared<std::string>(std::move(head));
    asio::async_write(
      conn->socket,
      asio::buffer(*data),
      [conn, data, tails = std::move(tails), is_head](std::error_code ec, std::size_t) mutable {
        if (ec) {
          return;
        }
        if (is_head) {
          ++conn->heads_written;
          count_when_delivered(conn);
        }
        if (tails.empty()) {
          return serve(conn);
        }
        conn->pause.expires_after(1ms);
        conn->pause.async_wait([conn, tails = std::move(tails)](std::error_code wait_ec) mutable {
          if (wait_ec) {
            return;
          }
          auto next = std::move(tails.front());
          tails.erase(tails.begin());
          respond(conn, std::move(next), std::move(tails), false);
        });
      });
  }

  // SIOCOUTQ is Linux-only. Elsewhere a head counts as delivered once its write completes, which
  // does not put it in the client's receive queue; the cases that depend on that require Linux.
  static void count_when_delivered(const std::shared_ptr<connection>& conn)
  {
#if defined(__linux__)
    int unacknowledged = 0;
    if (::ioctl(conn->socket.native_handle(), SIOCOUTQ, &unacknowledged) == 0 &&
        unacknowledged != 0) {
      return asio::post(conn->socket.get_executor(), [conn]() {
        count_when_delivered(conn);
      });
    }
#endif
    ++conn->heads_delivered;
  }

  asio::io_context io_{};
  asio::ip::tcp::acceptor acceptor_;
  std::uint16_t port_;
  std::atomic<std::size_t> accepted_{ 0 };
  std::atomic<std::size_t> heads_written_{ 0 };
  std::atomic<std::size_t> heads_delivered_{ 0 };
  // Touched only on io_.
  std::vector<std::pair<std::shared_ptr<connection>, std::string>> held_{};
  std::atomic<std::size_t> held_count_{ 0 };
  std::atomic<std::size_t> released_{ 0 };
  std::thread thread_{};
};

auto
pool_options() -> couchbase::core::cluster_options
{
  couchbase::core::cluster_options options{};
  options.enable_tls = false;
  options.network = "default";
  // Well past every case budget, so the idle timer does not tear a pooled session down mid-case.
  // A multiplier large enough to reach it makes the pool replace the session, which no assertion
  // depends on.
  options.idle_http_connection_timeout = 10min;
  return options;
}

// One node serving the management, query and analytics services on the loopback endpoint.
auto
loopback_config(std::uint16_t port) -> couchbase::core::topology::configuration
{
  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.hostname = "127.0.0.1";
  node.services_plain.management = port;
  node.services_plain.query = port;
  node.services_plain.analytics = port;
  config.nodes.push_back(node);
  return config;
}

auto
pool_credentials() -> couchbase::core::cluster_credentials
{
  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "pass";
  return creds;
}

// A one-shot hand-off between threads. std::promise synchronises through a futex wait inside
// libstdc++, which ThreadSanitizer does not observe, so every value passed through one is reported
// as a race; a mutex and condition variable are intercepted.
template<typename T>
class completion
{
public:
  // Notifies under the lock: the waiter may destroy this object as soon as it observes the value.
  void set(T value)
  {
    const std::scoped_lock lock(mutex_);
    if (value_.has_value()) {
      return;
    }
    value_.emplace(std::move(value));
    ready_.notify_all();
  }

  // Waits in slices of at most an hour: condition_variable::wait_for adds its duration to now(),
  // which overflows for a budget saturated at milliseconds::max().
  auto wait_for(std::chrono::milliseconds budget) -> std::optional<T>
  {
    const auto start = std::chrono::steady_clock::now();
    std::unique_lock lock(mutex_);
    while (!value_.has_value()) {
      const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
      if (elapsed >= budget) {
        return std::nullopt;
      }
      ready_.wait_for(lock, std::min<std::chrono::milliseconds>(budget - elapsed, 1h));
    }
    return value_;
  }

private:
  std::mutex mutex_{};
  std::condition_variable ready_{};
  std::optional<T> value_{};
};

// Releases the threads waiting on it together, once the expected number have arrived. Shared by
// pointer so a thread arriving after the bounded wait gave up still finds it alive.
struct start_line {
  void arrive_and_wait()
  {
    ++arrived;
    while (!go.load()) {
      std::this_thread::yield();
    }
  }

  void release_when(int expected)
  {
    const auto start = std::chrono::steady_clock::now();
    while (arrived.load() < expected && within(start, patience())) {
      std::this_thread::yield();
    }
    go = true;
  }

  std::atomic<int> arrived{ 0 };
  std::atomic_bool go{ false };
};

struct consumer_failure : std::exception {
};

void
instrument(http_session_manager& manager)
{
  auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
  manager.set_tracer(couchbase::core::tracing::tracer_wrapper::create(
    std::make_shared<couchbase::core::tracing::noop_tracer>(), labels));
  manager.set_meter(couchbase::core::metrics::meter_wrapper::create(
    std::make_shared<couchbase::core::metrics::noop_meter>(), labels));
  manager.set_app_telemetry_meter(std::make_shared<couchbase::core::app_telemetry_meter>());
}

// A session manager pointed at the loopback endpoint, with its io_context run by `threads`
// background threads for the fixture's lifetime. A consumer_failure thrown from a completion
// handler is counted and the runner resumes; any other exception terminates.
struct pool_fixture {
  explicit pool_fixture(std::size_t threads = 1)
  {
    instrument(*manager);
    manager->set_configuration(loopback_config(server.port()), options);

    for (std::size_t i = 0; i < threads; ++i) {
      runners.emplace_back([this]() {
        for (;;) {
          try {
            io.run();
            break;
          } catch (const consumer_failure&) {
            ++consumer_failures;
          }
        }
        ++exited;
      });
    }
  }

  pool_fixture(const pool_fixture&) = delete;
  pool_fixture(pool_fixture&&) = delete;
  auto operator=(const pool_fixture&) -> pool_fixture& = delete;
  auto operator=(pool_fixture&&) -> pool_fixture& = delete;

  ~pool_fixture()
  {
    shutdown();
  }

  // Closes the manager and waits, bounded, for the io_context to run out of work. Returns false
  // when it did not, after forcing the runners out.
  auto shutdown() -> bool
  {
    if (runners.empty()) {
      return drained;
    }
    // With one runner, every handler posted before this barrier has run once it does, so a posted
    // check_in lands before close() empties the pool and close() stops what it pooled. A stopped
    // io_context runs no barrier.
    if (!io.stopped()) {
      auto barrier = std::make_shared<completion<bool>>();
      asio::post(io, [barrier]() {
        barrier->set(true);
      });
      barrier->wait_for(patience());
    }
    manager->close();
    guard.reset();
    const auto start = std::chrono::steady_clock::now();
    const auto bound = scaled_budget(5s);
    while (exited.load() < runners.size() && within(start, bound)) {
      std::this_thread::sleep_for(1ms);
    }
    drained = exited.load() == runners.size();
    if (!drained) {
      io.stop();
    }
    for (auto& runner : runners) {
      runner.join();
    }
    runners.clear();
    return drained;
  }

  asio::io_context io{};
  loopback_http_server server{};
  std::shared_ptr<asio::ssl::context> ssl{ std::make_shared<asio::ssl::context>(
    asio::ssl::context::tls_client) };
  couchbase::core::tls_context_provider tls{ ssl };
  couchbase::core::cluster_options options{ pool_options() };
  couchbase::core::origin origin{ pool_credentials(), "127.0.0.1", server.port(), options };
  std::shared_ptr<http_session_manager> manager{
    std::make_shared<http_session_manager>("client-id", io, tls, origin)
  };
  asio::executor_work_guard<asio::io_context::executor_type> guard{ asio::make_work_guard(io) };
  std::vector<std::thread> runners{};
  std::atomic<std::size_t> exited{ 0 };
  std::atomic<std::size_t> consumer_failures{ 0 };
  bool drained{ false };
};

// Runs `fn` on `target`, an io_context or an executor, and waits for it, bounded. An assertion
// failing inside `fn` fails the calling case. A step still queued when the budget expires is
// abandoned, and one already running is waited for: `fn` captures the calling frame by reference,
// and the failure unwinds that frame.
template<typename Target, typename Fn>
void
run_on(Target&& target, Fn&& fn, std::chrono::milliseconds budget = patience())
{
  enum : int {
    queued,
    running,
    abandoned
  };
  auto state = std::make_shared<std::atomic<int>>(queued);
  auto done = std::make_shared<completion<std::exception_ptr>>();
  asio::post(std::forward<Target>(target), [state, done, fn = std::forward<Fn>(fn)]() mutable {
    if (int expected = queued; !state->compare_exchange_strong(expected, running)) {
      return;
    }
    try {
      fn();
      done->set(nullptr);
    } catch (...) {
      done->set(std::current_exception());
    }
  });
  auto result = done->wait_for(budget);
  if (!result.has_value()) {
    if (int expected = queued; state->compare_exchange_strong(expected, abandoned)) {
      fail("a posted step did not run within its budget");
    }
    while (!(result = done->wait_for(budget)).has_value()) {
    }
    if (*result) {
      std::rethrow_exception(*result);
    }
    fail("a posted step did not finish within its budget");
  }
  if (*result) {
    std::rethrow_exception(*result);
  }
}

auto
make_request(std::string path) -> couchbase::core::io::http_request
{
  couchbase::core::io::http_request request{};
  request.type = service;
  request.method = "GET";
  request.path = std::move(path);
  request.headers["connection"] = "keep-alive";
  return request;
}

using connected_handler =
  couchbase::core::utils::movable_function<void(std::error_code, std::shared_ptr<http_session>)>;

// What http_component does between check_out and the first write.
void
with_connection(const std::shared_ptr<http_session_manager>& manager,
                std::shared_ptr<http_session> session,
                connected_handler&& then)
{
  if (session->is_connected()) {
    return then({}, std::move(session));
  }
  // Clamped: now() + patience() overflows for a bound saturated at milliseconds::max().
  manager->connect_then_send_pending_op(session,
                                        {},
                                        std::chrono::steady_clock::now() +
                                          std::min<std::chrono::milliseconds>(patience(), 1h),
                                        std::move(then));
}

// A checked-out, connected, keep-alive session that has completed one round trip, as the
// manager's busy list holds it between a response and its check_in.
auto
borrow_connected(asio::io_context& io,
                 const std::shared_ptr<http_session_manager>& from,
                 couchbase::core::service_type type = service) -> std::shared_ptr<http_session>
{
  using outcome = std::pair<std::error_code, std::shared_ptr<http_session>>;
  auto done = std::make_shared<completion<outcome>>();
  asio::post(io, [manager = from, done, type]() {
    auto [ec, session] = manager->check_out(type, {});
    if (ec) {
      return done->set({ ec, nullptr });
    }
    with_connection(
      manager,
      session,
      [done](std::error_code connect_ec, std::shared_ptr<http_session> connected) {
        if (connect_ec) {
          return done->set({ connect_ec, nullptr });
        }
        auto request = make_request("/complete");
        connected->write_and_subscribe(
          request,
          [done, connected](std::error_code response_ec, couchbase::core::io::http_response&&) {
            done->set({ response_ec, connected });
          });
      });
  });
  auto result = done->wait_for(patience());
  if (!result.has_value()) {
    fail("a round trip to the loopback endpoint did not complete within its budget");
  }
  auto [ec, session] = std::move(*result);
  assert_success(ec, "a round trip to the loopback endpoint succeeds");
  assert_true(session && session->is_connected() && session->keep_alive(),
              "the borrowed session is connected and keep-alive");
  return session;
}

auto
borrow_connected(pool_fixture& f, couchbase::core::service_type type = service)
  -> std::shared_ptr<http_session>
{
  return borrow_connected(f.io, f.manager, type);
}

// Busy and idle sessions, by id. Pending sessions are not reported by export_diag_info.
auto
pooled_ids(http_session_manager& manager) -> std::set<std::string>
{
  couchbase::core::diag::diagnostics_result res{};
  manager.export_diag_info(res);
  std::set<std::string> ids;
  for (const auto& [type, endpoints] : res.services) {
    for (const auto& endpoint : endpoints) {
      ids.insert(endpoint.id);
    }
  }
  return ids;
}

auto
check_out(http_session_manager& manager) -> std::shared_ptr<http_session>
{
  auto [ec, session] = manager.check_out(service, {});
  assert_success(ec, "check_out succeeds against the configured node");
  return session;
}

// check_in must refuse a session stop() has already torn down. The control establishes that the
// pool republishes this session at all, so the refusal is attributable to the stop.
void
check_in_refuses_a_session_whose_stop_already_ran([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto first = borrow_connected(f);

  std::shared_ptr<http_session> reused;
  run_on(f.io, [&]() {
    f.manager->check_in(service, first);
    reused = check_out(*f.manager);
  });
  assert_eq(reused->id(), first->id(), "an unstopped session is republished and checked out again");

  run_on(reused->get_executor(), [&]() {
    reused->stop();
  });

  std::shared_ptr<http_session> after;
  bool published = true;
  run_on(f.io, [&]() {
    f.manager->check_in(service, reused);
    // on_stop removed the session from the busy list, so it is listed only if check_in published
    // it. Probed before check_out, which also drops a stopped session.
    published = pooled_ids(*f.manager).count(reused->id()) != 0;
    after = check_out(*f.manager);
  });
  assert_false(published, "check_in does not publish a session whose stop already ran");
  assert_ne(after->id(), reused->id(), "a stopped session is not republished by check_in");
  assert_false(after->is_stopped() || after->is_stopping(),
               "check_out hands out a live session once the stopped one is refused");
  assert_eq(pooled_ids(*f.manager).count(reused->id()),
            std::size_t{ 0 },
            "the stopped session is in neither the busy nor the idle list");
}

// A stop landing on a session already parked idle must remove it from the pool: the on_stop
// cleanup is what does that.
void
a_stop_after_publication_removes_the_session_from_the_pool([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto parked = borrow_connected(f);
  run_on(f.io, [&]() {
    f.manager->check_in(service, parked);
  });
  assert_eq(pooled_ids(*f.manager).count(parked->id()),
            std::size_t{ 1 },
            "the checked-in session is listed by the pool");

  run_on(parked->get_executor(), [&]() {
    parked->stop();
  });
  assert_eq(pooled_ids(*f.manager).count(parked->id()),
            std::size_t{ 0 },
            "a stop after publication removes the session from the idle list");

  std::shared_ptr<http_session> next;
  run_on(f.io, [&]() {
    next = check_out(*f.manager);
  });
  assert_ne(next->id(), parked->id(), "check_out does not hand out the stopped session");
  assert_false(next->is_stopped() || next->is_stopping(), "check_out hands out a live session");
}

enum class step {
  claim,
  check_in,
  stop,
  check_out
};

auto
describe(const std::array<step, 4>& order) -> std::string
{
  std::string text;
  for (const auto s : order) {
    if (!text.empty()) {
      text += ',';
    }
    switch (s) {
      case step::claim:
        text += "claim";
        break;
      case step::check_in:
        text += "check_in";
        break;
      case step::stop:
        text += "stop";
        break;
      case step::check_out:
        text += "check_out";
        break;
    }
  }
  return text;
}

template<typename Pred>
auto
position(const std::array<step, 4>& order, Pred pred) -> std::ptrdiff_t
{
  return std::find_if(order.begin(), order.end(), pred) - order.begin();
}

// The four events of a body teardown racing a clean end, in every order program order allows,
// on one thread: the claim precedes the stop it decides (close_impl posts stop() after taking
// the claim), and precedes the check_in because read_some runs the body's completion, which
// makes a claim impossible, before the stream-end handler checks the session in. The second
// request's check_out may land anywhere. After every step no session the second request holds is
// stopped or claimed, check_in has not published the claimed session, and once all four have run
// the claimed session is in no list.
void
every_claim_order_keeps_the_claimed_session_out_of_the_pool([[maybe_unused]] context& ctx)
{
  std::array<step, 4> order{ step::claim, step::check_in, step::stop, step::check_out };
  std::size_t orders_run = 0;
  do {
    const auto at = [&order](step s) {
      return position(order, [s](step o) {
        return o == s;
      });
    };
    if (at(step::claim) > at(step::stop) || at(step::claim) > at(step::check_in)) {
      continue;
    }
    ++orders_run;
    const auto label = describe(order);

    pool_fixture f{};
    auto claimed = borrow_connected(f);
    std::shared_ptr<http_session> second;
    run_on(claimed->get_executor(), [&]() {
      for (const auto s : order) {
        switch (s) {
          case step::claim:
            claimed->mark_stopping();
            break;
          case step::check_in:
            f.manager->check_in(service, claimed);
            // check_in arms the idle timer of a session it publishes, and the borrowed session
            // never had one armed. Probed here because check_out also skips a claimed session.
            assert_false(claimed->reset_idle(),
                         "order " + label + ": check_in does not publish the claimed session");
            break;
          case step::stop:
            claimed->stop();
            break;
          case step::check_out:
            second = check_out(*f.manager);
            break;
        }
        assert_false(second && (second->is_stopped() || second->is_stopping()),
                     "order " + label +
                       ": the second request holds only a live, unclaimed session");
      }
    });
    assert_ne(second->id(),
              claimed->id(),
              "order " + label + ": check_out does not yield the claimed session");
    assert_eq(pooled_ids(*f.manager).count(claimed->id()),
              std::size_t{ 0 },
              "order " + label + ": the claimed session is in neither the busy nor the idle list");

    std::shared_ptr<http_session> third;
    run_on(f.io, [&]() {
      third = check_out(*f.manager);
    });
    assert_ne(third->id(),
              claimed->id(),
              "order " + label + ": a later check_out does not yield the claimed session");
    assert_true(f.shutdown(),
                "order " + label + ": the io_context drains once the manager is closed");
  } while (std::next_permutation(order.begin(), order.end()));
  assert_eq(orders_run, std::size_t{ 8 }, "every order program order allows is run");
}

// check_out on its own must not hand out a session already claimed for teardown. A claim on an
// idle session requires a body to take it after its connection was published; read_some's order
// is what rules that out, and this case pins check_out's side of it without relying on that order.
void
check_out_does_not_hand_out_a_session_claimed_while_idle([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto parked = borrow_connected(f);
  std::shared_ptr<http_session> next;
  run_on(parked->get_executor(), [&]() {
    f.manager->check_in(service, parked);
    parked->mark_stopping();
    next = check_out(*f.manager);
    // The stop every claim is followed by. Without it the parked session's idle timer and liveness
    // read keep the io_context from draining.
    parked->stop();
  });
  assert_ne(next->id(), parked->id(), "check_out does not yield a session claimed for teardown");
}

// stop() reached concurrently from the session strand and from another thread -- a strand-posted
// stop against the inline stop() in http_command::cancel(), pending_http_operation's deadline or
// http_session_manager::close() -- must run the on_stop cleanup exactly once. stop()'s exchange
// admits one caller; a load and a store would let both run the teardown, racing on stream_ and
// on_stop_handler_. Only ThreadSanitizer detects that race reliably: the second teardown waits
// behind the first on the teardown's mutexes, so it typically finds on_stop_handler_ already moved
// out and the count stays at one. In a plain build the case checks that both stops return and that
// the cleanup runs once.
void
concurrent_stops_run_the_on_stop_handler_once([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread runner([&io]() {
    io.run();
  });

  auto options = pool_options();
  couchbase::core::origin origin{ pool_credentials(), "127.0.0.1", std::uint16_t{ 1 }, options };
  couchbase::core::topology::configuration config{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{
    config, options, cache, "127.0.0.1", std::uint16_t{ 1 }, "127.0.0.1", std::uint16_t{ 1 }
  };

  constexpr std::size_t iterations = 2000;
  std::size_t wrong_count = 0;
  std::size_t stalled = 0;
  for (std::size_t i = 0; i < iterations; ++i) {
    auto session = std::make_shared<http_session>(service,
                                                  "client-id",
                                                  "node-uuid",
                                                  io,
                                                  origin,
                                                  "127.0.0.1",
                                                  "1",
                                                  http_ctx,
                                                  /* pool_generation */ 0);
    auto calls = std::make_shared<std::atomic<int>>(0);
    session->on_stop([calls]() {
      ++*calls;
    });

    auto line = std::make_shared<start_line>();
    auto strand_done = std::make_shared<completion<bool>>();
    asio::post(session->get_executor(), [line, session, strand_done]() {
      line->arrive_and_wait();
      session->stop();
      strand_done->set(true);
    });
    std::thread inline_stopper([line, session]() {
      line->arrive_and_wait();
      session->stop();
    });
    line->release_when(2);
    inline_stopper.join();
    // Both stops have returned, so the winner's teardown has run.
    if (!strand_done->wait_for(patience()).has_value()) {
      ++stalled;
      break;
    }
    if (calls->load() != 1) {
      ++wrong_count;
    }
  }

  guard.reset();
  io.stop();
  runner.join();

  assert_eq(stalled, std::size_t{ 0 }, "the strand-posted stop runs within its budget");
  assert_eq(wrong_count,
            std::size_t{ 0 },
            "sessions whose on_stop cleanup did not run exactly once under two concurrent stops");
}

void
drain(couchbase::core::io::http_streaming_response_body body)
{
  body.next([body](std::string, bool has_more, std::error_code ec) mutable {
    if (has_more && !ec) {
      drain(std::move(body));
    }
  });
}

// Pulls until the body ends and reports how: a falsy error_code for a clean end.
void
drain_to_terminal(couchbase::core::io::http_streaming_response_body body,
                  std::shared_ptr<completion<std::error_code>> terminal)
{
  body.next([body, terminal](std::string, bool has_more, std::error_code ec) mutable {
    if (ec || !has_more) {
      return terminal->set(ec);
    }
    drain_to_terminal(std::move(body), std::move(terminal));
  });
}

// Concurrent borrowers against the real manager, each iteration one of: a buffered request
// completed and checked in; a streamed body closed from another io thread as its headers arrive,
// which almost always lands before the delayed tail and claims and stops the session; a streamed
// body left incomplete and closed, which claims and stops its session. Violations are
// recorded rather than asserted, since they are observed on io threads.
class pool_stress : public std::enable_shared_from_this<pool_stress>
{
public:
  pool_stress(asio::io_context& io,
              std::shared_ptr<http_session_manager> manager,
              std::size_t iterations,
              std::size_t chains)
    : io_{ io }
    , manager_{ std::move(manager) }
    , iterations_{ iterations }
    , chains_{ chains }
  {
  }

  auto start() -> std::shared_ptr<completion<bool>>
  {
    for (std::size_t c = 0; c < chains_; ++c) {
      asio::post(io_, [self = shared_from_this()]() {
        self->iterate();
      });
    }
    return done_;
  }

  // Every session the borrowers saw, each paired with a strand barrier: once it has run, every
  // stop posted to that strand before it has run too.
  auto settle(std::chrono::milliseconds budget) -> bool
  {
    std::vector<std::shared_ptr<http_session>> sessions;
    {
      const std::scoped_lock lock(mutex_);
      for (const auto& [id, session] : seen_) {
        sessions.push_back(session);
      }
    }
    auto remaining = std::make_shared<std::atomic<std::size_t>>(sessions.size());
    auto all = std::make_shared<completion<bool>>();
    if (sessions.empty()) {
      all->set(true);
    }
    for (const auto& session : sessions) {
      asio::post(session->get_executor(), [remaining, all]() {
        if (--*remaining == 0) {
          all->set(true);
        }
      });
    }
    return all->wait_for(budget).has_value();
  }

  // Sessions still listed by the pool that are stopped or claimed.
  auto dead_sessions_in_pool() -> std::vector<std::string>
  {
    const auto ids = pooled_ids(*manager_);
    const std::scoped_lock lock(mutex_);
    std::vector<std::string> dead;
    for (const auto& id : ids) {
      auto it = seen_.find(id);
      if (it != seen_.end() && (it->second->is_stopped() || it->second->is_stopping())) {
        dead.push_back(id);
      }
    }
    return dead;
  }

  auto violations() -> std::vector<std::string>
  {
    const std::scoped_lock lock(mutex_);
    return violations_;
  }

  auto still_held() -> std::size_t
  {
    const std::scoped_lock lock(mutex_);
    return static_cast<std::size_t>(
      std::count_if(holders_.begin(), holders_.end(), [](const auto& h) {
        return h.second != 0;
      }));
  }

  auto reuses() -> std::size_t
  {
    const std::scoped_lock lock(mutex_);
    return reuses_;
  }

  auto stopped_sessions() -> std::size_t
  {
    const std::scoped_lock lock(mutex_);
    return static_cast<std::size_t>(std::count_if(seen_.begin(), seen_.end(), [](const auto& s) {
      return s.second->is_stopped();
    }));
  }

  // Destroys the sessions outside mutex_: a destructor's teardown runs response handlers that
  // take it. The map is moved out under the lock and destroyed at the end of the statement, once
  // the lambda has released it.
  void forget()
  {
    [this]() {
      const std::scoped_lock lock(mutex_);
      return std::exchange(seen_, {});
    }();
  }

private:
  void iterate()
  {
    const auto i = next_.fetch_add(1);
    if (i >= iterations_) {
      if (++chains_done_ == chains_) {
        done_->set(true);
      }
      return;
    }
    auto [ec, checked_out] = manager_->check_out(service, {});
    if (ec) {
      violate("check_out failed: " + ec.message());
      return next();
    }
    auto session = std::move(checked_out);
    acquire(session);
    with_connection(
      manager_,
      session,
      [self = shared_from_this(), i, session](std::error_code connect_ec,
                                              std::shared_ptr<http_session> connected) {
        if (connect_ec || !connected) {
          self->violate("connecting to the loopback endpoint failed: " + connect_ec.message());
          self->release(session);
          return self->next();
        }
        if (connected != session) {
          self->release(session);
          self->acquire(connected);
        }
        switch (i % 3) {
          case 0:
            return self->buffered(connected);
          case 1:
            return self->streamed(connected, "/split");
          default:
            return self->streamed(connected, "/incomplete");
        }
      });
  }

  void next()
  {
    asio::post(io_, [self = shared_from_this()]() {
      self->iterate();
    });
  }

  void buffered(const std::shared_ptr<http_session>& session)
  {
    auto request = make_request("/complete");
    session->write_and_subscribe(
      request,
      [self = shared_from_this(), session](std::error_code ec,
                                           couchbase::core::io::http_response&&) {
        if (ec) {
          self->violate("a buffered request its borrower did not stop failed: " + ec.message());
        }
        self->release(session);
        self->manager_->check_in(service, session);
        self->next();
      });
  }

  void streamed(const std::shared_ptr<http_session>& session, std::string path)
  {
    auto request = make_request(std::move(path));
    auto closed = std::make_shared<std::atomic_bool>(false);
    auto ended = std::make_shared<std::atomic<int>>(0);
    session->write_and_stream(
      request,
      [self = shared_from_this(), closed](std::error_code ec,
                                          couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          if (!closed->load()) {
            self->violate("a streamed request its borrower did not close failed: " + ec.message());
          }
          return;
        }
        auto body = resp.body();
        closed->store(true);
        asio::post(self->io_, [body]() mutable {
          body.close();
        });
        drain(body);
      },
      [self = shared_from_this(), session, ended]() {
        if (++*ended != 1) {
          self->violate("a stream-end handler ran twice");
          return;
        }
        self->release(session);
        self->manager_->check_in(service, session);
        self->next();
      });
  }

  void acquire(const std::shared_ptr<http_session>& session)
  {
    const std::scoped_lock lock(mutex_);
    if (!seen_.emplace(session->id(), session).second) {
      ++reuses_;
    }
    if (session->is_stopped() || session->is_stopping()) {
      violations_.emplace_back("check_out returned a stopped or claimed session");
    }
    if (++holders_[session->id()] > 1) {
      violations_.emplace_back("two borrowers held one session at once");
    }
  }

  void release(const std::shared_ptr<http_session>& session)
  {
    const std::scoped_lock lock(mutex_);
    --holders_[session->id()];
  }

  void violate(std::string what)
  {
    const std::scoped_lock lock(mutex_);
    violations_.emplace_back(std::move(what));
  }

  asio::io_context& io_;
  std::shared_ptr<http_session_manager> manager_;
  std::size_t iterations_;
  std::size_t chains_;
  std::atomic<std::size_t> next_{ 0 };
  std::atomic<std::size_t> chains_done_{ 0 };
  std::shared_ptr<completion<bool>> done_{ std::make_shared<completion<bool>>() };
  std::mutex mutex_{};
  std::map<std::string, std::shared_ptr<http_session>> seen_{};
  std::map<std::string, int> holders_{};
  std::size_t reuses_{ 0 };
  std::vector<std::string> violations_{};
};

auto
first_of(const std::vector<std::string>& items) -> std::string
{
  return items.empty() ? std::string{} : items.front();
}

// The pool under concurrent borrowers on a multi-threaded io_context: a body's close races the
// clean end of its response, and sessions are republished and reused. No session is held by two
// borrowers at once, and at quiescence no listed session is stopped or claimed. Every stop here is
// posted to the strand of a busy session and every pull runs on that strand, so a stop reaching an
// idle session and a pull off the strand are covered by
// check_out_skips_an_idle_session_whose_stop_has_begun and
// a_pull_off_the_strand_racing_a_close_under_thread_sanitizer instead. The reuse and stop counts
// establish that both the republish path and the claim path were exercised.
void
concurrent_borrowers_never_share_or_inherit_a_dead_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{ 4 };
  // Most iterations stop their session and open a new connection. Kept to a few hundred per run,
  // so parallel test runs stay clear of the host's connection-tracking table.
  auto stress = std::make_shared<pool_stress>(f.io, f.manager, 600, 16);
  auto finished = stress->start();
  const bool completed = finished->wait_for(scaled_budget(15s)).has_value();
  const bool settled = completed && stress->settle(scaled_budget(5s));

  const auto violations = stress->violations();
  const auto dead = settled ? stress->dead_sessions_in_pool() : std::vector<std::string>{};
  const auto held = stress->still_held();
  const auto reuses = stress->reuses();
  const auto stopped = stress->stopped_sessions();
  stress->forget();
  const bool drained = f.shutdown();

  assert_true(completed, "every borrower chain finishes within its budget");
  assert_true(settled, "every session strand runs a barrier within its budget");
  assert_eq(
    violations.size(), std::size_t{ 0 }, "borrower violations, first: " + first_of(violations));
  assert_eq(dead.size(),
            std::size_t{ 0 },
            "stopped or claimed sessions left in the pool, first: " + first_of(dead));
  assert_eq(held, std::size_t{ 0 }, "sessions still held once every borrower has finished");
  assert_true(reuses > 0, "the pool republished and reused sessions under load");
  assert_true(stopped > 0, "borrowers claimed and stopped sessions under load");
  assert_true(drained, "the io_context drains once the manager is closed");
}

// A stop landing on an idle session on its strand while another thread checks out. Either order
// is legal; neither may leave the stopped session listed by the pool.
void
a_stop_racing_check_out_leaves_no_stopped_session_in_the_pool([[maybe_unused]] context& ctx)
{
  pool_fixture f{ 2 };
  constexpr std::size_t iterations = 200;
  for (std::size_t i = 0; i < iterations; ++i) {
    auto parked = borrow_connected(f);
    run_on(f.io, [&]() {
      f.manager->check_in(service, parked);
    });

    auto line = std::make_shared<start_line>();
    auto stop_done = std::make_shared<completion<bool>>();
    asio::post(parked->get_executor(), [line, parked, stop_done]() {
      line->arrive_and_wait();
      parked->stop();
      stop_done->set(true);
    });
    line->release_when(1);
    auto got = check_out(*f.manager);
    assert_true(stop_done->wait_for(patience()).has_value(),
                "the strand-posted stop runs within its budget");
    run_on(f.io, [&]() {
      f.manager->check_in(service, got);
    });
    assert_eq(pooled_ids(*f.manager).count(parked->id()),
              std::size_t{ 0 },
              "a stopped session is in neither the busy nor the idle list");
  }
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Pulls issued while a read is in flight would share the session's socket and input buffer, so the
// session queues them and starts each when the read before it completes. More pulls are issued
// than the body has parts; the ones queued past the end of the response report end-of-stream
// rather than reading the pooled connection. Each byte arrives once, in order, and every pull
// completes.
void
overlapping_pulls_deliver_each_byte_once_in_order([[maybe_unused]] context& ctx)
{
  constexpr std::size_t pulls = 4;
  struct pulled {
    std::size_t index{ 0 };
    std::string data{};
    bool has_more{ false };
    std::error_code ec{};
  };
  struct record {
    std::mutex mutex{};
    std::vector<pulled> entries{};
  };

  pool_fixture f{};
  auto session = borrow_connected(f);
  auto seen = std::make_shared<record>();
  auto all_done = std::make_shared<completion<bool>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/pieces");
    session->write_and_stream(
      request,
      [seen, all_done](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return all_done->set(false);
        }
        auto body = resp.body();
        for (std::size_t i = 0; i < pulls; ++i) {
          body.next([i, seen, all_done](std::string data, bool has_more, std::error_code pull_ec) {
            const std::scoped_lock lock(seen->mutex);
            seen->entries.push_back({ i, std::move(data), has_more, pull_ec });
            if (seen->entries.size() == pulls) {
              all_done->set(true);
            }
          });
        }
      },
      [manager = f.manager, session]() {
        manager->check_in(service, session);
      });
  });
  const auto finished = all_done->wait_for(patience());
  std::vector<pulled> result;
  {
    const std::scoped_lock lock(seen->mutex);
    result = seen->entries;
  }
  assert_true(finished.has_value() && *finished, "every overlapping pull completes");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  std::string body;
  for (std::size_t i = 0; i < result.size(); ++i) {
    assert_eq(result[i].index, i, "pulls complete in the order they were issued");
    assert_success(result[i].ec, "no pull fails");
    body += result[i].data;
  }
  assert_eq(body, std::string{ "abcdefghi" }, "each byte of the body is delivered once, in order");
  assert_false(result.back().has_more, "the last pull reports end-of-stream");
}

// Waits, bounded, until `done` holds; reports whether it did.
template<typename Predicate>
auto
wait_until(Predicate done) -> bool
{
  const auto start = std::chrono::steady_clock::now();
  while (!done()) {
    if (!within(start, scaled_budget(std::chrono::seconds{ 2 }))) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
  }
  return true;
}

// Regression: reads queued past the end of a response must not hold the in-flight flag once the
// stream-end handler has checked the connection in. A request that takes the connection out would
// queue its first read behind them, and that read would complete empty, as a clean end.
void
a_request_reusing_the_connection_does_not_drain_with_the_previous_response(
  [[maybe_unused]] context& ctx)
{
  constexpr std::size_t pulls = 8;
  // Three pulls read the parts of /pieces, the last of which completes the response; the rest queue
  // past its end.
  constexpr std::size_t queued_past_end = pulls - 3;
  struct reuse {
    std::mutex mutex{};
    std::size_t past_end_seen{ 0 };
    std::size_t past_end_at_second_response{ 0 };
    std::size_t heads_before_second{ 0 };
    bool second_head_read{ false };
    std::string body{};
    std::function<void()> pull{};
    std::optional<couchbase::core::io::http_streaming_response_body> stream{};
  };

  pool_fixture f{ 2 };
  auto session = borrow_connected(f);
  auto second = std::make_shared<reuse>();
  auto done = std::make_shared<completion<std::string>>();
  auto manager = f.manager;
  auto* server = &f.server;
  auto& io = f.io;
  // The first pull queued past the end starts the second request. The next holds the strand until
  // the second response's head has been read on the other io thread, so that read's completion
  // queues on the strand while this drain is still in progress.
  const auto past_end = [session, manager, second, done, server, &io]() {
    std::size_t seen = 0;
    {
      const std::scoped_lock lock(second->mutex);
      seen = ++second->past_end_seen;
    }
    if (seen == 2) {
      const auto heads_written = wait_until([server, second]() {
        return server->heads_written() > second->heads_before_second;
      });
      // Already expired, so its completion runs in the reactor pass that sees the head, or a
      // later one. With one other io thread, descriptor completions are queued ahead of timer
      // completions within a pass, so once it has run the head's read completion is on the strand.
      auto marker = std::make_shared<asio::steady_timer>(io);
      auto marked = std::make_shared<std::atomic_bool>(false);
      marker->expires_at(std::chrono::steady_clock::time_point::min());
      marker->async_wait([marker, marked](std::error_code) {
        marked->store(true);
      });
      const auto head_read = wait_until([&marked]() {
        return marked->load();
      });
      const std::scoped_lock lock(second->mutex);
      second->second_head_read = heads_written && head_read;
      return;
    }
    if (seen != 1) {
      return;
    }
    second->heads_before_second = server->heads_written();
    auto again = check_out(*manager);
    if (again != session) {
      return done->set("the connection was not reused");
    }
    auto request = make_request("/pieces");
    again->write_and_stream(
      request,
      [second, done](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return done->set(ec.message());
        }
        {
          const std::scoped_lock lock(second->mutex);
          second->past_end_at_second_response = second->past_end_seen;
        }
        second->stream = resp.body();
        second->pull = [second, done]() {
          second->stream->next([second, done](std::string data, bool has_more, std::error_code e) {
            second->body += data;
            if (e || !has_more) {
              second->pull = nullptr;
              return done->set(e ? e.message() : second->body);
            }
            second->pull();
          });
        };
        second->pull();
      },
      [manager, again]() {
        manager->check_in(service, again);
      });
  };
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/pieces");
    session->write_and_stream(
      request,
      [past_end](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return;
        }
        auto body = resp.body();
        for (std::size_t i = 0; i < pulls; ++i) {
          body.next([past_end](std::string data, bool has_more, std::error_code) {
            if (!has_more && data.empty()) {
              past_end();
            }
          });
        }
      },
      [manager, session]() {
        manager->check_in(service, session);
      });
  });
  const auto result = done->wait_for(patience());
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_true(result.has_value(), "the second request completes");
  {
    const std::scoped_lock lock(second->mutex);
    assert_true(second->second_head_read,
                "the second response's head is read while a pull past the end holds the strand");
    assert_true(second->past_end_at_second_response < queued_past_end,
                "the second response's first read is issued while the drain is in progress");
  }
  assert_eq(*result,
            std::string{ "abcdefghi" },
            "the request reusing the connection reads its own body in full");
}

// A session that no manager list holds, as a failover replacement is while it connects, to
// `port` on the loopback address. It serves generation 0, the generation of a manager that has
// never been closed. The context's referents outlive every use the session makes of them.
struct unlisted_session {
  unlisted_session(pool_fixture& f, std::uint16_t port)
    : http_ctx{ config, f.options, cache, "127.0.0.1", port, "127.0.0.1", port }
    , session{ std::make_shared<http_session>(service,
                                              "client-id",
                                              "node-uuid",
                                              f.io,
                                              f.origin,
                                              "127.0.0.1",
                                              std::to_string(port),
                                              http_ctx,
                                              /* pool_generation */ 0) }
  {
  }

  couchbase::core::topology::configuration config{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx;
  std::shared_ptr<http_session> session;
};

// A loopback port with nothing listening on it, so a connect to it is refused.
auto
refusing_port() -> std::uint16_t
{
  asio::io_context io;
  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  return acceptor.local_endpoint().port();
}

// Connects `session` and completes one keep-alive request on it, which is what makes a session
// poolable. Waits, bounded, and reports whether both succeeded.
auto
connect_and_wait(pool_fixture& f, const std::shared_ptr<http_session>& session) -> bool
{
  auto connected = std::make_shared<completion<bool>>();
  run_on(f.io, [&]() {
    session->connect([connected, session]() {
      connected->set(session->is_connected());
    });
  });
  if (const auto result = connected->wait_for(patience()); !result.has_value() || !*result) {
    return false;
  }
  auto answered = std::make_shared<completion<std::error_code>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/complete");
    session->write_and_subscribe(
      request, [answered](std::error_code ec, couchbase::core::io::http_response&&) {
        answered->set(ec);
      });
  });
  const auto ec = answered->wait_for(patience());
  return ec.has_value() && !*ec && session->keep_alive();
}

// close() drops the connections and leaves the manager serving: a later request opens a new
// connection, and check_in pools it.
void
a_manager_serves_new_requests_after_close([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto before = borrow_connected(f);
  run_on(f.io, [&]() {
    f.manager->check_in(service, before);
  });
  f.manager->close();
  assert_true(wait_until([&before]() {
                return before->is_stopped();
              }),
              "close() stops the session it pooled");

  auto after = borrow_connected(f);
  assert_ne(after->id(), before->id(), "a request after close() gets a new connection");
  std::shared_ptr<http_session> reused;
  run_on(f.io, [&]() {
    f.manager->check_in(service, after);
    reused = check_out(*f.manager);
  });
  assert_eq(reused->id(), after->id(), "a session created after close() is pooled and reused");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// The same contract through execute(). The first request pools its connection, close() stops
// it, and the second opens a new one through connect_then_send.
void
execute_after_close_sends_on_a_new_connection([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  couchbase::core::operations::management::freeform_request request{};
  request.type = service;
  request.method = "GET";
  request.path = "/complete";
  const auto execute = [&]() {
    auto done = std::make_shared<completion<std::pair<std::error_code, std::uint32_t>>>();
    run_on(f.io, [&]() {
      f.manager->execute(request,
                         [done](couchbase::core::operations::management::freeform_response&& resp) {
                           done->set({ resp.ctx.ec, resp.status });
                         });
    });
    return done->wait_for(patience());
  };

  const auto before = execute();
  assert_true(before.has_value(), "the request before close() completes");
  assert_success(before->first, "the request before close() succeeds");
  f.manager->close();
  const auto after = execute();
  assert_true(after.has_value(), "the request after close() completes");
  assert_success(after->first, "the request after close() succeeds");
  assert_eq(after->second, std::uint32_t{ 200 }, "the request after close() is answered");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_eq(f.server.accepted(),
            std::size_t{ 2 },
            "the request after close() connects anew rather than reusing a connection");
}

// close() stops each session on its strand. A stop() run on close()'s own thread races the strand's
// read and write handlers, and a queued do_write() dereferences the socket the stop releases. With
// the strand held, the stop has not run when close() returns, and runs once the strand is free.
void
close_stops_a_busy_session_on_its_strand([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);

  auto entered = std::make_shared<completion<bool>>();
  auto release = std::make_shared<completion<bool>>();
  asio::post(session->get_executor(), [entered, release]() {
    entered->set(true);
    release->wait_for(patience());
  });
  assert_true(entered->wait_for(patience()).has_value(), "the blocker holds the session strand");

  f.manager->close();
  const bool stopped_inline = session->is_stopped();
  release->set(true);
  assert_false(stopped_inline, "close() leaves the stop to the session's strand");
  assert_true(wait_until([&session]() {
                return session->is_stopped();
              }),
              "the stop runs once the strand is free");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// close()'s completion runs only once every session it stops has stopped. cluster close stops the
// io_context as soon as it completes, so a stop still queued then never runs, and the session and
// its on_stop cleanup, which holds the manager, are never released. With one session's strand
// held, the other stops and the completion still waits; it runs once the held stop has run.
void
close_completes_after_every_session_it_stops([[maybe_unused]] context& ctx)
{
  // Two io threads, so one session's strand runs while the other's is held.
  pool_fixture f{ 2 };
  auto held = borrow_connected(f);
  auto free = borrow_connected(f);

  auto entered = std::make_shared<completion<bool>>();
  auto release = std::make_shared<completion<bool>>();
  asio::post(held->get_executor(), [entered, release]() {
    entered->set(true);
    release->wait_for(patience());
  });
  assert_true(entered->wait_for(patience()).has_value(), "the blocker holds one session strand");

  auto ran = std::make_shared<std::atomic_bool>(false);
  auto completed = std::make_shared<completion<bool>>();
  f.manager->close([ran, completed, held, free]() {
    ran->store(true);
    completed->set(held->is_stopped() && free->is_stopped());
  });
  const bool other_stopped = wait_until([&free]() {
    return free->is_stopped();
  });
  const bool completed_early = ran->load();
  release->set(true);
  const auto all_stopped = completed->wait_for(patience());

  assert_true(other_stopped, "the session whose strand is free stops");
  assert_false(completed_early, "close() does not complete while a stop it posted is queued");
  assert_true(all_stopped.has_value(), "close() completes once the held stop has run");
  assert_true(*all_stopped, "every session is stopped when close() completes");

  bool completed_inline = false;
  f.manager->close([&completed_inline]() {
    completed_inline = true;
  });
  assert_true(completed_inline, "close() with no session to stop completes before it returns");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// close() completes only once the reads a stop aborts have completed. A pull queued behind one in
// flight holds the body, and the body holds the session, until the aborted read's completion
// drains the queue. cluster close stops the io_context as soon as close() completes, so a
// completion that ran before the drain would leave that pair unreleased.
void
close_completes_after_the_reads_a_stop_aborts([[maybe_unused]] context& ctx)
{
  std::weak_ptr<http_session> watched;
  auto completed = std::make_shared<completion<bool>>();
  auto queued = std::make_shared<completion<std::error_code>>();
  {
    pool_fixture f{};
    auto session = borrow_connected(f);
    watched = session;
    auto first = std::make_shared<completion<std::error_code>>();
    run_on(session->get_executor(), [&]() {
      auto request = make_request("/incomplete");
      session->write_and_stream(
        request,
        [first, queued](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
          if (ec) {
            return first->set(ec);
          }
          auto body = resp.body();
          // The first pull delivers the two bytes sent with the head. The second then parks on
          // the socket, and the third queues behind it.
          body.next([first](std::string, bool, std::error_code pull_ec) {
            first->set(pull_ec);
          });
          body.next([](std::string, bool, std::error_code) {
          });
          body.next([queued](std::string, bool, std::error_code pull_ec) {
            queued->set(pull_ec);
          });
        },
        []() {
        });
    });
    const auto first_ec = first->wait_for(patience());
    assert_true(first_ec.has_value() && !*first_ec, "the first pull delivers the head's bytes");
    session.reset();

    f.manager->close([&io = f.io, completed]() {
      completed->set(true);
      io.stop();
    });
    assert_true(completed->wait_for(patience()).has_value(), "close() completes");
  }
  const auto queued_ec = queued->wait_for(std::chrono::milliseconds{ 0 });
  assert_true(queued_ec.has_value(), "the queued pull completed before close() did");
  assert_eq(
    *queued_ec, couchbase::errc::common::request_canceled, "the queued pull reports the stop");
  assert_true(watched.expired(), "nothing holds the session once its io_context is gone");
}

// Every handler registered with on_reads_drained() runs, not only the last. A session listed twice
// gets two from close(), and a dropped one would leave close() never completing.
void
every_reads_drained_handler_runs([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  auto parked = std::make_shared<completion<bool>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/incomplete");
    session->write_and_stream(
      request,
      [parked](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return parked->set(false);
        }
        auto body = resp.body();
        // The first pull delivers the head's bytes; the second parks on the socket.
        body.next([parked, body](std::string, bool, std::error_code pull_ec) mutable {
          parked->set(!pull_ec);
          body.next([](std::string, bool, std::error_code) {
          });
        });
      },
      []() {
      });
  });
  const auto has_parked = parked->wait_for(patience());
  assert_true(has_parked.has_value() && *has_parked, "a read parks on the socket");

  auto runs = std::make_shared<std::atomic<int>>(0);
  run_on(session->get_executor(), [&]() {
    for (int i = 0; i < 2; ++i) {
      session->on_reads_drained([runs]() {
        ++*runs;
      });
    }
    session->stop();
  });
  assert_true(wait_until([&runs]() {
                return runs->load() == 2;
              }),
              "both handlers run once the aborted read completes");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Reads queued past the end of a response complete through a drain of their own, one handler at a
// time. on_reads_drained() must wait for that drain too: close() completing earlier lets the
// io_context stop with those reads, and what they hold, never completed.
void
reads_drained_waits_for_the_reads_queued_past_a_clean_end([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  auto pulls = std::make_shared<std::atomic<int>>(0);
  auto completed_when_drained = std::make_shared<completion<int>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/split");
    session->write_and_stream(
      request,
      [session, pulls, completed_when_drained](std::error_code ec,
                                               couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return completed_when_drained->set(-1);
        }
        auto body = resp.body();
        // The first pull reads the tail and ends the response; the other two queue behind it and
        // complete after the end.
        body.next([session, pulls, completed_when_drained](std::string, bool, std::error_code) {
          ++*pulls;
          session->on_reads_drained([pulls, completed_when_drained]() {
            completed_when_drained->set(pulls->load());
          });
        });
        for (int i = 0; i < 2; ++i) {
          body.next([pulls](std::string, bool, std::error_code) {
            ++*pulls;
          });
        }
      },
      []() {
      });
  });
  const auto seen = completed_when_drained->wait_for(patience());
  assert_true(seen.has_value(), "the drained handler runs");
  assert_eq(*seen, 3, "every pull has completed when the drained handler runs");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Regression: a connect that fails after close() had moved the lists out started a failover, and
// the replacement connected for an operation close() had ended. close() never stops a session it
// had in no list, so the callback has to: it creates no replacement and stops the session.
void
a_connect_failing_after_close_opens_no_replacement([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  unlisted_session unlisted{ f, refusing_port() };
  f.manager->close();
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(f.io, [&]() {
    f.manager->connect_then_send_pending_op(
      unlisted.session,
      {},
      std::chrono::steady_clock::now() + std::chrono::minutes{ 1 },
      [done](std::error_code ec, std::shared_ptr<http_session>) {
        done->set(ec);
      });
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the pending operation completes");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "a connect failing after close() completes the operation with request_canceled");
  assert_true(unlisted.session->is_stopped(), "the session close() could not reach is stopped");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_eq(f.server.accepted(),
            std::size_t{ 0 },
            "no replacement connects to the configured node for that operation");
}

// Regression: check_in pooled a connection from before close() after close() had moved the lists
// out, so its idle timer and liveness read kept the io_context busy. The control establishes that
// a manager never closed pools the same kind of session, so the refusal is attributable to the
// close.
void
check_in_of_a_session_from_before_close_stops_it([[maybe_unused]] context& ctx)
{
  {
    pool_fixture control{};
    unlisted_session unlisted{ control, control.server.port() };
    assert_true(connect_and_wait(control, unlisted.session), "the control session connects");
    run_on(control.io, [&]() {
      control.manager->check_in(service, unlisted.session);
    });
    assert_eq(pooled_ids(*control.manager).count(unlisted.session->id()),
              std::size_t{ 1 },
              "an open manager pools the session");
    assert_true(control.shutdown(), "the control's io_context drains once its manager is closed");
  }

  pool_fixture f{};
  unlisted_session unlisted{ f, f.server.port() };
  assert_true(connect_and_wait(f, unlisted.session), "the session connects");
  f.manager->close();
  run_on(f.io, [&]() {
    f.manager->check_in(service, unlisted.session);
  });
  const auto start = std::chrono::steady_clock::now();
  while (!unlisted.session->is_stopped() && within(start, patience())) {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
  }
  assert_true(unlisted.session->is_stopped(), "check_in stops a session from before close()");
  assert_eq(
    pooled_ids(*f.manager).count(unlisted.session->id()), std::size_t{ 0 }, "and does not pool it");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Regression: a pooled connection's liveness read receives the next response's head, and that
// request's write completion can run after the head was handed to the body. Its do_read() then
// read the socket beside read_some() and dropped a part of the body. The ordering is a race, so
// the case reuses one pooled connection for every iteration; the stream-end handler checks the
// connection in before the next iteration checks it out.
void
a_reused_connection_streams_every_body_whole([[maybe_unused]] context& ctx)
{
  constexpr std::size_t iterations = 200;
  struct pulled {
    std::string body{};
    std::function<void()> pull{};
    std::optional<couchbase::core::io::http_streaming_response_body> stream{};
  };

  // Declared before the fixture, whose shutdown can still run a queued check_out that inserts here.
  std::set<std::string> ids;
  std::string first_mangled;
  pool_fixture f{ 2 };
  std::size_t mangled = 0;
  for (std::size_t i = 0; i < iterations; ++i) {
    auto done = std::make_shared<completion<std::string>>();
    asio::post(f.io, [manager = f.manager, done, &ids]() {
      auto [ec, session] = manager->check_out(service, {});
      if (ec) {
        return done->set("check_out: " + ec.message());
      }
      ids.insert(session->id());
      with_connection(
        manager,
        session,
        [manager, done](std::error_code connect_ec, std::shared_ptr<http_session> s) {
          if (connect_ec) {
            return done->set("connect: " + connect_ec.message());
          }
          auto state = std::make_shared<pulled>();
          auto request = make_request("/pieces");
          s->write_and_stream(
            request,
            [state, done](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
              if (ec) {
                return done->set("response: " + ec.message());
              }
              state->stream = resp.body();
              state->pull = [state, done]() {
                state->stream->next(
                  [state, done](std::string data, bool has_more, std::error_code e) {
                    state->body += data;
                    if (e) {
                      state->pull = nullptr;
                      return done->set("pull: " + e.message());
                    }
                    if (has_more) {
                      return state->pull();
                    }
                    state->pull = nullptr;
                  });
              };
              state->pull();
            },
            [manager, s, state, done]() {
              manager->check_in(service, s);
              done->set(state->body);
            });
        });
    });
    const auto result = done->wait_for(patience());
    if (!result.has_value()) {
      fail("iteration " + std::to_string(i) + " did not complete within its budget");
    }
    if (*result != "abcdefghi" && mangled++ == 0) {
      first_mangled = *result;
    }
  }
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_eq(ids.size(), std::size_t{ 1 }, "every iteration reuses the one pooled connection");
  assert_eq(mangled,
            std::size_t{ 0 },
            "every body arrives whole over a reused connection; the first otherwise was \"" +
              first_mangled + "\"");
}

// Regression: a response carrying Connection: close must not leave its connection pooled. A
// streamed response records the header with its head and applies it when the body ends, in the
// same read or in a later pull. The controls establish that a keep-alive response is pooled.
void
a_connection_close_response_is_not_pooled([[maybe_unused]] context& ctx)
{
  struct variant {
    std::string path;
    bool streamed;
    std::string expected;
  };
  for (const auto& v : { variant{ "/complete", false, "reused" },
                         variant{ "/complete", true, "reused" },
                         variant{ "/close", false, "replaced" },
                         variant{ "/close", true, "replaced" },
                         variant{ "/close-split", true, "replaced" } }) {
    pool_fixture f{};
    auto done = std::make_shared<completion<std::string>>();
    asio::post(f.io, [manager = f.manager, done, v]() {
      auto [ec, session] = manager->check_out(service, {});
      if (ec) {
        return done->set("check_out: " + ec.message());
      }
      with_connection(
        manager,
        session,
        [manager, done, v](std::error_code connect_ec, std::shared_ptr<http_session> s) {
          if (connect_ec) {
            return done->set("connect: " + connect_ec.message());
          }
          // Checks the connection in once the response has ended and reports whether the next
          // checkout takes it again.
          auto end = [manager, done, s]() {
            manager->check_in(service, s);
            auto [next_ec, next] = manager->check_out(service, {});
            done->set(next_ec ? "check_out: " + next_ec.message()
                              : (next == s ? "reused" : "replaced"));
          };
          auto request = make_request(v.path);
          if (v.streamed) {
            return s->write_and_stream(
              request,
              [done](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
                if (ec) {
                  return done->set("response: " + ec.message());
                }
                drain(resp.body());
              },
              std::move(end));
          }
          s->write_and_subscribe(
            request, [done, end](std::error_code ec, couchbase::core::io::http_response&&) {
              if (ec) {
                return done->set("response: " + ec.message());
              }
              end();
            });
        });
    });
    const auto label = v.path + (v.streamed ? ", streamed" : ", buffered");
    const auto result = done->wait_for(patience());
    assert_true(result.has_value(), label + ": the response ends within its budget");
    assert_eq(*result, v.expected, label + ": the next checkout");
    assert_true(f.shutdown(), label + ": the io_context drains once the manager is closed");
  }
}

// A stop() from inside a streaming response handler, as an operation cancelled there makes, tears
// the session down while the read completion holds the response context. The stream-end handler
// must still run: reinstalled into the stopped session, the context is never ended, and it holds
// the session it belongs to.
void
a_stop_inside_a_streaming_response_handler_still_ends_the_stream([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  auto stopped = std::make_shared<std::atomic_bool>(false);
  auto ended = std::make_shared<completion<bool>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/incomplete");
    session->write_and_stream(
      request,
      [session, stopped](std::error_code ec, couchbase::core::io::http_streaming_response) {
        if (!ec) {
          session->stop();
          stopped->store(true);
        }
      },
      [ended]() {
        ended->set(true);
      });
  });
  const bool stream_ended = ended->wait_for(patience()).has_value();
  assert_true(stopped->load(), "the response handler stops the session");
  assert_true(stream_ended, "the stream-end handler runs after a stop inside the response handler");
}

// A pull issued from an io thread other than the session strand, racing a close from a third
// thread. The read is initiated on the strand, where the stop the close posts tears the stream
// down; initiated off it, the two touch the socket concurrently. Only ThreadSanitizer detects that
// race. In a plain build the case checks only that the close stops the session.
void
a_pull_off_the_strand_racing_a_close_under_thread_sanitizer([[maybe_unused]] context& ctx)
{
  pool_fixture f{ 4 };
  constexpr std::size_t iterations = 200;
  std::size_t not_stopped = 0;
  for (std::size_t i = 0; i < iterations; ++i) {
    auto session = borrow_connected(f);
    auto closed = std::make_shared<completion<bool>>();
    run_on(session->get_executor(), [&]() {
      auto request = make_request("/incomplete");
      session->write_and_stream(
        request,
        [&io = f.io, closed](std::error_code ec,
                             couchbase::core::io::http_streaming_response resp) {
          if (ec) {
            return closed->set(false);
          }
          auto body = resp.body();
          asio::post(io, [body]() {
            drain(body);
          });
          asio::post(io, [body, closed]() mutable {
            body.close();
            closed->set(true);
          });
        },
        []() {
        });
    });
    const auto was_closed = closed->wait_for(patience());
    assert_true(was_closed.has_value() && *was_closed, "the streamed body is closed");
    // close() posts stop() to the strand ahead of this barrier.
    run_on(session->get_executor(), []() {
    });
    if (!session->is_stopped()) {
      ++not_stopped;
    }
  }
  assert_eq(not_stopped, std::size_t{ 0 }, "sessions an incomplete body's close left running");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// A whole-stream deadline racing the tail of a response on a multi-threaded io_context. The
// expiry closes the body from whichever runner the timer completes on, while the read that ends
// the response completes on the session strand, and either may take the body's lock first. A
// session the body stops is never one check_in published, and the body reports either a clean end
// or the deadline. The expiry is swept from zero to twice the time one "/split" response takes on
// this runner, measured first, so the sweep scales with the machine; the case asserts that both
// orders were reached. Which order an iteration in the middle of the sweep reaches depends on the
// scheduler, so a regression fails a run with a rate, not always. Each order is pinned
// deterministically elsewhere: a tail that ends the body before check_in by
// the_body_has_ended_before_its_connection_is_checked_in, and a deadline that stops a parked
// session by a_socket_stream_abandoned_above_the_high_water_mark_releases_its_session.
void
a_deadline_racing_the_tail_either_stops_or_pools_the_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{ 4 };
  constexpr std::size_t iterations = 400;
  constexpr std::size_t sweep = 21;
  // Streams "/split", which writes its tail after the loopback endpoint's pause, with a deadline
  // `expiry` after the head. `published` is set by the stream-end handler from the state check_in
  // decides on, and stays unset if it never ran.
  auto stream_split = [&f](const std::shared_ptr<http_session>& session,
                           std::chrono::microseconds expiry,
                           const std::shared_ptr<std::atomic_bool>& published) {
    auto terminal = std::make_shared<completion<std::error_code>>();
    run_on(session->get_executor(), [&]() {
      auto request = make_request("/split");
      session->write_and_stream(
        request,
        [terminal, expiry](std::error_code ec, couchbase::core::io::http_streaming_response resp) {
          if (ec) {
            return terminal->set(ec);
          }
          auto body = resp.body();
          [[maybe_unused]] const auto armed =
            body.set_deadline(std::chrono::steady_clock::now() + expiry,
                              couchbase::core::io::deadline_terminal::ambiguous);
          drain_to_terminal(std::move(body), terminal);
        },
        [manager = f.manager, session, published]() {
          // check_in's refusal test, read on the strand check_in runs on.
          published->store(!session->is_stopping() && !session->is_stopped());
          manager->check_in(service, session);
        });
    });
    return terminal->wait_for(patience());
  };

  std::chrono::microseconds span{};
  {
    auto session = borrow_connected(f);
    const auto start = std::chrono::steady_clock::now();
    const auto ec = stream_split(session, patience(), std::make_shared<std::atomic_bool>(false));
    assert_true(ec.has_value() && !*ec, "a response with a distant deadline ends cleanly");
    span = 2 * std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start);
  }
  const auto step = span / static_cast<int>(sweep - 1);

  std::size_t clean = 0;
  std::size_t expired = 0;
  std::size_t published_then_stopped = 0;
  std::size_t unexpected = 0;
  std::size_t stalled = 0;
  std::error_code first_unexpected{};
  for (std::size_t i = 0; i < iterations; ++i) {
    auto session = borrow_connected(f);
    auto published = std::make_shared<std::atomic_bool>(false);
    const auto ec = stream_split(session, step * static_cast<int>(i % sweep), published);
    if (!ec.has_value()) {
      ++stalled;
      break;
    }
    // The body posts its stop to the strand before it reports the terminal, so once this barrier
    // has run, so has any stop the body decided.
    run_on(session->get_executor(), []() {
    });
    if (published->load() && session->is_stopped()) {
      ++published_then_stopped;
    }
    if (!*ec) {
      ++clean;
    } else if (*ec == couchbase::errc::common::ambiguous_timeout) {
      ++expired;
    } else if (++unexpected == 1) {
      first_unexpected = *ec;
    }
  }
  const bool drained = f.shutdown();

  assert_eq(stalled, std::size_t{ 0 }, "every body reports a terminal within its budget");
  assert_eq(published_then_stopped,
            std::size_t{ 0 },
            "sessions check_in published and the body's deadline then stopped");
  assert_eq(unexpected,
            std::size_t{ 0 },
            "terminals other than a clean end or the deadline, first: " +
              first_unexpected.message());
  assert_true(clean > 0 && expired > 0,
              "both orders are reached; clean ends: " + std::to_string(clean) +
                ", deadlines: " + std::to_string(expired));
  assert_true(drained, "the io_context drains once the manager is closed");
}
// A response handler the teardown runs may stop the session again, as an operation's completion
// can. That stop returns at once: a second teardown would lock current_response_mutex_, which
// cancel_current_response() holds while the handler runs, and deadlock the strand.
void
a_stop_from_a_handler_the_teardown_runs_returns_at_once([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(session->get_executor(), [&]() {
    // The loopback endpoint does not answer this path, so only the stop below ends the request.
    auto request = make_request("/held");
    session->write_and_subscribe(
      request, [session, done](std::error_code ec, couchbase::core::io::http_response&&) {
        session->stop();
        done->set(ec);
      });
    session->stop();
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the cancelled request completes within its budget");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "a stop cancels the request in flight");
}

// Runs stop() on the session strand and reports whether consumer_failure reached the caller.
auto
stop_throws_consumer_failure(const std::shared_ptr<http_session>& session) -> bool
{
  try {
    run_on(session->get_executor(), [&]() {
      session->stop();
    });
  } catch (const consumer_failure&) {
    return true;
  }
  return false;
}

// Regression: a connect callback that throws during stop() skips the rest of the teardown, and a
// later stop() returns without running it, so the request in flight is never cancelled and on_stop
// never removes the session from the busy list.
void
a_throwing_connect_callback_does_not_skip_the_teardown([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  auto done = std::make_shared<completion<std::error_code>>();
  assert_eq(pooled_ids(*f.manager).count(session->id()),
            std::size_t{ 1 },
            "the borrowed session is listed by the pool");

  bool propagated = false;
  run_on(session->get_executor(), [&]() {
    // The loopback endpoint does not answer this path, so only the stop ends the request.
    auto request = make_request("/held");
    session->write_and_subscribe(request,
                                 [done](std::error_code ec, couchbase::core::io::http_response&&) {
                                   done->set(ec);
                                 });
    // Stopped in the same strand handler, so the connect started here cannot complete and run
    // the callback on the io thread first.
    session->connect([]() {
      throw consumer_failure{};
    });
    try {
      session->stop();
    } catch (const consumer_failure&) {
      propagated = true;
    }
  });
  assert_true(propagated, "the connect callback's exception propagates out of stop()");
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(),
              "the request in flight completes although the connect callback threw");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "a stop cancels the request in flight");
  assert_eq(pooled_ids(*f.manager).count(session->id()),
            std::size_t{ 0 },
            "on_stop removes the session from the pool although the connect callback threw");
}

// Regression: a response handler that throws while stop() cancels it skips on_stop, so the stopped
// session stays in the busy list.
void
a_throwing_response_handler_does_not_skip_on_stop([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/held");
    session->write_and_subscribe(request,
                                 [](std::error_code, couchbase::core::io::http_response&&) {
                                   throw consumer_failure{};
                                 });
  });
  assert_eq(pooled_ids(*f.manager).count(session->id()),
            std::size_t{ 1 },
            "the borrowed session is listed by the pool");

  assert_true(stop_throws_consumer_failure(session),
              "the response handler's exception propagates out of stop()");
  assert_eq(pooled_ids(*f.manager).count(session->id()),
            std::size_t{ 0 },
            "on_stop removes the session from the pool although the response handler threw");
}

// Regression: a read_some() callback that throws on an IO error left the failed connection running
// and listed as busy, since the session was stopped only after the callback returned.
void
a_throwing_reader_on_a_read_error_still_stops_the_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  // Held to the end: a dropped body stops its session, which would cancel the read below instead
  // of failing it with the transport error.
  auto bodies = std::make_shared<std::vector<couchbase::core::io::http_streaming_response>>();
  // Joins the runners before the bodies are released, on every exit, so a response handler still
  // running cannot touch the vector while it is cleared. The fixture's own shutdown then returns at
  // once.
  const std::shared_ptr<void> release_bodies(nullptr, [bodies, &f](void*) {
    f.shutdown();
    bodies->clear();
  });
  auto read_ec = std::make_shared<completion<std::error_code>>();
  run_on(session->get_executor(), [&]() {
    auto request = make_request("/reset");
    session->write_and_stream(
      request,
      [session, bodies, read_ec](std::error_code ec,
                                 couchbase::core::io::http_streaming_response resp) {
        if (ec) {
          return;
        }
        bodies->push_back(std::move(resp));
        // Parks on the socket until the endpoint closes it.
        session->read_some([read_ec](std::string, bool, std::error_code read_error) {
          read_ec->set(read_error);
          throw consumer_failure{};
        });
      },
      []() {
      });
  });
  assert_true(wait_until([&f]() {
                return f.consumer_failures.load() == 1;
              }),
              "the reader's exception reaches the io_context runner");
  const auto ec = read_ec->wait_for(patience());
  assert_true(ec.has_value() && *ec && *ec != couchbase::errc::common::request_canceled,
              "the reader sees the transport error, not a stop; got " +
                (ec.has_value() ? ec->message() : std::string{ "nothing" }));
  run_on(session->get_executor(), []() {
  });
  assert_true(session->is_stopped(), "the session the read error failed is stopped");
  assert_eq(pooled_ids(*f.manager).count(session->id()),
            std::size_t{ 0 },
            "on_stop removes the failed session from the pool although the reader threw");
}

// close() can stop a checked-out session before its caller calls connect(), and the teardown then
// runs before the callback is stored. That callback must still run: nothing else invokes it, and it
// holds the session.
void
a_connect_after_the_teardown_runs_its_callback([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  std::shared_ptr<http_session> session;
  run_on(f.io, [&]() {
    session = check_out(*f.manager);
  });
  assert_false(session->is_connected(), "the checked-out session is still to connect");
  f.manager->close();

  auto done = std::make_shared<completion<bool>>();
  run_on(f.io, [&]() {
    session->connect([done, session]() {
      done->set(session->is_connected());
    });
  });
  const auto connected = done->wait_for(patience());
  assert_true(connected.has_value(), "the connect callback runs within its budget");
  assert_false(*connected, "a connect on a torn-down session does not connect");
}

// close() stops a session whose connect is in progress, and stop() runs the connect callback. The
// callback must treat a stopped session as terminal. Treated as a failed connect, it would be
// replaced by a new session that opens a socket after the manager has closed.
void
a_close_while_connecting_cancels_the_pending_operation([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(f.io, [&]() {
    auto session = check_out(*f.manager);
    assert_false(session->is_connected(), "the checked-out session is still to connect");
    with_connection(f.manager, session, [done](std::error_code ec, std::shared_ptr<http_session>) {
      done->set(ec);
    });
    f.manager->close();
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the pending operation completes within its budget");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "a close during connect cancels the pending operation");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_eq(
    f.server.accepted(), std::size_t{ 0 }, "no connection is opened after the manager is closed");
}

// The same race on execute()'s path: the request completes with request_canceled instead of being
// sent over a replacement session.
void
a_close_while_connecting_cancels_the_request([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  couchbase::core::operations::management::collection_update_request request{};
  request.bucket_name = "bucket";
  request.scope_name = "scope";
  request.collection_name = "collection";
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(f.io, [&]() {
    f.manager->execute(
      request, [done](couchbase::core::operations::management::collection_update_response&& resp) {
        done->set(resp.ctx.ec);
      });
    f.manager->close();
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the request completes within its budget");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "a close during connect cancels the request");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
  assert_eq(
    f.server.accepted(), std::size_t{ 0 }, "no connection is opened after the manager is closed");
}

// check_in can reach set_idle() after its is_stopped() test while a stop() on another thread tears
// the session down. An idle wait armed after the teardown's cancel holds the stopped session until
// the idle timeout, so set_idle() arms nothing once the session is stopped. The wait would keep the
// io_context from draining, which is what is asserted: reset_idle() refuses a stopped session
// whether or not a wait is armed.
void
set_idle_arms_no_timer_on_a_stopped_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  run_on(session->get_executor(), [&]() {
    session->stop();
  });
  run_on(f.io, [&]() {
    session->set_idle(24h);
  });
  assert_true(f.shutdown(), "set_idle() arms no idle timer on a stopped session");
}

// reset_idle() must refuse an idle session whose stop() has set stopped_ and not yet cancelled
// the idle timer, which another thread's stop() leaves open to a concurrent check_out. stop() runs
// the connect callback between the two, so a check_out from that callback lands in the window.
void
check_out_skips_an_idle_session_whose_stop_has_begun([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto parked = borrow_connected(f);
  std::shared_ptr<http_session> next;
  run_on(parked->get_executor(), [&]() {
    f.manager->check_in(service, parked);
    parked->connect([&]() {
      next = check_out(*f.manager);
    });
    parked->stop();
  });
  assert_true(next != nullptr, "stop() runs the connect callback");
  assert_ne(
    next->id(), parked->id(), "check_out does not yield an idle session whose stop() has begun");
}

// A request whose encode_to fails completes inside send(), so on a fresh session its handler and
// check_in run inline in connect_then_send's callback. check_in takes sessions_mutex_, so the
// callback must call send_to() after releasing it: the io_context has one runner, so a
// self-deadlock there leaves the barrier below unrun.
void
an_encode_failure_on_a_fresh_session_completes_and_checks_in([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  couchbase::core::operations::management::collection_update_request request{};
  request.bucket_name = "bucket";
  request.scope_name = "scope";
  request.collection_name = "collection";
  // Below -1, which encode_to rejects before anything is written.
  request.max_expiry = -2;
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(f.io, [&]() {
    f.manager->execute(
      request, [done](couchbase::core::operations::management::collection_update_response&& resp) {
        done->set(resp.ctx.ec);
      });
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the handler runs within its budget");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "the handler receives the encode error");
  run_on(f.io, []() {
  });
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// http_component over a cluster that is never opened. The fixture's manager is replaced by the
// cluster's, so pool_fixture's helpers and its shutdown act on the manager the component
// dispatches through.
struct component_fixture {
  explicit component_fixture(std::size_t threads = 1)
    : f{ threads }
  {
    f.manager = cluster.http_session_manager().second;
    instrument(*f.manager);
    f.manager->set_configuration(loopback_config(f.server.port()), f.options);
  }

  component_fixture(const component_fixture&) = delete;
  component_fixture(component_fixture&&) = delete;
  auto operator=(const component_fixture&) -> component_fixture& = delete;
  auto operator=(component_fixture&&) -> component_fixture& = delete;

  // The manager holds a reference to the cluster's origin, so it is closed and released before the
  // cluster is destroyed.
  ~component_fixture()
  {
    close_cluster();
    f.shutdown();
    f.manager.reset();
  }

  // Closes the cluster, which closes its session manager and releases its hold on the io_context,
  // then the fixture.
  auto shutdown() -> bool
  {
    assert_true(close_cluster(), "the cluster's close completes");
    return f.shutdown();
  }

  auto close_cluster() -> bool
  {
    const auto closed = std::make_shared<completion<bool>>();
    cluster.close([closed]() {
      closed->set(true);
    });
    return closed->wait_for(patience()).value_or(false);
  }

  // Adds a second node, which refuses connections, and returns its endpoint. update_config() keeps
  // the node index set_configuration() left at zero, so a request naming the refusing node fails
  // over to the loopback endpoint, listed first.
  auto add_refusing_node() -> std::string
  {
    const auto refused = refusing_port();
    auto config = loopback_config(f.server.port());
    auto refusing = config.nodes.front();
    refusing.services_plain.management = refused;
    config.nodes.push_back(refusing);
    f.manager->update_config(config);
    return "127.0.0.1:" + std::to_string(refused);
  }

  pool_fixture f{};
  couchbase::core::cluster cluster{ f.io };
  couchbase::core::http_component component{ f.io, couchbase::core::core_sdk_shim{ cluster } };
};

auto
free_form_request(std::string path) -> couchbase::core::http_request
{
  couchbase::core::http_request request{};
  request.service = service;
  request.method = "GET";
  request.path = std::move(path);
  request.headers["connection"] = "keep-alive";
  request.timeout = std::chrono::minutes{ 1 };
  return request;
}

void
drain_component_body(couchbase::core::http_response_body body,
                     std::shared_ptr<completion<std::error_code>> terminal)
{
  body.next([body, terminal](std::string, bool has_more, std::error_code ec) mutable {
    if (ec || !has_more) {
      return terminal->set(ec);
    }
    drain_component_body(std::move(body), std::move(terminal));
  });
}

// Once a request the failover replacement served has ended, the replacement is the only session
// listed, and it is idle: the next request to its node reuses it. A replacement left busy is
// listed too, but the next request opens another connection.
void
assert_only_the_replacement_is_pooled(component_fixture& c)
{
  std::set<std::string> listed;
  std::shared_ptr<http_session> next;
  // With one runner this runs after the handler that ended the request, check_in included.
  run_on(c.f.io, [&]() {
    listed = pooled_ids(*c.f.manager);
    auto [ec, session] =
      c.f.manager->check_out(service, "127.0.0.1:" + std::to_string(c.f.server.port()));
    assert_success(ec, "check_out succeeds against the live node");
    next = std::move(session);
  });
  assert_eq(listed.size(), std::size_t{ 1 }, "one session is listed once the request has ended");
  assert_eq(listed.count(next->id()),
            std::size_t{ 1 },
            "the next request to the replacement's node reuses it, so it was checked in");
  assert_true(c.shutdown(), "the io_context drains once the manager is closed");
}

// Regression: the stream-end handler checked in the session it checked out, which the failover had
// stopped, and left the replacement that served the stream busy until close().
void
a_streamed_request_checks_in_its_failover_replacement([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto request = free_form_request("/split");
  request.endpoint = c.add_refusing_node();
  auto terminal = std::make_shared<completion<std::error_code>>();
  auto op = c.component.do_http_request(
    request, [terminal](couchbase::core::http_response resp, std::error_code ec) {
      if (ec) {
        return terminal->set(ec);
      }
      drain_component_body(resp.body(), terminal);
    });
  assert_true(op.has_value(), "the request is dispatched");
  const auto ec = terminal->wait_for(patience());
  assert_true(ec.has_value(), "the stream ends within its budget");
  assert_success(*ec, "the replacement serves the stream to a clean end");
  assert_only_the_replacement_is_pooled(c);
}

// Regression: the buffered completion checked in the session it checked out, which the failover had
// stopped, and left the replacement that served the response busy until close().
void
a_buffered_request_checks_in_its_failover_replacement([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto request = free_form_request("/complete");
  request.endpoint = c.add_refusing_node();
  auto done = std::make_shared<completion<std::error_code>>();
  auto op = c.component.do_http_request_buffered(
    request, [done](couchbase::core::buffered_http_response, std::error_code ec) {
      done->set(ec);
    });
  assert_true(op.has_value(), "the request is dispatched");
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the request completes within its budget");
  assert_success(*ec, "the replacement serves the response");
  assert_only_the_replacement_is_pooled(c);
}

// The response-first cases need the answer in the client's receive queue, which SIOCOUTQ
// observes, and a reactor that queues a socket completion ahead of a timer completion from the same
// pass, which epoll does. Both are Linux-only.
class needs_linux_completion_ordering : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "SIOCOUTQ delivery and epoll completion ordering (Linux)";
  }

  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
#if defined(__linux__)
    return check_result::ok();
#else
    return check_result::missing("SIOCOUTQ and the epoll completion order exist only on Linux");
#endif
  }
};

// CXXCBC-1046: a deadline completion queued behind a handled response stopped the session.
// The runner is held so that one reactor pass queues the response ahead of the deadline.
struct request_deadline_setup {
  explicit request_deadline_setup(pool_fixture& fixture)
    : f{ fixture }
  {
    auto [ec, m] = cluster.http_session_manager();
    assert_success(ec, "an unopened cluster has a session manager");
    manager = std::move(m);
    instrument(*manager);
    manager->set_configuration(loopback_config(f.server.port()), f.options);
    session = borrow_connected(f.io, manager);
    run_on(f.io, [&]() {
      manager->check_in(service, session);
    });
    // check_in posts the read that waits on the idle connection to the strand. Armed before the
    // request, it is the read the response completes.
    run_on(session->get_executor(), []() {
    });
  }

  request_deadline_setup(const request_deadline_setup&) = delete;
  request_deadline_setup(request_deadline_setup&&) = delete;
  auto operator=(const request_deadline_setup&) -> request_deadline_setup& = delete;
  auto operator=(request_deadline_setup&&) -> request_deadline_setup& = delete;

  ~request_deadline_setup()
  {
    manager->close();
  }

  [[nodiscard]] auto component() -> couchbase::core::http_component
  {
    return couchbase::core::http_component{ f.io, couchbase::core::core_sdk_shim{ cluster } };
  }

  // Returns once the runner has been held past both the delivery of the answer and the deadline.
  template<typename Issue>
  void issue_and_hold_past_the_deadline(Issue&& issue, std::chrono::milliseconds timeout)
  {
    const auto held = f.server.held();
    const auto heads = f.server.heads_delivered();
    run_on(f.io, std::forward<Issue>(issue));
    const auto expired_after = std::chrono::steady_clock::now() + timeout;
    const auto start = std::chrono::steady_clock::now();
    while (f.server.held() == held && within(start, patience())) {
      std::this_thread::yield();
    }
    assert_true(f.server.held() > held, "the endpoint receives the request");
    // A write completion still posted to the strand would queue the read behind the deadline.
    run_on(f.io, []() {
    });
    run_on(session->get_executor(), []() {
    });
    bool answered = false;
    run_on(f.io, [&]() {
      f.server.release_held();
      const auto hold_start = std::chrono::steady_clock::now();
      while ((f.server.heads_delivered() == heads ||
              std::chrono::steady_clock::now() <= expired_after) &&
             within(hold_start, patience())) {
        std::this_thread::yield();
      }
      answered = f.server.heads_delivered() > heads;
    });
    assert_true(answered, "the answer is delivered while the runner is held");
  }

  pool_fixture& f;
  couchbase::core::cluster cluster{ f.io };
  std::shared_ptr<http_session_manager> manager{};
  std::shared_ptr<http_session> session{};
};

// Long enough that the request is written, and its write completion has run, before the deadline
// expires. The runner is held past it regardless, so a longer one costs only wall time.
auto
held_request_timeout() -> std::chrono::milliseconds
{
  return scaled_budget(300ms);
}

// No response ever comes, so the deadline ends the request.
constexpr auto unanswered_request_timeout = 10ms;

auto
freeform(std::string path, std::chrono::milliseconds timeout)
  -> couchbase::core::operations::management::freeform_request
{
  couchbase::core::operations::management::freeform_request request{};
  request.type = service;
  request.method = "GET";
  request.path = std::move(path);
  request.timeout = timeout;
  return request;
}

auto
free_form(std::string path, std::chrono::milliseconds timeout) -> couchbase::core::http_request
{
  couchbase::core::http_request request{};
  request.service = service;
  request.method = "GET";
  request.path = std::move(path);
  request.timeout = timeout;
  return request;
}

// Waits for the request's completion, then for everything queued behind it, which includes a
// deadline completion queued by the same reactor pass as the response.
auto
wait_then_drain(pool_fixture& f, completion<std::error_code>& done)
  -> std::optional<std::error_code>
{
  auto ec = done.wait_for(patience());
  run_on(f.io, []() {
  });
  return ec;
}

void
a_command_deadline_queued_behind_its_response_leaves_the_session_pooled(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto done = std::make_shared<completion<std::error_code>>();
    setup.issue_and_hold_past_the_deadline(
      [&]() {
        setup.manager->execute(
          freeform("/held/complete", held_request_timeout()),
          [done](couchbase::core::operations::management::freeform_response&& resp) {
            done->set(resp.ctx.ec);
          });
      },
      held_request_timeout());
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_success(*ec, "the response is handled before the deadline completion runs");
    assert_false(setup.session->is_stopped(), "the deadline completion leaves the session running");
    assert_eq(pooled_ids(*setup.manager).count(setup.session->id()),
              std::size_t{ 1 },
              "the session the response checked in stays in the pool");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

void
a_buffered_request_deadline_queued_behind_its_response_leaves_the_session_pooled(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto component = setup.component();
    auto done = std::make_shared<completion<std::error_code>>();
    setup.issue_and_hold_past_the_deadline(
      [&]() {
        auto op = component.do_http_request_buffered(
          free_form("/held/complete", held_request_timeout()),
          [done](couchbase::core::buffered_http_response, std::error_code ec) {
            done->set(ec);
          });
        if (!op) {
          done->set(op.error());
        }
      },
      held_request_timeout());
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_success(*ec, "the response is handled before the deadline completion runs");
    assert_false(setup.session->is_stopped(), "the deadline completion leaves the session running");
    assert_eq(pooled_ids(*setup.manager).count(setup.session->id()),
              std::size_t{ 1 },
              "the session the response checked in stays in the pool");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

// "/incomplete" sends two of the five bytes it announces, so the stream is still open on the
// session when the deadline completion runs.
void
a_streamed_request_deadline_queued_behind_its_response_leaves_the_stream_open(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto component = setup.component();
    auto done = std::make_shared<completion<std::error_code>>();
    auto response = std::make_shared<couchbase::core::http_response>();
    setup.issue_and_hold_past_the_deadline(
      [&]() {
        auto op = component.do_http_request(
          free_form("/held/incomplete", held_request_timeout()),
          [done, response](couchbase::core::http_response resp, std::error_code ec) {
            *response = std::move(resp);
            done->set(ec);
          });
        if (!op) {
          done->set(op.error());
        }
      },
      held_request_timeout());
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_success(*ec, "the response is handled before the deadline completion runs");
    assert_false(setup.session->is_stopped(), "the deadline completion leaves the session running");

    using chunk = std::tuple<std::string, bool, std::error_code>;
    auto pulled = std::make_shared<completion<chunk>>();
    response->body().next([pulled](std::string data, bool has_more, std::error_code pull_ec) {
      pulled->set({ std::move(data), has_more, pull_ec });
    });
    const auto first = pulled->wait_for(patience());
    response->close();
    assert_true(first.has_value(), "the first pull completes");
    assert_success(std::get<2>(*first), "the stream is not ended by its request deadline");
    assert_eq(std::get<0>(*first), std::string{ "ab" }, "the body delivers what was sent");
    assert_true(std::get<1>(*first), "the body has more to come");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

void
a_command_deadline_before_its_response_times_out_and_stops_the_session(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto done = std::make_shared<completion<std::error_code>>();
    auto stopped_first = std::make_shared<std::atomic_bool>(false);
    run_on(f.io, [&]() {
      setup.manager->execute(freeform("/unanswered", unanswered_request_timeout),
                             [done, stopped_first, session = setup.session](
                               couchbase::core::operations::management::freeform_response&& resp) {
                               *stopped_first = session->is_stopped() || session->is_stopping();
                               done->set(resp.ctx.ec);
                             });
    });
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_eq(*ec,
              std::error_code{ couchbase::errc::common::ambiguous_timeout },
              "the deadline ends the request");
    assert_true(setup.session->is_stopped(), "the deadline stops the session");
    // Regression: stopped after the completion, the session was checked in first, and another
    // request could take it before the stop.
    assert_true(stopped_first->load(), "the session is stopped before the request completes");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

void
a_buffered_request_deadline_before_its_response_times_out_and_stops_the_session(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto component = setup.component();
    auto done = std::make_shared<completion<std::error_code>>();
    auto stopped_first = std::make_shared<std::atomic_bool>(false);
    run_on(f.io, [&]() {
      auto op = component.do_http_request_buffered(
        free_form("/unanswered", unanswered_request_timeout),
        [done, stopped_first, session = setup.session](couchbase::core::buffered_http_response,
                                                       std::error_code ec) {
          *stopped_first = session->is_stopped() || session->is_stopping();
          done->set(ec);
        });
      if (!op) {
        done->set(op.error());
      }
    });
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_eq(*ec,
              std::error_code{ couchbase::errc::common::ambiguous_timeout },
              "the deadline ends the request");
    assert_true(setup.session->is_stopped(), "the deadline stops the session");
    // Regression: stopped after the completion, the session was checked in first, and another
    // request could take it before the stop.
    assert_true(stopped_first->load(), "the session is stopped before the request completes");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

void
a_streamed_request_deadline_before_its_response_times_out_and_stops_the_session(
  [[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  {
    request_deadline_setup setup{ f };
    auto component = setup.component();
    auto done = std::make_shared<completion<std::error_code>>();
    auto stopped_first = std::make_shared<std::atomic_bool>(false);
    run_on(f.io, [&]() {
      auto op = component.do_http_request(free_form("/unanswered", unanswered_request_timeout),
                                          [done, stopped_first, session = setup.session](
                                            couchbase::core::http_response, std::error_code ec) {
                                            *stopped_first =
                                              session->is_stopped() || session->is_stopping();
                                            done->set(ec);
                                          });
      if (!op) {
        done->set(op.error());
      }
    });
    const auto ec = wait_then_drain(f, *done);
    assert_true(ec.has_value(), "the request completes");
    assert_eq(*ec,
              std::error_code{ couchbase::errc::common::ambiguous_timeout },
              "the deadline ends the request");
    assert_true(setup.session->is_stopped(), "the deadline stops the session");
    assert_true(stopped_first->load(), "the session is stopped before the request completes");
  }
  assert_true(f.shutdown(), "the io_context drains once the managers are closed");
}

// Dispatches `request` as a streamed or a buffered free-form operation, reporting its error code
// to `done`.
auto
dispatch(component_fixture& c,
         const couchbase::core::http_request& request,
         bool streamed,
         const std::shared_ptr<completion<std::error_code>>& done)
  -> std::shared_ptr<couchbase::core::pending_operation>
{
  auto dispatched =
    streamed
      ? c.component.do_http_request(request,
                                    [done](couchbase::core::http_response, std::error_code ec) {
                                      done->set(ec);
                                    })
      : c.component.do_http_request_buffered(
          request, [done](couchbase::core::buffered_http_response, std::error_code ec) {
            done->set(ec);
          });
  assert_true(dispatched.has_value(), "the request is dispatched");
  return *dispatched;
}

// A connected keep-alive session parked idle, which the next request's check_out for `type`
// takes.
auto
park_idle(pool_fixture& f, couchbase::core::service_type type = service)
  -> std::shared_ptr<http_session>
{
  auto session = borrow_connected(f, type);
  run_on(f.io, [&]() {
    f.manager->check_in(type, session);
  });
  return session;
}

auto
alive(const std::weak_ptr<void>& probe) -> std::string
{
  return probe.expired() ? "expired" : "alive";
}

// Regression: a completed buffered operation the caller still holds must not keep the session the
// pool has since evicted and stopped.
void
a_completed_buffered_operation_does_not_hold_its_evicted_session([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto done = std::make_shared<completion<std::error_code>>();
  std::shared_ptr<couchbase::core::pending_operation> op;
  run_on(c.f.io, [&]() {
    op = dispatch(c, free_form_request("/complete"), false, done);
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the request completes within its budget");
  assert_success(*ec, "the request succeeds");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the request takes the parked session");

  // With one runner this runs after the handler that completed the request, check_in included.
  run_on(c.f.io, [&]() {
    assert_eq(pooled_ids(*c.f.manager).size(),
              std::size_t{ 1 },
              "the completed request checks its session in");
    c.f.manager->update_config({});
  });
  assert_true(wait_until([&c]() {
                return pooled_ids(*c.f.manager).empty();
              }),
              "the pool evicts the session once its node leaves the configuration");
  const bool released = wait_until([&session_probe]() {
    return session_probe.expired();
  });
  assert_true(released, "the operation does not keep the evicted session alive");
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// Regression: an operation completed with request_canceled by its session's stop must not keep
// that session alive while the caller holds the operation.
void
an_operation_cancelled_by_its_session_stop_does_not_hold_the_session([[maybe_unused]] context& ctx)
{
  std::string outcome;
  bool all_released = true;
  for (const bool streamed : { false, true }) {
    component_fixture c{};
    auto parked = park_idle(c.f);
    const std::weak_ptr<http_session> session_probe = parked;

    auto done = std::make_shared<completion<std::error_code>>();
    std::shared_ptr<couchbase::core::pending_operation> op;
    run_on(c.f.io, [&]() {
      // The loopback endpoint does not answer this path, so only the stop ends the request.
      op = dispatch(c, free_form_request("/held"), streamed, done);
    });
    run_on(parked->get_executor(), [&]() {
      parked->stop();
    });
    parked.reset();
    const auto ec = done->wait_for(patience());
    assert_true(ec.has_value(), "the request completes within its budget");
    assert_eq(*ec,
              std::error_code{ couchbase::errc::common::request_canceled },
              "the session's stop cancels the request");
    assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the request takes the parked session");

    const bool released = wait_until([&session_probe]() {
      return session_probe.expired();
    });
    all_released = all_released && released;
    outcome += std::string{ streamed ? ", streamed: " : "buffered: " } + alive(session_probe);
    assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
  }
  assert_true(all_released,
              "the operation does not keep its stopped session alive; session probe " + outcome);
}

// Regression: a streamed body dropped unread, with no pull in flight, must release its operation
// and its session without a close().
void
a_streamed_body_dropped_unread_releases_its_operation_and_session([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto done = std::make_shared<completion<std::error_code>>();
  std::weak_ptr<couchbase::core::pending_operation> op_probe;
  run_on(c.f.io, [&]() {
    // The endpoint sends the head and part of the body; the rest never comes.
    op_probe = dispatch(c, free_form_request("/incomplete"), true, done);
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the response head arrives within its budget");
  assert_success(*ec, "the response head is delivered");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the request takes the parked session");

  const bool released = wait_until([&]() {
    return op_probe.expired() && session_probe.expired();
  });
  assert_true(released,
              "a dropped body releases its operation and session without close(); operation " +
                alive(op_probe) + ", session " + alive(session_probe));
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// Regression: an unread body dropped with a deadline armed must release its session when it is
// dropped. The deadline's wait held the body until expiry, and the body held the session.
void
a_streamed_body_dropped_with_a_deadline_armed_releases_its_session([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto done = std::make_shared<completion<std::error_code>>();
  auto armed = std::make_shared<std::optional<couchbase::core::io::deadline_state>>();
  run_on(c.f.io, [&]() {
    // The endpoint sends the head and part of the body; the rest never comes.
    auto op = c.component.do_http_request(
      free_form_request("/incomplete"),
      [done, armed](couchbase::core::http_response resp, std::error_code ec) {
        if (!ec) {
          *armed =
            resp.body().set_deadline(std::chrono::steady_clock::now() + std::chrono::minutes{ 10 },
                                     couchbase::core::io::deadline_terminal::ambiguous);
        }
        done->set(ec);
      });
    assert_true(op.has_value(), "the request is dispatched");
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the response head arrives within its budget");
  assert_success(*ec, "the response head is delivered");
  assert_true(armed->has_value() && **armed == couchbase::core::io::deadline_state::armed,
              "the deadline is armed on the unread body");

  const bool released = wait_until([&session_probe]() {
    return session_probe.expired();
  });
  assert_true(released,
              "a body dropped with a deadline armed releases its session; session " +
                alive(session_probe));
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// Regression: a streamed body held past cluster close and dropped afterwards must not post a stop
// for its stopped session. The io_context has stopped, so a posted stop would hold the session
// until the io_context is destroyed.
void
a_streamed_body_dropped_after_shutdown_releases_its_session([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto done = std::make_shared<completion<std::error_code>>();
  auto response = std::make_shared<couchbase::core::http_response>();
  run_on(c.f.io, [&]() {
    // The endpoint sends the head and part of the body; the rest never comes.
    auto op = c.component.do_http_request(
      free_form_request("/incomplete"),
      [done, response](couchbase::core::http_response resp, std::error_code ec) {
        *response = std::move(resp);
        done->set(ec);
      });
    assert_true(op.has_value(), "the request is dispatched");
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the response head arrives within its budget");
  assert_success(*ec, "the response head is delivered");

  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
  assert_false(session_probe.expired(), "the unread body holds its session through shutdown");
  response.reset();
  const bool released = wait_until([&session_probe]() {
    return session_probe.expired();
  });
  assert_true(released,
              "a body dropped after shutdown releases its session; session " +
                alive(session_probe));
}

// Dispatches a request on a session still to connect and cancels it before the connect completes.
// Returns, once the connect attempt has ended, the number of sessions listed busy or idle and the
// number of connections the endpoint accepted that the client has not closed. `op` keeps the
// operation alive past the connect.
auto
left_after_a_cancel_while_connecting(component_fixture& c,
                                     couchbase::core::http_request request,
                                     bool streamed,
                                     std::shared_ptr<couchbase::core::pending_operation>& op)
  -> std::pair<std::size_t, std::size_t>
{
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(c.f.io, [&]() {
    op = dispatch(c, request, streamed, done);
    op->cancel();
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the cancelled request completes within its budget");
  assert_eq(*ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "cancel() completes the request");
  // The connect callback holds the operation until the connect attempt ends, whether the session
  // connected or was stopped, and a request sent on the session holds it until the response ends.
  // Sampled before then, the counts below pass with the session still pending.
  assert_true(wait_until([&op]() {
                return op.use_count() == 1;
              }),
              "the connect attempt ends and nothing but the caller holds the operation");
  run_on(c.f.io, []() {
  });
  // The endpoint counts accepts and closes on its own thread. The pair the predicate last read is
  // returned, also when the wait runs out, so the caller asserts on the reading the wait judged.
  // released() is read first: a close follows its accept, so the difference cannot underflow.
  std::pair<std::size_t, std::size_t> left{};
  wait_until([&c, &left]() {
    const auto released = c.f.server.released();
    const auto accepted = c.f.server.accepted();
    left = { pooled_ids(*c.f.manager).size(), accepted - released };
    return left.first == 0 && left.second == 0;
  });
  return left;
}

// Closes the cluster, drops the fixture's reference to the manager, and returns whether the
// manager is then released.
auto
manager_released(component_fixture& c) -> bool
{
  const std::weak_ptr<http_session_manager> manager_probe = c.f.manager;
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
  c.f.manager.reset();
  const bool released = wait_until([&manager_probe]() {
    return manager_probe.expired();
  });
  return released;
}

// Regression: an operation cancelled while its checked-out session connects must not leave that
// session listed busy, or its connection open, once the connect completes. An operation the caller
// still holds must not keep the manager alive.
void
an_operation_cancelled_while_its_session_connects_leaves_no_session_busy(
  [[maybe_unused]] context& ctx)
{
  std::array<std::pair<std::size_t, std::size_t>, 2> left{};
  std::array<bool, 2> released{};
  for (const bool streamed : { false, true }) {
    component_fixture c{};
    std::shared_ptr<couchbase::core::pending_operation> op;
    left[streamed ? 1 : 0] =
      left_after_a_cancel_while_connecting(c, free_form_request("/complete"), streamed, op);
    released[streamed ? 1 : 0] = manager_released(c);
  }
  assert_true(left[0] == std::pair<std::size_t, std::size_t>{} &&
                left[1] == std::pair<std::size_t, std::size_t>{},
              "no session is left busy or connected once the connect completes; listed, open: "
              "buffered " +
                std::to_string(left[0].first) + ", " + std::to_string(left[0].second) +
                "; streamed " + std::to_string(left[1].first) + ", " +
                std::to_string(left[1].second));
  assert_true(released[0] && released[1],
              std::string{ "the held operation does not keep the manager alive; buffered: " } +
                (released[0] ? "released" : "alive") +
                ", streamed: " + (released[1] ? "released" : "alive"));
}

// Regression: an operation cancelled before its failover replacement connects must not leave the
// replacement listed busy or its connection open. An operation the caller still holds must not
// keep the manager alive. The request names a node that refuses connections, so the replacement
// connects to the live node.
void
an_operation_cancelled_before_its_failover_replacement_connects_leaves_no_session_busy(
  [[maybe_unused]] context& ctx)
{
  std::array<std::pair<std::size_t, std::size_t>, 2> left{};
  std::array<bool, 2> released{};
  for (const bool streamed : { false, true }) {
    component_fixture c{};
    auto request = free_form_request("/complete");
    request.endpoint = c.add_refusing_node();
    std::shared_ptr<couchbase::core::pending_operation> op;
    left[streamed ? 1 : 0] = left_after_a_cancel_while_connecting(c, request, streamed, op);
    released[streamed ? 1 : 0] = manager_released(c);
  }
  assert_true(left[0] == std::pair<std::size_t, std::size_t>{} &&
                left[1] == std::pair<std::size_t, std::size_t>{},
              "no replacement is left busy or connected once its connect completes; listed, "
              "open: buffered " +
                std::to_string(left[0].first) + ", " + std::to_string(left[0].second) +
                "; streamed " + std::to_string(left[1].first) + ", " +
                std::to_string(left[1].second));
  assert_true(released[0] && released[1],
              std::string{ "the held operation does not keep the manager alive; buffered: " } +
                (released[0] ? "released" : "alive") +
                ", streamed: " + (released[1] ? "released" : "alive"));
}

// A stop racing the install of a response context by write_and_stream() or
// write_and_subscribe() completes the handlers and leaves nothing alive. A context installed into
// a stopped session would hold its owner, the owner would hold the session, and nothing would end
// either. The case runs the stop before the write and after it.
void
a_stop_racing_the_response_context_install_leaves_nothing_alive([[maybe_unused]] context& ctx)
{
  struct owner {
    std::shared_ptr<http_session> session;
  };
  std::weak_ptr<http_session_manager> manager_probe;
  std::string outcome;
  bool all_released = true;
  {
    pool_fixture f{};
    manager_probe = f.manager;
    for (const bool stop_first : { true, false }) {
      for (const bool streamed : { false, true }) {
        auto o = std::make_shared<owner>(owner{ borrow_connected(f) });
        auto done = std::make_shared<completion<std::error_code>>();
        run_on(o->session->get_executor(), [&]() {
          if (stop_first) {
            o->session->stop();
          }
          // The loopback endpoint does not answer this path, so only the stop ends the request.
          auto request = make_request("/held");
          if (streamed) {
            o->session->write_and_stream(
              request,
              [o, done](std::error_code ec, couchbase::core::io::http_streaming_response) {
                done->set(ec);
              },
              [o]() {
              });
          } else {
            o->session->write_and_subscribe(
              request, [o, done](std::error_code ec, couchbase::core::io::http_response&&) {
                done->set(ec);
              });
          }
          if (!stop_first) {
            o->session->stop();
          }
        });
        const auto ec = done->wait_for(patience());
        assert_true(ec.has_value(), "the request completes within its budget");
        assert_eq(*ec,
                  std::error_code{ couchbase::errc::common::request_canceled },
                  "the stop cancels the request");
        const std::weak_ptr<owner> owner_probe = o;
        const std::weak_ptr<http_session> session_probe = o->session;
        o.reset();
        const bool released = wait_until([&]() {
          return owner_probe.expired() && session_probe.expired();
        });
        all_released = all_released && released;
        outcome += std::string{ outcome.empty() ? "" : "; " } +
                   (stop_first ? "stop first, " : "install first, ") +
                   (streamed ? "streamed: " : "buffered: ") + "owner " + alive(owner_probe) +
                   ", session " + alive(session_probe);
      }
    }
    assert_true(f.shutdown(), "the io_context drains once the manager is closed");
    f.manager.reset();
  }
  assert_true(all_released, "nothing holds the owner or the session; " + outcome);
  assert_true(manager_probe.expired(), "nothing holds the manager once the fixture releases it");
}

// Regression: send_to() on an http_command that has already completed must stop its session. The
// session is listed busy, and nothing else checks it in.
void
a_command_completed_before_send_stops_its_busy_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);
  assert_eq(pooled_ids(*f.manager).size(), std::size_t{ 1 }, "the session is listed busy");

  using command =
    couchbase::core::operations::http_command<couchbase::core::operations::http_noop_request>;
  couchbase::core::operations::http_noop_request request{};
  request.type = service;
  const auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
  const auto cmd =
    std::make_shared<command>(f.io,
                              request,
                              f.manager->tracer(),
                              couchbase::core::metrics::meter_wrapper::create(
                                std::make_shared<couchbase::core::metrics::noop_meter>(), labels),
                              std::make_shared<couchbase::core::app_telemetry_meter>(),
                              std::chrono::minutes{ 1 });
  auto done = std::make_shared<completion<std::error_code>>();
  run_on(f.io, [&]() {
    cmd->start([done](couchbase::core::operations::http_noop_response&& resp) {
      done->set(resp.ctx.ec);
    });
    cmd->set_command_session(session);
    cmd->invoke_handler(couchbase::errc::common::request_canceled, {});
  });
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the command completes within its budget");
  run_on(session->get_executor(), [&]() {
    cmd->send_to();
  });
  assert_true(session->is_stopped(), "send_to() stops the session of a completed command");
  assert_true(wait_until([&f]() {
                return pooled_ids(*f.manager).empty();
              }),
              "the stopped session leaves the busy list");
  // Fails if the command owns its session.
  const std::weak_ptr<http_session> session_probe = session;
  session.reset();
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "the command does not keep its stopped session alive");
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Regression: a deadline that fires before send_to() must not check the session in. cancel() and
// send_to() both stop that session, and a request that checked it out in between would fail.
// The completion's error context names the assigned hostname and port, and no endpoint as
// dispatched.
void
a_command_completed_before_send_does_not_pool_its_session([[maybe_unused]] context& ctx)
{
  pool_fixture f{};
  auto session = borrow_connected(f);

  using command =
    couchbase::core::operations::http_command<couchbase::core::operations::http_noop_request>;
  couchbase::core::operations::http_noop_request request{};
  request.type = service;
  const auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
  const auto cmd =
    std::make_shared<command>(f.io,
                              request,
                              f.manager->tracer(),
                              couchbase::core::metrics::meter_wrapper::create(
                                std::make_shared<couchbase::core::metrics::noop_meter>(), labels),
                              std::make_shared<couchbase::core::app_telemetry_meter>(),
                              std::chrono::minutes{ 1 });
  std::shared_ptr<http_session> offered;
  std::shared_ptr<http_session> taken;
  couchbase::core::error_context::http reported{};
  run_on(f.io, [&]() {
    // The completion http_session_manager::execute() installs, with what it checks in recorded.
    cmd->start([&f, &offered, &taken, &reported, weak = std::weak_ptr<command>{ cmd }](
                 couchbase::core::operations::http_noop_response&& resp) {
      reported = resp.ctx;
      if (const auto c = weak.lock(); c) {
        offered = c->session_for_check_in();
        f.manager->check_in(service, offered);
        // A request checking out before cancel() stops the session.
        auto [ec, next] = f.manager->check_out(service, {});
        taken = next;
      }
    });
    cmd->set_command_session(session);
    cmd->cancel(couchbase::errc::common::unambiguous_timeout);
  });
  assert_false(static_cast<bool>(offered), "a completion before send_to() checks in no session");
  assert_eq(reported.hostname,
            session->http_context().hostname,
            "a completion before dispatch reports the assigned target hostname");
  assert_eq(reported.port,
            session->http_context().port,
            "a completion before dispatch reports the assigned target port");
  assert_false(reported.last_dispatched_to.has_value(),
               "a completion before dispatch reports no dispatched endpoint");
  assert_true(static_cast<bool>(taken), "the next check_out returns a session");
  assert_ne(taken->id(), session->id(), "the completed command's session is not pooled");

  run_on(session->get_executor(), [&]() {
    cmd->send_to();
  });
  assert_true(session->is_stopped(), "the completed command's session is stopped");
  assert_false(taken->is_stopped(), "the session the next request took is not stopped");
  run_on(f.io, [&]() {
    f.manager->check_in(service, taken);
  });
  taken.reset();
  assert_true(f.shutdown(), "the io_context drains once the manager is closed");
}

// Regression: a buffered operation cancelled between its session's connect and the connect
// callback's send_to() must leave that session neither pooled nor busy. The next request must
// complete. A log callback holds the runner in that window.
void
an_operation_cancelled_between_connect_and_send_leaves_no_session_busy(
  [[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto reached = std::make_shared<completion<bool>>();
  auto release = std::make_shared<completion<bool>>();
  auto held = std::make_shared<std::atomic_bool>(false);
  couchbase::core::logger::register_log_callback(
    [reached, release, held](std::string_view msg,
                             couchbase::core::logger::level /* level */,
                             const couchbase::core::logger::log_location& /* location */) {
      if (msg.find("connected to") == std::string_view::npos || held->exchange(true)) {
        return;
      }
      reached->set(true);
      release->wait_for(patience());
    });
  // Destroyed before the fixture, so a failing assertion does not leave the runner held.
  struct gate_guard {
    explicit gate_guard(std::shared_ptr<completion<bool>> gate)
      : release{ std::move(gate) }
    {
    }
    gate_guard(const gate_guard&) = delete;
    gate_guard(gate_guard&&) = delete;
    auto operator=(const gate_guard&) -> gate_guard& = delete;
    auto operator=(gate_guard&&) -> gate_guard& = delete;
    ~gate_guard()
    {
      release->set(true);
      couchbase::core::logger::unregister_log_callback();
    }
    std::shared_ptr<completion<bool>> release;
  } guard{ release };

  auto first = std::make_shared<completion<std::error_code>>();
  std::shared_ptr<couchbase::core::pending_operation> op;
  run_on(c.f.io, [&]() {
    op = dispatch(c, free_form_request("/complete"), false, first);
  });
  assert_true(reached->wait_for(patience()).has_value(), "the checked-out session connects");
  // The runner is held: the connect callback, and send_to() with it, has not run.
  op->cancel();
  const auto first_ec = first->wait_for(patience());
  assert_true(first_ec.has_value(), "the cancelled request completes within its budget");
  assert_eq(*first_ec,
            std::error_code{ couchbase::errc::common::request_canceled },
            "cancel() completes the request");

  auto second = std::make_shared<completion<std::error_code>>();
  auto next = dispatch(c, free_form_request("/complete"), false, second);
  release->set(true);
  const auto second_ec = second->wait_for(patience());
  assert_true(second_ec.has_value(), "the next request completes within its budget");
  assert_success(*second_ec, "the next request succeeds");
  assert_true(wait_until([&c]() {
                return pooled_ids(*c.f.manager).size() == 1;
              }),
              "only the next request's session stays pooled");
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// The request a wrapper sends; the loopback endpoint answers it as the path `id` names.
auto
query_for(std::string id) -> couchbase::core::operations::query_request
{
  couchbase::core::operations::query_request request{};
  request.statement = "SELECT 1";
  request.client_context_id = std::move(id);
  return request;
}

auto
analytics_for(std::string id) -> couchbase::core::operations::analytics_request
{
  couchbase::core::operations::analytics_request request{};
  request.statement = "SELECT 1";
  request.client_context_id = std::move(id);
  return request;
}

// A request timeout longer than a case's waits. The stream's idle timer takes the request timeout,
// so it cannot release the session while a case waits.
constexpr std::chrono::milliseconds unreachable_timeout = std::chrono::minutes{ 10 };

// What a stream handler was given. `handle` is the only reference the case keeps to the stream;
// resetting the optional drops it.
template<typename Stream>
struct opened_stream {
  std::shared_ptr<completion<std::error_code>> done{
    std::make_shared<completion<std::error_code>>()
  };
  std::shared_ptr<std::optional<Stream>> handle{ std::make_shared<std::optional<Stream>>() };
  std::shared_ptr<std::atomic<int>> calls{ std::make_shared<std::atomic<int>>(0) };
};

// Opens a stream from the calling thread, as a wrapper does, through cluster::query_stream() or
// cluster::analytics_query_stream(). With `streaming`, it builds the stream component as those
// functions do, with `*streaming` as its options and unreachable_timeout as its default timeout.
// A cluster that is never opened offers no way to set cluster_options::streaming.
template<typename Stream, typename Request>
auto
open_stream(component_fixture& c,
            Request request,
            std::optional<couchbase::core::row_streamer_options> streaming = {})
  -> opened_stream<Stream>
{
  opened_stream<Stream> opened{};
  auto handler = [done = opened.done, handle = opened.handle, calls = opened.calls](
                   Stream stream, auto error_ctx) {
    ++*calls;
    *handle = std::move(stream);
    done->set(error_ctx.ec);
  };
  constexpr bool is_query = std::is_same_v<Request, couchbase::core::operations::query_request>;
  if (streaming) {
    using component_type = std::conditional_t<is_query,
                                              couchbase::core::query_stream_component,
                                              couchbase::core::analytics_stream_component>;
    const component_type component{
      c.f.io,
      couchbase::core::http_component{ c.f.io, couchbase::core::core_sdk_shim{ c.cluster } },
      unreachable_timeout,
      *streaming,
    };
    component.execute(std::move(request), std::move(handler));
  } else if constexpr (is_query) {
    c.cluster.query_stream(std::move(request), std::move(handler));
  } else {
    c.cluster.analytics_query_stream(std::move(request), std::move(handler));
  }
  return opened;
}

// Opens a stream and asserts that its handler hands out a live stream.
template<typename Stream, typename Request>
auto
open_live_stream(component_fixture& c,
                 Request request,
                 std::optional<couchbase::core::row_streamer_options> streaming = {})
  -> opened_stream<Stream>
{
  auto opened = open_stream<Stream>(c, std::move(request), streaming);
  const auto ec = opened.done->wait_for(patience());
  assert_true(ec.has_value(), "the stream handler runs within its budget");
  assert_success(*ec, "the stream opens");
  assert_true(opened.handle->has_value(), "the stream handler is given a stream");
  return opened;
}

using pulled = std::pair<std::optional<std::string>, std::error_code>;

// One next_row() from the calling thread; empty if it does not complete within patience().
template<typename Stream>
auto
pull(Stream& stream) -> std::optional<pulled>
{
  auto done = std::make_shared<completion<pulled>>();
  stream.next_row([done](std::optional<std::string> row, std::error_code ec) {
    done->set({ std::move(row), ec });
  });
  return done->wait_for(patience());
}

// A buffered query through cluster::execute(), as a wrapper sends one.
auto
execute_query(component_fixture& c, std::string id) -> std::optional<std::error_code>
{
  auto done = std::make_shared<completion<std::error_code>>();
  c.cluster.execute(query_for(std::move(id)),
                    [done](couchbase::core::operations::query_response&& resp) {
                      done->set(resp.ctx.ec);
                    });
  return done->wait_for(patience());
}

// A query stream read to its end checks its session in. The handle the consumer still holds does
// not keep that session alive once the pool evicts it.
void
a_query_stream_read_to_the_end_returns_its_session_to_the_pool([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  const auto session_id = parked->id();
  parked.reset();

  auto opened = open_live_stream<couchbase::core::query_stream>(c, query_for("/rows"));
  std::size_t rows = 0;
  std::optional<pulled> last;
  while ((last = pull(**opened.handle)).has_value() && last->first.has_value()) {
    ++rows;
  }
  assert_true(last.has_value(), "every pull completes within its budget");
  assert_success(last->second, "the stream ends cleanly");
  assert_eq(rows, query_rows, "every row is delivered");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the stream takes the parked session");
  assert_true(wait_until([&]() {
                return pooled_ids(*c.f.manager).count(session_id) == 1;
              }),
              "the ended stream checks its session in");

  const auto next = execute_query(c, "/rows");
  assert_true(next.has_value(), "the next query completes within its budget");
  assert_success(*next, "the next query succeeds");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the next query reuses the connection");

  run_on(c.f.io, [&]() {
    c.f.manager->update_config({});
  });
  assert_true(wait_until([&c]() {
                return pooled_ids(*c.f.manager).empty();
              }),
              "the pool evicts the session once its node leaves the configuration");
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "the finished stream handle does not hold its session; session " +
                alive(session_probe));
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// Where a case drops the stream's last handle.
enum class drop_point : std::uint8_t {
  // After one row, from the calling thread.
  after_one_row,
  // Inside the handler of the pull that delivered the first row, a pull issued on the io thread.
  inside_the_pull_handler,
  // After the error terminal, from the calling thread.
  after_the_error_terminal,
};

// Opens a stream, drops its last handle at `point` with the rest of the body still to come, and
// returns whether the session was then released and its connection closed. Asserts that the next
// query opens a new connection and that nothing holds the manager.
template<typename Stream, typename Request>
auto
released_after_a_drop(Request request,
                      couchbase::core::service_type type,
                      const std::string& label,
                      drop_point point,
                      std::optional<couchbase::core::row_streamer_options> streaming = {}) -> bool
{
  component_fixture c{};
  auto parked = park_idle(c.f, type);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  request.timeout = unreachable_timeout;
  auto opened = open_live_stream<Stream>(c, std::move(request), streaming);
  if (point == drop_point::inside_the_pull_handler) {
    auto first = std::make_shared<completion<bool>>();
    // Issued on the io thread, so the handler's reset cannot run while next_row() is still
    // returning on this one.
    run_on(c.f.io, [&]() {
      (**opened.handle)
        .next_row([handle = opened.handle, first](std::optional<std::string> row, std::error_code) {
          handle->reset();
          first->set(row.has_value());
        });
    });
    const auto had_row = first->wait_for(patience());
    assert_true(had_row.value_or(false), label + ": the first row arrives");
  } else if (point == drop_point::after_one_row) {
    const auto first = pull(**opened.handle);
    assert_true(first.has_value() && first->first.has_value(), label + ": the first row arrives");
    opened.handle->reset();
  } else {
    std::optional<pulled> last;
    while ((last = pull(**opened.handle)).has_value() && last->first.has_value()) {
    }
    assert_true(last.has_value(), label + ": every pull completes within its budget");
    assert_eq(last->second,
              std::error_code{ couchbase::errc::common::parsing_failure },
              label + ": the stream ends with the lexer's error");
    opened.handle->reset();
  }
  assert_eq(
    c.f.server.accepted(), std::size_t{ 1 }, label + ": the stream takes the parked session");

  const bool released = wait_until([&]() {
    return session_probe.expired() && c.f.server.released() == 1;
  });
  // Listed busy while it is still alive, so only a released session is checked against the pool.
  assert_true(!released || pooled_ids(*c.f.manager).empty(),
              label + ": the stopped session is not pooled");
  const auto next = execute_query(c, "/rows");
  assert_true(next.has_value(), label + ": the next query completes within its budget");
  assert_success(*next, label + ": the next query succeeds");
  assert_eq(c.f.server.accepted(), std::size_t{ 2 }, label + ": the next query connects anew");
  assert_true(manager_released(c), label + ": nothing holds the manager once the cluster closes");
  return released;
}

// Asserts that a drop at `point` releases the session for both query and analytics streams.
void
assert_a_drop_releases_the_session(
  const std::string& path,
  drop_point point,
  std::optional<couchbase::core::row_streamer_options> streaming = {})
{
  const bool query = released_after_a_drop<couchbase::core::query_stream>(
    query_for(path), couchbase::core::service_type::query, "query", point, streaming);
  const bool analytics = released_after_a_drop<couchbase::core::analytics_stream>(
    analytics_for(path), couchbase::core::service_type::analytics, "analytics", point, streaming);
  assert_true(query && analytics,
              std::string{ "a dropped stream releases its session and connection; query: " } +
                (query ? "released" : "alive") +
                ", analytics: " + (analytics ? "released" : "alive"));
}

// A stream dropped mid-body, as a wrapper's `break` or exception leaves it, stops its session and
// closes its connection. Neither cluster close nor the stream's idle timer is needed for that.
void
a_stream_dropped_after_one_row_stops_its_session([[maybe_unused]] context& ctx)
{
  assert_a_drop_releases_the_session("/one-row", drop_point::after_one_row);
}

// The last handle dropped inside its own pull's handler, on the io thread, stops the session. The
// runner stays free to serve the next query.
void
a_stream_dropped_inside_its_pull_handler_stops_its_session([[maybe_unused]] context& ctx)
{
  assert_a_drop_releases_the_session("/one-row", drop_point::inside_the_pull_handler);
}

// A stream dropped after a lexer error on malformed JSON stops its session. Without the stop, the
// read-ahead goes on reading the rest of the announced body, which never comes.
void
a_stream_dropped_after_its_error_terminal_stops_its_session([[maybe_unused]] context& ctx)
{
  assert_a_drop_releases_the_session("/malformed", drop_point::after_the_error_terminal);
}

// A stream dropped while its read-ahead is paused at the high-water mark stops its session. The
// endpoint writes the preamble and every row in one write, so they arrive in the read that opens
// the stream. A zero high-water mark allows no further read while a row is buffered. A one-row
// channel holds one row and leaves the rest in queued sends.
void
a_stream_dropped_while_its_read_ahead_is_paused_stops_its_session([[maybe_unused]] context& ctx)
{
  couchbase::core::row_streamer_options streaming{};
  streaming.high_water_bytes = 0;
  streaming.low_water_bytes = 0;
  streaming.row_buffer_size = 1;
  assert_a_drop_releases_the_session("/many-rows", drop_point::after_one_row, streaming);
}

// Whether session `id` is idle in the pool: a check_out on the io thread takes it, and it is
// checked straight back in. With no idle session the check_out creates one that never connects, and
// the check_in drops it.
auto
is_idle(component_fixture& c, couchbase::core::service_type type, const std::string& id) -> bool
{
  bool idle = false;
  run_on(c.f.io, [&]() {
    auto [ec, session] = c.f.manager->check_out(type, {});
    if (ec || !session) {
      return;
    }
    idle = session->is_connected() && session->id() == id;
    c.f.manager->check_in(type, session);
  });
  return idle;
}

// Reads a stream to its clean end, drops the handle while bytes after the JSON document are still
// to come, then lets the endpoint send them. Returns whether the session was then checked in. If
// it was, asserts that the next buffered request to the same service reuses its connection.
template<typename Stream, typename Request>
auto
checked_in_after_a_drop_past_the_document(Request request,
                                          couchbase::core::service_type type,
                                          const std::string& label) -> bool
{
  component_fixture c{};
  auto parked = park_idle(c.f, type);
  const auto session_id = parked->id();
  parked.reset();

  request.timeout = unreachable_timeout;
  auto next_request = request;
  next_request.client_context_id = "/rows";
  auto opened = open_live_stream<Stream>(c, std::move(request));
  std::optional<pulled> last;
  while ((last = pull(**opened.handle)).has_value() && last->first.has_value()) {
  }
  assert_true(last.has_value(), label + ": every pull completes within its budget");
  assert_success(last->second, label + ": the stream ends cleanly");
  opened.handle->reset();
  assert_eq(c.f.server.held(), std::size_t{ 1 }, label + ": the endpoint holds the last byte");
  c.f.server.release_held();

  const bool checked_in = wait_until([&]() {
    return is_idle(c, type, session_id);
  });
  if (checked_in) {
    auto done = std::make_shared<completion<std::error_code>>();
    c.cluster.execute(std::move(next_request), [done](auto&& resp) {
      done->set(resp.ctx.ec);
    });
    const auto next = done->wait_for(patience());
    assert_true(next.has_value(), label + ": the next query completes within its budget");
    assert_success(*next, label + ": the next query succeeds");
    assert_eq(
      c.f.server.accepted(), std::size_t{ 1 }, label + ": the next query reuses the connection");
  }
  assert_true(c.shutdown(), label + ": the io_context drains once the cluster is closed");
  return checked_in;
}

// A stream dropped after its clean terminal, with bytes after the JSON document still to come, is
// not cancelled: its read-ahead reads them and checks the connection in for the next query.
void
a_stream_dropped_after_its_document_checks_its_session_in([[maybe_unused]] context& ctx)
{
  const bool query = checked_in_after_a_drop_past_the_document<couchbase::core::query_stream>(
    query_for("/trailing"), couchbase::core::service_type::query, "query");
  const bool analytics =
    checked_in_after_a_drop_past_the_document<couchbase::core::analytics_stream>(
      analytics_for("/trailing"), couchbase::core::service_type::analytics, "analytics");
  assert_true(query && analytics,
              std::string{ "a stream dropped past its document checks its session in; query: " } +
                (query ? "checked in" : "not checked in") +
                ", analytics: " + (analytics ? "checked in" : "not checked in"));
}

// A cancel() from another thread completes every pull exactly once. It races a parked pull while a
// second runner delivers the rest of the body. The parked pull gets a row or the cancellation, and
// the pull after a row gets the cancellation. Once the cluster closes, nothing holds the session.
void
a_cancel_from_another_thread_completes_a_parked_pull_once([[maybe_unused]] context& ctx)
{
  component_fixture c{ 2 };
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto opened = open_live_stream<couchbase::core::query_stream>(c, query_for("/one-row"));
  auto& stream = **opened.handle;
  const auto first = pull(stream);
  assert_true(first.has_value() && first->first.has_value(), "the first row arrives");

  auto calls = std::make_shared<std::atomic<int>>(0);
  int pulls = 0;
  auto next = [&]() {
    ++pulls;
    auto done = std::make_shared<completion<pulled>>();
    stream.next_row([calls, done](std::optional<std::string> row, std::error_code ec) {
      ++*calls;
      done->set({ std::move(row), ec });
    });
    return done;
  };
  auto parked_pull = next();
  c.f.server.release_held();
  stream.cancel();
  auto last = parked_pull->wait_for(patience());
  if (last.has_value() && last->first.has_value()) {
    last = next()->wait_for(patience());
  }
  assert_true(last.has_value(), "every pull completes within its budget");
  assert_false(last->first.has_value(), "the pull after a row delivers none");
  assert_eq(last->second,
            std::error_code{ couchbase::errc::common::request_canceled },
            "the stream ends with the cancellation");

  opened.handle->reset();
  assert_true(manager_released(c), "nothing holds the manager once the cluster closes");
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "nothing holds the stream's session; session " + alive(session_probe));
  assert_eq(calls->load(), pulls, "every pull completes exactly once");
}

// A stream handle held across cluster close reports an error to the next pull. Freed after the
// io_context has stopped, as a wrapper's garbage collector frees it, it releases its session and
// the manager and queues nothing. A queued handler would hold the streamer and its body until the
// io_context is destroyed.
void
a_stream_handle_freed_after_cluster_close_releases_its_session([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto opened = open_live_stream<couchbase::core::query_stream>(c, query_for("/one-row"));
  const auto first = pull(**opened.handle);
  assert_true(first.has_value() && first->first.has_value(), "the first row arrives");

  assert_true(c.close_cluster(), "the cluster's close completes");
  const auto after = pull(**opened.handle);
  assert_true(after.has_value(), "a pull after the close completes within its budget");
  assert_false(after->first.has_value(), "a pull after the close delivers no row");
  assert_true(static_cast<bool>(after->second),
              "a pull after the close reports an error, got " + after->second.message());

  const std::weak_ptr<http_session_manager> manager_probe = c.f.manager;
  assert_true(c.f.shutdown(), "the io_context drains with the stream handle still held");
  c.f.manager.reset();
  c.f.io.restart();
  assert_eq(c.f.io.poll(), std::size_t{ 0 }, "nothing is queued before the handle is freed");
  opened.handle->reset();
  assert_true(wait_until([&]() {
                return session_probe.expired() && manager_probe.expired();
              }),
              "the handle freed after close releases the session and the manager; session " +
                alive(session_probe) + ", manager " + alive(manager_probe));
  c.f.io.restart();
  const auto queued = c.f.io.poll();
  assert_eq(queued, std::size_t{ 0 }, "the handle freed after close queues nothing");
}

// A buffered query pending when the cluster closes completes its handler exactly once, with an
// error, and does not keep its session or the manager alive.
void
a_buffered_query_pending_at_cluster_close_completes_once([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto calls = std::make_shared<std::atomic<int>>(0);
  auto done = std::make_shared<completion<std::error_code>>();
  c.cluster.execute(query_for("/held/rows"),
                    [calls, done](couchbase::core::operations::query_response&& resp) {
                      ++*calls;
                      done->set(resp.ctx.ec);
                    });
  assert_true(wait_until([&c]() {
                return c.f.server.held() == 1;
              }),
              "the query reaches the endpoint");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the query takes the parked session");

  assert_true(c.close_cluster(), "the cluster's close completes");
  const auto ec = done->wait_for(patience());
  assert_true(ec.has_value(), "the pending query completes within its budget");
  assert_true(static_cast<bool>(*ec), "the pending query reports an error, got " + ec->message());

  c.f.server.release_held();
  assert_true(manager_released(c), "nothing holds the manager once the cluster closes");
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "nothing holds the query's session; session " + alive(session_probe));
  assert_eq(calls->load(), 1, "the query handler runs exactly once");
}

// The error context of a timed-out query names the endpoint the query was dispatched on. Nothing
// holds the session afterwards.
void
a_timed_out_query_reports_its_endpoint_after_the_session_is_released([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  const auto local = parked->local_address();
  const auto remote = parked->remote_address();
  parked.reset();

  auto done = std::make_shared<completion<couchbase::core::error_context::query>>();
  run_on(c.f.io, [&]() {
    auto request = query_for("/unanswered");
    request.timeout = unanswered_request_timeout;
    c.cluster.execute(std::move(request),
                      [done](couchbase::core::operations::query_response&& resp) {
                        done->set(resp.ctx);
                      });
  });
  const auto error_ctx = done->wait_for(patience());
  assert_true(error_ctx.has_value(), "the query completes within its budget");
  assert_eq(error_ctx->ec,
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "the deadline ends the query");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the query takes the parked session");
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "nothing holds the timed-out query's session; session " + alive(session_probe));

  assert_eq(error_ctx->last_dispatched_to.value_or(""), remote, "last_dispatched_to");
  assert_eq(error_ctx->last_dispatched_from.value_or(""), local, "last_dispatched_from");
  assert_eq(error_ctx->hostname, std::string{ "127.0.0.1" }, "hostname");
  assert_eq(error_ctx->port, c.f.server.port(), "port");
  assert_true(c.shutdown(), "the io_context drains once the cluster is closed");
}

// Regression: a timed-out free-form operation must report the endpoint it was dispatched on after
// its session is released.
void
a_timed_out_free_form_operation_reports_its_endpoint_after_the_session_is_released(
  [[maybe_unused]] context& ctx)
{
  std::string outcome;
  std::string expected;
  bool all_reported = true;
  for (const bool streamed : { false, true }) {
    const std::string label = streamed ? "streamed: " : "buffered: ";
    component_fixture c{};
    auto parked = park_idle(c.f);
    const std::weak_ptr<http_session> session_probe = parked;
    const auto local = parked->local_address();
    const auto remote = parked->remote_address();
    parked.reset();

    auto done = std::make_shared<completion<std::error_code>>();
    std::shared_ptr<couchbase::core::pending_operation> op;
    run_on(c.f.io, [&]() {
      op = dispatch(c, free_form("/unanswered", unanswered_request_timeout), streamed, done);
    });
    const auto ec = done->wait_for(patience());
    assert_true(ec.has_value(), label + "the request completes within its budget");
    assert_eq(*ec,
              std::error_code{ couchbase::errc::common::ambiguous_timeout },
              label + "the deadline ends the request");
    assert_eq(
      c.f.server.accepted(), std::size_t{ 1 }, label + "the request takes the parked session");
    assert_true(wait_until([&session_probe]() {
                  return session_probe.expired();
                }),
                label + "nothing holds the timed-out request's session; session " +
                  alive(session_probe));

    const auto info =
      std::dynamic_pointer_cast<couchbase::core::pending_operation_connection_info>(op);
    assert_true(info != nullptr, label + "the operation reports its connection");
    const auto host = "127.0.0.1:" + std::to_string(c.f.server.port());
    const bool reported = info->dispatched_to() == remote && info->dispatched_from() == local &&
                          info->dispatched_to_host() == host;
    all_reported = all_reported && reported;
    const auto describe_endpoint =
      [&label](const std::string& to, const std::string& from, const std::string& to_host) {
        return label + "to=\"" + to + "\" from=\"" + from + "\" to_host=\"" + to_host + "\"; ";
      };
    outcome +=
      describe_endpoint(info->dispatched_to(), info->dispatched_from(), info->dispatched_to_host());
    expected += describe_endpoint(remote, local, host);
    assert_true(c.shutdown(), label + "the io_context drains once the cluster is closed");
  }
  assert_true(all_reported,
              "the operation reports the endpoint it was dispatched on; got " + outcome +
                "expected " + expected);
}

// A cluster close landing before the stream's preamble completes the stream handler exactly once,
// with an error, and leaves nothing alive.
void
a_cluster_close_before_the_preamble_completes_the_stream_handler_once([[maybe_unused]] context& ctx)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto opened = open_stream<couchbase::core::query_stream>(c, query_for("/held/rows"));
  assert_true(wait_until([&c]() {
                return c.f.server.held() == 1;
              }),
              "the query reaches the endpoint");
  assert_eq(c.f.server.accepted(), std::size_t{ 1 }, "the query takes the parked session");

  assert_true(c.close_cluster(), "the cluster's close completes");
  const auto ec = opened.done->wait_for(patience());
  assert_true(ec.has_value(), "the stream handler runs within its budget");
  assert_true(static_cast<bool>(*ec), "the stream handler reports an error, got " + ec->message());

  c.f.server.release_held();
  assert_true(manager_released(c), "nothing holds the manager once the cluster closes");
  opened.handle->reset();
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "nothing holds the stream's session; session " + alive(session_probe));
  assert_eq(opened.calls->load(), 1, "the stream handler runs exactly once");
}

// Pulls until the stream reports its end or an error, counting the ends reported.
template<typename Stream>
void
drain_rows(std::shared_ptr<std::optional<Stream>> handle,
           std::shared_ptr<std::atomic<int>> ends,
           std::shared_ptr<completion<std::error_code>> terminal)
{
  (*handle)->next_row(
    [handle, ends, terminal](std::optional<std::string> row, std::error_code ec) mutable {
      if (ec || !row.has_value()) {
        ++*ends;
        return terminal->set(ec);
      }
      drain_rows(std::move(handle), std::move(ends), std::move(terminal));
    });
}

// Releases the rest of a "/one-row" body either before the cluster closes, with the clean end
// delivered first, or after the close completes. Asserts that the closed manager pools no session,
// that nothing stays alive, and that the stream reports its end once.
void
assert_a_tail_around_cluster_close_leaves_nothing_pooled(bool tail_before_close)
{
  component_fixture c{};
  auto parked = park_idle(c.f, couchbase::core::service_type::query);
  const std::weak_ptr<http_session> session_probe = parked;
  parked.reset();

  auto opened = open_live_stream<couchbase::core::query_stream>(c, query_for("/one-row"));
  const auto first = pull(**opened.handle);
  assert_true(first.has_value() && first->first.has_value(), "the first row arrives");

  auto ends = std::make_shared<std::atomic<int>>(0);
  auto terminal = std::make_shared<completion<std::error_code>>();
  drain_rows(opened.handle, ends, terminal);
  if (tail_before_close) {
    c.f.server.release_held();
    const auto ec = terminal->wait_for(patience());
    assert_true(ec.has_value(), "the stream ends within its budget");
    assert_success(*ec, "the stream ends cleanly before the close");
    assert_true(c.close_cluster(), "the cluster's close completes");
  } else {
    assert_true(c.close_cluster(), "the cluster's close completes");
    c.f.server.release_held();
    assert_true(terminal->wait_for(patience()).has_value(), "the stream ends within its budget");
  }
  assert_true(pooled_ids(*c.f.manager).empty(), "no session is pooled in the closed manager");

  opened.handle->reset();
  assert_true(manager_released(c), "nothing holds the manager once the cluster closes");
  assert_true(wait_until([&session_probe]() {
                return session_probe.expired();
              }),
              "nothing holds the stream's session; session " + alive(session_probe));
  assert_eq(ends->load(), 1, "the stream reports its end exactly once");
}

// A stream that ended cleanly before cluster close leaves no session pooled once the close
// completes.
void
a_stream_tail_delivered_before_cluster_close_leaves_nothing_pooled([[maybe_unused]] context& ctx)
{
  assert_a_tail_around_cluster_close_leaves_nothing_pooled(true);
}

// The rest of a stream's body, arriving after cluster close, does not check the session into the
// closed manager.
void
a_stream_tail_arriving_after_cluster_close_does_not_pool_the_session([[maybe_unused]] context& ctx)
{
  assert_a_tail_around_cluster_close_leaves_nothing_pooled(false);
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      // timeout::slow: every case bounds its own waits, pool_fixture's teardown included, and the
      // budget has to outlast those bounds for a stall to be reported as a failure rather than a
      // timeout.
      { CASE(check_in_refuses_a_session_whose_stop_already_ran), {}, timeout::slow },
      { CASE(a_manager_serves_new_requests_after_close), {}, timeout::slow },
      { CASE(execute_after_close_sends_on_a_new_connection), {}, timeout::slow },
      { CASE(close_stops_a_busy_session_on_its_strand), {}, timeout::slow },
      { CASE(close_completes_after_every_session_it_stops), {}, timeout::slow },
      { CASE(close_completes_after_the_reads_a_stop_aborts), {}, timeout::slow },
      { CASE(every_reads_drained_handler_runs), {}, timeout::slow },
      { CASE(reads_drained_waits_for_the_reads_queued_past_a_clean_end), {}, timeout::slow },
      { CASE(a_connect_failing_after_close_opens_no_replacement), {}, timeout::slow },
      { CASE(check_in_of_a_session_from_before_close_stops_it), {}, timeout::slow },
      { CASE(a_stop_after_publication_removes_the_session_from_the_pool), {}, timeout::slow },
      { CASE(check_out_does_not_hand_out_a_session_claimed_while_idle), {}, timeout::slow },
      { CASE(check_out_skips_an_idle_session_whose_stop_has_begun), {}, timeout::slow },
      { CASE(a_connection_close_response_is_not_pooled), {}, timeout::slow },
      { CASE(a_stop_inside_a_streaming_response_handler_still_ends_the_stream), {}, timeout::slow },
      { CASE(a_stop_from_a_handler_the_teardown_runs_returns_at_once), {}, timeout::slow },
      { CASE(a_throwing_connect_callback_does_not_skip_the_teardown), {}, timeout::slow },
      { CASE(a_throwing_response_handler_does_not_skip_on_stop), {}, timeout::slow },
      { CASE(a_throwing_reader_on_a_read_error_still_stops_the_session), {}, timeout::slow },
      { CASE(overlapping_pulls_deliver_each_byte_once_in_order), {}, timeout::slow },
      { CASE(a_reused_connection_streams_every_body_whole), {}, timeout::slow },
      { CASE(a_request_reusing_the_connection_does_not_drain_with_the_previous_response),
        {},
        timeout::slow },
      { CASE(a_connect_after_the_teardown_runs_its_callback), {}, timeout::slow },
      { CASE(a_close_while_connecting_cancels_the_pending_operation), {}, timeout::slow },
      { CASE(a_close_while_connecting_cancels_the_request), {}, timeout::slow },
      { CASE(set_idle_arms_no_timer_on_a_stopped_session), {}, timeout::slow },
      { CASE(an_encode_failure_on_a_fresh_session_completes_and_checks_in), {}, timeout::slow },
      { CASE(every_claim_order_keeps_the_claimed_session_out_of_the_pool), {}, timeout::slow },
      { CASE(concurrent_stops_run_the_on_stop_handler_once), {}, timeout::slow },
      { CASE(concurrent_borrowers_never_share_or_inherit_a_dead_session), {}, timeout::slow },
      { CASE(a_stop_racing_check_out_leaves_no_stopped_session_in_the_pool), {}, timeout::slow },
      { CASE(a_pull_off_the_strand_racing_a_close_under_thread_sanitizer), {}, timeout::slow },
      { CASE(a_deadline_racing_the_tail_either_stops_or_pools_the_session), {}, timeout::slow },
      { CASE(a_streamed_request_checks_in_its_failover_replacement), {}, timeout::slow },
      { CASE(a_buffered_request_checks_in_its_failover_replacement), {}, timeout::slow },
      { CASE(a_command_deadline_queued_behind_its_response_leaves_the_session_pooled),
        { std::make_shared<const needs_linux_completion_ordering>() },
        timeout::slow },
      { CASE(a_buffered_request_deadline_queued_behind_its_response_leaves_the_session_pooled),
        { std::make_shared<const needs_linux_completion_ordering>() },
        timeout::slow },
      { CASE(a_streamed_request_deadline_queued_behind_its_response_leaves_the_stream_open),
        { std::make_shared<const needs_linux_completion_ordering>() },
        timeout::slow },
      { CASE(a_command_deadline_before_its_response_times_out_and_stops_the_session),
        {},
        timeout::slow },
      { CASE(a_buffered_request_deadline_before_its_response_times_out_and_stops_the_session),
        {},
        timeout::slow },
      { CASE(a_streamed_request_deadline_before_its_response_times_out_and_stops_the_session),
        {},
        timeout::slow },
      { CASE(a_completed_buffered_operation_does_not_hold_its_evicted_session), {}, timeout::slow },
      { CASE(an_operation_cancelled_by_its_session_stop_does_not_hold_the_session),
        {},
        timeout::slow },
      { CASE(a_streamed_body_dropped_unread_releases_its_operation_and_session),
        {},
        timeout::slow },
      { CASE(a_streamed_body_dropped_after_shutdown_releases_its_session), {}, timeout::slow },
      { CASE(a_streamed_body_dropped_with_a_deadline_armed_releases_its_session),
        {},
        timeout::slow },
      { CASE(an_operation_cancelled_while_its_session_connects_leaves_no_session_busy),
        {},
        timeout::slow },
      { CASE(
          an_operation_cancelled_before_its_failover_replacement_connects_leaves_no_session_busy),
        {},
        timeout::slow },
      { CASE(a_stop_racing_the_response_context_install_leaves_nothing_alive), {}, timeout::slow },
      { CASE(a_command_completed_before_send_stops_its_busy_session), {}, timeout::slow },
      { CASE(a_command_completed_before_send_does_not_pool_its_session), {}, timeout::slow },
      { CASE(an_operation_cancelled_between_connect_and_send_leaves_no_session_busy),
        {},
        timeout::slow },
      { CASE(a_query_stream_read_to_the_end_returns_its_session_to_the_pool), {}, timeout::slow },
      { CASE(a_stream_dropped_after_one_row_stops_its_session), {}, timeout::slow },
      { CASE(a_stream_dropped_inside_its_pull_handler_stops_its_session), {}, timeout::slow },
      { CASE(a_stream_dropped_after_its_error_terminal_stops_its_session), {}, timeout::slow },
      { CASE(a_stream_dropped_while_its_read_ahead_is_paused_stops_its_session),
        {},
        timeout::slow },
      { CASE(a_stream_dropped_after_its_document_checks_its_session_in), {}, timeout::slow },
      { CASE(a_cancel_from_another_thread_completes_a_parked_pull_once), {}, timeout::slow },
      { CASE(a_stream_handle_freed_after_cluster_close_releases_its_session), {}, timeout::slow },
      { CASE(a_buffered_query_pending_at_cluster_close_completes_once), {}, timeout::slow },
      { CASE(a_timed_out_query_reports_its_endpoint_after_the_session_is_released),
        {},
        timeout::slow },
      { CASE(a_timed_out_free_form_operation_reports_its_endpoint_after_the_session_is_released),
        {},
        timeout::slow },
      { CASE(a_cluster_close_before_the_preamble_completes_the_stream_handler_once),
        {},
        timeout::slow },
      { CASE(a_stream_tail_delivered_before_cluster_close_leaves_nothing_pooled),
        {},
        timeout::slow },
      { CASE(a_stream_tail_arriving_after_cluster_close_does_not_pool_the_session),
        {},
        timeout::slow },
    },
  };
}

} // namespace couchbase::test
