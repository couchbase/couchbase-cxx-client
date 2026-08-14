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

// Transport-level retry for KV couchbase2 operations (CXXCBC-896), specifically the property that a
// retried operation stays inside its overall time budget. Drives a real core::cluster against a
// localhost KvService so the whole path -- routing, retry decision, backoff, re-dispatch and the
// per-attempt deadline -- is exercised together; the budget is a property of their interaction and
// is not observable from any one of them. Env-agnostic: local server, no external gateway.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/test_runner.hxx"

#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/document_id.hxx"
#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/best_effort_retry_strategy.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::kv::v1;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::document_id;
using ::couchbase::core::origin;
using ::couchbase::core::utils::parse_connection_string;
using namespace std::chrono_literals;

// Answers UNAVAILABLE (retryable: service_not_available) immediately for the first two dispatches,
// then stops answering. The pause is what exposes the budget: by the time the third attempt is
// issued most of the operation's time is already spent on backoff, so the deadline that attempt
// receives is the whole question.
class stalling_kv_service final : public v1::KvService::Service
{
public:
  std::atomic<int> calls{ 0 };

  auto Get(grpc::ServerContext* context,
           const v1::GetRequest* /* request */,
           v1::GetResponse* /* response */) -> grpc::Status override
  {
    if (++calls <= 2) {
      return { grpc::StatusCode::UNAVAILABLE, "try again" };
    }
    // Outlasts any budget this test grants, so the client's deadline is what ends the call -- but
    // polled rather than slept through, so the handler returns once the client gives up instead of
    // running on into the suite's teardown and holding the server past Shutdown().
    const auto give_up_by = std::chrono::steady_clock::now() + 5s;
    while (!context->IsCancelled() && std::chrono::steady_clock::now() < give_up_by) {
      std::this_thread::sleep_for(5ms);
    }
    return { grpc::StatusCode::UNAVAILABLE, "still unavailable" };
  }
};

// Never answers anything but UNAVAILABLE, and answers it immediately. The retry strategy always
// permits a retry for service_not_available, so nothing here ever ends the loop from below: what
// ends it is the operation's own deadline.
class unavailable_kv_service final : public v1::KvService::Service
{
public:
  std::atomic<int> calls{ 0 };

  auto Get(grpc::ServerContext* /* context */,
           const v1::GetRequest* /* request */,
           v1::GetResponse* /* response */) -> grpc::Status override
  {
    ++calls;
    return { grpc::StatusCode::UNAVAILABLE, "never available" };
  }

  // An unimplemented RPC answers UNIMPLEMENTED, which is terminal rather than retryable, so a
  // mutation against a Get-only service would end on its first attempt without ever entering the
  // loop this file exists to test -- and the dispatch counter, incremented only in Get, would sit
  // at zero.
  auto Upsert(grpc::ServerContext* /* context */,
              const v1::UpsertRequest* /* request */,
              v1::UpsertResponse* /* response */) -> grpc::Status override
  {
    ++calls;
    return { grpc::StatusCode::UNAVAILABLE, "never available" };
  }
};

// Fails a fixed number of dispatches with UNAVAILABLE and then serves the document, so the number
// of retries the operation performs is decided by the service rather than by timing.
class flaky_kv_service final : public v1::KvService::Service
{
public:
  explicit flaky_kv_service(int failures)
    : failures_{ failures }
  {
  }

  std::atomic<int> calls{ 0 };

  auto Get(grpc::ServerContext* /* context */,
           const v1::GetRequest* /* request */,
           v1::GetResponse* response) -> grpc::Status override
  {
    if (++calls <= failures_) {
      return { grpc::StatusCode::UNAVAILABLE, "not yet" };
    }
    response->set_content_uncompressed("{}");
    response->set_cas(42);
    return grpc::Status::OK;
  }

private:
  const int failures_;
};

// A retried operation must finish at its deadline, not at "deadline + one more full timeout".
//
// The budget is 400ms and the backoff is a flat 150ms, so the third attempt starts around 300ms in
// with roughly 100ms of budget left, against a server that has stopped answering. Given the
// remaining budget the operation ends at ~400ms; given a fresh full-length one it ends at ~700ms.
// The assertion is placed between those two outcomes, so it fails if an attempt is ever handed more
// time than the operation has left.
void
a_retried_kv_operation_stays_within_its_budget([[maybe_unused]] context& ctx)
{
  int port = 0;
  stalling_kv_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed }; // no credentials -> plaintext allowed
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 150ms;
    });

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "open(couchbase2://) succeeds");

  ops::get_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  request.timeout = 400ms;

  const auto started = std::chrono::steady_clock::now();
  std::promise<ops::get_response> done;
  cluster.execute(std::move(request), [&done](ops::get_response response) {
    done.set_value(std::move(response));
  });
  const auto result = done.get_future().get();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - started);

  // Tear down before asserting. A failed assertion throws, and unwinding past a still-joinable
  // std::thread calls std::terminate -- which would replace the assertion's message with a bare
  // "terminate called", hiding exactly the failure this case exists to report.
  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());

  // The call count is reported, not just tested: the two ways this can fail -- the first attempt
  // never reaching the service, or a retry not being issued -- are indistinguishable from a bare
  // "expected >= 2", and this case previously failed about one run in seven on a loaded machine
  // with no way to tell which had happened.
  assert_true(service.calls.load() >= 2,
              "the operation was retried at least once (dispatches reaching the service: " +
                std::to_string(service.calls.load()) + ")");
  // A timeout, not the last attempt's transport error: the operation is entitled to its whole
  // budget, so it ends on its deadline rather than on whichever UNAVAILABLE arrived last. The read
  // is idempotent, so the timeout is the unambiguous one.
  assert_true(result.ctx.ec() == couchbase::errc::common::unambiguous_timeout,
              "the operation ends on its deadline, reported as a timeout, got: " +
                result.ctx.ec().message());
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  assert_true(elapsed < 2000ms,
              "a retried operation finishes at its deadline, not a full timeout past it");
#else
  assert_true(elapsed < 600ms,
              "a retried operation finishes at its deadline, not a full timeout past it");
#endif
#elif defined(__SANITIZE_THREAD__)
  assert_true(elapsed < 2000ms,
              "a retried operation finishes at its deadline, not a full timeout past it");
#else
  assert_true(elapsed < 600ms,
              "a retried operation finishes at its deadline, not a full timeout past it");
#endif
}

// A request issued through the public API carries no timeout unless the caller asked for one, and
// the strategy never refuses a retry for service_not_available. The bound therefore has to come
// from the cluster's own key_value_timeout; without that fallback the loop has nothing to stop it
// and the caller's handler is never invoked -- not a timeout, not an error, no completion at all.
//
// key_value_timeout is set well below the stock default so the case both runs quickly and names
// where the bound comes from: a bound sourced from anywhere else would not honour it. The wait is
// bounded so that "never completes" fails the case rather than hanging the suite.
void
an_operation_without_a_timeout_is_bounded_by_the_cluster_default([[maybe_unused]] context& ctx)
{
  int port = 0;
  unavailable_kv_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().key_value_timeout = 400ms;
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 50ms;
    });

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "open(couchbase2://) succeeds");

  ops::get_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  // No request.timeout: this is what a caller who passes no options sends.

  const auto started = std::chrono::steady_clock::now();
  std::promise<ops::get_response> done;
  auto answered = done.get_future();
  cluster.execute(std::move(request), [&done](ops::get_response response) {
    done.set_value(std::move(response));
  });
  const auto arrived = answered.wait_for(5s) == std::future_status::ready;
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - started);
  // Only retrieved once known ready: get() on an unfulfilled future would block here, turning a
  // dropped completion into a hung suite rather than the named failure asserted below.
  const auto terminal_ec = arrived ? answered.get().ctx.ec() : std::error_code{};

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());

  assert_true(arrived, "an operation carrying no timeout of its own still completes");
  assert_true(elapsed < 2s, "and is bounded by the cluster's key_value_timeout");
  assert_true(service.calls.load() >= 2,
              "it did retry before giving up (dispatches reaching the service: " +
                std::to_string(service.calls.load()) + ")");
  // Which answer the bound produces, not merely that one arrives. Ending on the deadline is a
  // timeout; ending on the last attempt's response would surface the transport's temporary_failure
  // instead, which is what the classic transport would never report for this condition.
  assert_true(terminal_ec == couchbase::errc::common::unambiguous_timeout,
              "and reports the deadline as a timeout, got: " + terminal_ec.message());
}

// The loop accumulates attempts and reasons across re-issues, and RFC 77 makes both part of the
// error context. They are reported from the request the loop carries, so what the caller reads is
// what the loop actually did -- the count was previously pinned at zero in make_error_context, so
// an operation retried twenty-nine times reported none of them.
//
// The service decides the number of retries rather than the clock, which is what makes the exact
// count assertable.
void
a_retried_operation_reports_its_attempts_and_reasons([[maybe_unused]] context& ctx)
{
  int port = 0;
  flaky_kv_service service{ 2 };
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 50ms;
    });

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "open(couchbase2://) succeeds");

  ops::get_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  request.timeout = 5s; // generous: the service, not the deadline, ends the retrying here

  std::promise<ops::get_response> done;
  cluster.execute(std::move(request), [&done](ops::get_response response) {
    done.set_value(std::move(response));
  });
  const auto result = done.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());

  assert_false(static_cast<bool>(result.ctx.ec()), "the third attempt succeeds");
  assert_eq(result.ctx.retry_attempts(), std::size_t{ 2 }, "both retries are reported");
  assert_eq(result.ctx.retry_reasons().count(couchbase::retry_reason::service_not_available),
            std::size_t{ 1 },
            "and the reason they were retried for is reported with them");
}

// A mutation that exhausted its budget after being dispatched is ambiguous, and must say so.
//
// Once the loop ends an operation on its deadline rather than on the last attempt's response, the
// path that reports it has to decide the same question the classic transport decides in
// mcbp_command::cancel(): whether the server may have applied the write. It is decided by whether
// anything was ever sent, not by whether the final attempt was -- the attempt that ends the
// operation sends nothing, but its predecessors did. A caller that declines to replay ambiguous
// mutations depends on this, and unambiguous_timeout would tell it the write provably never landed.
//
// The read in the sibling cases covers the other arm: idempotent, so unambiguous either way.
void
a_retried_mutation_that_runs_out_of_budget_is_ambiguous([[maybe_unused]] context& ctx)
{
  int port = 0;
  unavailable_kv_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 50ms;
    });

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "open(couchbase2://) succeeds");

  ops::upsert_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  request.value = couchbase::core::utils::to_binary("{}");
  request.timeout = 400ms;

  std::promise<ops::upsert_response> done;
  auto answered = done.get_future();
  cluster.execute(std::move(request), [&done](ops::upsert_response response) {
    done.set_value(std::move(response));
  });
  const auto arrived = answered.wait_for(5s) == std::future_status::ready;
  const auto terminal_ec = arrived ? answered.get().ctx.ec() : std::error_code{};

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());

  assert_true(arrived, "the mutation completes");
  assert_true(service.calls.load() >= 2,
              "it was dispatched more than once, so the server may have seen it (dispatches: " +
                std::to_string(service.calls.load()) + ")");
  assert_true(terminal_ec == couchbase::errc::common::ambiguous_timeout,
              "a dispatched mutation that ran out of budget is ambiguous, got: " +
                terminal_ec.message());
}

// Closing the cluster while an operation sits in its backoff must still answer the caller. The
// re-entry does not touch protostellar_ directly -- it re-reads through the locked accessor, which
// returns an empty pointer once close() has run, and answers cluster_closed. Two properties are
// pinned here: that the completion is delivered and not dropped, whichever side of the close the
// timer lands on, and that the answer is cluster_closed rather than the last retryable response,
// which would tell the caller to try again against a cluster that is gone.
//
// The bounded wait is what keeps a dropped completion a failing assertion rather than a hung suite.
void
closing_the_cluster_during_a_backoff_still_answers([[maybe_unused]] context& ctx)
{
  int port = 0;
  unavailable_kv_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  // Long enough that the close below lands inside a backoff rather than between attempts.
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 300ms;
    });

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "open(couchbase2://) succeeds");

  ops::get_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  request.timeout = 10s; // the close, not the deadline, is what ends this operation

  std::promise<ops::get_response> done;
  auto answered = done.get_future();
  cluster.execute(std::move(request), [&done](ops::get_response response) {
    done.set_value(std::move(response));
  });

  // Wait for the first attempt to have been made and failed, so the operation is provably in a
  // backoff when the cluster closes rather than still connecting. Bounded: if the attempt never
  // reaches the service the case has not set up what it claims to test, and that should be a named
  // failure rather than a spin until the harness times the whole suite out.
  const auto first_attempt_by = std::chrono::steady_clock::now() + 5s;
  while (service.calls.load() < 1 && std::chrono::steady_clock::now() < first_attempt_by) {
    std::this_thread::sleep_for(5ms);
  }
  const auto reached_the_service = service.calls.load() >= 1;

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();

  const auto arrived = answered.wait_for(5s) == std::future_status::ready;
  auto answer_ec = std::error_code{};
  if (arrived) {
    answer_ec = answered.get().ctx.ec();
  }

  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());

  assert_true(reached_the_service,
              "the first attempt reached the service, so a backoff was entered");
  assert_true(arrived, "closing the cluster mid-backoff still answers the caller");
  // Not merely "an answer arrived": the last retryable response would report temporary_failure,
  // which tells a caller to try again against a cluster that is gone. Asserting the code is what
  // makes this case able to see which of the two it got.
  assert_true(answer_ec == errc::network::cluster_closed,
              "closing mid-backoff answers cluster_closed, got: " + answer_ec.message());
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_kv_retry",
    {
      { "a_retried_kv_operation_stays_within_its_budget",
        a_retried_kv_operation_stays_within_its_budget,
        {},
        timeout::slow },
      { "an_operation_without_a_timeout_is_bounded_by_the_cluster_default",
        an_operation_without_a_timeout_is_bounded_by_the_cluster_default,
        {},
        timeout::slow },
      { "a_retried_operation_reports_its_attempts_and_reasons",
        a_retried_operation_reports_its_attempts_and_reasons,
        {},
        timeout::slow },
      { "a_retried_mutation_that_runs_out_of_budget_is_ambiguous",
        a_retried_mutation_that_runs_out_of_budget_is_ambiguous,
        {},
        timeout::slow },
      { "closing_the_cluster_during_a_backoff_still_answers",
        closing_the_cluster_during_a_backoff_still_answers,
        {},
        timeout::slow },
    },
  };
}

} // namespace couchbase::test
