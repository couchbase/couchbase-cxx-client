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

// Transport-level retry for non-KV couchbase2 operations (CXXCBC-906). Query/analytics/search/
// view/management requests carry no retry_context, so cluster_impl::execute_protostellar_http
// drives the retry. Drives a real core::cluster (plaintext couchbase2:// to a localhost
// QueryService) so the routing + re-dispatch is exercised end to end: a UNAVAILABLE
// (service_not_available) query is retried and then succeeds, while an INVALID_ARGUMENT query is
// delivered without a retry. The two ways the loop ends are covered as well -- an exhausted budget
// and a cluster closed mid-backoff -- since both decide what the caller is finally told.
// Env-agnostic: a local in-process-style server, no external gateway.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/test_runner.hxx"

#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/origin.hxx"
#include "core/protostellar/query_proto.hxx"
#include "core/utils/connection_string.hxx"
#include "core/utils/json_stream_control.hxx"

#include <couchbase/best_effort_retry_strategy.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <future>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace query_v1 = ::couchbase::query::v1;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::origin;
using ::couchbase::core::utils::parse_connection_string;
using ::couchbase::core::utils::json::stream_control;
using namespace std::chrono_literals;

// Statements steer the server's behavior so the test can assert exact dispatch counts:
//  "retry"           - UNAVAILABLE (→ service_not_available) once, then a success. Retryable.
//  "fatal"           - always INVALID_ARGUMENT. Non-retryable.
//  "stream-then-fail" - streams one row, then fails with UNAVAILABLE. A row_callback consumer marks
//                       the op delivered, so CXXCBC-910 rewrites the error to request_canceled,
//                       which is NOT retryable -- a partially-delivered stream must never be
//                       replayed.
class retry_query_service final : public query_v1::QueryService::Service
{
public:
  std::atomic<int> retry_calls{ 0 };
  std::atomic<int> fatal_calls{ 0 };
  std::atomic<int> stream_calls{ 0 };
  std::atomic<int> always_fail_calls{ 0 };

  auto Query(grpc::ServerContext* /* context */,
             const query_v1::QueryRequest* request,
             grpc::ServerWriter<query_v1::QueryResponse>* writer) -> grpc::Status override
  {
    if (request->statement() == "always-fail") {
      ++always_fail_calls;
      return { grpc::StatusCode::UNAVAILABLE, "temporary failure" };
    }
    if (request->statement() == "fatal") {
      ++fatal_calls;
      return { grpc::StatusCode::INVALID_ARGUMENT, "permanent failure" };
    }
    if (request->statement() == "stream-then-fail") {
      ++stream_calls;
      query_v1::QueryResponse batch;
      batch.add_rows("{\"row\":1}");
      writer->Write(batch);
      return { grpc::StatusCode::UNAVAILABLE, "dropped mid-stream" };
    }
    if (++retry_calls == 1) {
      return { grpc::StatusCode::UNAVAILABLE, "try again" };
    }
    query_v1::QueryResponse tail;
    auto* meta = tail.mutable_meta_data();
    meta->set_request_id("req-1");
    meta->set_status(query_v1::QueryResponse_MetaData_Status_STATUS_SUCCESS);
    writer->Write(tail);
    return grpc::Status::OK;
  }
};

void
non_kv_retry_dispatch_semantics()
{
  int port = 0;
  retry_query_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed }; // no credentials -> plaintext allowed
  cluster_origin.options().enable_tls = false;
  // Deterministic, fast backoff so the retry does not stretch the test.
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 1ms;
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
  const auto open_ec = opened.get_future().get();

  // Retryable: UNAVAILABLE on the first dispatch, success on the retry.
  ops::query_request retryable;
  retryable.statement = "retry";
  retryable.timeout = 10s;
  std::promise<ops::query_response> retried;
  cluster.execute(std::move(retryable), [&retried](ops::query_response r) {
    retried.set_value(std::move(r));
  });
  const auto retried_result = retried.get_future().get();

  // Non-retryable: INVALID_ARGUMENT must be delivered as-is, with no retry.
  ops::query_request terminal;
  terminal.statement = "fatal";
  terminal.timeout = 10s;
  std::promise<ops::query_response> failed;
  cluster.execute(std::move(terminal), [&failed](ops::query_response r) {
    failed.set_value(std::move(r));
  });
  const auto failed_result = failed.get_future().get();

  // Partial stream: a row reaches the row_callback, then UNAVAILABLE. CXXCBC-910 turns that into
  // request_canceled (non-retryable), so the op must NOT be replayed even though UNAVAILABLE would
  // otherwise be retryable -- replaying a partially-delivered stream is unsafe.
  ops::query_request partial;
  partial.statement = "stream-then-fail";
  partial.timeout = 10s;
  partial.row_callback = [](std::string) {
    return stream_control::next_row;
  };
  std::promise<ops::query_response> partial_done;
  cluster.execute(std::move(partial), [&partial_done](ops::query_response r) {
    partial_done.set_value(std::move(r));
  });
  const auto partial_result = partial_done.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown();
  server->Wait();

  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds");
  assert_false(static_cast<bool>(retried_result.ctx.ec),
               "a retryable non-KV op eventually succeeds");
  assert_eq(service.retry_calls.load(), 2, "the query was dispatched twice (one retry)");
  assert_eq(retried_result.ctx.retry_attempts,
            std::size_t{ 1 },
            "non-KV retried operation reports its retry attempts (CXXCBC-921)");
  assert_true(
    retried_result.ctx.retry_reasons.count(couchbase::retry_reason::service_not_available) > 0,
    "non-KV retried operation reports service_not_available in retry reasons (CXXCBC-921)");
  assert_true(static_cast<bool>(failed_result.ctx.ec), "a terminal non-KV op fails");
  assert_eq(service.fatal_calls.load(), 1, "a non-retryable op is not retried");
  assert_true(partial_result.ctx.ec == couchbase::errc::common::request_canceled,
              "a delivered-then-failed stream surfaces request_canceled");
  assert_eq(service.stream_calls.load(), 1, "a partially-delivered stream is not retried");
}

void
non_kv_retry_budget_exhaustion()
{
  int port = 0;
  retry_query_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().default_retry_strategy_ =
    couchbase::make_best_effort_retry_strategy([](std::size_t) {
      return 1ms;
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
  const auto open_ec = opened.get_future().get();

  ops::query_request req;
  req.statement = "always-fail";
  req.timeout = 50ms;
  std::promise<ops::query_response> done;
  cluster.execute(std::move(req), [&done](ops::query_response r) {
    done.set_value(std::move(r));
  });

  const auto res = done.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown();
  server->Wait();

  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds");
  assert_true(res.ctx.ec == couchbase::errc::common::unambiguous_timeout,
              "always-failing query fails with unambiguous_timeout when budget is exhausted");
  assert_true(service.always_fail_calls.load() > 1,
              "multiple attempts occurred before budget exhaustion");
  assert_true(res.ctx.retry_attempts > 0,
              "retry_attempts is non-zero after exhausted budget (CXXCBC-921)");
  assert_true(res.ctx.retry_reasons.count(couchbase::retry_reason::service_not_available) > 0,
              "retry_reasons records service_not_available after budget exhaustion (CXXCBC-921)");
}

// Closing the cluster while a non-KV operation sits in its backoff must answer the caller, and must
// not answer with the retryable transport error the next attempt would have replaced -- a
// temporary_failure tells a caller to try again against a cluster that is gone. The KV path pins
// the same property in kv_retry.cxx; this is the non-KV half, and it is the only observer of the
// retry state carried onto that answer.
//
// "always-fail" answers UNAVAILABLE forever, so the loop cannot end on its own and any close before
// the deadline lands inside a backoff. Two orderings are reachable and the case accepts either,
// because close() latches stopped_ on the caller's thread but only posts the teardown that clears
// the component: the attempt's completion can be processed before the close, in which case the
// timer re-enters, finds no component and answers cluster_closed, or after it, in which case the
// retry loop answers the close itself. Where the close's own drain cancelled a call already in
// flight the answer is request_canceled, terminal and equally not an invitation to retry.
//
// The wait for a second attempt is what establishes that a retry was recorded, and the bounded wait
// for the answer is what keeps a dropped completion a failing assertion rather than a hung suite.
void
closing_the_cluster_during_a_non_kv_backoff_still_answers()
{
  int port = 0;
  retry_query_service service;
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
  const auto open_ec = opened.get_future().get();

  ops::query_request req;
  req.statement = "always-fail";
  req.timeout = 10s; // the close, not the deadline, is what ends this operation
  std::promise<ops::query_response> done;
  auto answered = done.get_future();
  cluster.execute(std::move(req), [&done](ops::query_response r) {
    done.set_value(std::move(r));
  });

  // Wait for a SECOND attempt to reach the service. One call proves only that the service was
  // reached, which is a server-side fact: the client may not have processed that failure yet, so
  // nothing has been recorded and the assertion on retry_attempts below has no ground. A second
  // call cannot happen until the first failure came back and the retry loop acted on it, so it is
  // the earliest point at which a retry attempt is known to be recorded.
  const auto retried_by = std::chrono::steady_clock::now() + 5s;
  while (service.always_fail_calls.load() < 2 && std::chrono::steady_clock::now() < retried_by) {
    std::this_thread::sleep_for(5ms);
  }
  const auto retried = service.always_fail_calls.load() >= 2;

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();

  const auto arrived = answered.wait_for(5s) == std::future_status::ready;
  auto res = ops::query_response{};
  if (arrived) {
    res = answered.get();
  }

  guard.reset();
  io_thread.join();
  server->Shutdown();
  server->Wait();

  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds");
  assert_true(retried, "a second attempt reached the service, so a retry was recorded");
  assert_true(arrived, "closing the cluster mid-backoff still answers the caller");
  // Two answers are correct and both are terminal: cluster_closed when the close was observed
  // before the next attempt went out, and request_canceled when the close's own drain cancelled the
  // call already in flight. The one that must never arrive is temporary_failure -- it tells the
  // caller to try again against a cluster that is gone, and it is what this case exists to exclude.
  assert_true(res.ctx.ec == couchbase::errc::network::cluster_closed ||
                res.ctx.ec == couchbase::errc::common::request_canceled,
              "closing mid-backoff answers terminally, got: " + res.ctx.ec.message());
  // The retry state is carried onto that answer rather than dropped: an operation that retried
  // before the close must not be reported as never retried.
  assert_true(res.ctx.retry_attempts > 0,
              "the answer still reports the attempts that preceded the close");
  assert_true(res.ctx.retry_reasons.count(couchbase::retry_reason::service_not_available) > 0,
              "the answer still reports service_not_available");
}

class fail_fast_retry_strategy final : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    return couchbase::retry_action::do_not_retry();
  }
  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "fail_fast";
  }
};

void
custom_retry_strategy_rejects_non_kv()
{
  int port = 0;
  retry_query_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed };
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().default_retry_strategy_ = std::make_shared<fail_fast_retry_strategy>();

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
  const auto open_ec = opened.get_future().get();

  ops::query_request req;
  req.statement = "always-fail";
  req.timeout = 5s;
  std::promise<ops::query_response> done;
  cluster.execute(std::move(req), [&done](ops::query_response r) {
    done.set_value(std::move(r));
  });

  const auto res = done.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown();
  server->Wait();

  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds");
  assert_true(static_cast<bool>(res.ctx.ec), "custom do_not_retry strategy fails immediately");
  assert_eq(service.always_fail_calls.load(), 1, "exactly 1 attempt was made");
  assert_eq(res.ctx.retry_attempts, std::size_t{ 0 }, "retry_attempts is 0 when no retry occurred");
  assert_true(res.ctx.retry_reasons.empty(),
              "retry_reasons is empty when do_not_retry prevents retrying");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_retry",
    {
      { "non_kv_retry_dispatch_semantics",
        non_kv_retry_dispatch_semantics,
        timeout::integration,
        test_env::agnostic },
      { "closing_the_cluster_during_a_non_kv_backoff_still_answers",
        closing_the_cluster_during_a_non_kv_backoff_still_answers,
        timeout::slow },
      { "non_kv_retry_budget_exhaustion",
        non_kv_retry_budget_exhaustion,
        timeout::integration,
        test_env::agnostic },
      { "custom_retry_strategy_rejects_non_kv",
        custom_retry_strategy_rejects_non_kv,
        timeout::integration,
        test_env::agnostic },
    },
  };
}

} // namespace couchbase::test
