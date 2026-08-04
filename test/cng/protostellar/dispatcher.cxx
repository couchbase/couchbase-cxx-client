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

// Tests for the gRPC<->asio bridge (CXXCBC-889). Drives a real unary RPC against an
// in-process gRPC server and asserts: the completion handler runs on the io_context thread; a
// client deadline surfaces as DEADLINE_EXCEEDED; pending_call::cancel() surfaces as CANCELLED; and
// destroying the dispatcher with a call still in flight cancels and drains it rather than blocking.
// Env-agnostic (no external server).

#include "framework/test_runner.hxx"

#include "callback_queue_keepalive.hxx"

#include "core/protostellar/dispatcher.hxx"

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace couchbase::cng::test
{
namespace
{
namespace kv = ::couchbase::kv::v1;
using ::couchbase::core::protostellar::dispatcher;
using namespace std::chrono_literals;

// Minimal KvService server. Get echoes the key into the value; an optional delay lets the
// deadline/cancellation paths be exercised (the delay loop honours server-side cancellation so
// server shutdown does not block).
class test_kv_service final : public kv::KvService::Service
{
public:
  explicit test_kv_service(std::chrono::milliseconds delay)
    : delay_{ delay }
  {
  }

  auto Get(grpc::ServerContext* context, const kv::GetRequest* request, kv::GetResponse* response)
    -> grpc::Status override
  {
    if (delay_.count() > 0) {
      const auto deadline = std::chrono::steady_clock::now() + delay_;
      while (std::chrono::steady_clock::now() < deadline) {
        if (context->IsCancelled()) {
          return { grpc::StatusCode::CANCELLED, "cancelled" };
        }
        std::this_thread::sleep_for(10ms);
      }
    }
    response->set_content_uncompressed("value:" + request->key());
    response->set_cas(0xdead'beefULL);
    return grpc::Status::OK;
  }

private:
  std::chrono::milliseconds delay_;
};

// Owns an in-process gRPC server and hands out channels to it.
class in_process_server
{
public:
  explicit in_process_server(std::chrono::milliseconds delay)
    : service_{ delay }
  {
    // Pin gRPC's process-global callback completion queue for the lifetime of this binary.
    // Without it, destroying the last channel between cases drives a teardown that races
    // gRPC's own polling threads and aborts the process (CXXCBC-919).
    pin_callback_queue();

    grpc::ServerBuilder builder;
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
  }

  in_process_server(const in_process_server&) = delete;
  in_process_server(in_process_server&&) = delete;
  auto operator=(const in_process_server&) -> in_process_server& = delete;
  auto operator=(in_process_server&&) -> in_process_server& = delete;

  ~in_process_server()
  {
    // Immediate deadline: cancels the in-flight (possibly sleeping) call rather than waiting it
    // out.
    server_->Shutdown(std::chrono::system_clock::now());
    server_->Wait();
  }

  [[nodiscard]] auto channel() -> std::shared_ptr<grpc::Channel>
  {
    return server_->InProcessChannel(grpc::ChannelArguments{});
  }

private:
  test_kv_service service_;
  std::unique_ptr<grpc::Server> server_;
};

// Issue a single Get through the dispatcher and pump the io_context (via a work guard, so there
// is no run()-before-post race) until the handler fires. Returns the status, the response, and
// the thread the handler ran on.
struct call_outcome {
  grpc::Status status{};
  kv::GetResponse response{};
  std::thread::id handler_thread{};
};

auto
run_get(std::chrono::milliseconds timeout, in_process_server& server, bool cancel_immediately)
  -> call_outcome
{
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  auto stub = kv::KvService::NewStub(disp.channel());

  kv::GetRequest request;
  request.set_key("k1");

  call_outcome outcome;
  const auto pending = disp.unary<kv::GetResponse>(
    timeout,
    [&](grpc::ClientContext& ctx,
        kv::GetResponse& response,
        std::function<void(grpc::Status)> callback) {
      stub->async()->Get(&ctx, &request, &response, std::move(callback));
    },
    [&](grpc::Status status, kv::GetResponse response) {
      outcome.status = std::move(status);
      outcome.response = std::move(response);
      outcome.handler_thread = std::this_thread::get_id();
      work.reset(); // let io.run() return
    });

  if (cancel_immediately) {
    pending.cancel();
  }

  io.run();
  return outcome;
}

void
delivers_response_on_io_thread()
{
  in_process_server server{ 0ms };
  const auto run_thread = std::this_thread::get_id();
  const auto outcome = run_get(timeout::network, server, /*cancel_immediately=*/false);

  assert_true(outcome.status.ok(), "unary call succeeds");
  assert_eq(outcome.response.content_uncompressed(),
            std::string{ "value:k1" },
            "response body round-trips");
  assert_eq(outcome.response.cas(), 0xdead'beefULL, "cas round-trips");
  assert_true(outcome.handler_thread == run_thread, "handler ran on the io_context thread");
}

void
deadline_surfaces_as_deadline_exceeded()
{
  in_process_server server{ 2000ms }; // server delays well beyond the client deadline
  const auto outcome = run_get(100ms, server, /*cancel_immediately=*/false);
  assert_true(outcome.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED,
              "short deadline yields DEADLINE_EXCEEDED");
}

void
cancellation_surfaces_as_cancelled()
{
  in_process_server server{ 2000ms };
  const auto outcome = run_get(timeout::network, server, /*cancel_immediately=*/true);
  assert_true(outcome.status.error_code() == grpc::StatusCode::CANCELLED,
              "cancelled call yields CANCELLED");
}

// The three cases above never reach ~dispatcher() with a call outstanding: each pumps io.run() to
// completion, so the handler has fired and the call has deregistered itself before the dispatcher
// is destroyed, leaving cancel_and_drain() nothing to cancel or wait for. This case destroys the
// dispatcher while the call is still on gRPC's threadpool -- by never running the io_context at all
// -- so the destructor has to TryCancel the call and block until its completion callback has run.
// The channel, stub and request are declared outside the inner block deliberately: the call is
// still in flight when the block ends, so they have to outlive the dispatcher whose destructor
// drains it.
void
destructor_cancels_and_drains_an_in_flight_call()
{
  in_process_server server{ 2000ms }; // the server outlasts the drain unless the call is cancelled
  const auto channel = server.channel();
  auto stub = kv::KvService::NewStub(channel);

  kv::GetRequest request;
  request.set_key("k1");

  auto handler_ran = false;
  const auto started = std::chrono::steady_clock::now();
  asio::io_context io;
  {
    dispatcher disp{ io, channel };
    disp.unary<kv::GetResponse>(
      timeout::network,
      [&](grpc::ClientContext& ctx,
          kv::GetResponse& response,
          std::function<void(grpc::Status)> callback) {
        stub->async()->Get(&ctx, &request, &response, std::move(callback));
      },
      [&](grpc::Status, kv::GetResponse) {
        handler_ran = true;
      });
    // No io.run() here, which is the point: the completion is posted but never executed, so the
    // call is genuinely in flight when ~dispatcher() runs.
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_false(handler_ran, "the completion is queued on an io_context that is never run");
  assert_true(elapsed < 1500ms,
              "the destructor cancels the call rather than waiting out the server delay");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_dispatcher",
    {
      { "delivers_response_on_io_thread", delivers_response_on_io_thread, timeout::network },
      { "deadline_surfaces_as_deadline_exceeded",
        deadline_surfaces_as_deadline_exceeded,
        timeout::network },
      { "cancellation_surfaces_as_cancelled",
        cancellation_surfaces_as_cancelled,
        timeout::network },
      { "destructor_cancels_and_drains_an_in_flight_call",
        destructor_cancels_and_drains_an_in_flight_call,
        timeout::network },
    },
  };
}

} // namespace couchbase::cng::test
