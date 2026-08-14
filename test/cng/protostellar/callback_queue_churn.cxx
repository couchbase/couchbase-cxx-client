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

// Regression test for CXXCBC-919: repeatedly creating and destroying an in-process server, channel
// and dispatcher must not abort the process.
//
// Each cycle drops the last reference to gRPC's process-global callback completion queue, which
// makes gRPC shut it down, join its "nexting" threads and delete it. Doing that hundreds of times
// in one process races the nexting threads still polling the queue and aborts with
//
//     ref_counted.h:183]  assertion failed: prior > 0
//
// See callback_queue_keepalive.hxx for the mechanism and why pinning the queue avoids it.
//
// Environment knobs, for investigating the underlying gRPC race rather than testing our side of it:
//
// Both are read through safe_getenv, so an empty value counts as unset rather than as "set".
//
//   CNG_CALLBACK_QUEUE_CYCLES=<n>     cycles per run (default below)
//   CNG_CALLBACK_QUEUE_NO_KEEPALIVE=1 skip the keepalive, i.e. reproduce the abort. Measured at
//                                     7 aborts in 40 runs at 200 cycles against gRPC 1.48.4;
//                                     0 in 40 with the keepalive.
//
// This case is deliberately cheap -- 200 cycles is about 80 ms -- so it can act as a canary without
// slowing the suite.

#include "framework/test_runner.hxx"

#include "callback_queue_keepalive.hxx"

#include "core/protostellar/dispatcher.hxx"

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace couchbase::test
{
namespace
{
namespace kv = ::couchbase::kv::v1;
using ::couchbase::core::protostellar::dispatcher;
using namespace std::chrono_literals;

constexpr int default_cycles{ 200 };

class echo_service final : public kv::KvService::Service
{
public:
  auto Get(grpc::ServerContext* /* context */,
           const kv::GetRequest* request,
           kv::GetResponse* response) -> grpc::Status override
  {
    response->set_content_uncompressed("value:" + request->key());
    response->set_cas(0xdead'beefULL);
    return grpc::Status::OK;
  }
};

// One create/use/destroy cycle. The server is destroyed too, not just the channel: the server holds
// its own reference to the callback queue, so keeping it alive would pin the count above zero and
// the teardown under test would never run.
void
one_cycle()
{
  echo_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  auto channel = server->InProcessChannel(grpc::ChannelArguments{});

  {
    asio::io_context io;
    dispatcher disp{ io, channel };
    auto stub = kv::KvService::NewStub(channel);
    auto request = std::make_shared<kv::GetRequest>();
    request->set_key("k");

    auto guard = asio::make_work_guard(io);
    disp.unary<kv::GetResponse>(
      1000ms,
      [stub = stub.get(), request](grpc::ClientContext& context,
                                   kv::GetResponse& response,
                                   std::function<void(grpc::Status)> callback) {
        stub->async()->Get(&context, request.get(), &response, std::move(callback));
      },
      [&guard](grpc::Status /* status */, kv::GetResponse /* response */) mutable {
        guard.reset();
      });
    io.run();
  }

  channel.reset();
  server->Shutdown(std::chrono::system_clock::now());
  server->Wait();
}

[[nodiscard]] auto
cycles_from_environment() -> int
{
  const auto raw = safe_getenv("CNG_CALLBACK_QUEUE_CYCLES");
  if (!raw.has_value()) {
    return default_cycles;
  }
  const auto parsed = std::strtol(raw->c_str(), nullptr, 10);
  return (parsed > 0) ? static_cast<int>(parsed) : default_cycles;
}

void
cycling_servers_does_not_abort_the_process([[maybe_unused]] context& ctx)
{
  if (!safe_getenv("CNG_CALLBACK_QUEUE_NO_KEEPALIVE").has_value()) {
    pin_callback_queue();
  }

  const auto cycles = cycles_from_environment();
  int completed{ 0 };
  for (int i = 0; i < cycles; ++i) {
    one_cycle();
    ++completed;
  }

  // Surviving the loop *is* the property under test: the abort this pins down kills the process
  // outright, so there is no failure mode to observe other than reaching the end.
  assert_eq(completed, cycles, "every teardown cycle completed without aborting the process");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_callback_queue_churn",
    {
      { "cycling_servers_does_not_abort_the_process",
        cycling_servers_does_not_abort_the_process,
        {},
        timeout::network },
    },
  };
}

} // namespace couchbase::test
