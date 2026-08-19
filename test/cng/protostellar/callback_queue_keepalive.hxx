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

// Pins gRPC's process-global callback completion queue for the lifetime of a test binary
// (CXXCBC-919).
//
// gRPC serves the callback API from a completion queue chosen per channel in
// `Channel::CallbackCQ()` (src/cpp/client/channel_cc.cc):
//
//     if (grpc_iomgr_run_in_background()) {
//       callback_cq = new grpc::CompletionQueue(...GRPC_CQ_CALLBACK...);  // per channel
//     } else {
//       callback_cq = CompletionQueue::CallbackAlternativeCQ();           // process-global
//     }
//
// On POSIX that predicate is `grpc_event_engine_run_in_background()`, which is false for the
// iomgr-backed EventEngine that distribution builds of gRPC use, so the second branch is taken.
// That queue is reference counted: a channel takes a reference on its first callback-API call and
// drops it when destroyed, and when the count reaches zero gRPC shuts the queue down, joins its
// "nexting" threads and deletes it (src/cpp/common/completion_queue_cc.cc). Driving that teardown
// races the nexting threads still polling the queue and aborts the process:
//
//     ref_counted.h:183]  assertion failed: prior > 0
//
// A test that stands up an in-process server per case destroys the last channel between cases and
// so runs that teardown repeatedly. Holding one of these objects keeps the count above zero for the
// whole binary, so the teardown happens once at exit rather than between every case.
//
// This does not fix the underlying race, which is gRPC's; it keeps it out of the blast radius of
// tests that are about our own transport. The production exposure — an application that opens and
// closes clusters in a loop cycles the same queue — is tracked in CXXCBC-919.

#pragma once

#include "core/protostellar/dispatcher.hxx"

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <chrono>
#include <functional>
#include <memory>
#include <utility>

namespace couchbase::test
{

// Minimal service so the keepalive channel has something to call. The reference on the callback
// queue is taken lazily, on a channel's first callback-API call, so constructing a channel is not
// enough -- one real call has to complete.
class keepalive_service final : public ::couchbase::kv::v1::KvService::Service
{
public:
  auto Get(grpc::ServerContext* /* context */,
           const ::couchbase::kv::v1::GetRequest* /* request */,
           ::couchbase::kv::v1::GetResponse* response) -> grpc::Status override
  {
    response->set_cas(1);
    return grpc::Status::OK;
  }
};

class callback_queue_keepalive
{
public:
  callback_queue_keepalive()
  {
    grpc::ServerBuilder builder;
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    channel_ = server_->InProcessChannel(grpc::ChannelArguments{});

    asio::io_context io;
    ::couchbase::core::protostellar::dispatcher dispatcher{ io, channel_ };
    auto stub = ::couchbase::kv::v1::KvService::NewStub(channel_);
    auto request = std::make_shared<::couchbase::kv::v1::GetRequest>();
    request->set_key("callback-queue-keepalive");

    auto guard = asio::make_work_guard(io);
    dispatcher.unary<::couchbase::kv::v1::GetResponse>(
      std::chrono::seconds{ 5 },
      [stub = stub.get(), request](grpc::ClientContext& context,
                                   ::couchbase::kv::v1::GetResponse& response,
                                   std::function<void(grpc::Status)> callback) {
        stub->async()->Get(&context, request.get(), &response, std::move(callback));
      },
      [&guard](grpc::Status /* status */, ::couchbase::kv::v1::GetResponse /* response */) mutable {
        guard.reset();
      });
    io.run();
  }

  ~callback_queue_keepalive()
  {
    channel_.reset();
    if (server_) {
      server_->Shutdown(std::chrono::system_clock::now());
      server_->Wait();
    }
  }

  callback_queue_keepalive(const callback_queue_keepalive&) = delete;
  callback_queue_keepalive(callback_queue_keepalive&&) = delete;
  auto operator=(const callback_queue_keepalive&) -> callback_queue_keepalive& = delete;
  auto operator=(callback_queue_keepalive&&) -> callback_queue_keepalive& = delete;

private:
  keepalive_service service_{};
  std::unique_ptr<grpc::Server> server_{};
  std::shared_ptr<grpc::Channel> channel_{};
};

// Call once from any test case that stands up in-process servers. The first call pins the queue;
// the object lives until the process exits.
inline void
pin_callback_queue()
{
  static const callback_queue_keepalive keepalive{};
}

} // namespace couchbase::test
