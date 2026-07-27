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

// Streaming row_callback wiring for query over couchbase2 (CXXCBC-910 / -0025). Drives the
// component against an in-process QueryService: rows delivered incrementally via row_callback,
// stream_control::stop draining to the terminal metadata, and a mid-stream error surfacing as
// request_canceled once rows have reached the consumer. Env-agnostic (in-process server).

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/operations/document_query.hxx"
#include "core/protostellar/component.hxx"
#include "core/protostellar/query_proto.hxx"
#include "core/utils/json_stream_control.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::cng::test
{
namespace
{
namespace v1 = ::couchbase::query::v1;
namespace ops = ::couchbase::core::operations;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using ::couchbase::core::utils::json::stream_control;
using namespace std::chrono_literals;

// Branches on the statement so a single service covers every case: "ok" streams two batches plus a
// metadata tail; "error" streams one row then fails with a retryable status (UNAVAILABLE).
class test_query_service final : public v1::QueryService::Service
{
public:
  // Counts what actually reached the server, which is how a case distinguishes "rejected before
  // dispatch" from "dispatched and then failed" -- the response alone cannot tell them apart.
  std::atomic<int> calls_received{ 0 };

  auto Query(grpc::ServerContext* /* context */,
             const v1::QueryRequest* request,
             grpc::ServerWriter<v1::QueryResponse>* writer) -> grpc::Status override
  {
    calls_received.fetch_add(1);
    {
      v1::QueryResponse batch;
      batch.add_rows("{\"row\":1}");
      batch.add_rows("{\"row\":2}");
      writer->Write(batch);
    }
    if (request->statement() == "error") {
      return { grpc::StatusCode::UNAVAILABLE, "gateway went away mid-stream" };
    }
    {
      v1::QueryResponse batch;
      batch.add_rows("{\"row\":3}");
      writer->Write(batch);
    }
    {
      v1::QueryResponse tail;
      auto* meta = tail.mutable_meta_data();
      meta->set_request_id("req-1");
      meta->set_client_context_id("ctx-1");
      meta->set_status(v1::QueryResponse_MetaData_Status_STATUS_SUCCESS);
      writer->Write(tail);
    }
    return grpc::Status::OK;
  }
};

class in_process_query_server
{
public:
  in_process_query_server()
  {
    grpc::ServerBuilder builder;
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
  }
  in_process_query_server(const in_process_query_server&) = delete;
  in_process_query_server(in_process_query_server&&) = delete;
  auto operator=(const in_process_query_server&) -> in_process_query_server& = delete;
  auto operator=(in_process_query_server&&) -> in_process_query_server& = delete;
  ~in_process_query_server()
  {
    server_->Shutdown(std::chrono::system_clock::now());
  }
  [[nodiscard]] auto channel() -> std::shared_ptr<grpc::Channel>
  {
    return server_->InProcessChannel(grpc::ChannelArguments{});
  }
  [[nodiscard]] auto service() -> test_query_service&
  {
    return service_;
  }

private:
  test_query_service service_;
  std::unique_ptr<grpc::Server> server_;
};

auto
make_component(asio::io_context& io, in_process_query_server& server) -> component
{
  return component{ io,
                    component_config{ server.channel(),
                                      cluster_credentials{},
                                      { 5000ms, 5000ms, 5000ms, 5000ms, 5000ms, 5000ms, 5000ms },
                                      { false } } };
}

void
row_callback_delivers_rows_incrementally()
{
  in_process_query_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto comp = make_component(io, server);

  std::vector<std::string> delivered;
  ops::query_request request;
  request.statement = "ok";
  request.row_callback = [&delivered](std::string row) {
    delivered.push_back(std::move(row));
    return stream_control::next_row;
  };

  ops::query_response outcome;
  comp.execute(std::move(request), [&](ops::query_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec), "query succeeded");
  assert_eq(delivered.size(), std::size_t{ 3 }, "every row reached the callback");
  assert_eq(delivered.at(0), std::string{ "{\"row\":1}" }, "row order preserved");
  assert_eq(delivered.at(2), std::string{ "{\"row\":3}" }, "last row delivered");
  assert_true(outcome.rows.empty(), "rows are not also buffered into the response");
  assert_eq(outcome.meta.client_context_id, std::string{ "ctx-1" }, "terminal metadata captured");
}

void
row_callback_stop_drains_to_metadata()
{
  in_process_query_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto comp = make_component(io, server);

  std::vector<std::string> delivered;
  ops::query_request request;
  request.statement = "ok";
  request.row_callback = [&delivered](std::string row) {
    delivered.push_back(std::move(row));
    return stream_control::stop; // stop after the very first row
  };

  ops::query_response outcome;
  comp.execute(std::move(request), [&](ops::query_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec), "stream still completes cleanly");
  assert_eq(delivered.size(), std::size_t{ 1 }, "no rows delivered after stop");
  assert_eq(outcome.meta.client_context_id,
            std::string{ "ctx-1" },
            "terminal metadata still captured after stop");
}

void
mid_stream_error_after_delivery_maps_to_request_canceled()
{
  in_process_query_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto comp = make_component(io, server);

  std::vector<std::string> delivered;
  ops::query_request request;
  request.statement = "error";
  request.row_callback = [&delivered](std::string row) {
    delivered.push_back(std::move(row));
    return stream_control::next_row;
  };

  ops::query_response outcome;
  comp.execute(std::move(request), [&](ops::query_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(delivered.empty(), "rows reached the consumer before the error");
  assert_true(outcome.ctx.ec == couchbase::errc::common::request_canceled,
              "a retryable error after delivery becomes request_canceled");
}

// The same mid-stream failure without a row_callback. The buffered path has no consumer to deliver
// to, but the gateway has still executed the statement, so replaying it is just as unsafe: a
// `DELETE FROM ... WHERE ...` that streamed a result message before the connection dropped would be
// applied twice by the retry loop. The rows the case never reads are the point -- they are what
// proves the statement ran.
void
a_buffered_mid_stream_error_maps_to_request_canceled()
{
  in_process_query_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto comp = make_component(io, server);

  ops::query_request request;
  request.statement = "error";

  ops::query_response outcome;
  comp.execute(std::move(request), [&](ops::query_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(outcome.rows.empty(), "the gateway's row was buffered before the error");
  assert_true(outcome.ctx.ec == couchbase::errc::common::request_canceled,
              "a retryable error after the gateway sent rows becomes request_canceled");
}

// The exhausted-budget guard on a non-KV path. The streaming services resolve a timeout exactly as
// the KV overloads do, so a non-positive one would otherwise reach the dispatcher and become a
// stream with no deadline -- one that cluster::close() then waits on. Asserting the server saw
// nothing is what makes this a real check rather than a restatement of the error code.
void
an_exhausted_budget_is_rejected_before_dispatch()
{
  in_process_query_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto comp = make_component(io, server);

  ops::query_request request;
  request.statement = "SELECT 1";
  request.timeout = 0ms;

  std::optional<ops::query_response> outcome;
  int completions = 0;
  std::thread::id handler_thread;
  comp.execute(std::move(request), [&](ops::query_response response) {
    ++completions;
    outcome = std::move(response);
    handler_thread = std::this_thread::get_id();
    work.reset();
  });
  io.run();

  assert_true(outcome.has_value(), "the handler runs");
  assert_eq(completions, 1, "the handler runs exactly once");
  assert_true(outcome->ctx.ec == couchbase::errc::common::unambiguous_timeout,
              "a query that was never sent definitively did not happen");
  assert_eq(server.service().calls_received.load(), 0, "nothing reached the server");
  assert_true(handler_thread == std::this_thread::get_id(),
              "the rejection is delivered on the io thread like every other completion");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_query_streaming",
    {
      { "an_exhausted_budget_is_rejected_before_dispatch",
        an_exhausted_budget_is_rejected_before_dispatch,
        timeout::network },
      { "row_callback_delivers_rows_incrementally",
        row_callback_delivers_rows_incrementally,
        timeout::network },
      { "row_callback_stop_drains_to_metadata",
        row_callback_stop_drains_to_metadata,
        timeout::network },
      { "a_buffered_mid_stream_error_maps_to_request_canceled",
        a_buffered_mid_stream_error_maps_to_request_canceled,
        timeout::network },
      { "mid_stream_error_after_delivery_maps_to_request_canceled",
        mid_stream_error_after_delivery_maps_to_request_canceled,
        timeout::network },
    },
  };
}

} // namespace couchbase::cng::test
