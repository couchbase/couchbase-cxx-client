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

// Tests for the Protostellar KV component (CXXCBC-891): a real KV op is encoded, dispatched
// over gRPC to an in-process server, and decoded into a core response delivered on the
// io_context, with the error context reflecting the gRPC status. Env-agnostic (in-process
// server, no external cluster). The connect-time wiring into cluster_impl is a separate change.

#include "framework/test_runner.hxx"

#include "callback_queue_keepalive.hxx"

#include "core/cluster_credentials.hxx"
#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/protostellar/component.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/error_codes.hxx>

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>

namespace couchbase::cng::test
{
namespace
{
namespace v1 = ::couchbase::kv::v1;
namespace ops = ::couchbase::core::operations;
namespace pk = ::couchbase::core::protostellar::kv;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::document_id;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using namespace std::chrono_literals;

class test_kv_service final : public v1::KvService::Service
{
public:
  auto Get(grpc::ServerContext* context, const v1::GetRequest* request, v1::GetResponse* response)
    -> grpc::Status override
  {
    record_auth(context);
    if (request->key() == "missing") {
      return { grpc::StatusCode::NOT_FOUND, "no such document" };
    }
    if (request->key() == "compressed") {
      response->set_content_compressed("dummy_compressed_data");
      response->set_cas(0x2222ULL);
      return grpc::Status::OK;
    }
    response->set_content_uncompressed("value:" + request->key());
    response->set_cas(0x2222ULL);
    response->set_content_flags(0x06U);
    return grpc::Status::OK;
  }

  auto Upsert(grpc::ServerContext* context,
              const v1::UpsertRequest* request,
              v1::UpsertResponse* response) -> grpc::Status override
  {
    record_auth(context);
    if (request->key() == "fail") {
      return { grpc::StatusCode::UNAVAILABLE, "node unavailable" };
    }
    response->set_cas(0x3333ULL);
    auto* token = response->mutable_mutation_token();
    token->set_bucket_name(request->bucket_name());
    token->set_vbucket_id(7U);
    token->set_vbucket_uuid(0x1111ULL);
    token->set_seq_no(99ULL);
    return grpc::Status::OK;
  }

  auto Insert(grpc::ServerContext* context,
              const v1::InsertRequest* request,
              v1::InsertResponse* response) -> grpc::Status override
  {
    record_auth(context);
    if (request->key() == "exists") {
      return { grpc::StatusCode::ALREADY_EXISTS, "document exists" };
    }
    response->set_cas(0x4444ULL);
    auto* token = response->mutable_mutation_token();
    token->set_bucket_name(request->bucket_name());
    token->set_vbucket_id(1U);
    token->set_vbucket_uuid(0x5555ULL);
    token->set_seq_no(101ULL);
    return grpc::Status::OK;
  }

  auto Replace(grpc::ServerContext* context,
               const v1::ReplaceRequest* request,
               v1::ReplaceResponse* response) -> grpc::Status override
  {
    record_auth(context);
    if (request->key() == "missing") {
      return { grpc::StatusCode::NOT_FOUND, "no such document" };
    }
    response->set_cas(0x5555ULL);
    auto* token = response->mutable_mutation_token();
    token->set_bucket_name(request->bucket_name());
    token->set_vbucket_id(2U);
    token->set_vbucket_uuid(0x6666ULL);
    token->set_seq_no(102ULL);
    return grpc::Status::OK;
  }

  auto Remove(grpc::ServerContext* context,
              const v1::RemoveRequest* request,
              v1::RemoveResponse* response) -> grpc::Status override
  {
    record_auth(context);
    if (request->key() == "missing") {
      return { grpc::StatusCode::NOT_FOUND, "no such document" };
    }
    response->set_cas(0x6666ULL);
    auto* token = response->mutable_mutation_token();
    token->set_bucket_name(request->bucket_name());
    token->set_vbucket_id(3U);
    token->set_vbucket_uuid(0x7777ULL);
    token->set_seq_no(103ULL);
    return grpc::Status::OK;
  }

  std::string last_auth_header{};

  // Test knobs. `reply_delay` makes every handler sleep before answering, so a client-side deadline
  // can fire while the call is genuinely in flight. `calls_received` counts the requests that
  // actually reached the server, which is how a case distinguishes "rejected before dispatch" from
  // "dispatched and then failed" -- the two are indistinguishable from the response alone.
  std::atomic<std::chrono::milliseconds> reply_delay{ std::chrono::milliseconds::zero() };
  std::atomic<int> calls_received{ 0 };

private:
  void record_auth(grpc::ServerContext* context)
  {
    calls_received.fetch_add(1);
    const auto& meta = context->client_metadata();
    auto it = meta.find("authorization");
    if (it != meta.end()) {
      last_auth_header = std::string(it->second.data(), it->second.length());
    }
    if (const auto delay = reply_delay.load(); delay > std::chrono::milliseconds::zero()) {
      std::this_thread::sleep_for(delay);
    }
  }
};

class in_process_server
{
public:
  in_process_server()
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
    server_->Shutdown(std::chrono::system_clock::now());
  }
  [[nodiscard]] auto channel() -> std::shared_ptr<grpc::Channel>
  {
    return server_->InProcessChannel(grpc::ChannelArguments{});
  }
  [[nodiscard]] auto service() -> test_kv_service&
  {
    return service_;
  }

private:
  test_kv_service service_;
  std::unique_ptr<grpc::Server> server_;
};

auto
make_id(std::string key) -> document_id
{
  return document_id{ "b", "s", "c", std::move(key) };
}

void
get_round_trips_on_the_io_thread()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("k1");

  ops::get_response outcome;
  std::thread::id handler_thread;
  comp.execute(std::move(request), [&](ops::get_response response) {
    outcome = std::move(response);
    handler_thread = std::this_thread::get_id();
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "get succeeded");
  assert_eq(cu::to_string(outcome.value), std::string{ "value:k1" }, "value round-trips");
  assert_eq(outcome.cas.value(), 0x2222ULL, "cas round-trips");
  assert_true(handler_thread == std::this_thread::get_id(), "handler ran on io thread");
}

void
get_maps_not_found_into_the_error_context()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("missing");

  ops::get_response outcome;
  comp.execute(std::move(request), [&](ops::get_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "NOT_FOUND surfaces as document_not_found");
}

void
upsert_returns_cas_and_mutation_token()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::upsert_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("body");

  ops::upsert_response outcome;
  comp.execute(std::move(request), [&](ops::upsert_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "upsert succeeded");
  assert_eq(outcome.cas.value(), 0x3333ULL, "cas round-trips");
  assert_eq(outcome.token.partition_id(), static_cast<std::uint16_t>(7), "token vbucket id");
  assert_eq(outcome.token.sequence_number(), static_cast<std::uint64_t>(99), "token seqno");
}

void
get_handles_compressed_content_with_feature_not_available()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("compressed");

  ops::get_response outcome;
  comp.execute(std::move(request), [&](ops::get_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.ctx.ec() == couchbase::errc::common::feature_not_available,
              "compressed content surfaces feature_not_available");
}

void
insert_returns_cas_and_mutation_token()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::insert_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("body");

  ops::insert_response outcome;
  comp.execute(std::move(request), [&](ops::insert_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "insert succeeded");
  assert_eq(outcome.cas.value(), 0x4444ULL, "cas round-trips");
  assert_eq(outcome.token.partition_id(), static_cast<std::uint16_t>(1), "token vbucket id");
  assert_eq(outcome.token.sequence_number(), static_cast<std::uint64_t>(101), "token seqno");
}

void
insert_maps_already_exists_into_error_context()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::insert_request request;
  request.id = make_id("exists");
  request.value = cu::to_binary("body");

  ops::insert_response outcome;
  comp.execute(std::move(request), [&](ops::insert_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_exists,
              "ALREADY_EXISTS surfaces as document_exists");
}

void
replace_returns_cas_and_mutation_token()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::replace_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("body");

  ops::replace_response outcome;
  comp.execute(std::move(request), [&](ops::replace_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "replace succeeded");
  assert_eq(outcome.cas.value(), 0x5555ULL, "cas round-trips");
  assert_eq(outcome.token.partition_id(), static_cast<std::uint16_t>(2), "token vbucket id");
  assert_eq(outcome.token.sequence_number(), static_cast<std::uint64_t>(102), "token seqno");
}

void
replace_maps_not_found_into_error_context()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::replace_request request;
  request.id = make_id("missing");
  request.value = cu::to_binary("body");

  ops::replace_response outcome;
  comp.execute(std::move(request), [&](ops::replace_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "NOT_FOUND surfaces as document_not_found");
}

void
remove_returns_cas_and_mutation_token()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::remove_request request;
  request.id = make_id("k1");

  ops::remove_response outcome;
  comp.execute(std::move(request), [&](ops::remove_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "remove succeeded");
  assert_eq(outcome.cas.value(), 0x6666ULL, "cas round-trips");
  assert_eq(outcome.token.partition_id(), static_cast<std::uint16_t>(3), "token vbucket id");
  assert_eq(outcome.token.sequence_number(), static_cast<std::uint64_t>(103), "token seqno");
}

void
remove_maps_not_found_into_error_context()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::remove_request request;
  request.id = make_id("missing");

  ops::remove_response outcome;
  comp.execute(std::move(request), [&](ops::remove_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "NOT_FOUND surfaces as document_not_found");
}

void
authorization_header_passed_to_grpc_context()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  cluster_credentials creds;
  creds.username = "Administrator";
  creds.password = "password";

  component comp{ io, component_config{ server.channel(), creds, 5000ms } };

  ops::get_request request;
  request.id = make_id("k1");

  ops::get_response outcome;
  comp.execute(std::move(request), [&](ops::get_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_false(static_cast<bool>(outcome.ctx.ec()), "get succeeded");
  assert_false(server.service().last_auth_header.empty(), "authorization header received");
  assert_eq(server.service().last_auth_header,
            "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA==",
            "auth header matches expected basic auth");
}

// The deadline actually fires. Nothing else in this file exercises the timeout the component
// resolves: every other case leaves request.timeout unset against a server that answers at once, so
// an execute() that ignored the value entirely would still pass them all.
//
// Two properties are asserted together because they are only observable on this path: that
// request.timeout wins over the component default (50ms against a 5000ms default and a server that
// takes 400ms -- if the default were used the call would succeed instead), and that a timed-out
// read reports unambiguous_timeout, since a get cannot have changed the document.
void
get_honours_the_request_timeout()
{
  in_process_server server;
  server.service().reply_delay.store(400ms);
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("k1");
  request.timeout = 50ms;

  std::optional<ops::get_response> outcome;
  int completions = 0;
  comp.execute(std::move(request), [&](ops::get_response response) {
    ++completions;
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.has_value(), "the handler runs");
  assert_eq(completions, 1, "the handler runs exactly once");
  assert_true(outcome->ctx.ec() == couchbase::errc::common::unambiguous_timeout,
              "a timed-out read is unambiguous: a get cannot have mutated the document");
}

// The mutating arm of the same mapping. Kept as a separate case because a single shared
// classification would satisfy the read-only assertion above by accident.
void
mutation_timeout_is_reported_as_ambiguous()
{
  in_process_server server;
  server.service().reply_delay.store(400ms);
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::upsert_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("v");
  request.timeout = 50ms;

  std::optional<ops::upsert_response> outcome;
  comp.execute(std::move(request), [&](ops::upsert_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.has_value(), "the handler runs");
  assert_true(outcome->ctx.ec() == couchbase::errc::common::ambiguous_timeout,
              "a timed-out mutation is ambiguous: the write may already have been applied");
}

// An already-exhausted budget must not be dispatched. dispatcher::unary only sets a deadline when
// the timeout is positive, so forwarding a non-positive one would produce a call with no deadline
// at all -- the opposite of what "no time left" should mean.
//
// calls_received is what makes this a real assertion rather than a restatement of the error code:
// it proves the request was rejected before reaching the wire, not dispatched and then failed.
void
an_exhausted_budget_is_rejected_before_dispatch()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("k1");
  request.timeout = 0ms;

  std::optional<ops::get_response> outcome;
  std::thread::id handler_thread;
  int completions = 0;
  comp.execute(std::move(request), [&](ops::get_response response) {
    ++completions;
    outcome = std::move(response);
    handler_thread = std::this_thread::get_id();
    work.reset();
  });
  io.run();

  assert_true(outcome.has_value(), "the handler runs");
  assert_eq(completions, 1, "the handler runs exactly once");
  assert_true(outcome->ctx.ec() == couchbase::errc::common::unambiguous_timeout,
              "an operation that was never sent definitively did not happen");
  assert_eq(server.service().calls_received.load(), 0, "nothing reached the server");
  assert_true(handler_thread == std::this_thread::get_id(),
              "the rejection is delivered on the io thread like every other completion");
}

// A non-positive default is rejected on the same path, which is the case that does not depend on
// the caller: request.timeout is unset here, so the component's own default is what resolves to
// zero.
void
an_exhausted_default_timeout_is_also_rejected()
{
  in_process_server server;
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 0ms } };

  ops::remove_request request;
  request.id = make_id("k1");

  std::optional<ops::remove_response> outcome;
  comp.execute(std::move(request), [&](ops::remove_response response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();

  assert_true(outcome.has_value(), "the handler runs");
  assert_true(outcome->ctx.ec() == couchbase::errc::common::unambiguous_timeout,
              "a non-positive default is rejected exactly like a non-positive request timeout");
  assert_eq(server.service().calls_received.load(), 0, "nothing reached the server");
}

// Cancelling an in-flight call still completes the handler, exactly once. A cancellation that
// dropped the completion would hang whatever is waiting on it, which is worse than the error.
void
cancellation_completes_the_handler_once()
{
  in_process_server server;
  server.service().reply_delay.store(400ms);
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, 5000ms } };

  ops::get_request request;
  request.id = make_id("k1");

  std::optional<ops::get_response> outcome;
  int completions = 0;
  auto call = comp.execute(std::move(request), [&](ops::get_response response) {
    ++completions;
    outcome = std::move(response);
    work.reset();
  });
  call.cancel();
  io.run();

  assert_true(outcome.has_value(), "a cancelled call still completes its handler");
  assert_eq(completions, 1, "the handler runs exactly once");
  assert_true(outcome->ctx.ec() == couchbase::errc::common::request_canceled,
              "cancellation surfaces as request_canceled");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_component",
    {
      { "get_round_trips_on_the_io_thread", get_round_trips_on_the_io_thread, timeout::network },
      { "get_maps_not_found_into_the_error_context",
        get_maps_not_found_into_the_error_context,
        timeout::network },
      { "get_handles_compressed_content_with_feature_not_available",
        get_handles_compressed_content_with_feature_not_available,
        timeout::network },
      { "upsert_returns_cas_and_mutation_token",
        upsert_returns_cas_and_mutation_token,
        timeout::network },
      { "insert_returns_cas_and_mutation_token",
        insert_returns_cas_and_mutation_token,
        timeout::network },
      { "insert_maps_already_exists_into_error_context",
        insert_maps_already_exists_into_error_context,
        timeout::network },
      { "replace_returns_cas_and_mutation_token",
        replace_returns_cas_and_mutation_token,
        timeout::network },
      { "replace_maps_not_found_into_error_context",
        replace_maps_not_found_into_error_context,
        timeout::network },
      { "remove_returns_cas_and_mutation_token",
        remove_returns_cas_and_mutation_token,
        timeout::network },
      { "remove_maps_not_found_into_error_context",
        remove_maps_not_found_into_error_context,
        timeout::network },
      { "get_honours_the_request_timeout", get_honours_the_request_timeout, timeout::network },
      { "mutation_timeout_is_reported_as_ambiguous",
        mutation_timeout_is_reported_as_ambiguous,
        timeout::network },
      { "an_exhausted_budget_is_rejected_before_dispatch",
        an_exhausted_budget_is_rejected_before_dispatch,
        timeout::network },
      { "an_exhausted_default_timeout_is_also_rejected",
        an_exhausted_default_timeout_is_also_rejected,
        timeout::network },
      { "cancellation_completes_the_handler_once",
        cancellation_completes_the_handler_once,
        timeout::network },
      { "authorization_header_passed_to_grpc_context",
        authorization_header_passed_to_grpc_context,
        timeout::network },
    },
  };
}

} // namespace couchbase::cng::test
