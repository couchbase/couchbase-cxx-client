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

// Dispatch-level coverage for the rest of the KV surface (CXXCBC-894).
//
// component.cxx drives get/get_projected/upsert/insert/replace/remove through component::execute().
// The nine operations here -- touch, exists, get_and_lock, unlock, get_and_touch, increment,
// decrement, append, prepend -- had encode/decode coverage in kv_converter.cxx and nothing that put
// them through the real gRPC path. Two classes of defect fall in that gap: an operation wired to
// the wrong stub still encodes correctly, and a response guard keyed on the wrong message type
// still compiles. Both need a real round trip to show up.
//
// The expectations here are taken from the protocol rather than from this implementation, so that a
// case fails when the client stops honouring the contract instead of merely when the code changes.
// The reference behaviour is the one the gateway and the other SDKs agree on -- gocbcoreps'
// impl_kv_test.go is the closest analogue, and its expectations are named in the cases below where
// they are not obvious. The notable ones:
//
//   * `exists` on a missing document is a success carrying result=false, never a NOT_FOUND.
//   * NOT_FOUND is not by itself "document not found": the resource_type in the google.rpc
//     ResourceInfo detail selects between bucket/scope/collection/document (RFC 77).
//   * a locked document surfaces as FAILED_PRECONDITION carrying a PreconditionFailure violation of
//     type LOCKED, and unlocking one that is not locked as NOT_LOCKED.
//   * `unlock` with a zero cas is rejected by the server as INVALID_ARGUMENT.
//
// Env-agnostic: an in-process gRPC server, no cluster.

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/protostellar/component.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/error_codes.hxx>

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>

#include <snappy.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::test
{
namespace
{
namespace v1 = ::couchbase::kv::v1;
namespace ops = ::couchbase::core::operations;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::document_id;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using namespace std::chrono_literals;

// A gateway reports the specific failure in the google.rpc.Status details rather than in the gRPC
// status code, which alone is too coarse to distinguish "no such bucket" from "no such document".
// Building the real payload here is what makes these cases exercise the mapping the gateway drives;
// a status carrying only a code would take a different branch in error_utils entirely.
template<typename Detail>
[[nodiscard]] auto
rich_status(grpc::StatusCode code, const std::string& message, const Detail& detail) -> grpc::Status
{
  google::rpc::Status rich;
  rich.set_code(static_cast<std::int32_t>(code));
  rich.set_message(message);
  rich.add_details()->PackFrom(detail);
  return { code, message, rich.SerializeAsString() };
}

[[nodiscard]] auto
resource_not_found(const std::string& resource_type) -> grpc::Status
{
  google::rpc::ResourceInfo info;
  info.set_resource_type(resource_type);
  info.set_resource_name("test");
  return rich_status(grpc::StatusCode::NOT_FOUND, resource_type + " not found", info);
}

[[nodiscard]] auto
precondition_failed(const std::string& violation_type) -> grpc::Status
{
  google::rpc::PreconditionFailure failure;
  failure.add_violations()->set_type(violation_type);
  return rich_status(grpc::StatusCode::FAILED_PRECONDITION, violation_type, failure);
}

// Keys that steer the double into a specific failure. Spelling the trigger into the key keeps each
// case's intent visible at the call site instead of in server state set up three lines earlier.
constexpr const char* key_missing = "missing";
constexpr const char* key_locked = "locked";
constexpr const char* key_not_locked = "not-locked";
constexpr const char* key_cas_mismatch = "cas-mismatch";
constexpr const char* key_compressed = "compressed";
constexpr const char* key_missing_bucket = "missing-bucket";
constexpr const char* key_missing_scope = "missing-scope";
constexpr const char* key_missing_collection = "missing-collection";

// What the compressed arm carries. It has to be a real snappy frame: an undecodable one is a
// server fault and takes the internal_server_failure path, which would not tell us whether
// decompression works.
const std::string compressed_plaintext{ "hello_snappy_decompressed_value" };

[[nodiscard]] auto
snappy_frame() -> std::string
{
  std::string compressed;
  snappy::Compress(compressed_plaintext.data(), compressed_plaintext.size(), &compressed);
  return compressed;
}

// Records what each request carried so a case can assert the encoding directly, rather than infer
// it from a response this same double produced.
struct recorded_request {
  std::string key{};
  std::string bucket{};
  std::string scope{};
  std::string collection{};
  std::uint64_t cas{ 0 };
  std::uint32_t lock_time_secs{ 0 };
  std::optional<std::uint32_t> expiry_secs{};
  std::optional<std::int64_t> expiry_time_secs{};
  std::string content{};
  std::uint64_t delta{ 0 };
  std::optional<std::int64_t> initial{};
  bool has_cas{ false };
};

class kv_surface_service final : public v1::KvService::Service
{
public:
  auto Touch(grpc::ServerContext* /* context */,
             const v1::TouchRequest* request,
             v1::TouchResponse* response) -> grpc::Status override
  {
    record(request);
    record_expiry(request);
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    response->set_cas(0xa001ULL);
    return grpc::Status::OK;
  }

  auto Exists(grpc::ServerContext* /* context */,
              const v1::ExistsRequest* request,
              v1::ExistsResponse* response) -> grpc::Status override
  {
    record(request);
    // Deliberately container_failure and not fail_for: a missing document is a successful Exists
    // carrying result=false. That is the contract the gateway implements -- reporting NOT_FOUND
    // here would make "does it exist" unanswerable -- so the double must not raise for it either.
    if (auto status = container_failure(request->key()); !status.ok()) {
      return status;
    }
    const bool present = request->key() != std::string{ key_missing };
    response->set_result(present);
    response->set_cas(present ? 0xa002ULL : 0ULL);
    return grpc::Status::OK;
  }

  auto GetAndLock(grpc::ServerContext* /* context */,
                  const v1::GetAndLockRequest* request,
                  v1::GetAndLockResponse* response) -> grpc::Status override
  {
    record(request);
    last.lock_time_secs = request->lock_time_secs();
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    if (request->key() == std::string{ key_compressed }) {
      response->set_content_compressed(snappy_frame());
      response->set_cas(0xa003ULL);
      return grpc::Status::OK;
    }
    response->set_content_uncompressed("locked:" + request->key());
    response->set_cas(0xa003ULL);
    response->set_content_flags(0x06U);
    return grpc::Status::OK;
  }

  auto Unlock(grpc::ServerContext* /* context */,
              const v1::UnlockRequest* request,
              v1::UnlockResponse* /* response */) -> grpc::Status override
  {
    record(request);
    last.cas = request->cas();
    if (request->cas() == 0) {
      return { grpc::StatusCode::INVALID_ARGUMENT, "cas cannot be zero" };
    }
    return fail_for(request->key());
  }

  auto GetAndTouch(grpc::ServerContext* /* context */,
                   const v1::GetAndTouchRequest* request,
                   v1::GetAndTouchResponse* response) -> grpc::Status override
  {
    record(request);
    record_expiry(request);
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    if (request->key() == std::string{ key_compressed }) {
      response->set_content_compressed(snappy_frame());
      response->set_cas(0xa004ULL);
      return grpc::Status::OK;
    }
    response->set_content_uncompressed("touched:" + request->key());
    response->set_cas(0xa004ULL);
    response->set_content_flags(0x06U);
    return grpc::Status::OK;
  }

  auto Increment(grpc::ServerContext* /* context */,
                 const v1::IncrementRequest* request,
                 v1::IncrementResponse* response) -> grpc::Status override
  {
    record(request);
    record_expiry(request);
    record_counter(request);
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    response->set_content(42);
    response->set_cas(0xa005ULL);
    set_token(response->mutable_mutation_token(), request->bucket_name(), 11U, 0xb005ULL, 205ULL);
    return grpc::Status::OK;
  }

  auto Decrement(grpc::ServerContext* /* context */,
                 const v1::DecrementRequest* request,
                 v1::DecrementResponse* response) -> grpc::Status override
  {
    record(request);
    record_expiry(request);
    record_counter(request);
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    response->set_content(7);
    response->set_cas(0xa006ULL);
    set_token(response->mutable_mutation_token(), request->bucket_name(), 12U, 0xb006ULL, 206ULL);
    return grpc::Status::OK;
  }

  auto Append(grpc::ServerContext* /* context */,
              const v1::AppendRequest* request,
              v1::AppendResponse* response) -> grpc::Status override
  {
    record(request);
    last.content = request->content();
    // has_cas(), not cas() != 0: the field tracks presence, and "absent" is what distinguishes an
    // unconditional append from one requiring cas 0. Deriving it from the value cannot tell the
    // two apart, which is exactly the bug a mutation to the encoder would slip through.
    last.has_cas = request->has_cas();
    last.cas = request->cas();
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    response->set_cas(0xa007ULL);
    set_token(response->mutable_mutation_token(), request->bucket_name(), 13U, 0xb007ULL, 207ULL);
    return grpc::Status::OK;
  }

  auto Prepend(grpc::ServerContext* /* context */,
               const v1::PrependRequest* request,
               v1::PrependResponse* response) -> grpc::Status override
  {
    record(request);
    last.content = request->content();
    // has_cas(), not cas() != 0: the field tracks presence, and "absent" is what distinguishes an
    // unconditional append from one requiring cas 0. Deriving it from the value cannot tell the
    // two apart, which is exactly the bug a mutation to the encoder would slip through.
    last.has_cas = request->has_cas();
    last.cas = request->cas();
    if (auto status = fail_for(request->key()); !status.ok()) {
      return status;
    }
    response->set_cas(0xa008ULL);
    set_token(response->mutable_mutation_token(), request->bucket_name(), 14U, 0xb008ULL, 208ULL);
    return grpc::Status::OK;
  }

  recorded_request last{};

  // The server outlives every case, so each one starts by wiping what the last left behind.
  // Without this a programmed status would leak into the next case and fail it somewhere
  // unrelated to its own subject.
  void reset_state()
  {
    last = recorded_request{};
    programmed = grpc::Status::OK;
  }

  // When set, every handler returns this instead of consulting the key. Lets a case hand the double
  // a status transcribed from the gateway without teaching the key-trigger table about it, and
  // keeps the catalogue (defined below, after this class) out of the double's name lookup.
  grpc::Status programmed{ grpc::Status::OK };

private:
  template<typename Request>
  void record(const Request* request)
  {
    last = recorded_request{};
    last.key = request->key();
    last.bucket = request->bucket_name();
    last.scope = request->scope_name();
    last.collection = request->collection_name();
  }

  template<typename Request>
  void record_expiry(const Request* request)
  {
    if (request->has_expiry_secs()) {
      last.expiry_secs = request->expiry_secs();
    }
    if (request->has_expiry_time()) {
      last.expiry_time_secs = request->expiry_time().seconds();
    }
  }

  template<typename Request>
  void record_counter(const Request* request)
  {
    last.delta = request->delta();
    if (request->has_initial()) {
      last.initial = request->initial();
    }
  }

  static void set_token(v1::MutationToken* token,
                        const std::string& bucket,
                        std::uint32_t vbucket_id,
                        std::uint64_t vbucket_uuid,
                        std::uint64_t seq_no)
  {
    token->set_bucket_name(bucket);
    token->set_vbucket_id(vbucket_id);
    token->set_vbucket_uuid(vbucket_uuid);
    token->set_seq_no(seq_no);
  }

  // Failures that are about the container rather than the document. Split out because Exists
  // reports a missing *document* as a successful false and must still fail on a missing bucket.
  [[nodiscard]] auto container_failure(const std::string& key) const -> grpc::Status
  {
    if (!programmed.ok()) {
      return programmed;
    }
    if (key == key_missing_bucket) {
      return resource_not_found("bucket");
    }
    if (key == key_missing_scope) {
      return resource_not_found("scope");
    }
    if (key == key_missing_collection) {
      return resource_not_found("collection");
    }
    return grpc::Status::OK;
  }

  [[nodiscard]] auto fail_for(const std::string& key) const -> grpc::Status
  {
    if (auto status = container_failure(key); !status.ok()) {
      return status;
    }
    if (key == key_missing) {
      return resource_not_found("document");
    }
    if (key == key_locked) {
      return precondition_failed("LOCKED");
    }
    if (key == key_not_locked) {
      return precondition_failed("NOT_LOCKED");
    }
    if (key == key_cas_mismatch) {
      return { grpc::StatusCode::ABORTED, "cas mismatch" };
    }
    return grpc::Status::OK;
  }
};

// A handle onto ONE process-wide in-process server and channel.
//
// Each case still writes `in_process_server server;` and gets a clean slate, but the gRPC server
// and channel underneath are built once for the whole binary and never torn down.
//
// This is not premature tidying, it is a workaround for a defect in gRPC. Every grpc::Channel takes
// a reference on gRPC's process-global callback completion queue on its first callback-API call and
// drops it when destroyed; when that count reaches zero gRPC shuts the queue down and joins its
// polling threads, and driving that repeatedly races those threads and aborts the whole process:
//
//     ref_counted.h:183]  assertion failed: prior > 0
//
// That is CXXCBC-919. A server-and-channel per case was measured aborting roughly one run in twenty
// once this suite grew to 38 cases -- and because it aborts the process rather than failing a case,
// it takes the entire binary down and tells you nothing about which case was running. Building the
// pair once removes the churn completely instead of making it rarer.
//
// The pair is deliberately leaked rather than held in a destructible static: letting it be torn
// down at exit would run exactly the teardown this avoids, at the least debuggable moment there is.
class in_process_server
{
public:
  in_process_server()
  {
    shared().service.reset_state();
  }
  in_process_server(const in_process_server&) = delete;
  in_process_server(in_process_server&&) = delete;
  auto operator=(const in_process_server&) -> in_process_server& = delete;
  auto operator=(in_process_server&&) -> in_process_server& = delete;
  ~in_process_server()
  {
    shared().service.reset_state();
  }
  [[nodiscard]] auto channel() -> std::shared_ptr<grpc::Channel>
  {
    return shared().channel;
  }
  [[nodiscard]] auto service() -> kv_surface_service&
  {
    return shared().service;
  }

private:
  struct shared_state {
    kv_surface_service service{};
    std::unique_ptr<grpc::Server> server{};
    std::shared_ptr<grpc::Channel> channel{};
  };

  [[nodiscard]] static auto shared() -> shared_state&
  {
    static shared_state* state = [] {
      auto* fresh = new shared_state{}; // NOLINT(cppcoreguidelines-owning-memory) -- see above
      grpc::ServerBuilder builder;
      builder.RegisterService(&fresh->service);
      fresh->server = builder.BuildAndStart();
      fresh->channel = fresh->server->InProcessChannel(grpc::ChannelArguments{});
      return fresh;
    }();
    return *state;
  }
};

[[nodiscard]] auto
make_id(std::string key) -> document_id
{
  return document_id{ "b", "s", "c", std::move(key) };
}

// Runs one operation to completion on the io_context and hands back its response. Every case needs
// the same five lines around execute(); factoring them out keeps each case about the expectation it
// exists to state.
template<typename Request>
[[nodiscard]] auto
run(in_process_server& server, Request request)
{
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  // Named fields, and the timeouts left at their defaults: component_config gains members as the
  // series progresses, and a positional list silently rebinds its tail into a new sub-object when
  // it does. Nothing here is about timeouts, so the defaults are what these cases want anyway.
  component_config config;
  config.channel = server.channel();
  config.credentials = cluster_credentials{};
  component comp{ io, config };

  typename Request::response_type outcome;
  comp.execute(std::move(request), [&](typename Request::response_type response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();
  return outcome;
}

// --- touch --------------------------------------------------------------------------------------

void
touch_encodes_a_relative_expiry_and_returns_the_cas()
{
  in_process_server server;
  ops::touch_request request;
  request.id = make_id("k1");
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "touch succeeded");
  assert_eq(outcome.cas.value(), 0xa001ULL, "cas round-trips");
  assert_true(server.service().last.expiry_secs.has_value(),
              "an expiry inside the relative window is sent as expiry_secs");
  assert_eq(server.service().last.expiry_secs.value(), 20U, "the expiry reaches the wire intact");
  assert_eq(server.service().last.key, std::string{ "k1" }, "the key reaches the wire");
}

void
touch_encodes_an_expiry_beyond_thirty_days_as_an_absolute_time()
{
  in_process_server server;
  ops::touch_request request;
  request.id = make_id("k1");
  // Above the 30-day cutoff the server would read a relative value as a unix timestamp in the past
  // and delete the document, so the client has to switch representations rather than pass it on.
  request.expiry = 60U * 60U * 24U * 31U;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "touch succeeded");
  assert_false(server.service().last.expiry_secs.has_value(),
               "a value past the cutoff is not sent as a relative expiry");
  assert_true(server.service().last.expiry_time_secs.has_value(),
              "it is sent as an absolute expiry_time instead");
}

void
touch_maps_a_missing_document_into_the_error_context()
{
  in_process_server server;
  ops::touch_request request;
  request.id = make_id(key_missing);
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "NOT_FOUND with resource_type=document surfaces as document_not_found");
}

// --- exists -------------------------------------------------------------------------------------

void
exists_reports_a_present_document()
{
  in_process_server server;
  ops::exists_request request;
  request.id = make_id("k1");

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "exists succeeded");
  assert_true(outcome.document_exists, "a present document is reported as existing");
  assert_eq(outcome.cas.value(), 0xa002ULL, "cas round-trips");
}

void
exists_reports_a_missing_document_as_a_success()
{
  in_process_server server;
  ops::exists_request request;
  request.id = make_id(key_missing);

  const auto outcome = run(server, std::move(request));

  // The whole point of exists is to answer the question without raising; a document_not_found here
  // would force every caller to treat the expected answer as an exceptional one.
  assert_false(static_cast<bool>(outcome.ctx.ec()),
               "a missing document is a successful exists, not an error");
  assert_false(outcome.document_exists, "and it is reported as not existing");
}

// --- get_and_lock -------------------------------------------------------------------------------

void
get_and_lock_encodes_the_lock_time_and_returns_the_value()
{
  in_process_server server;
  ops::get_and_lock_request request;
  request.id = make_id("k1");
  request.lock_time = 30;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "get_and_lock succeeded");
  assert_eq(cu::to_string(outcome.value), std::string{ "locked:k1" }, "value round-trips");
  assert_eq(outcome.cas.value(), 0xa003ULL, "cas round-trips");
  assert_eq(server.service().last.lock_time_secs, 30U, "the lock time reaches the wire");
}

void
get_and_lock_maps_a_locked_document_into_document_locked()
{
  in_process_server server;
  ops::get_and_lock_request request;
  request.id = make_id(key_locked);
  request.lock_time = 30;

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_locked,
              "a LOCKED precondition violation surfaces as document_locked");
}

void
get_and_lock_decompresses_compressed_content()
{
  in_process_server server;
  ops::get_and_lock_request request;
  request.id = make_id(key_compressed);
  request.lock_time = 30;

  const auto outcome = run(server, std::move(request));

  // The value has to come back intact: handing back the raw snappy frame, or an empty body, are
  // the two ways this goes wrong quietly.
  assert_false(static_cast<bool>(outcome.ctx.ec()),
               "get_and_lock with compressed content succeeded");
  assert_eq(cu::to_string(outcome.value), compressed_plaintext, "the body is decompressed");
}

// --- unlock -------------------------------------------------------------------------------------

void
unlock_sends_the_cas_and_completes()
{
  in_process_server server;
  ops::unlock_request request;
  request.id = make_id("k1");
  request.cas = couchbase::cas{ 0xfeedULL };

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "unlock succeeded");
  assert_eq(server.service().last.cas, 0xfeedULL, "the cas reaches the wire");
}

void
unlock_maps_a_cas_mismatch()
{
  in_process_server server;
  ops::unlock_request request;
  request.id = make_id(key_cas_mismatch);
  request.cas = couchbase::cas{ 0xfeedULL };

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::common::cas_mismatch,
              "ABORTED surfaces as cas_mismatch");
}

void
unlock_maps_a_document_that_is_not_locked()
{
  in_process_server server;
  ops::unlock_request request;
  request.id = make_id(key_not_locked);
  request.cas = couchbase::cas{ 0xfeedULL };

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_locked,
              "a NOT_LOCKED precondition violation surfaces as document_not_locked");
}

// --- get_and_touch ------------------------------------------------------------------------------

void
get_and_touch_encodes_the_expiry_and_returns_the_value()
{
  in_process_server server;
  ops::get_and_touch_request request;
  request.id = make_id("k1");
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "get_and_touch succeeded");
  assert_eq(cu::to_string(outcome.value), std::string{ "touched:k1" }, "value round-trips");
  assert_eq(outcome.cas.value(), 0xa004ULL, "cas round-trips");
  assert_true(server.service().last.expiry_secs.has_value(), "the expiry is sent as expiry_secs");
  assert_eq(server.service().last.expiry_secs.value(), 20U, "the expiry reaches the wire intact");
}

void
get_and_touch_decompresses_compressed_content()
{
  in_process_server server;
  ops::get_and_touch_request request;
  request.id = make_id(key_compressed);
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()),
               "get_and_touch with compressed content succeeded");
  assert_eq(cu::to_string(outcome.value), compressed_plaintext, "the body is decompressed");
}

// --- increment / decrement ----------------------------------------------------------------------

void
increment_encodes_the_delta_and_initial_and_returns_the_counter()
{
  in_process_server server;
  ops::increment_request request;
  request.id = make_id("k1");
  request.delta = 5;
  request.initial_value = 100;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "increment succeeded");
  assert_eq(outcome.content, std::uint64_t{ 42 }, "the counter value round-trips");
  assert_eq(outcome.cas.value(), 0xa005ULL, "cas round-trips");
  assert_eq(outcome.token.sequence_number(), 205ULL, "the mutation token round-trips");
  assert_eq(server.service().last.delta, std::uint64_t{ 5 }, "the delta reaches the wire");
  assert_true(server.service().last.initial.has_value(), "the initial value is sent when set");
  assert_eq(server.service().last.initial.value(),
            std::int64_t{ 100 },
            "the initial value reaches the wire");
}

void
increment_omits_the_initial_value_when_it_is_unset()
{
  in_process_server server;
  ops::increment_request request;
  request.id = make_id("k1");
  request.delta = 5;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "increment succeeded");
  // Sending initial=0 instead of leaving it unset would turn "fail if absent" into "create at
  // zero", which is a different operation from the caller's point of view.
  assert_false(server.service().last.initial.has_value(),
               "no initial value is sent when the caller did not ask for one");
}

void
decrement_encodes_the_delta_and_initial_and_returns_the_counter()
{
  in_process_server server;
  ops::decrement_request request;
  request.id = make_id("k1");
  request.delta = 3;
  request.initial_value = 50;

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "decrement succeeded");
  assert_eq(outcome.content, std::uint64_t{ 7 }, "the counter value round-trips");
  assert_eq(outcome.cas.value(), 0xa006ULL, "cas round-trips");
  assert_eq(outcome.token.sequence_number(), 206ULL, "the mutation token round-trips");
  assert_eq(server.service().last.delta, std::uint64_t{ 3 }, "the delta reaches the wire");
  assert_true(server.service().last.initial.has_value(), "the initial value is sent when set");
  assert_eq(server.service().last.initial.value(),
            std::int64_t{ 50 },
            "the initial value reaches the wire");
}

void
decrement_reaches_the_decrement_stub_and_not_increment()
{
  in_process_server server;
  ops::decrement_request request;
  request.id = make_id("k1");
  request.delta = 3;

  const auto outcome = run(server, std::move(request));

  // The two counter operations share their encoder and their decoder, so the only thing that makes
  // a decrement a decrement is which stub it is dispatched to. The double answers them with
  // different values precisely so that a mis-wiring is visible here.
  assert_eq(outcome.content, std::uint64_t{ 7 }, "the response came from the Decrement handler");
  assert_eq(outcome.cas.value(), 0xa006ULL, "and carries the Decrement handler's cas");
}

// --- append / prepend ---------------------------------------------------------------------------

void
append_sends_the_content_and_returns_the_cas_and_token()
{
  in_process_server server;
  ops::append_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("-suffix");

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "append succeeded");
  assert_eq(outcome.cas.value(), 0xa007ULL, "cas round-trips");
  assert_eq(outcome.token.sequence_number(), 207ULL, "the mutation token round-trips");
  assert_eq(
    server.service().last.content, std::string{ "-suffix" }, "the content reaches the wire");
}

void
append_omits_the_cas_when_the_caller_did_not_set_one()
{
  in_process_server server;
  ops::append_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("-suffix");

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "append succeeded");
  // A zero cas is not "cas zero", it is "no cas requirement". Sending it would turn an
  // unconditional append into one the server must match against, which no document can satisfy.
  assert_false(server.service().last.has_cas, "no cas is sent when the caller did not set one");
}

void
append_sends_the_cas_when_the_caller_set_one()
{
  in_process_server server;
  ops::append_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("-suffix");
  request.cas = couchbase::cas{ 0xbeefULL };

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "append succeeded");
  assert_true(server.service().last.has_cas, "a cas requirement is sent when set");
  assert_eq(server.service().last.cas, 0xbeefULL, "the cas reaches the wire intact");
}

void
append_maps_a_cas_mismatch()
{
  in_process_server server;
  ops::append_request request;
  request.id = make_id(key_cas_mismatch);
  request.value = cu::to_binary("-suffix");
  request.cas = couchbase::cas{ 0xbeefULL };

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::common::cas_mismatch,
              "ABORTED surfaces as cas_mismatch");
}

void
append_maps_a_missing_document()
{
  in_process_server server;
  ops::append_request request;
  request.id = make_id(key_missing);
  request.value = cu::to_binary("-suffix");

  const auto outcome = run(server, std::move(request));

  // Appending to a document that is not there is document_not_found, never document_exists -- the
  // two are easy to transpose because append and insert are both "create-ish" mutations.
  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "appending to a missing document is document_not_found");
}

void
prepend_sends_the_content_and_reaches_the_prepend_stub()
{
  in_process_server server;
  ops::prepend_request request;
  request.id = make_id("k1");
  request.value = cu::to_binary("prefix-");

  const auto outcome = run(server, std::move(request));

  assert_false(static_cast<bool>(outcome.ctx.ec()), "prepend succeeded");
  assert_eq(
    server.service().last.content, std::string{ "prefix-" }, "the content reaches the wire");
  // append and prepend differ only in the stub they reach; the distinct cas proves which answered.
  assert_eq(outcome.cas.value(), 0xa008ULL, "the response came from the Prepend handler");
  assert_eq(outcome.token.sequence_number(), 208ULL, "the mutation token round-trips");
}

// --- the gateway's own error catalogue --------------------------------------------------------
//
// Everything below is transcribed from the server rather than inferred from another SDK's tests.
// stellar-gateway's docs/ERRORS.md states that "All errors produced by CNG are centrally located
// within two files", one of which is gateway/dataimpl/server_v1/errorhandler.go; that file is
// therefore the specification, and gateway/dataimpl/server_v1/kvserver.go says which of those
// errors each RPC can actually produce.
//
// Reproducing the status exactly matters. Our mapping keys on the packed google.rpc detail, and
// the gateway does not use one detail type uniformly: a CAS mismatch travels as ErrorInfo while a
// locked document travels as PreconditionFailure. A test that sent a bare status code would take a
// different branch of map_status() than production ever does.

enum class gateway_error {
  doc_missing,
  doc_cas_mismatch,
  doc_locked,
  doc_not_locked,
  doc_not_numeric,
  value_too_large,
  durability_impossible,
  sync_write_ambiguous,
  scope_missing,
  collection_missing,
  collection_no_read_access,
  collection_no_write_access,
  generic,
};

// The document key that steers the double into each status.
[[nodiscard]] auto
gateway_key(gateway_error which) -> const char*
{
  switch (which) {
    case gateway_error::doc_missing:
      return "gw-doc-missing";
    case gateway_error::doc_cas_mismatch:
      return "gw-cas-mismatch";
    case gateway_error::doc_locked:
      return "gw-locked";
    case gateway_error::doc_not_locked:
      return "gw-not-locked";
    case gateway_error::doc_not_numeric:
      return "gw-not-numeric";
    case gateway_error::value_too_large:
      return "gw-value-too-large";
    case gateway_error::durability_impossible:
      return "gw-durability-impossible";
    case gateway_error::sync_write_ambiguous:
      return "gw-sync-write-ambiguous";
    case gateway_error::scope_missing:
      return "gw-scope-missing";
    case gateway_error::collection_missing:
      return "gw-collection-missing";
    case gateway_error::collection_no_read_access:
      return "gw-no-read-access";
    case gateway_error::collection_no_write_access:
      return "gw-no-write-access";
    case gateway_error::generic:
      return "gw-generic";
  }
  return "gw-unknown";
}

// One constructor from errorhandler.go each, reproduced field for field.
[[nodiscard]] auto
gateway_status(gateway_error which) -> grpc::Status
{
  switch (which) {
    case gateway_error::doc_missing: // NewDocMissingStatus
      return resource_not_found("document");
    case gateway_error::scope_missing: // NewScopeMissingStatus
      return resource_not_found("scope");
    case gateway_error::collection_missing: // NewCollectionMissingStatus
      return resource_not_found("collection");
    case gateway_error::doc_cas_mismatch: { // NewDocCasMismatchStatus
      // ErrorInfo, not PreconditionFailure -- the one place the gateway uses that detail type for
      // a KV error the client has to distinguish.
      google::rpc::ErrorInfo info;
      info.set_reason("CAS_MISMATCH");
      return rich_status(grpc::StatusCode::ABORTED, "cas did not match", info);
    }
    case gateway_error::doc_locked: // NewDocLockedStatus
      return precondition_failed("LOCKED");
    case gateway_error::doc_not_locked: // NewDocNotLockedStatus
      return precondition_failed("NOT_LOCKED");
    case gateway_error::doc_not_numeric: // NewDocNotNumericStatus
      return precondition_failed("DOC_NOT_NUMERIC");
    case gateway_error::value_too_large: // NewValueTooLargeStatus
      return precondition_failed("VALUE_TOO_LARGE");
    case gateway_error::durability_impossible: // NewDurabilityImpossibleStatus
      return precondition_failed("DURABILITY_IMPOSSIBLE");
    case gateway_error::sync_write_ambiguous: // NewSyncWriteAmbiguousStatus
      // Deliberately detail-free: the constructor attaches nothing, so the client has only the
      // code to work from.
      return { grpc::StatusCode::DEADLINE_EXCEEDED, "sync write timed out" };
    case gateway_error::collection_no_read_access:    // NewCollectionNoReadAccessStatus
    case gateway_error::collection_no_write_access: { // NewCollectionNoWriteAccessStatus
      google::rpc::ResourceInfo info;
      info.set_resource_type("collection");
      info.set_resource_name("b/s/c");
      return rich_status(grpc::StatusCode::PERMISSION_DENIED, "no access", info);
    }
    case gateway_error::generic: // NewGenericStatus, context.Canceled arm
      return { grpc::StatusCode::CANCELLED, "The request was cancelled." };
  }
  return grpc::Status::OK;
}

// What the SDK must report for each. `mutating` only matters for the detail-free
// DEADLINE_EXCEEDED: a read cannot have changed anything, so calling it ambiguous would be wrong.
[[nodiscard]] auto
gateway_expectation(gateway_error which, bool mutating) -> std::error_code
{
  switch (which) {
    case gateway_error::doc_missing:
      return couchbase::errc::key_value::document_not_found;
    case gateway_error::scope_missing:
      return couchbase::errc::common::scope_not_found;
    case gateway_error::collection_missing:
      return couchbase::errc::common::collection_not_found;
    case gateway_error::doc_cas_mismatch:
      return couchbase::errc::common::cas_mismatch;
    case gateway_error::doc_locked:
      return couchbase::errc::key_value::document_locked;
    case gateway_error::doc_not_locked:
      return couchbase::errc::key_value::document_not_locked;
    case gateway_error::doc_not_numeric:
      return couchbase::errc::key_value::delta_invalid;
    case gateway_error::value_too_large:
      return couchbase::errc::key_value::value_too_large;
    case gateway_error::durability_impossible:
      return couchbase::errc::key_value::durability_impossible;
    case gateway_error::sync_write_ambiguous:
      return mutating ? couchbase::errc::common::ambiguous_timeout
                      : couchbase::errc::common::unambiguous_timeout;
    case gateway_error::collection_no_read_access:
    case gateway_error::collection_no_write_access:
      return couchbase::errc::common::authentication_failure;
    case gateway_error::generic:
      return couchbase::errc::common::request_canceled;
  }
  return {};
}

// Drives one operation once per status its RPC can actually return, and checks the mapping. The
// applicable set per operation is taken from kvserver.go, so a case covers exactly what the server
// can do to it -- no more (which would test fiction) and no less.
template<typename MakeRequest>
void
check_gateway_errors(std::initializer_list<gateway_error> applicable,
                     bool mutating,
                     MakeRequest make_request)
{
  // One server and one channel for the whole loop, deliberately.
  //
  // Every grpc::Channel takes a reference on gRPC's process-global callback completion queue on its
  // first callback-API call and drops it when destroyed; when that count reaches zero gRPC shuts
  // the queue down and joins its polling threads, and driving that repeatedly races those threads
  // and aborts the process with `ref_counted.h: assertion failed: prior > 0` (CXXCBC-919 -- a
  // defect in gRPC, not here). A channel per status would multiply this suite's channel count by
  // the size of each set, ten for append alone, which is enough to hit it regularly; it was hit, at
  // commit 25, before this loop shared its channel. One channel per case also runs faster.
  in_process_server server;
  auto channel = server.channel();

  for (const auto which : applicable) {
    server.service().programmed = gateway_status(which);

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    component_config config;
    config.channel = channel;
    config.credentials = cluster_credentials{};
    component comp{ io, config };

    auto request = make_request(gateway_key(which));
    using response_type = typename decltype(request)::response_type;
    response_type outcome;
    comp.execute(std::move(request), [&](response_type response) {
      outcome = std::move(response);
      work.reset();
    });
    io.run();

    assert_true(outcome.ctx.ec() == gateway_expectation(which, mutating),
                std::string{ gateway_key(which) } +
                  " maps as the gateway intends, got: " + outcome.ctx.ec().message());
  }
}

void
touch_maps_every_status_the_gateway_can_return()
{
  // kvserver.go Touch: CollectionMissing, CollectionNoWriteAccess, DocLocked, DocMissing, Generic,
  // ScopeMissing. Touch takes the *write* access path even though it changes only the expiry.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::generic,
                         gateway_error::scope_missing },
                       true,
                       [](const char* key) {
                         ops::touch_request request;
                         request.id = make_id(key);
                         request.expiry = 20;
                         return request;
                       });
}

void
exists_maps_every_status_the_gateway_can_return()
{
  // kvserver.go Exists: CollectionMissing, CollectionNoReadAccess, Generic, ScopeMissing -- and
  // notably NOT DocMissing. The server cannot report a missing document as an error here, which is
  // the contract exists_reports_a_missing_document_as_a_success relies on.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_read_access,
                         gateway_error::generic,
                         gateway_error::scope_missing },
                       false,
                       [](const char* key) {
                         ops::exists_request request;
                         request.id = make_id(key);
                         return request;
                       });
}

void
get_and_lock_maps_every_status_the_gateway_can_return()
{
  // kvserver.go GetAndLock: CollectionMissing, CollectionNoReadAccess, DocLocked, DocMissing,
  // Generic, ScopeMissing. DocLocked here is the double-lock case -- locking an already-locked
  // document is refused rather than queued.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_read_access,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::generic,
                         gateway_error::scope_missing },
                       true,
                       [](const char* key) {
                         ops::get_and_lock_request request;
                         request.id = make_id(key);
                         request.lock_time = 30;
                         return request;
                       });
}

void
unlock_maps_every_status_the_gateway_can_return()
{
  // kvserver.go Unlock: CollectionMissing, CollectionNoWriteAccess, DocCasMismatch, DocMissing,
  // DocNotLocked, Generic, ScopeMissing. There is deliberately no DocLocked in that set -- see
  // unlock_reports_a_bad_cas_as_cas_mismatch_not_locked below.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_cas_mismatch,
                         gateway_error::doc_missing,
                         gateway_error::doc_not_locked,
                         gateway_error::generic,
                         gateway_error::scope_missing },
                       true,
                       [](const char* key) {
                         ops::unlock_request request;
                         request.id = make_id(key);
                         request.cas = couchbase::cas{ 0xfeedULL };
                         return request;
                       });
}

void
get_and_touch_maps_every_status_the_gateway_can_return()
{
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_read_access,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::generic,
                         gateway_error::scope_missing },
                       true,
                       [](const char* key) {
                         ops::get_and_touch_request request;
                         request.id = make_id(key);
                         request.expiry = 20;
                         return request;
                       });
}

void
increment_maps_every_status_the_gateway_can_return()
{
  // kvserver.go Increment adds DocNotNumeric, DurabilityImpossible and SyncWriteAmbiguous, and has
  // no CasMismatch -- IncrementRequest carries no cas field to mismatch against.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::doc_not_numeric,
                         gateway_error::durability_impossible,
                         gateway_error::generic,
                         gateway_error::scope_missing,
                         gateway_error::sync_write_ambiguous },
                       true,
                       [](const char* key) {
                         ops::increment_request request;
                         request.id = make_id(key);
                         request.delta = 1;
                         return request;
                       });
}

void
decrement_maps_every_status_the_gateway_can_return()
{
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::doc_not_numeric,
                         gateway_error::durability_impossible,
                         gateway_error::generic,
                         gateway_error::scope_missing,
                         gateway_error::sync_write_ambiguous },
                       true,
                       [](const char* key) {
                         ops::decrement_request request;
                         request.id = make_id(key);
                         request.delta = 1;
                         return request;
                       });
}

void
append_maps_every_status_the_gateway_can_return()
{
  // kvserver.go Append/Prepend are the only two of these nine that can return both CasMismatch and
  // ValueTooLarge, and neither can return DocNotNumeric.
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_cas_mismatch,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::durability_impossible,
                         gateway_error::generic,
                         gateway_error::scope_missing,
                         gateway_error::sync_write_ambiguous,
                         gateway_error::value_too_large },
                       true,
                       [](const char* key) {
                         ops::append_request request;
                         request.id = make_id(key);
                         request.value = cu::to_binary("-suffix");
                         return request;
                       });
}

void
prepend_maps_every_status_the_gateway_can_return()
{
  check_gateway_errors({ gateway_error::collection_missing,
                         gateway_error::collection_no_write_access,
                         gateway_error::doc_cas_mismatch,
                         gateway_error::doc_locked,
                         gateway_error::doc_missing,
                         gateway_error::durability_impossible,
                         gateway_error::generic,
                         gateway_error::scope_missing,
                         gateway_error::sync_write_ambiguous,
                         gateway_error::value_too_large },
                       true,
                       [](const char* key) {
                         ops::prepend_request request;
                         request.id = make_id(key);
                         request.value = cu::to_binary("prefix-");
                         return request;
                       });
}

// --- the contested corners, each pinned with its source ---------------------------------------

void
unlock_reports_a_bad_cas_as_cas_mismatch_not_locked()
{
  // The classic protocol makes this opcode-dependent: core/protocol/status.cxx maps status
  // `locked` to cas_mismatch when the opcode is `unlock`, and to document_locked otherwise. The
  // gateway reaches the same answer a different way -- kvserver.go Unlock translates
  // memdx.ErrCasMismatch into NewDocCasMismatchStatus and never emits DocLocked at all -- so both
  // transports agree, and a caller unlocking with a stale cas sees cas_mismatch on either.
  in_process_server server;
  server.service().programmed = gateway_status(gateway_error::doc_cas_mismatch);
  ops::unlock_request request;
  request.id = make_id(gateway_key(gateway_error::doc_cas_mismatch));
  request.cas = couchbase::cas{ 0xfeedULL };

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::common::cas_mismatch,
              "a stale cas on unlock is cas_mismatch, matching the classic transport");
  assert_true(outcome.ctx.ec() != couchbase::errc::key_value::document_locked,
              "and specifically not document_locked");
}

void
a_cas_mismatch_arrives_as_error_info_not_precondition_failure()
{
  // NewDocCasMismatchStatus packs ErrorInfo{Reason: "CAS_MISMATCH"}; every other contested KV
  // error packs PreconditionFailure. A mapping that only ever looked at PreconditionFailure would
  // still be correct here by accident, because ABORTED alone is enough -- this case exists so that
  // the accident is deliberate and stays true if the detail handling is reworked.
  in_process_server server;
  server.service().programmed = gateway_status(gateway_error::doc_cas_mismatch);
  ops::append_request request;
  request.id = make_id(gateway_key(gateway_error::doc_cas_mismatch));
  request.value = cu::to_binary("-suffix");

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::common::cas_mismatch,
              "an ErrorInfo-bearing ABORTED still maps to cas_mismatch");
}

void
a_locked_document_is_reported_so_the_retry_layer_can_see_it()
{
  // The contested one. The classic transport does not surface a locked document to the caller at
  // all: core/bucket.cxx and core/io/mcbp_command.hxx raise retry_reason::key_value_locked, which
  // core/impl/retry_reason.cxx lists under both always_retry and allows_non_idempotent_retry, so
  // the operation is retried until the lock clears or the deadline does.
  //
  // The component deliberately does NOT retry here: it reports document_locked, and cluster.cxx
  // turns that into retry_reason::key_value_locked (via protostellar::retry_reason_for) one layer
  // up, which is where the retry strategy lives for every other retryable couchbase2 error too.
  // Retrying inside the component would put a second, invisible retry loop underneath that one.
  //
  // So this assertion is about the contract between the two layers, not about what a user sees.
  in_process_server server;
  server.service().programmed = gateway_status(gateway_error::doc_locked);
  ops::touch_request request;
  request.id = make_id(gateway_key(gateway_error::doc_locked));
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::key_value::document_locked,
              "the component reports document_locked rather than absorbing it");
}

void
a_detail_free_deadline_is_ambiguous_for_a_mutation_and_not_for_a_read()
{
  // NewSyncWriteAmbiguousStatus attaches no details at all, so the code is the only signal. A
  // mutation that timed out may already have been applied; a read cannot have changed anything, so
  // reporting it as ambiguous would claim it might have mutated state it never touched.
  in_process_server mutating_server;
  mutating_server.service().programmed = gateway_status(gateway_error::sync_write_ambiguous);
  ops::touch_request touch;
  touch.id = make_id(gateway_key(gateway_error::sync_write_ambiguous));
  touch.expiry = 20;
  const auto mutation = run(mutating_server, std::move(touch));
  assert_true(mutation.ctx.ec() == couchbase::errc::common::ambiguous_timeout,
              "a timed-out mutation is ambiguous");

  in_process_server reading_server;
  reading_server.service().programmed = gateway_status(gateway_error::sync_write_ambiguous);
  ops::exists_request exists;
  exists.id = make_id(gateway_key(gateway_error::sync_write_ambiguous));
  const auto read = run(reading_server, std::move(exists));
  assert_true(read.ctx.ec() == couchbase::errc::common::unambiguous_timeout,
              "a timed-out read is unambiguous");
}

void
an_unrecognised_server_error_is_reported_as_cancelled()
{
  // Surprising, and worth pinning precisely because it is surprising. NewGenericStatus is the
  // gateway's fallback for anything it cannot classify, and its context.Canceled arm returns
  // codes.Canceled -- so a server-side failure the gateway does not recognise reaches the client
  // as request_canceled rather than as internal_server_failure.
  in_process_server server;
  server.service().programmed = gateway_status(gateway_error::generic);
  ops::touch_request request;
  request.id = make_id(gateway_key(gateway_error::generic));
  request.expiry = 20;

  const auto outcome = run(server, std::move(request));

  assert_true(outcome.ctx.ec() == couchbase::errc::common::request_canceled,
              "the gateway's generic fallback surfaces as request_canceled");
}

// --- resource-type routing ------------------------------------------------------------------

void
a_not_found_resource_type_selects_the_specific_error()
{
  // NOT_FOUND alone cannot say what was missing. RFC 77 puts that in the ResourceInfo detail, and
  // collapsing all four onto document_not_found would tell an operator with a typo in a bucket name
  // that their document is missing.
  struct expectation {
    const char* key;
    std::error_code expected;
    const char* what;
  };
  const std::array<expectation, 4> cases{ {
    { key_missing_bucket, couchbase::errc::common::bucket_not_found, "bucket" },
    { key_missing_scope, couchbase::errc::common::scope_not_found, "scope" },
    { key_missing_collection, couchbase::errc::common::collection_not_found, "collection" },
    { key_missing, couchbase::errc::key_value::document_not_found, "document" },
  } };

  for (const auto& c : cases) {
    in_process_server server;
    ops::touch_request request;
    request.id = make_id(c.key);
    request.expiry = 20;

    const auto outcome = run(server, std::move(request));

    assert_true(outcome.ctx.ec() == c.expected,
                std::string{ "resource_type=" } + c.what + " selects its own error");
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_component_kv_operations",
    {
      { "touch_encodes_a_relative_expiry_and_returns_the_cas",
        touch_encodes_a_relative_expiry_and_returns_the_cas,
        timeout::network },
      { "touch_encodes_an_expiry_beyond_thirty_days_as_an_absolute_time",
        touch_encodes_an_expiry_beyond_thirty_days_as_an_absolute_time,
        timeout::network },
      { "touch_maps_a_missing_document_into_the_error_context",
        touch_maps_a_missing_document_into_the_error_context,
        timeout::network },
      { "exists_reports_a_present_document", exists_reports_a_present_document, timeout::network },
      { "exists_reports_a_missing_document_as_a_success",
        exists_reports_a_missing_document_as_a_success,
        timeout::network },
      { "get_and_lock_encodes_the_lock_time_and_returns_the_value",
        get_and_lock_encodes_the_lock_time_and_returns_the_value,
        timeout::network },
      { "get_and_lock_maps_a_locked_document_into_document_locked",
        get_and_lock_maps_a_locked_document_into_document_locked,
        timeout::network },
      { "get_and_lock_decompresses_compressed_content",
        get_and_lock_decompresses_compressed_content,
        timeout::network },
      { "unlock_sends_the_cas_and_completes",
        unlock_sends_the_cas_and_completes,
        timeout::network },
      { "unlock_maps_a_cas_mismatch", unlock_maps_a_cas_mismatch, timeout::network },
      { "unlock_maps_a_document_that_is_not_locked",
        unlock_maps_a_document_that_is_not_locked,
        timeout::network },
      { "get_and_touch_encodes_the_expiry_and_returns_the_value",
        get_and_touch_encodes_the_expiry_and_returns_the_value,
        timeout::network },
      { "get_and_touch_decompresses_compressed_content",
        get_and_touch_decompresses_compressed_content,
        timeout::network },
      { "increment_encodes_the_delta_and_initial_and_returns_the_counter",
        increment_encodes_the_delta_and_initial_and_returns_the_counter,
        timeout::network },
      { "increment_omits_the_initial_value_when_it_is_unset",
        increment_omits_the_initial_value_when_it_is_unset,
        timeout::network },
      { "decrement_encodes_the_delta_and_initial_and_returns_the_counter",
        decrement_encodes_the_delta_and_initial_and_returns_the_counter,
        timeout::network },
      { "decrement_reaches_the_decrement_stub_and_not_increment",
        decrement_reaches_the_decrement_stub_and_not_increment,
        timeout::network },
      { "append_sends_the_content_and_returns_the_cas_and_token",
        append_sends_the_content_and_returns_the_cas_and_token,
        timeout::network },
      { "append_omits_the_cas_when_the_caller_did_not_set_one",
        append_omits_the_cas_when_the_caller_did_not_set_one,
        timeout::network },
      { "append_sends_the_cas_when_the_caller_set_one",
        append_sends_the_cas_when_the_caller_set_one,
        timeout::network },
      { "append_maps_a_cas_mismatch", append_maps_a_cas_mismatch, timeout::network },
      { "append_maps_a_missing_document", append_maps_a_missing_document, timeout::network },
      { "prepend_sends_the_content_and_reaches_the_prepend_stub",
        prepend_sends_the_content_and_reaches_the_prepend_stub,
        timeout::network },
      { "a_not_found_resource_type_selects_the_specific_error",
        a_not_found_resource_type_selects_the_specific_error,
        timeout::network },
      { "touch_maps_every_status_the_gateway_can_return",
        touch_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "exists_maps_every_status_the_gateway_can_return",
        exists_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "get_and_lock_maps_every_status_the_gateway_can_return",
        get_and_lock_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "unlock_maps_every_status_the_gateway_can_return",
        unlock_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "get_and_touch_maps_every_status_the_gateway_can_return",
        get_and_touch_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "increment_maps_every_status_the_gateway_can_return",
        increment_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "decrement_maps_every_status_the_gateway_can_return",
        decrement_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "append_maps_every_status_the_gateway_can_return",
        append_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "prepend_maps_every_status_the_gateway_can_return",
        prepend_maps_every_status_the_gateway_can_return,
        timeout::network },
      { "unlock_reports_a_bad_cas_as_cas_mismatch_not_locked",
        unlock_reports_a_bad_cas_as_cas_mismatch_not_locked,
        timeout::network },
      { "a_cas_mismatch_arrives_as_error_info_not_precondition_failure",
        a_cas_mismatch_arrives_as_error_info_not_precondition_failure,
        timeout::network },
      { "a_locked_document_is_reported_so_the_retry_layer_can_see_it",
        a_locked_document_is_reported_so_the_retry_layer_can_see_it,
        timeout::network },
      { "a_detail_free_deadline_is_ambiguous_for_a_mutation_and_not_for_a_read",
        a_detail_free_deadline_is_ambiguous_for_a_mutation_and_not_for_a_read,
        timeout::network },
      { "an_unrecognised_server_error_is_reported_as_cancelled",
        an_unrecognised_server_error_is_reported_as_cancelled,
        timeout::network },
    },
  };
}

} // namespace couchbase::test
