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

// Unit tests for Protostellar error mapping (CXXCBC-893). Pure, no server (env-agnostic).

#include "framework/test_runner.hxx"

#include "core/protostellar/error_utils.hxx"

#include "core/document_id.hxx"
#include "core/error_context/key_value_error_context.hxx"

#include <couchbase/error_codes.hxx>

#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>

#include <string>
#include <system_error>

namespace couchbase::cng::test
{
namespace
{
namespace ps = ::couchbase::core::protostellar;

// Builds a grpc::Status whose serialized google.rpc.Status carries one packed detail block.
template<typename Detail>
auto
status_with_detail(grpc::StatusCode code, const Detail& detail) -> grpc::Status
{
  google::rpc::Status rich;
  rich.set_code(static_cast<std::int32_t>(code));
  if (!rich.add_details()->PackFrom(detail)) {
    throw std::runtime_error("status_with_detail: PackFrom failed");
  }
  return grpc::Status{ code, "generic grpc message", rich.SerializeAsString() };
}

auto
precondition(const std::string& type) -> google::rpc::PreconditionFailure
{
  google::rpc::PreconditionFailure failure;
  failure.add_violations()->set_type(type);
  return failure;
}

auto
resource(const std::string& resource_type) -> google::rpc::ResourceInfo
{
  google::rpc::ResourceInfo info;
  info.set_resource_type(resource_type);
  return info;
}

void
ok_maps_to_success()
{
  assert_false(
    static_cast<bool>(ps::map_status_code(grpc::StatusCode::OK, ps::operation_kind::mutating)),
    "OK is not an error");
}

void
status_codes_map_to_expected_errc()
{
  assert_true(ps::map_status_code(grpc::StatusCode::NOT_FOUND, ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_not_found,
              "NOT_FOUND -> document_not_found");
  assert_true(ps::map_status_code(grpc::StatusCode::ALREADY_EXISTS, ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_exists,
              "ALREADY_EXISTS -> document_exists");
  assert_true(ps::map_status_code(grpc::StatusCode::ABORTED, ps::operation_kind::mutating) ==
                couchbase::errc::common::cas_mismatch,
              "ABORTED -> cas_mismatch");
  assert_true(
    ps::map_status_code(grpc::StatusCode::DEADLINE_EXCEEDED, ps::operation_kind::mutating) ==
      couchbase::errc::common::ambiguous_timeout,
    "DEADLINE_EXCEEDED -> ambiguous_timeout (a mutation may have applied)");
  assert_true(ps::map_status_code(grpc::StatusCode::CANCELLED, ps::operation_kind::mutating) ==
                couchbase::errc::common::request_canceled,
              "CANCELLED -> request_canceled");
  assert_true(
    ps::map_status_code(grpc::StatusCode::UNAUTHENTICATED, ps::operation_kind::mutating) ==
      couchbase::errc::common::authentication_failure,
    "UNAUTHENTICATED -> authentication_failure");
  assert_true(ps::map_status_code(grpc::StatusCode::UNIMPLEMENTED, ps::operation_kind::mutating) ==
                couchbase::errc::common::feature_not_available,
              "UNIMPLEMENTED -> feature_not_available");
  assert_true(ps::map_status_code(grpc::StatusCode::UNAVAILABLE, ps::operation_kind::mutating) ==
                couchbase::errc::common::temporary_failure,
              "UNAVAILABLE -> temporary_failure");
  assert_true(ps::map_status_code(grpc::StatusCode::INTERNAL, ps::operation_kind::mutating) ==
                couchbase::errc::common::internal_server_failure,
              "INTERNAL -> internal_server_failure");
}

// DEADLINE_EXCEEDED is the one code whose mapping depends on what the operation does. Reporting a
// timed-out read as ambiguous claims the operation may have changed state it cannot have touched,
// and a caller that refuses to replay ambiguous operations would then give up on one that is
// perfectly safe to repeat.
//
// The distinction is read-only-ness, not idempotency: the two disagree precisely where it matters,
// since an upsert is idempotent and still mutates. Asserted through every layer that carries the
// bit, so none of them can quietly drop it.
void
deadline_exceeded_depends_on_the_operation_kind()
{
  assert_true(
    ps::map_status_code(grpc::StatusCode::DEADLINE_EXCEEDED, ps::operation_kind::read_only) ==
      couchbase::errc::common::unambiguous_timeout,
    "a timed-out read changed nothing, so it is unambiguous");
  assert_true(
    ps::map_status_code(grpc::StatusCode::DEADLINE_EXCEEDED, ps::operation_kind::mutating) ==
      couchbase::errc::common::ambiguous_timeout,
    "a timed-out mutation may have applied, so it stays ambiguous");

  const grpc::Status expired{ grpc::StatusCode::DEADLINE_EXCEEDED, "deadline exceeded" };
  assert_true(ps::map_status(expired, ps::operation_kind::read_only) ==
                couchbase::errc::common::unambiguous_timeout,
              "map_status carries the kind through to the code");
  assert_true(ps::map_status(expired, ps::operation_kind::mutating) ==
                couchbase::errc::common::ambiguous_timeout,
              "map_status carries the kind through to the code");

  const couchbase::core::document_id id{ "b", "s", "c", "k" };
  assert_true(ps::make_error_context(expired, id, ps::operation_kind::read_only).ec() ==
                couchbase::errc::common::unambiguous_timeout,
              "make_error_context carries the kind through to the context");
  assert_true(ps::make_error_context(expired, id, ps::operation_kind::mutating).ec() ==
                couchbase::errc::common::ambiguous_timeout,
              "make_error_context carries the kind through to the context");
}

void
map_status_uses_the_status_code()
{
  const grpc::Status status{ grpc::StatusCode::NOT_FOUND, "missing" };
  assert_true(ps::map_status(status, ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_not_found,
              "map_status keys on the code");
}

void
precondition_details_map_to_specific_kv_errors()
{
  assert_true(ps::map_status(
                status_with_detail(grpc::StatusCode::FAILED_PRECONDITION, precondition("LOCKED")),
                ps::operation_kind::mutating) == couchbase::errc::key_value::document_locked,
              "PreconditionFailure LOCKED -> document_locked");
  assert_true(ps::map_status(status_with_detail(grpc::StatusCode::FAILED_PRECONDITION,
                                                precondition("NOT_LOCKED")),
                             ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_not_locked,
              "NOT_LOCKED -> document_not_locked");
  assert_true(ps::map_status(status_with_detail(grpc::StatusCode::FAILED_PRECONDITION,
                                                precondition("DOC_NOT_JSON")),
                             ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_not_json,
              "DOC_NOT_JSON -> document_not_json");
  assert_true(ps::map_status(status_with_detail(grpc::StatusCode::FAILED_PRECONDITION,
                                                precondition("PATH_VALUE_OUT_OF_RANGE")),
                             ps::operation_kind::mutating) ==
                couchbase::errc::key_value::number_too_big,
              "PATH_VALUE_OUT_OF_RANGE -> number_too_big");
  assert_true(ps::map_status(status_with_detail(grpc::StatusCode::FAILED_PRECONDITION,
                                                precondition("VALUE_TOO_LARGE")),
                             ps::operation_kind::mutating) ==
                couchbase::errc::key_value::value_too_large,
              "VALUE_TOO_LARGE -> value_too_large");

  // FAILED_PRECONDITION with no (recognized) detail falls back to internal_server_failure.
  const grpc::Status bare{ grpc::StatusCode::FAILED_PRECONDITION, "no details" };
  assert_true(ps::map_status(bare, ps::operation_kind::mutating) ==
                couchbase::errc::common::internal_server_failure,
              "bare FAILED_PRECONDITION -> internal_server_failure");
}

void
resource_info_selects_typed_not_found_and_exists()
{
  assert_true(ps::map_status(status_with_detail(grpc::StatusCode::NOT_FOUND, resource("bucket")),
                             ps::operation_kind::mutating) ==
                couchbase::errc::common::bucket_not_found,
              "NOT_FOUND + bucket -> bucket_not_found");
  assert_true(
    ps::map_status(status_with_detail(grpc::StatusCode::NOT_FOUND, resource("collection")),
                   ps::operation_kind::mutating) == couchbase::errc::common::collection_not_found,
    "NOT_FOUND + collection -> collection_not_found");
  assert_true(
    ps::map_status(status_with_detail(grpc::StatusCode::NOT_FOUND, resource("queryindex")),
                   ps::operation_kind::mutating) == couchbase::errc::common::index_not_found,
    "NOT_FOUND + queryindex -> index_not_found");
  assert_true(
    ps::map_status(status_with_detail(grpc::StatusCode::ALREADY_EXISTS, resource("bucket")),
                   ps::operation_kind::mutating) == couchbase::errc::management::bucket_exists,
    "ALREADY_EXISTS + bucket -> bucket_exists");
  assert_true(
    ps::map_status(status_with_detail(grpc::StatusCode::ALREADY_EXISTS, resource("scope")),
                   ps::operation_kind::mutating) == couchbase::errc::management::scope_exists,
    "ALREADY_EXISTS + scope -> scope_exists");

  // Without a ResourceInfo, NOT_FOUND/ALREADY_EXISTS stay document-scoped (backward compatible).
  const grpc::Status bare{ grpc::StatusCode::NOT_FOUND, "no details" };
  assert_true(ps::map_status(bare, ps::operation_kind::mutating) ==
                couchbase::errc::key_value::document_not_found,
              "bare NOT_FOUND -> document_not_found");
}

void
error_message_prefers_rich_details()
{
  google::rpc::Status rich;
  rich.set_code(static_cast<std::int32_t>(grpc::StatusCode::NOT_FOUND));
  rich.set_message("document 'foo' not found in collection");
  const grpc::Status with_details{ grpc::StatusCode::NOT_FOUND,
                                   "generic grpc message",
                                   rich.SerializeAsString() };
  assert_eq(ps::error_message(with_details),
            std::string{ "document 'foo' not found in collection" },
            "rich google.rpc.Status message wins");

  const grpc::Status without_details{ grpc::StatusCode::NOT_FOUND, "generic grpc message" };
  assert_eq(ps::error_message(without_details),
            std::string{ "generic grpc message" },
            "falls back to the gRPC message");
}

void
make_error_context_maps_code_and_attaches_message()
{
  const couchbase::core::document_id id{ "bucket", "scope", "collection", "key" };

  const grpc::Status failure{ grpc::StatusCode::NOT_FOUND, "document 'key' not found" };
  const auto ctx = ps::make_error_context(failure, id, ps::operation_kind::mutating);
  assert_true(ctx.ec() == couchbase::errc::key_value::document_not_found,
              "make_error_context maps the status code");
  assert_eq(ctx.id(), std::string{ "key" }, "carries the document key");
  assert_eq(ctx.bucket(), std::string{ "bucket" }, "carries the bucket");
  assert_true(ctx.extended_error_info().has_value(), "failure attaches extended error info");
  assert_eq(ctx.extended_error_info()->context(),
            std::string{ "document 'key' not found" },
            "server message lands in the error context");

  const grpc::Status ok{};
  const auto ok_ctx = ps::make_error_context(ok, id, ps::operation_kind::mutating);
  assert_false(static_cast<bool>(ok_ctx.ec()), "OK yields a success context");
  assert_false(ok_ctx.extended_error_info().has_value(), "success carries no extended error info");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_error_utils",
    {
      { "ok_maps_to_success", ok_maps_to_success },
      { "status_codes_map_to_expected_errc", status_codes_map_to_expected_errc },
      { "map_status_uses_the_status_code", map_status_uses_the_status_code },
      { "deadline_exceeded_depends_on_the_operation_kind",
        deadline_exceeded_depends_on_the_operation_kind },
      { "precondition_details_map_to_specific_kv_errors",
        precondition_details_map_to_specific_kv_errors },
      { "resource_info_selects_typed_not_found_and_exists",
        resource_info_selects_typed_not_found_and_exists },
      { "error_message_prefers_rich_details", error_message_prefers_rich_details },
      { "make_error_context_maps_code_and_attaches_message",
        make_error_context_maps_code_and_attaches_message },
    },
  };
}

} // namespace couchbase::cng::test
