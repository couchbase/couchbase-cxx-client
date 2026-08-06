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

#include "core/protostellar/error_utils.hxx"

#include "core/document_id.hxx"
#include "core/error_context/key_value_error_context.hxx"

#include <couchbase/error_codes.hxx>

#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>

#include <optional>
#include <string>

namespace couchbase::core::protostellar
{
namespace
{
// Returns the first detail block of type T packed into the rich status, if any. Per RFC 77 ("Other
// errors") only the first applicable block is considered.
template<typename T>
auto
find_detail(const google::rpc::Status& rich) -> std::optional<T>
{
  for (const auto& any : rich.details()) {
    T message;
    if (any.UnpackTo(&message)) {
      return message;
    }
  }
  return std::nullopt;
}

// FAILED_PRECONDITION -> the specific KV error carried by PreconditionFailure.type
// (errorhandler.go).
auto
map_precondition(const std::string& type) -> std::error_code
{
  if (type == "LOCKED") {
    return errc::key_value::document_locked;
  }
  if (type == "NOT_LOCKED") {
    return errc::key_value::document_not_locked;
  }
  if (type == "DOC_TOO_DEEP") {
    return errc::key_value::value_too_deep;
  }
  if (type == "DOC_NOT_JSON") {
    return errc::key_value::document_not_json;
  }
  if (type == "PATH_MISMATCH") {
    return errc::key_value::path_mismatch;
  }
  if (type == "VALUE_OUT_OF_RANGE") {
    return errc::key_value::value_invalid;
  }
  if (type == "PATH_VALUE_OUT_OF_RANGE") {
    return errc::key_value::number_too_big;
  }
  if (type == "VALUE_TOO_LARGE") {
    return errc::key_value::value_too_large;
  }
  if (type == "DOC_NOT_NUMERIC") {
    // Increment/Decrement against a document that is not a counter. Only those two operations can
    // produce it, and without this it arrives as internal_server_failure -- telling the caller the
    // cluster is broken when their document simply is not a number.
    return errc::key_value::delta_invalid;
  }
  if (type == "FLUSH_DISABLED") {
    // The one management precondition the gateway raises. Without it a flush against a bucket that
    // simply has flush turned off reports internal_server_failure, where the caller can act on
    // knowing that flush is disabled.
    return errc::management::bucket_not_flushable;
  }
  if (type == "DURABILITY_IMPOSSIBLE") {
    // Fewer live nodes than the requested durability needs. Actionable by the caller (ask for less,
    // or grow the cluster), so it must not be flattened into a generic server failure either.
    return errc::key_value::durability_impossible;
  }
  // POISONED_CAS deliberately falls through: the gateway can emit it, but the SDK has no error for
  // a CAS that the server considers corrupt, and inventing one here would be a guess. Revisit if
  // couchbase::errc grows a member for it.
  return errc::common::internal_server_failure;
}

// NOT_FOUND/ALREADY_EXISTS carry a ResourceInfo whose type selects the specific SDK error.
auto
map_not_found(const std::string& resource_type) -> std::error_code
{
  if (resource_type == "bucket") {
    return errc::common::bucket_not_found;
  }
  if (resource_type == "scope") {
    return errc::common::scope_not_found;
  }
  if (resource_type == "collection") {
    return errc::common::collection_not_found;
  }
  if (resource_type == "queryindex" || resource_type == "searchindex") {
    return errc::common::index_not_found;
  }
  if (resource_type == "path") {
    return errc::key_value::path_not_found;
  }
  return errc::key_value::document_not_found;
}

auto
map_already_exists(const std::string& resource_type) -> std::error_code
{
  if (resource_type == "bucket") {
    return errc::management::bucket_exists;
  }
  if (resource_type == "scope") {
    return errc::management::scope_exists;
  }
  if (resource_type == "collection") {
    return errc::management::collection_exists;
  }
  if (resource_type == "queryindex" || resource_type == "searchindex") {
    return errc::common::index_exists;
  }
  if (resource_type == "path") {
    return errc::key_value::path_exists;
  }
  return errc::key_value::document_exists;
}
} // namespace

auto
map_status_code(grpc::StatusCode code, operation_kind kind) -> std::error_code
{
  switch (code) {
    case grpc::StatusCode::OK:
      return {};
    case grpc::StatusCode::NOT_FOUND:
      return errc::key_value::document_not_found;
    case grpc::StatusCode::ALREADY_EXISTS:
      return errc::key_value::document_exists;
    case grpc::StatusCode::ABORTED:
      // gRPC ABORTED is the canonical concurrency-conflict code; for KV that is a CAS mismatch.
      return errc::common::cas_mismatch;
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      // A deadline on a mutation is genuinely ambiguous -- the write may already have been applied
      // server-side -- so it must not tell the retry layer it is safe to replay a
      // possibly-non-idempotent operation. A read changed nothing by definition, so reporting it
      // as ambiguous would be strictly wrong: it claims the operation may have mutated state that
      // it cannot have touched, and a caller that declines to retry ambiguous operations would
      // give up on an operation that is perfectly safe to repeat.
      return kind == operation_kind::read_only ? errc::common::unambiguous_timeout
                                               : errc::common::ambiguous_timeout;
    case grpc::StatusCode::CANCELLED:
      return errc::common::request_canceled;
    case grpc::StatusCode::UNAUTHENTICATED:
    case grpc::StatusCode::PERMISSION_DENIED:
      return errc::common::authentication_failure;
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::OUT_OF_RANGE:
      return errc::common::invalid_argument;
    case grpc::StatusCode::UNIMPLEMENTED:
      return errc::common::feature_not_available;
    case grpc::StatusCode::UNAVAILABLE:
      return errc::common::temporary_failure;
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return errc::common::rate_limited;
    case grpc::StatusCode::INTERNAL:
    case grpc::StatusCode::UNKNOWN:
    case grpc::StatusCode::DATA_LOSS:
    case grpc::StatusCode::FAILED_PRECONDITION:
    default:
      return errc::common::internal_server_failure;
  }
}

auto
map_status(const grpc::Status& status, operation_kind kind) -> std::error_code
{
  const auto code = status.error_code();

  // The typed google.rpc.Status details refine three codes into specific SDK errors (RFC 77 "Other
  // errors" + errorhandler.go). Everything else is keyed on the bare gRPC code, plus `kind` for
  // DEADLINE_EXCEEDED (see map_status_code).
  google::rpc::Status rich;
  const bool have_rich =
    !status.error_details().empty() && rich.ParseFromString(status.error_details());

  switch (code) {
    case grpc::StatusCode::FAILED_PRECONDITION:
      if (have_rich) {
        if (auto failure = find_detail<google::rpc::PreconditionFailure>(rich);
            failure && failure->violations_size() > 0) {
          return map_precondition(failure->violations(0).type());
        }
      }
      return errc::common::internal_server_failure;
    case grpc::StatusCode::NOT_FOUND:
      if (have_rich) {
        if (auto info = find_detail<google::rpc::ResourceInfo>(rich)) {
          return map_not_found(info->resource_type());
        }
      }
      return errc::key_value::document_not_found;
    case grpc::StatusCode::ALREADY_EXISTS:
      if (have_rich) {
        if (auto info = find_detail<google::rpc::ResourceInfo>(rich)) {
          return map_already_exists(info->resource_type());
        }
      }
      return errc::key_value::document_exists;
    default:
      return map_status_code(code, kind);
  }
}

auto
error_message(const grpc::Status& status) -> std::string
{
  if (const auto& details = status.error_details(); !details.empty()) {
    google::rpc::Status rich;
    if (rich.ParseFromString(details) && !rich.message().empty()) {
      return rich.message();
    }
  }
  return status.error_message();
}

auto
make_error_context(const grpc::Status& status, const document_id& id, operation_kind kind)
  -> key_value_error_context
{
  const auto ec = map_status(status, kind);
  std::optional<key_value_extended_error_info> extended{};
  if (!status.ok()) {
    if (auto message = error_message(status); !message.empty()) {
      extended.emplace(std::string{}, std::move(message));
    }
  }
  return key_value_error_context{
    {},
    ec,
    {},
    {},
    0,
    {},
    id.key(),
    id.bucket(),
    id.scope(),
    id.collection(),
    0,
    {},
    couchbase::cas{},
    {},
    std::move(extended),
  };
}

} // namespace couchbase::core::protostellar
