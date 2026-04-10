/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "common.hxx"
#include "../exceptions.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/mutation_state.hxx>

#include "core/logger/logger.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <google/protobuf/util/time_util.h>
#ifdef _WIN32
#undef GetCurrentTime
#endif
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <tao/json/to_string.hpp>
#include <tao/json/value.hpp>

#include <future>
#include <iostream>

#include "shared.basic.pb.h"

namespace fit_cxx::commands::common
{
auto
create_new_result() -> protocol::run::Result
{
  protocol::run::Result proto_res;
  auto initiated = google::protobuf::util::TimeUtil::GetCurrentTime();
  proto_res.mutable_initiated()->CopyFrom(initiated);
  return proto_res;
}

void
convert_error_code(const std::error_code ec, protocol::shared::Exception* exception)
{
  if (ec.category() == std::system_category() || ec.category() == std::generic_category() ||
      ec.category() == std::future_category() || ec.category() == std::iostream_category()) {
    exception->mutable_other()->set_name(ec.message());
    return;
  }
  auto cb_exception = exception->mutable_couchbase();
  cb_exception->set_name(ec.message());
  if (ec.category() == couchbase::core::impl::key_value_category()) {
    switch (ec.value()) {
      case static_cast<int>(couchbase::errc::key_value::document_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::document_irretrievable):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_UNRETRIEVABLE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::document_locked):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_LOCKED_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::document_not_locked):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_NOT_LOCKED_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::value_too_large):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_VALUE_TOO_LARGE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::document_exists):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::durability_level_not_available):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DURABILITY_LEVEL_NOT_AVAILABLE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::durability_impossible):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DURABILITY_IMPOSSIBLE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::durability_ambiguous):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DURABILITY_AMBIGUOUS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::durable_write_in_progress):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DURABLE_WRITE_IN_PROGRESS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::durable_write_re_commit_in_progress):
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::
                                 SDK_DURABLE_WRITE_RECOMMIT_IN_PROGRESS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PATH_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_mismatch):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PATH_MISMATCH_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_invalid):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PATH_INVALID_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_too_big):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PATH_TOO_BIG_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_too_deep):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PATH_TOO_DEEP_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::value_too_deep):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_VALUE_TOO_DEEP_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::value_invalid):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_VALUE_INVALID_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::document_not_json):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DOCUMENT_NOT_JSON_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::number_too_big):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_NUMBER_TOO_BIG_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::delta_invalid):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DELTA_INVALID_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::path_exists):
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_PATH_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::xattr_unknown_macro):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_XATTR_UNKNOWN_MACRO_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::xattr_invalid_key_combo):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_XATTR_INVALID_KEY_COMBO_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::xattr_unknown_virtual_attribute):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_XATTR_UNKNOWN_VIRTUAL_ATTRIBUTE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::xattr_cannot_modify_virtual_attribute):
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::
                                 SDK_XATTR_CANNOT_MODIFY_VIRTUAL_ATTRIBUTE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::key_value::xattr_no_access):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_XATTR_NO_ACCESS_EXCEPTION);
        break;
      default:
        // key_value error_codes 131, 133 and 134 not represented in FIT
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_COUCHBASE_EXCEPTION);
    }
  }
  if (ec.category() == couchbase::core::impl::common_category()) {
    switch (ec.value()) {
      case static_cast<int>(couchbase::errc::common::request_canceled):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_REQUEST_CANCELLED_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::invalid_argument):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_INVALID_ARGUMENT_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::service_not_available):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_SERVICE_NOT_AVAILABLE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::internal_server_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_INTERNAL_SERVER_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::authentication_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_AUTHENTICATION_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::temporary_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_TEMPORARY_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::parsing_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PARSING_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::cas_mismatch):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_CAS_MISMATCH_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::bucket_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_BUCKET_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::collection_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_COLLECTION_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::unsupported_operation):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_UNSUPPORTED_OPERATION_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::ambiguous_timeout):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_AMBIGUOUS_TIMEOUT_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::unambiguous_timeout):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_UNAMBIGUOUS_TIMEOUT_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::feature_not_available):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_FEATURE_NOT_AVAILABLE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::scope_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_SCOPE_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::index_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_INDEX_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::index_exists):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_INDEX_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::encoding_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_ENCODING_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::decoding_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_DECODING_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::rate_limited):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_RATE_LIMITED_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::common::quota_limited):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_QUOTA_LIMITED_EXCEPTION);
        break;
      default:
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_COUCHBASE_EXCEPTION);
    }
  }
  if (ec.category() == couchbase::core::impl::query_category()) {
    switch (ec.value()) {
      case static_cast<int>(couchbase::errc::query::planning_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PLANNING_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::query::index_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_INDEX_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::query::prepared_statement_failure):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_PREPARED_STATEMENT_FAILURE_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::query::dml_failure):
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_DML_FAILURE_EXCEPTION);
        break;
    }
  }
  if (ec.category() == couchbase::core::impl::management_category()) {
    switch (ec.value()) {
      case static_cast<int>(couchbase::errc::management::collection_exists):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_COLLECTION_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::scope_exists):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_SCOPE_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::user_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_USER_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::group_not_found):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_GROUP_NOT_FOUND_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::bucket_exists):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_BUCKET_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::user_exists):
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_USER_EXISTS_EXCEPTION);
        break;
      case static_cast<int>(couchbase::errc::management::bucket_not_flushable):
        cb_exception->set_type(
          protocol::shared::CouchbaseExceptionType::SDK_BUCKET_NOT_FLUSHABLE_EXCEPTION);
        break;
      default:
        // management error codes > 607 not represented in FIT
        cb_exception->set_type(protocol::shared::CouchbaseExceptionType::SDK_COUCHBASE_EXCEPTION);
    }
  }
  // TODO: other categories too...
}

void
convert_error(const couchbase::error& err, protocol::shared::Exception* exception)
{
  convert_error_code(err.ec(), exception);
  if (exception->has_couchbase()) {
    exception->mutable_couchbase()->set_serialized(err.ctx().to_json());
  }
}

auto
to_durability_level(protocol::shared::Durability level) -> couchbase::durability_level
{
  if (level == protocol::shared::NONE) {
    return couchbase::durability_level::none;
  }
  if (level == protocol::shared::MAJORITY) {
    return couchbase::durability_level::majority;
  }
  if (level == protocol::shared::MAJORITY_AND_PERSIST_TO_ACTIVE) {
    return couchbase::durability_level::majority_and_persist_to_active;
  }
  if (level == protocol::shared::PERSIST_TO_MAJORITY) {
    return couchbase::durability_level::persist_to_majority;
  }
  throw performer_exception::unimplemented(std::string("Unknown durability level specified"));
}

auto
to_persist_to(protocol::shared::ObserveBased observe) -> couchbase::persist_to
{
  switch (observe.persistto()) {
    case protocol::shared::PersistTo::PERSIST_TO_NONE:
      return couchbase::persist_to::none;
    case protocol::shared::PersistTo::PERSIST_TO_ONE:
      return couchbase::persist_to::one;
    case protocol::shared::PersistTo::PERSIST_TO_TWO:
      return couchbase::persist_to::two;
    case protocol::shared::PersistTo::PERSIST_TO_THREE:
      return couchbase::persist_to::three;
    case protocol::shared::PersistTo::PERSIST_TO_FOUR:
      return couchbase::persist_to::four;
    case protocol::shared::PersistTo::PERSIST_TO_ACTIVE:
      return couchbase::persist_to::active;
    default:
      throw performer_exception::unimplemented("Unknown persist-to value specified");
  }
}

auto
to_replicate_to(protocol::shared::ObserveBased observe) -> couchbase::replicate_to
{
  switch (observe.replicateto()) {
    case protocol::shared::ReplicateTo::REPLICATE_TO_NONE:
      return couchbase::replicate_to::none;
    case protocol::shared::ReplicateTo::REPLICATE_TO_ONE:
      return couchbase::replicate_to::one;
    case protocol::shared::ReplicateTo::REPLICATE_TO_TWO:
      return couchbase::replicate_to::two;
    case protocol::shared::ReplicateTo::REPLICATE_TO_THREE:
      return couchbase::replicate_to::three;
    default:
      throw performer_exception::unimplemented("Unknown replicate-to value specified");
  }
}

auto
to_content(const protocol::shared::Content& proto_content) -> content
{
  if (proto_content.has_passthrough_string()) {
    return proto_content.passthrough_string();
  }
  if (proto_content.has_byte_array()) {
    return couchbase::core::utils::to_binary(proto_content.byte_array());
  }
  if (proto_content.has_convert_to_json()) {
    return couchbase::core::utils::json::parse(proto_content.convert_to_json());
  }
  if (proto_content.has_null()) {
    return nullptr;
  }
  throw performer_exception::unimplemented(
    fmt::format("Unrecognised content type {}", proto_content.DebugString()));
}

auto
to_mutation_state(const protocol::shared::MutationState& state) -> couchbase::mutation_state
{
  couchbase::mutation_state mutation_state{};

  for (const auto& token : state.tokens()) {
    couchbase::mutation_token mutation_token(static_cast<std::uint64_t>(token.partition_uuid()),
                                             static_cast<std::uint64_t>(token.sequence_number()),
                                             static_cast<std::uint16_t>(token.partition_id()),
                                             token.bucket_name());
    couchbase::mutation_result result(couchbase::cas{}, mutation_token);
    mutation_state.add(result);
  }

  return mutation_state;
}

auto
from_durability_level(couchbase::durability_level level) -> protocol::shared::Durability
{
  if (level == couchbase::durability_level::none) {
    return protocol::shared::NONE;
  }
  if (level == couchbase::durability_level::majority) {
    return protocol::shared::MAJORITY;
  }
  if (level == couchbase::durability_level::majority_and_persist_to_active) {
    return protocol::shared::MAJORITY_AND_PERSIST_TO_ACTIVE;
  }
  if (level == couchbase::durability_level::persist_to_majority) {
    return protocol::shared::PERSIST_TO_MAJORITY;
  }
  throw performer_exception::unimplemented(std::string("Unexpected durability level from SDK"));
}

auto
to_time_point(const google::protobuf::Timestamp& timestamp) -> std::chrono::system_clock::time_point
{
  auto seconds = timestamp.seconds();
  auto nanos = timestamp.nanos();
  std::chrono::nanoseconds duration(seconds * static_cast<int64_t>(1e9) + nanos);
  return std::chrono::system_clock::time_point(
    std::chrono::duration_cast<std::chrono::system_clock::duration>(duration));
}

auto
to_binary(const std::string& value) -> std::vector<std::byte>
{
  std::vector<std::byte> out;
  out.reserve(value.size());
  out.insert(out.end(),
             reinterpret_cast<const std::byte*>(value.data()),
             reinterpret_cast<const std::byte*>(value.data() + value.size()));
  return out;
}

void
validate_content(const protocol::shared::ContentTypes& content,
                 const protocol::shared::ContentAs& content_as,
                 const std::vector<std::byte>& expected_content_bytes)
{
  spdlog::debug(
    "validating content: {}. Content as: {}", content.DebugString(), content_as.DebugString());
  switch (content_as.as_case()) {
    case protocol::shared::ContentAs::kAsString: {
      auto expected_string =
        std::string{ reinterpret_cast<const char*>(expected_content_bytes.data()),
                     expected_content_bytes.size() };
      if (expected_string != content.content_as_string()) {
        throw performer_exception::failed_precondition(
          fmt::format("content does not match expectation. Got: '{}'. Expected: '{}'",
                      content.content_as_string(),
                      expected_string));
      }
      break;
    }
    case protocol::shared::ContentAs::kAsByteArray: {
      if (expected_content_bytes != to_binary(content.content_as_bytes())) {
        throw performer_exception::failed_precondition(
          fmt::format("content does not match expectation. Got: '{}'. Expected: '{}'",
                      content.content_as_bytes(),
                      std::string{ reinterpret_cast<const char*>(expected_content_bytes.data()),
                                   expected_content_bytes.size() }));
      }
      break;
    }
    case protocol::shared::ContentAs::kAsJsonArray:
    case protocol::shared::ContentAs::kAsJsonObject: {
      auto expected_json = couchbase::codec::tao_json_serializer::deserialize<tao::json::value>(
        expected_content_bytes);
      auto actual_json = couchbase::codec::tao_json_serializer::deserialize<tao::json::value>(
        to_binary(content.content_as_bytes()));
      if (expected_json != actual_json) {
        throw performer_exception::failed_precondition(
          fmt::format("content does not match expectation. Got: '{}'. Expected: '{}'",
                      tao::json::to_string(actual_json),
                      tao::json::to_string(expected_json)));
      }
      break;
    }
    default:
      throw performer_exception::unimplemented("the performer cannot validate the content when "
                                               "content_as type is integer, float or boolean");
  }
}

void
validate_content_as(const protocol::shared::ContentAsPerformerValidation& validation,
                    const std::function<protocol::shared::ContentTypes()>& content_fn)
{
  protocol::shared::ContentTypes content{};
  try {
    content = content_fn();
  } catch (const std::system_error& e) {
    if (!validation.expect_success()) {
      // Content deserialization expected to fail, and it did. We have nothing else to validate.
      return;
    }
    throw performer_exception::failed_precondition(
      fmt::format("content deserialization expected to succeed, but failed. ec=`{}`, msg=`{}`",
                  e.code().message(),
                  e.what()));
  }
  if (!validation.expect_success()) {
    throw performer_exception::failed_precondition(
      "content deserialization expected to fail, but succeeded");
  }
  if (validation.has_expected_content_bytes()) {
    validate_content(
      content, validation.content_as(), to_binary(validation.expected_content_bytes()));
  }
}
} // namespace fit_cxx::commands::common
