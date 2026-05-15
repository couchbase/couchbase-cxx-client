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

#pragma once

#include "exceptions.hxx"
#include "performer.pb.h"

#include <core/meta/features.hxx>
#include <core/transactions/internal/transactions_cleanup.hxx>
#include <couchbase/fmt/error.hxx>

#include <grpc++/grpc++.h>
#include <spdlog/spdlog.h>

namespace TxnSvcUtils
{
inline protocol::transactions::AttemptStates
convertAttemptState(couchbase::core::transactions::attempt_state state)
{
  switch (state) {
    case couchbase::core::transactions::attempt_state::NOT_STARTED:
      return protocol::transactions::AttemptStates::NOTHING_WRITTEN;
    case couchbase::core::transactions::attempt_state::PENDING:
      return protocol::transactions::AttemptStates::PENDING;
    case couchbase::core::transactions::attempt_state::ABORTED:
      return protocol::transactions::AttemptStates::ABORTED;
    case couchbase::core::transactions::attempt_state::COMMITTED:
      return protocol::transactions::AttemptStates::COMMITTED;
    case couchbase::core::transactions::attempt_state::COMPLETED:
      return protocol::transactions::AttemptStates::COMPLETED;
    case couchbase::core::transactions::attempt_state::ROLLED_BACK:
      return protocol::transactions::AttemptStates::ROLLED_BACK;
    case couchbase::core::transactions::attempt_state::UNKNOWN:
      return protocol::transactions::AttemptStates::UNKNOWN;
    default:
      throw performer_exception::internal("unrecognized AttemptState");
  }
}

inline protocol::transactions::TransactionException
convert_transaction_err(std::error_code err)
{
  if (err == couchbase::errc::transaction::failed) {
    return protocol::transactions::TransactionException::EXCEPTION_FAILED;
  }
  if (err == couchbase::errc::transaction::expired) {
    return protocol::transactions::TransactionException::EXCEPTION_EXPIRED;
  }
  if (err == couchbase::errc::transaction::ambiguous) {
    return protocol::transactions::TransactionException::EXCEPTION_COMMIT_AMBIGUOUS;
  }
  if (err == couchbase::errc::transaction::failed_post_commit) {
    return protocol::transactions::TransactionException::EXCEPTION_FAILED_POST_COMMIT;
  }
  if (err == std::error_code()) {
    return protocol::transactions::TransactionException::NO_EXCEPTION_THROWN;
  }
  return protocol::transactions::TransactionException::EXCEPTION_UNKNOWN;
}

inline protocol::transactions::ExternalException
convert_transaction_op_errc(std::error_code ec)
{
  if (!ec) {
    return protocol::transactions::ExternalException::NotSet;
  }
  if (ec == couchbase::errc::transaction_op::generic) {
    return protocol::transactions::ExternalException::CouchbaseException;
  }
  if (ec == couchbase::errc::transaction_op::document_not_found) {
    return protocol::transactions::ExternalException::DocumentNotFoundException;
  }
  if (ec == couchbase::errc::transaction_op::document_exists) {
    return protocol::transactions::ExternalException::DocumentExistsException;
  }
  if (ec == couchbase::errc::transaction_op::parsing_failure) {
    return protocol::transactions::ExternalException::ParsingFailure;
  }
  if (ec == couchbase::errc::transaction_op::previous_operation_failed) {
    return protocol::transactions::ExternalException::PreviousOperationFailed;
  }
  if (ec == couchbase::errc::transaction_op::transaction_already_committed) {
    return protocol::transactions::ExternalException::TransactionAlreadyCommitted;
  }
  if (ec == couchbase::errc::transaction_op::transaction_already_aborted) {
    return protocol::transactions::ExternalException::TransactionAlreadyAborted;
  }
  if (ec == couchbase::errc::transaction_op::transaction_aborted_externally) {
    return protocol::transactions::ExternalException::TransactionAbortedExternally;
  }
  if (ec == couchbase::errc::transaction_op::service_not_available) {
    return protocol::transactions::ExternalException::ServiceNotAvailableException;
  }
  if (ec == couchbase::errc::transaction_op::rollback_not_permitted) {
    return protocol::transactions::ExternalException::RollbackNotPermitted;
  }
  if (ec == couchbase::errc::transaction_op::request_canceled) {
    return protocol::transactions::ExternalException::RequestCanceledException;
  }
  if (ec == couchbase::errc::transaction_op::illegal_state) {
    return protocol::transactions::ExternalException::IllegalStateException;
  }
  if (ec == couchbase::errc::transaction_op::forward_compatibility_failure) {
    return protocol::transactions::ExternalException::ForwardCompatibilityFailure;
  }
  if (ec == couchbase::errc::transaction_op::feature_not_available) {
    return protocol::transactions::ExternalException::FeatureNotAvailableException;
  }
  if (ec == couchbase::errc::transaction_op::document_already_in_transaction) {
    return protocol::transactions::ExternalException::DocumentAlreadyInTransaction;
  }
  if (ec == couchbase::errc::transaction_op::concurrent_operations_detected_on_same_document) {
    return protocol::transactions::ExternalException::ConcurrentOperationsDetectedOnSameDocument;
  }
  if (ec == couchbase::errc::transaction_op::commit_not_permitted) {
    return protocol::transactions::ExternalException::CommitNotPermitted;
  }
  if (ec == couchbase::errc::transaction_op::active_transaction_record_full) {
    return protocol::transactions::ExternalException::ActiveTransactionRecordFull;
  }
  if (ec == couchbase::errc::transaction_op::active_transaction_record_entry_not_found) {
    return protocol::transactions::ExternalException::ActiveTransactionRecordEntryNotFound;
  }
  if (ec == couchbase::errc::transaction_op::active_transaction_record_not_found) {
    return protocol::transactions::ExternalException::ActiveTransactionRecordNotFound;
  }
  if (ec == couchbase::errc::transaction_op::document_unretrievable) {
    return protocol::transactions::ExternalException::DocumentUnretrievableException;
  }
  spdlog::warn("Unknown transaction op error code encountered: {}", ec.message());
  return protocol::transactions::ExternalException::Unknown;
}

template<typename T, typename R>
void
update_common_options(T& conf,
                      R& options,
                      std::shared_ptr<fit_cxx::Connection> conn,
                      std::vector<fit_cxx::TxnSvcHook>& hooks)
{
  if (conf.has_timeout_millis()) {
    options.timeout(std::chrono::milliseconds(conf.timeout_millis()));
  }
  if (conf.has_durability()) {
    switch (conf.durability()) {
      case protocol::shared::Durability::MAJORITY:
        options.durability_level(couchbase::durability_level::majority);
        break;
      case protocol::shared::Durability::MAJORITY_AND_PERSIST_TO_ACTIVE:
        options.durability_level(couchbase::durability_level::majority_and_persist_to_active);
        break;
      case protocol::shared::Durability::PERSIST_TO_MAJORITY:
        options.durability_level(couchbase::durability_level::persist_to_majority);
        break;
      case protocol::shared::Durability::NONE:
        options.durability_level(couchbase::durability_level::none);
        break;
      default:
        options.durability_level(couchbase::durability_level::majority); // the default
    }
  }
  if (conf.has_metadata_collection()) {
    options.metadata_collection({ conf.metadata_collection().bucket_name(),
                                  conf.metadata_collection().scope_name(),
                                  conf.metadata_collection().collection_name() });
  }

  hooks.reserve(static_cast<std::size_t>(conf.hook_size()));
  auto pair = fit_cxx::TxnSvcHook::convert_hooks(conf.hook(), hooks, conn);
  options.test_factories(pair.first, pair.second);
}

inline void
create_transaction_result(const couchbase::error& err,
                          const couchbase::transactions::transaction_result& res,
                          std::shared_ptr<couchbase::core::transactions::transactions> txn,
                          protocol::transactions::TransactionResult* response)
{
  // fill in the response
  response->set_unstaging_complete(res.unstaging_complete);
  if (txn->cleanup().config().cleanup_config.cleanup_client_attempts) {
    response->set_cleanup_requests_valid(true);
    response->set_cleanup_requests_pending(
      static_cast<std::int32_t>(txn->cleanup().cleanup_queue_length()));
  }
  response->set_transaction_id(res.transaction_id);
  response->set_unstaging_complete(res.unstaging_complete);
  if (err.ec()) {
    response->set_exception(convert_transaction_err(err.ec()));
    response->set_exception_cause(convert_transaction_op_errc(err.cause()->ec()));
  }
}

inline void
create_transaction_cleanup_attempt(
  const couchbase::core::transactions::transactions_cleanup_attempt& attempt,
  protocol::transactions::TransactionCleanupAttempt* response)
{
  response->mutable_atr()->set_doc_id(attempt.atr_id().key());
  response->mutable_atr()->set_bucket_name(attempt.atr_id().bucket());
  response->mutable_atr()->set_collection_name(attempt.atr_id().collection());
  response->mutable_atr()->set_scope_name(attempt.atr_id().scope());
  response->set_success(attempt.success());
  response->set_attempt_id(attempt.attempt_id());
  response->set_state(convertAttemptState(attempt.state()));
}

inline ::protocol::transactions::Caps
convert_extension_to_performer_caps(const std::string& ext)
{
  static std::map<std::string, ::protocol::transactions::Caps> ext_map{
    { "TI", ::protocol::transactions::Caps::EXT_TRANSACTION_ID },
    { "DC", ::protocol::transactions::Caps::EXT_DEFERRED_COMMIT },
    { "TO", ::protocol::transactions::Caps::EXT_TIME_OPT_UNSTAGING },
    { "MO", ::protocol::transactions::Caps::EXT_MEMORY_OPT_UNSTAGING },
    { "CM", ::protocol::transactions::Caps::EXT_CUSTOM_METADATA_COLLECTION },
    { "BM", ::protocol::transactions::Caps::EXT_BINARY_METADATA },
    { "QU", ::protocol::transactions::Caps::EXT_QUERY },
    { "SD", ::protocol::transactions::Caps::EXT_STORE_DURABILITY },
    { "BF3787", ::protocol::transactions::Caps::BF_CBD_3787 },
    { "BF3794", ::protocol::transactions::Caps::BF_CBD_3794 },
    { "BF3705", ::protocol::transactions::Caps::BF_CBD_3705 },
    { "BF3838", ::protocol::transactions::Caps::BF_CBD_3838 },
    { "RC", ::protocol::transactions::Caps::EXT_REMOVE_COMPLETED },
    { "UA", ::protocol::transactions::Caps::EXT_UNKNOWN_ATR_STATES },
    { "CO", ::protocol::transactions::Caps::EXT_ALL_KV_COMBINATIONS },
    { "BF3791", ::protocol::transactions::Caps::BF_CBD_3791 },
    { "SQ", ::protocol::transactions::Caps::EXT_SINGLE_QUERY },
    { "SI", ::protocol::transactions::Caps::EXT_SDK_INTEGRATION },
    { "QC", ::protocol::transactions::Caps::EXT_QUERY_CONTEXT },
    { "IX", ::protocol::transactions::Caps::EXT_INSERT_EXISTING },
    { "TS", ::protocol::transactions::Caps::EXT_THREAD_SAFE },
    { "PU", ::protocol::transactions::Caps::EXT_PARALLEL_UNSTAGING },
    { "BS", ::protocol::transactions::Caps::EXT_BINARY_SUPPORT },
    { "RP", ::protocol::transactions::Caps::EXT_REPLICA_FROM_PREFERRED_GROUP },
    { "RX", ::protocol::transactions::Caps::EXT_REPLACE_BODY_WITH_XATTR },
    { "GM", ::protocol::transactions::Caps::EXT_GET_MULTI },
    { "RPP1", ::protocol::transactions::Caps::EXT_REPLICA_FROM_PREFERRED_GROUP_PATCH1 },
  };
  if (auto it = ext_map.find(ext); it != ext_map.end()) {
    return it->second;
  }
  spdlog::error("unknown transactions extension '{}'", ext);
  throw std::invalid_argument(fmt::format("unknown transactions extension '{}'", ext));
}

inline couchbase::transactions::transaction_query_options
to_transactions_query_options(const protocol::transactions::CommandQuery& cmd)
{
  couchbase::transactions::transaction_query_options options{};

  if (!cmd.has_query_options()) {
    return options;
  }

  auto proto_opts = cmd.query_options();
  if (proto_opts.has_scan_consistency()) {
    if (proto_opts.has_scan_consistency()) {
      switch (proto_opts.scan_consistency()) {
        case protocol::shared::REQUEST_PLUS:
          options.scan_consistency(couchbase::query_scan_consistency::request_plus);
          break;
        case protocol::shared::NOT_BOUNDED:
          options.scan_consistency(couchbase::query_scan_consistency::not_bounded);
          break;
        default:
          throw performer_exception::unimplemented("query scan consistency type not recognised");
      }
    }
  }
  for (const auto& [key, val] : proto_opts.raw()) {
    options.raw(key, val);
  }
  if (proto_opts.has_adhoc()) {
    options.ad_hoc(proto_opts.adhoc());
  }
  if (proto_opts.has_profile()) {
    if (proto_opts.profile() == "off") {
      options.profile(couchbase::query_profile::off);
    } else if (proto_opts.profile() == "phases") {
      options.profile(couchbase::query_profile::phases);
    } else if (proto_opts.profile() == "timings") {
      options.profile(couchbase::query_profile::timings);
    } else {
      throw performer_exception::unimplemented("query profile type not recognised");
    }
  }
  if (proto_opts.has_readonly()) {
    options.readonly(proto_opts.readonly());
  }
  if (proto_opts.parameters_positional_size() > 0) {
    std::vector<std::vector<std::byte>> positional_params{};
    for (const auto& param : proto_opts.parameters_positional()) {
      positional_params.emplace_back(couchbase::codec::tao_json_serializer::serialize(param));
    }
    options.encoded_positional_parameters(positional_params);
  }
  if (proto_opts.parameters_named_size() > 0) {
    std::map<std::string, std::vector<std::byte>, std::less<>> named_params{};
    std::map<std::string, std::string> m{};
    for (const auto& [key, val] : proto_opts.parameters_named()) {
      named_params.emplace(key, couchbase::codec::tao_json_serializer::serialize(val));
    }
    options.encoded_named_parameters(named_params);
  }
  if (proto_opts.has_pipeline_cap()) {
    options.pipeline_cap(static_cast<std::uint64_t>(proto_opts.pipeline_cap()));
  }
  if (proto_opts.has_pipeline_batch()) {
    options.pipeline_batch(static_cast<std::uint64_t>(proto_opts.pipeline_batch()));
  }
  if (proto_opts.has_scan_cap()) {
    options.scan_cap(static_cast<std::uint64_t>(proto_opts.scan_cap()));
  }
  if (proto_opts.has_scan_wait_millis()) {
    options.scan_wait(std::chrono::milliseconds{ proto_opts.scan_wait_millis() });
  }
#ifdef COUCHBASE_CXX_CLIENT_TRANSACTION_QUERY_OPTIONS_HAVE_FLEX_INDEX
  if (proto_opts.has_flex_index()) {
    options.flex_index(proto_opts.flex_index());
  }
#endif
  return options;
}

inline auto
validate_external_error(const protocol::transactions::ExternalException& expected,
                        const couchbase::error& err) -> bool
{
  spdlog::debug("Checking for external error: {}",
                protocol::transactions::ExternalException_Name(expected));

  if (expected == protocol::transactions::NotSet) {
    return true;
  }
  return TxnSvcUtils::convert_transaction_op_errc(err.ec()) == expected;
}

inline auto
validate_transaction_operation_failed_error(const protocol::transactions::ErrorWrapper& expected,
                                            const couchbase::error& err) -> bool
{
  if (err.ec() != couchbase::errc::transaction_op::transaction_op_failed) {
    return false;
  }
  if (expected.has_cause() && expected.cause().has_exception()) {
    if (!err.cause().has_value()) {
      return false;
    }
    if (!validate_external_error(expected.cause().exception(), err.cause().value())) {
      return false;
    }
  }
  // TODO(DC): Add validation for to_raise, auto_rollback_attempt and retry_transaction once we
  // provide
  //  a way to access them in the Public API
  return true;
}

inline auto
validate_op_result(const google::protobuf::RepeatedPtrField<protocol::transactions::ExpectedResult>&
                     expected_results,
                   const couchbase::error& err) -> bool
{
  if (expected_results.empty()) {
    return true;
  }

  // At least one of the expected results must be satisfied
  for (const auto& expected : expected_results) {
    spdlog::debug("Checking error \"{}\" against expected result: {}", err, expected.DebugString());
    switch (expected.result_case()) {
      case protocol::transactions::ExpectedResult::kSuccess:
        if (!err) {
          return true;
        }
        break;
      case protocol::transactions::ExpectedResult::kAnythingAllowed:
        return true;
      case protocol::transactions::ExpectedResult::kError:
        if (validate_transaction_operation_failed_error(expected.error(), err)) {
          return true;
        }
        break;
      case protocol::transactions::ExpectedResult::kException:
        if (validate_external_error(expected.exception(), err)) {
          return true;
        }
        break;
      default:
        throw performer_exception::internal(
          fmt::format("Unexpected 'expected result' definition: {}", expected.DebugString()));
    }
  }
  return false;
}
} // namespace TxnSvcUtils
