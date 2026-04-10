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

#include "service.hxx"

#include "batcher.hxx"
#include "commands/bucket_management.hxx"
#include "commands/collection_management.hxx"
#include "commands/common.hxx"
#include "commands/key_value.hxx"
#include "commands/query.hxx"
#include "commands/query_index_management.hxx"
#include "commands/search.hxx"
#include "commands/search_index_management.hxx"
#include "connection.hxx"
#include "exceptions.hxx"
#include "metrics/metrics_reporter.hxx"
#include "performer.pb.h"
#include "transactions.commands.pb.h"

#include <core/meta/features.hxx>
#include <core/meta/version.hxx>
#include <core/transactions/attempt_context_impl.hxx>
#include <core/transactions/forward_compat.hxx>
#include <core/utils/binary.hxx>

#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/error.hxx>
#include <couchbase/fmt/error.hxx>

#include <google/protobuf/repeated_ptr_field.h>
#include <google/protobuf/util/time_util.h>
#ifdef _WIN32
#undef GetCurrentTime
#endif

#include <spdlog/fmt/ostr.h>

#include <array>
#include <list>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

constexpr auto TXN_TIMEOUT = std::chrono::seconds(
  120); // a txn over 2 minutes will terminate, return, and the tests can continue.
constexpr auto CLIENT_RECORD_PROCESS_TIMEOUT =
  std::chrono::seconds(30); // a get_active_clients() call that takes over 30 seconds should abort.

grpc::Status
TxnService::disconnectConnections(
  grpc::ServerContext* /*context*/,                                  // FIXME(SA): use parameter
  const protocol::shared::DisconnectConnectionsRequest* /*request*/, // FIXME(SA): use parameter
  protocol::shared::DisconnectConnectionsResponse* /*response*/)     // FIXME(SA): use parameter
{
  std::scoped_lock<std::mutex> lock(mutex_);
  spdlog::info("clearing {} connections...", connections_.size());
  for (const auto& [key, conn] : connections_) {
    spdlog::trace("closing connection {}", key);
    conn->cluster()->close().get();
  }
  connections_.clear();
  return grpc::Status::OK;
}

grpc::Status
TxnService::startTxn(ConnectionPtr conn,
                     const protocol::transactions::TransactionCreateRequest* request,
                     protocol::transactions::TransactionResult* response,
                     std::atomic<bool>& timed_out,
                     Counters& counters)
{
  int attempt_count = 0;
  std::vector<fit_cxx::TxnSvcHook> hooks;
  couchbase::transactions::transaction_options opts;
  grpc::Status grpc_status{};

  [[maybe_unused]] auto max_time_for_lambda = std::chrono::seconds(16); // FIXME(SA): use variable
  auto start_time = std::chrono::steady_clock::now();
  if (request->has_options()) {
    TxnSvcUtils::update_common_options(request->options(), opts, conn, hooks);
  }
  // add latches
  for (const auto& l : request->latches()) {
    fit_cxx::TxnLatch latch(
      l.name(), static_cast<std::uint32_t>(l.initial_count())); // FIXME(SA): check signess
    conn->add_latch(latch);
  }

  auto expiry =
    dynamic_cast<couchbase::core::transactions::transactions&>(*conn->cluster()->transactions())
      .config()
      .timeout;
  auto limit = expiry + std::chrono::seconds(1);
  if (opts.timeout()) {
    limit = std::chrono::duration_cast<std::chrono::nanoseconds>(1.1 * opts.timeout().value());
  }
  auto [err, res] = conn->cluster()->transactions()->run(
    [&](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      int attempt_to_use = std::min(attempt_count, request->attempts_size() - 1);
      spdlog::trace(
        "{} starting attempt {}, using {}", request->name(), attempt_count, attempt_to_use);
      attempt_count++;
      // now get the specific attempt we want to execute...
      const auto& attempt = request->attempts(attempt_to_use);
      // and iterate over the commands in that attempt
      for (const protocol::transactions::TransactionCommand& command : attempt.commands()) {
        spdlog::trace("{} executing command: {}", request->name(), command.DebugString());
        try {
          auto op_err = execute_command(conn, ctx, command, request->name(), counters);
          if (timed_out) {
            // this means we wrapped this in a call that limited the duration of the transaction,
            // and now we have exceeded that.   The "timeout hack" was done previously when we
            // noticed that you could create a lambda that executed like 20000 operations, yet timed
            // out during it and the _entire_ lambda is still run.
            // TODO: combine these - perhaps just always run the txn with something like the limit
            // defined in the timeout hack.
            std::string msg{ "txn exceeded max time - aborting" };
            spdlog::error(msg);
            // throwing should fail the txn, without a retry.
            grpc_status = grpc::Status(grpc::StatusCode::ABORTED, msg);
            throw std::runtime_error(msg);
          }
          if (op_err) {
            return op_err;
          }
          auto elapsed = std::chrono::steady_clock::now() - start_time;
          if (elapsed > limit) {
            spdlog::info("timeout hack: lambda still running after {}ms, returning",
                         std::chrono::duration_cast<std::chrono::milliseconds>(limit).count());
            return {};
          }
        } catch (const performer_exception& e) {
          grpc_status = e.to_grpc_status();
          break;
        }
      }
      spdlog::trace("{} attempt {} complete", request->name(), attempt_count);
      return {};
    },
    opts);

  if (!grpc_status.ok()) {
    spdlog::trace("startTxn returning non-OK status: {}", grpc_status.error_message());
    return grpc_status;
  }
  TxnSvcUtils::create_transaction_result(
    err,
    res,
    std::dynamic_pointer_cast<couchbase::core::transactions::transactions>(
      conn->cluster()->transactions()),
    response);
  spdlog::trace("startTxn responding with {}", response->DebugString());
  return grpc::Status::OK;
}

grpc::Status
TxnService::transactionCreate(grpc::ServerContext* /*context*/, // FIXME(SA): use parameter
                              const protocol::transactions::TransactionCreateRequest* request,
                              protocol::transactions::TransactionResult* response)
{
  // limit length of the DebugString in cases of very large transaction lambdas...
  spdlog::info("transactionCreate called with {}", request->DebugString().substr(0, 2048));
  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }
  std::atomic<bool> timed_out{ false };
  Counters counters;
  return conn->run_with_timeout<grpc::Status>(
    std::chrono::seconds(TXN_TIMEOUT),
    [&]() {
      return startTxn(conn, request, response, timed_out, counters);
    },
    [&]() -> grpc::Status {
      timed_out = true;
      return { grpc::CANCELLED, "transactionCreate timed out!" };
    });
}

couchbase::core::document_id
to_couchbase_docid(const protocol::transactions::DocId& docid)
{
  return { docid.bucket_name(), docid.scope_name(), docid.collection_name(), docid.doc_id() };
}

auto
to_collection(const protocol::transactions::DocId& docid,
              const std::shared_ptr<couchbase::cluster>& cluster) -> couchbase::collection
{
  return cluster->bucket(docid.bucket_name())
    .scope(docid.scope_name())
    .collection(docid.collection_name());
}

couchbase::collection
to_collection(ConnectionPtr conn, const protocol::shared::DocLocation& loc)
{
  if (loc.has_specific()) {
    const auto& coll = loc.specific().collection();
    return conn->cluster()
      ->bucket(coll.bucket_name())
      .scope(coll.scope_name())
      .collection(coll.collection_name());
  }
  if (loc.has_pool()) {
    const auto& coll = loc.pool().collection();
    return conn->cluster()
      ->bucket(coll.bucket_name())
      .scope(coll.scope_name())
      .collection(coll.collection_name());
  }
  if (loc.has_uuid()) {
    const auto& coll = loc.uuid().collection();
    return conn->cluster()
      ->bucket(coll.bucket_name())
      .scope(coll.scope_name())
      .collection(coll.collection_name());
  }
  throw performer_exception::internal(
    fmt::format("to_collection doesn't understand {}", loc.DebugString()));
}

couchbase::collection
to_collection(ConnectionPtr conn, const protocol::shared::Collection coll)
{
  return conn->cluster()
    ->bucket(coll.bucket_name())
    .scope(coll.scope_name())
    .collection(coll.collection_name());
}

couchbase::scope
to_scope(ConnectionPtr conn, const protocol::shared::Scope scope)
{
  return conn->cluster()->bucket(scope.bucket_name()).scope(scope.scope_name());
}

couchbase::bucket
to_bucket(ConnectionPtr conn, std::string name)
{
  return conn->cluster()->bucket(name);
}

std::string
TxnService::to_key(const protocol::shared::DocLocation& loc, Counters& counters)
{
  if (loc.has_specific()) {
    return loc.specific().id();
  }
  if (loc.has_uuid()) {
    return couchbase::core::uuid::to_string(couchbase::core::uuid::random());
  }
  if (loc.has_pool()) {
    std::int64_t pool_size = loc.pool().pool_size();
    std::string id_preface = loc.pool().id_preface();
    if (loc.pool().has_random()) {
      std::uniform_int_distribution<std::int64_t> distribution(0, pool_size - 1);
      auto random_number = distribution(generator_);
      return id_preface + std::to_string(random_number);
    }
    if (loc.pool().has_counter()) {
      auto counter_id = loc.pool().counter().counter().counter_id();
      auto counter_value = counters.get_and_increment_counter(counter_id);
      return id_preface + std::to_string(counter_value % pool_size);
    }
    throw performer_exception::unimplemented(
      fmt::format("to_key can't handle {}", loc.DebugString()));
  }
  throw performer_exception::unimplemented(
    fmt::format("to_key can't handle {}", loc.DebugString()));
}

// TODO(SA): deal with transcoders.  For now, we only work for json docs!!
std::string
content_as_string(const protocol::shared::Content& content)
{
  // TODO(SA): Operations using this will change to use the version under fit_cxx::common which
  // properly returns the different types
  switch (content.content_case()) {
    case protocol::shared::Content::ContentCase::kConvertToJson:
      return content.convert_to_json();
    case protocol::shared::Content::ContentCase::kPassthroughString:
      return content.passthrough_string();
    case protocol::shared::Content::ContentCase::kByteArray:
      return content.byte_array();
    case protocol::shared::Content::ContentCase::kNull:
      return "null";
    default:
      throw performer_exception::internal(
        fmt::format("content_as_string called, but content not set {}", content.DebugString()));
  }
}

grpc::Status
TxnService::transactionCleanup(grpc::ServerContext* /*context*/, // FIXME(SA): use parameter
                               const protocol::transactions::TransactionCleanupRequest* request,
                               protocol::transactions::TransactionCleanupAttempt* response)
{
  spdlog::info("transactionCleanup called with {}", request->DebugString());
  couchbase::transactions::transactions_config config;
  std::vector<fit_cxx::TxnSvcHook> hooks;
  hooks.reserve(static_cast<std::size_t>(request->hook_size()));

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }

  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  try {
    auto txn = std::dynamic_pointer_cast<couchbase::core::transactions::transactions>(
      conn->cluster()->transactions());
    txn->cleanup().config().cleanup_hooks = std::move(hook_pair.second);
    auto id = to_couchbase_docid(request->atr());
    couchbase::core::transactions::atr_cleanup_entry entry(
      id, request->attempt_id(), txn->cleanup());
    couchbase::core::transactions::transactions_cleanup_attempt attempt(entry);
    txn->cleanup().force_cleanup_entry(entry, attempt);
    TxnSvcUtils::create_transaction_cleanup_attempt(attempt, response);
    spdlog::trace("transactionCleanup returning {}", response->DebugString());
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return { ::grpc::CANCELLED, e.what() };
  }
}

grpc::Status
TxnService::transactionCleanupATR(
  grpc::ServerContext* /*context*/, // FIXME(SA): use parameter
  const protocol::transactions::TransactionCleanupATRRequest* request,
  protocol::transactions::TransactionCleanupATRResult* response)
{
  // we must make our own factory (no transactionfactoryref in the request)
  spdlog::info("transactionCleanupATR called with {}", request->DebugString());
  std::vector<fit_cxx::TxnSvcHook> hooks;
  hooks.reserve(static_cast<std::size_t>(request->hook_size()));

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }
  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  try {
    auto txn = std::dynamic_pointer_cast<couchbase::core::transactions::transactions>(
      conn->cluster()->transactions());
    txn->config().attempt_context_hooks = std::move(hook_pair.first);
    txn->cleanup().config().cleanup_hooks = std::move(hook_pair.second);

    auto atr_id = to_couchbase_docid(request->atr());
    std::vector<couchbase::core::transactions::transactions_cleanup_attempt> results;
    auto stats = txn->cleanup().force_cleanup_atr(atr_id, results);
    for (const auto& attempt : results) {
      TxnSvcUtils::create_transaction_cleanup_attempt(attempt, response->add_result());
    }
    response->set_num_entries(static_cast<std::int32_t>(stats.num_entries));
    response->set_num_expired_entries(static_cast<std::int32_t>(results.size()));
    spdlog::trace("clean atr returning {}", response->DebugString());
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return grpc::Status(::grpc::CANCELLED, e.what());
  }
}

template<typename Command, typename Result>
void
verify_op_result(const Command& /* cmd */, const Result& /* result */)
{
  /* do nothing by default */
}

template<>
void
verify_op_result(const protocol::transactions::CommandGet& cmd,
                 const couchbase::transactions::transaction_get_result& result)
{
  if (cmd.has_content_as_validation()) {
    fit_cxx::commands::common::validate_content_as(
      cmd.content_as_validation(), [&]() -> protocol::shared::ContentTypes {
        protocol::shared::ContentTypes content;
        fit_cxx::commands::common::result_to_content(result,
                                                     fit_cxx::commands::common::to_transcoder(cmd),
                                                     cmd.content_as_validation().content_as(),
                                                     &content);
        return content;
      });
  }
}

template<>
void
verify_op_result(const protocol::transactions::CommandGetOptional& cmd,
                 const couchbase::transactions::transaction_get_result& result)
{
  if (cmd.has_content_as_validation()) {
    fit_cxx::commands::common::validate_content_as(
      cmd.content_as_validation(), [&]() -> protocol::shared::ContentTypes {
        protocol::shared::ContentTypes content;
        fit_cxx::commands::common::result_to_content(result,
                                                     fit_cxx::commands::common::to_transcoder(cmd),
                                                     cmd.content_as_validation().content_as(),
                                                     &content);
        return content;
      });
  }
}

template<>
void
verify_op_result(const protocol::transactions::CommandGetReplicaFromPreferredServerGroup& cmd,
                 const couchbase::transactions::transaction_get_result& result)
{
  if (cmd.has_content_as_validation()) {
    fit_cxx::commands::common::validate_content_as(
      cmd.content_as_validation(), [&]() -> protocol::shared::ContentTypes {
        protocol::shared::ContentTypes content;
        fit_cxx::commands::common::result_to_content(result,
                                                     fit_cxx::commands::common::to_transcoder(cmd),
                                                     cmd.content_as_validation().content_as(),
                                                     &content);
        return content;
      });
  }
}

template<typename TransactionGetMultiResult>
void
verify_op_result(const protocol::transactions::CommandGetMulti& cmd,
                 const std::optional<TransactionGetMultiResult>& result)
{
  if (!result) {
    return;
  }
  for (std::size_t idx = 0; idx < static_cast<std::size_t>(cmd.specs().size()); ++idx) {
    const auto& proto_spec = cmd.specs()[static_cast<int>(idx)];
    if (result->exists(idx) != proto_spec.expect_present()) {
      throw performer_exception::failed_precondition(
        fmt::format("Expected exists at index {} to be {}, was {}",
                    idx,
                    proto_spec.expect_present(),
                    result->exists(idx)));
    }
    if (proto_spec.has_content_as_validation()) {
      fit_cxx::commands::common::validate_content_as(
        proto_spec.content_as_validation(), [&]() -> protocol::shared::ContentTypes {
          protocol::shared::ContentTypes content;
          fit_cxx::commands::common::multi_result_to_content(
            result.value(),
            idx,
            fit_cxx::commands::common::to_spec_transcoder(proto_spec),
            proto_spec.content_as_validation().content_as(),
            &content);
          return content;
        });
    }
  }
}

template<typename Command, typename Operation>
auto
execute_op(const Command& cmd,
           std::shared_ptr<couchbase::transactions::attempt_context> ctx,
           bool do_not_propagate_error,
           const std::string& logger_prefix,
           [[maybe_unused]] bool is_insert,
           [[maybe_unused]] bool is_query,
           Operation op) -> couchbase::error
{
  spdlog::debug("{} calling op", logger_prefix);
  auto [err, result] = op(ctx);
  spdlog::debug(R"({} called op, got {} (cause: {}))",
                logger_prefix,
                err,
                err.cause() ? err.cause()->ec().message() : "<unset>");
  auto pass = TxnSvcUtils::validate_op_result(cmd.expected_result(), err);
  if (!pass) {
    std::string expected_results_debug_string{};
    for (const protocol::transactions::ExpectedResult& expected : cmd.expected_result()) {
      expected_results_debug_string += expected.ShortDebugString() + "\n";
    }
    auto msg = fmt::format("{} Transaction operation did not have the expected result.\n"
                           "  Expected results: {}  Error from SDK: {} (cause: {})",
                           logger_prefix,
                           expected_results_debug_string,
                           err,
                           err.cause() ? err.cause()->ec().message() : "<unset>");
    spdlog::error(msg);
    throw performer_exception::failed_precondition(msg);
  }
  if (do_not_propagate_error) {
    return {};
  }
  verify_op_result(cmd, result);
  return err;
}

template<typename Cmd>
void
check_for_wait(const Cmd& cmd)
{
  if (cmd.wait_msecs() > 0) {
    spdlog::trace("waiting {}ms before executing cmd {}", cmd.wait_msecs(), cmd.DebugString());
    std::this_thread::sleep_for(std::chrono::milliseconds(cmd.wait_msecs()));
    spdlog::trace("done waiting {}ms, executing cmd {}", cmd.wait_msecs(), cmd.DebugString());
  }
}

auto
TxnService::to_get_multi_specs(ConnectionPtr conn,
                               const protocol::transactions::TransactionCommand& cmd,
                               Counters& counters)
  -> std::vector<couchbase::transactions::transaction_get_multi_spec>
{
  std::vector<couchbase::transactions::transaction_get_multi_spec> specs;
  specs.reserve(static_cast<std::size_t>(cmd.get_multi().specs_size()));

  for (const auto& spec : cmd.get_multi().specs()) {
    specs.emplace_back(to_collection(conn, spec.location()), to_key(spec.location(), counters));
  }

  return specs;
}

auto
TxnService::to_get_multi_options(const protocol::transactions::TransactionCommand& cmd)
  -> couchbase::transactions::transaction_get_multi_options
{
  couchbase::transactions::transaction_get_multi_options options;
  switch (cmd.get_multi().options().mode()) {
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_PRIORITISE_LATENCY:
      options.mode(couchbase::transactions::transaction_get_multi_mode::prioritise_latency);
      break;
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_DISABLE_READ_SKEW_DETECTION:
      options.mode(
        couchbase::transactions::transaction_get_multi_mode::disable_read_skew_detection);
      break;
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_PRIORITISE_READ_SKEW_DETECTION:
      options.mode(
        couchbase::transactions::transaction_get_multi_mode::prioritise_read_skew_detection);
      break;
    default:
      break;
  }
  return {};
}

auto
TxnService::to_get_multi_replicas_from_preferred_server_group_specs(
  ConnectionPtr conn,
  const protocol::transactions::TransactionCommand& cmd,
  Counters& counters)
  -> std::vector<
    couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_spec>
{
  std::vector<
    couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_spec>
    specs;
  specs.reserve(static_cast<std::size_t>(cmd.get_multi().specs_size()));

  for (const auto& spec : cmd.get_multi().specs()) {
    specs.emplace_back(to_collection(conn, spec.location()), to_key(spec.location(), counters));
  }
  return specs;
}

auto
TxnService::to_get_multi_replicas_from_preferred_server_group_options(
  const protocol::transactions::TransactionCommand& cmd)
  -> couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_options
{
  couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_options
    options;
  switch (cmd.get_multi().options().mode()) {
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_PRIORITISE_LATENCY:
      options.mode(
        couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_mode::
          prioritise_latency);
      break;
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_DISABLE_READ_SKEW_DETECTION:
      options.mode(
        couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_mode::
          disable_read_skew_detection);
      break;
    case protocol::transactions::
      TransactionGetMultiOptions_TransactionGetMultiMode_PRIORITISE_READ_SKEW_DETECTION:
      options.mode(
        couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_mode::
          prioritise_read_skew_detection);
      break;
    default:
      break;
  }
  return {};
}

couchbase::error
TxnService::execute_command(ConnectionPtr conn,
                            std::shared_ptr<couchbase::transactions::attempt_context> ctx,
                            const protocol::transactions::TransactionCommand& cmd,
                            const std::string& logger_prefix,
                            Counters& counters,
                            bool is_batch)
{
  spdlog::trace("{} executing {}", logger_prefix, cmd.DebugString());
  check_for_wait(cmd);
  if (cmd.has_test_fail()) {
    throw performer_exception::failed_precondition("Forcing test failure");
  }
  if (cmd.has_get()) {
    auto coll = to_collection(cmd.get().doc_id(), conn->cluster());
    auto id = cmd.get().doc_id().doc_id();
    return execute_op(
      cmd.get(),
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        auto [err, result] = c->get(coll, id);
        if (!err.ec()) {
          stashed_result_ = result;
          if (cmd.get().has_stash_in_slot()) {
            stashed_result_slots_[static_cast<unsigned int>(cmd.get().stash_in_slot())] = result;
          }
        }
        return std::pair{ err, result };
      });
  }
  if (cmd.has_get_from_preferred_server_group()) {
    auto coll = to_collection(cmd.get_from_preferred_server_group().doc_id(), conn->cluster());
    auto id = cmd.get_from_preferred_server_group().doc_id().doc_id();
    return execute_op(cmd.get_from_preferred_server_group(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, result] = c->get_replica_from_preferred_server_group(coll, id);
                        if (!err.ec()) {
                          stashed_result_ = result;
                          if (cmd.get_from_preferred_server_group().has_stash_in_slot()) {
                            stashed_result_slots_[static_cast<unsigned int>(
                              cmd.get_from_preferred_server_group().stash_in_slot())] = result;
                          }
                        }
                        fmt::println(stderr, "{}, {}", err.message(), err.ec().message());
                        return std::pair{ err, result };
                      });
  }
  if (cmd.has_get_multi()) {
    if (cmd.get_multi().get_multi_replicas_from_preferred_server_group()) {
      return execute_op(
        cmd.get_multi(),
        ctx,
        cmd.do_not_propagate_error(),
        logger_prefix,
        false,
        false,
        [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
          auto [err, result] = c->get_multi_replicas_from_preferred_server_group(
            to_get_multi_replicas_from_preferred_server_group_specs(conn, cmd, counters),
            to_get_multi_replicas_from_preferred_server_group_options(cmd));
          fmt::println("get_multi_replicas: err.message: {}, error.ec.message: {}",
                       err.message(),
                       err.ec().message());
          return std::pair{ err, result };
        });
    }
    return execute_op(cmd.get_multi(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, result] = c->get_multi(to_get_multi_specs(conn, cmd, counters),
                                                          to_get_multi_options(cmd));
                        fmt::println("get_multi: err.message: {}, err.ec.message: {}",
                                     err.message(),
                                     err.ec().message());
                        return std::pair{ err, result };
                      });
  }
  if (cmd.has_get_optional()) {
    auto coll = to_collection(cmd.get_optional().get().doc_id(), conn->cluster());
    auto id = cmd.get_optional().get().doc_id().doc_id();
    return execute_op(cmd.get_optional().get(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, result] = c->get(coll, id);
                        if (cmd.get_optional().expect_doc_present()) {
                          if (err.ec() == couchbase::errc::transaction_op::document_not_found) {
                            // expected it to be present and it isn't
                            throw performer_exception::failed_precondition(
                              "expected doc to be present and it isn't");
                          }
                          // so stash it.
                          stashed_result_ = result;
                          return std::pair{ err, result };
                        }
                        // we expect the doc to be absent, so there better be an error...
                        if (!err.ec()) {
                          throw performer_exception::failed_precondition(
                            "expected doc to be absent, but it isn't");
                        }
                        // otherwise this was successful
                        return std::pair{ couchbase::error{}, result };
                      });
  }
  if (cmd.has_insert()) {
    auto coll = to_collection(cmd.insert().doc_id(), conn->cluster());
    auto id = cmd.insert().doc_id().doc_id();
    return execute_op(
      cmd.insert(),
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      true,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> attempt_ctx) {
        if (cmd.insert().has_content()) {
          auto transcoder = fit_cxx::commands::common::to_transcoder(cmd.insert());
          auto content = fit_cxx::commands::common::to_content(cmd.insert().content());
          auto [err, result] = std::visit(
            fit_cxx::commands::common::overloaded{
              [&, coll, id](auto data, couchbase::codec::default_json_transcoder coder) {
                return attempt_ctx->template insert<decltype(coder)>(coll, id, std::move(data));
              },
              [&, coll, id](std::vector<std::byte> data,
                            couchbase::codec::raw_binary_transcoder coder) {
                return attempt_ctx->template insert<decltype(coder)>(coll, id, std::move(data));
              },
              [&, coll, id](std::vector<std::byte> data,
                            couchbase::codec::raw_json_transcoder coder) {
                return attempt_ctx->template insert<decltype(coder)>(coll, id, std::move(data));
              },
              [&, coll, id](std::string data, couchbase::codec::raw_json_transcoder coder) {
                return attempt_ctx->template insert<decltype(coder)>(coll, id, std::move(data));
              },
              [&, coll, id](std::string data, couchbase::codec::raw_string_transcoder coder) {
                return attempt_ctx->template insert<decltype(coder)>(coll, id, std::move(data));
              },
              [&, coll, id](auto data, std::monostate) {
                return attempt_ctx->insert(coll, id, std::move(data));
              },
              [&](auto /*data*/, auto /*coder*/)
                -> std::pair<couchbase::error, couchbase::transactions::transaction_get_result> {
                throw performer_exception::unimplemented(
                  "The SDK does not support the given content/transcoder combination");
              },
            },
            content,
            transcoder);
          return std::pair{ err, result };
        }
        auto [err, result] =
          attempt_ctx->template insert<couchbase::codec::default_json_transcoder>(
            coll, id, couchbase::core::utils::json::parse(cmd.insert().content_json()));
        return std::pair{ err, result };
      });
  }
  if (cmd.has_replace()) {
    auto coll = to_collection(cmd.replace().doc_id(), conn->cluster());
    auto id = cmd.replace().doc_id().doc_id();
    return execute_op(
      cmd.replace(),
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> attempt_ctx) {
        if (cmd.replace().use_stashed_result()) {
          if (const auto& stashed_result = stashed_result_; stashed_result) {
            if (cmd.replace().has_content()) {
              auto transcoder = fit_cxx::commands::common::to_transcoder(cmd.replace());
              auto content = fit_cxx::commands::common::to_content(cmd.replace().content());
              auto [err, result] = std::visit(
                fit_cxx::commands::common::overloaded{
                  [&, stashed_result](auto data, couchbase::codec::default_json_transcoder coder) {
                    return attempt_ctx->template replace<decltype(coder)>(stashed_result.value(),
                                                                          std::move(data));
                  },
                  [&, stashed_result](std::vector<std::byte> data,
                                      couchbase::codec::raw_binary_transcoder coder) {
                    return attempt_ctx->template replace<decltype(coder)>(stashed_result.value(),
                                                                          std::move(data));
                  },
                  [&, stashed_result](std::vector<std::byte> data,
                                      couchbase::codec::raw_json_transcoder coder) {
                    return attempt_ctx->template replace<decltype(coder)>(stashed_result.value(),
                                                                          std::move(data));
                  },
                  [&, stashed_result](std::string data,
                                      couchbase::codec::raw_json_transcoder coder) {
                    return attempt_ctx->template replace<decltype(coder)>(stashed_result.value(),
                                                                          std::move(data));
                  },
                  [&, stashed_result](std::string data,
                                      couchbase::codec::raw_string_transcoder coder) {
                    return attempt_ctx->template replace<decltype(coder)>(stashed_result.value(),
                                                                          std::move(data));
                  },
                  [&, stashed_result](auto data, std::monostate) {
                    return attempt_ctx->replace(stashed_result.value(), std::move(data));
                  },
                  [&](auto /*data*/, auto /*coder*/)
                    -> std::pair<couchbase::error,
                                 couchbase::transactions::transaction_get_result> {
                    throw performer_exception::unimplemented(
                      "The SDK does not support the given content/transcoder combination");
                  },
                },
                content,
                transcoder);
              return std::pair{ err, result };
            }
            auto [err, result] =
              attempt_ctx->template replace<couchbase::codec::default_json_transcoder>(
                stashed_result_.value(),
                couchbase::core::utils::json::parse(cmd.replace().content_json()));
            return std::pair{ err, result };
          }
          std::string msg("You have not done a get operation yet, cannot "
                          "use stashed result in replace");
          spdlog::warn(msg);
          throw performer_exception::internal(msg);
        }
        // get it first.  <sigh>
        auto res = attempt_ctx->get(coll, id);
        if (res.first.ec() == couchbase::errc::transaction_op::document_not_found) {
          throw couchbase::core::transactions::transaction_operation_failed(
            couchbase::core::transactions::FAIL_DOC_NOT_FOUND, "doc not found")
            .cause(couchbase::core::transactions::external_exception::DOCUMENT_NOT_FOUND_EXCEPTION);
        }
        auto doc = res.second;
        if (cmd.replace().has_content()) {
          auto transcoder = fit_cxx::commands::common::to_transcoder(cmd.replace());
          auto content = fit_cxx::commands::common::to_content(cmd.replace().content());
          auto [err, result] = std::visit(
            fit_cxx::commands::common::overloaded{
              [&, doc](auto data, couchbase::codec::default_json_transcoder coder) {
                return attempt_ctx->template replace<decltype(coder)>(doc, std::move(data));
              },
              [&, doc](std::vector<std::byte> data, couchbase::codec::raw_binary_transcoder coder) {
                return attempt_ctx->template replace<decltype(coder)>(doc, std::move(data));
              },
              [&, doc](std::vector<std::byte> data, couchbase::codec::raw_json_transcoder coder) {
                return attempt_ctx->template replace<decltype(coder)>(doc, std::move(data));
              },
              [&, doc](std::string data, couchbase::codec::raw_json_transcoder coder) {
                return attempt_ctx->template replace<decltype(coder)>(doc, std::move(data));
              },
              [&, doc](std::string data, couchbase::codec::raw_string_transcoder coder) {
                return attempt_ctx->template replace<decltype(coder)>(doc, std::move(data));
              },
              [&, doc](auto data, std::monostate) {
                return attempt_ctx->replace(doc, std::move(data));
              },
              [&](auto /*data*/, auto /*coder*/)
                -> std::pair<couchbase::error, couchbase::transactions::transaction_get_result> {
                throw performer_exception::unimplemented(
                  "The SDK does not support the given content/transcoder combination");
              },
            },
            content,
            transcoder);
          return std::pair{ err, result };
        }
        auto [err, result] =
          attempt_ctx->template replace<couchbase::codec::default_json_transcoder>(
            doc, couchbase::core::utils::json::parse(cmd.replace().content_json()));
        return std::pair{ err, result };
      });
  }
  if (cmd.has_remove()) {
    auto coll = to_collection(cmd.remove().doc_id(), conn->cluster());
    auto id = cmd.remove().doc_id().doc_id();
    return execute_op(cmd.remove(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        if (cmd.remove().use_stashed_result()) {
                          if (stashed_result_) {
                            return std::pair{ c->remove(stashed_result_.value()),
                                              couchbase::transactions::transaction_get_result{} };
                          }
                          std::string msg("You have not performed a get operation yet, cannot "
                                          "use stashed result in remove");
                          spdlog::warn(msg);
                          throw performer_exception::internal(msg);
                        }
                        // get it first.  <sigh>
                        auto [err, doc] = c->get(coll, id);
                        if (err) {
                          return std::pair{ err, doc };
                        }
                        return std::pair{ c->remove(doc),
                                          couchbase::transactions::transaction_get_result{} };
                      });
  }
  if (cmd.has_get_v2()) {
    auto req = cmd.get_v2();
    auto coll = to_collection(conn, req.location());
    auto id = to_key(req.location(), counters);
    return execute_op(
      req,
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        auto [err, result] = c->get(coll, id);
        if (!err.ec()) {
          stashed_result_ = result;
          if (cmd.get().has_stash_in_slot()) {
            stashed_result_slots_[static_cast<unsigned int>(cmd.get().stash_in_slot())] = result;
          }
        }
        return std::pair{ err, result };
      });
  }
  if (cmd.has_replace_v2()) {
    auto req = cmd.replace_v2();
    auto coll = to_collection(conn, req.location());
    auto id = to_key(req.location(), counters);
    auto content_str = content_as_string(req.content());
    auto content = couchbase::core::utils::json::parse(std::string_view{ content_str });
    return execute_op(
      req,
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        if (req.has_use_stashed_slot()) {
          auto stashed = stashed_result_slots_[static_cast<unsigned int>(req.use_stashed_slot())];
          auto [err, res] = c->replace(stashed, content);
          return std::pair{ err, res };
        }
        auto [_err, doc] = c->get(coll, id);
        if (_err) {
          return std::pair{ _err, doc };
        }
        auto [err, _res] = c->replace(doc, content);
        return std::pair{ err, _res };
      });
  }
  if (cmd.has_remove_v2()) {
    auto req = cmd.remove_v2();
    auto coll = to_collection(conn, req.location());
    auto id = to_key(req.location(), counters);
    return execute_op(
      req,
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        if (req.has_use_stashed_slot()) {
          auto stashed = stashed_result_slots_[static_cast<unsigned int>(req.use_stashed_slot())];
          return std::pair{ c->remove(stashed), couchbase::transactions::transaction_get_result{} };
        }
        auto [err, doc] = c->get(coll, id);
        if (err) {
          return std::pair{ err, doc };
        }
        return std::pair{ c->remove(doc), couchbase::transactions::transaction_get_result{} };
      });
  }
  if (cmd.has_insert_v2()) {
    auto req = cmd.insert_v2();
    auto coll = to_collection(conn, req.location());
    auto id = to_key(req.location(), counters);
    auto insert_content_str = content_as_string(req.content());
    auto content = couchbase::core::utils::json::parse(std::string_view{ insert_content_str });
    return execute_op(req,
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      true,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, res] = c->insert(coll, id, content);
                        return std::pair{ err, res };
                      });
  }
  if (cmd.has_get_v2()) {
    auto req = cmd.get_v2();
    auto coll = to_collection(conn, req.location());
    auto key = to_key(req.location(), counters);
    return coll.remove(key, {}).get().first.ec();
  }
  if (cmd.has_commit()) {
    if (is_batch) {
      auto core_ctx =
        std::dynamic_pointer_cast<couchbase::core::transactions::attempt_context_impl>(ctx);
      core_ctx->commit();
    } else {
      spdlog::trace("ignoring commit command");
    }
  } else if (cmd.has_rollback()) {
    if (is_batch) {
      auto core_ctx =
        std::dynamic_pointer_cast<couchbase::core::transactions::attempt_context_impl>(ctx);
      core_ctx->rollback();
    } else {
      // this should just raise an exception
      throw couchbase::core::transactions::transaction_operation_failed(
        couchbase::core::transactions::error_class::FAIL_OTHER, "got cmd rollback");
    }
  } else if (cmd.has_query()) {
    auto opts = TxnSvcUtils::to_transactions_query_options(cmd.query());
    return execute_op(cmd.query(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      true,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto scope = conn->cluster()
                                       ->bucket(cmd.query().scope().bucket_name())
                                       .scope(cmd.query().scope().scope_name());
                        if (cmd.query().has_scope()) {
                          auto [err, result] = c->query(scope, cmd.query().statement(), opts);
                          return std::pair{ err, result };
                        }
                        auto [err, result] = c->query(cmd.query().statement(), opts);
                        return std::pair{ err, result };
                      });
  } else if (cmd.has_replace_regular_kv()) {
    auto id = to_couchbase_docid(cmd.replace_regular_kv().doc_id());
    auto coll = to_collection(cmd.replace_regular_kv().doc_id(), conn->cluster());
    return coll
      .replace(
        id.key(), couchbase::core::utils::json::parse(cmd.replace_regular_kv().content_json()), {})
      .get()
      .first.ec();
  } else if (cmd.has_remove_regular_kv()) {
    auto id = to_couchbase_docid(cmd.remove_regular_kv().doc_id());
    auto coll = to_collection(cmd.remove_regular_kv().doc_id(), conn->cluster());
    return coll.remove(id.key(), {}).get().first.ec();
  } else if (cmd.has_insert_regular_kv()) {
    auto id = to_couchbase_docid(cmd.insert_regular_kv().doc_id());
    auto coll = to_collection(cmd.insert_regular_kv().doc_id(), conn->cluster());
    return coll
      .insert(
        id.key(), couchbase::core::utils::json::parse(cmd.insert_regular_kv().content_json()), {})
      .get()
      .first.ec();
  } else if (cmd.has_throw_exception()) {
    return { couchbase::errc::transaction_op::generic, "got cmd ThrowException, returning error" };
  } else if (cmd.has_wait_on_latch()) {
    conn->should_read(true);
    conn->get_latch(cmd.wait_on_latch().latch_name()).wait();
    conn->should_read(false);
  } else if (cmd.has_set_latch()) {
    conn->get_latch(cmd.set_latch().latch_name()).set();
    conn->call_latch_callback(cmd.set_latch().latch_name());
  } else if (cmd.has_parallelize()) {
    uint32_t in_flight = 0;
    std::mutex mtx;
    std::condition_variable cv;
    // since we are fed the _synchronous_ transaction context, lets make N threads, have each of the
    // threads eat commands off the list of commands and perform them synchronously.   That insures
    // if we make the number of threads equal to the parallelism requested, we will always have that
    // level of parallelism.
    auto cmd_iterator = cmd.parallelize().commands().begin();
    auto thread_fn = [&]() {
      std::unique_lock<std::mutex> lock(mtx);

      do {
        cv.wait(lock, [&cmd, &in_flight]() {
          return in_flight < static_cast<std::uint32_t>(cmd.parallelize().parallelism());
        });
        if (cmd_iterator == cmd.parallelize().commands().end()) {
          spdlog::trace("batch thread reached end of commands");
          return;
        }
        const auto& c = *cmd_iterator;
        cmd_iterator++;
        in_flight++;
        spdlog::trace("async executing command, {} now in-flight", in_flight);
        lock.unlock();
        execute_command(conn, ctx, c, logger_prefix, counters, true);
        lock.lock();
        in_flight--;
        lock.unlock();
        cv.notify_one();
        lock.lock();
      } while (true);
    };
    std::list<std::future<void>> futures{};
    for (std::int32_t i = 0; i < cmd.parallelize().parallelism(); i++) {
      futures.emplace_back(std::async(std::launch::async, thread_fn));
    }
    // now wait for 'em
    for (auto& f : futures) {
      f.get();
    }
  } else {
    throw performer_exception::unimplemented("Do not understand transaction command " +
                                             cmd.DebugString());
  }
  return {};
}

grpc::Status
TxnService::clientRecordProcess(grpc::ServerContext* /*context*/,
                                const protocol::transactions::ClientRecordProcessRequest* request,
                                protocol::transactions::ClientRecordProcessResponse* response)
{
  spdlog::info("clientRecordProcess called with {}", request->DebugString());
  std::vector<fit_cxx::TxnSvcHook> hooks;
  hooks.reserve(static_cast<std::size_t>(request->hook_size()));

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }

  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  auto txn = std::dynamic_pointer_cast<couchbase::core::transactions::transactions>(
    conn->cluster()->transactions());
  txn->config().attempt_context_hooks = std::move(hook_pair.first);
  txn->cleanup().config().cleanup_hooks = std::move(hook_pair.second);
  return conn->run_with_timeout<grpc::Status>(
    CLIENT_RECORD_PROCESS_TIMEOUT,
    [&] {
      try {
        auto details = txn->cleanup().get_active_clients(
          { request->bucket_name(), request->scope_name(), request->collection_name() },
          request->client_uuid());
        response->set_num_active_clients(static_cast<std::int32_t>(details.num_active_clients));
        response->set_index_of_this_client(static_cast<std::int32_t>(details.index_of_this_client));
        response->set_client_uuid(details.client_uuid);
        for (const auto& id : details.expired_client_ids) {
          response->add_expired_client_ids(id);
        }
        response->set_success(true);
        response->set_num_existing_clients(static_cast<std::int32_t>(details.num_existing_clients));
        response->set_num_active_clients(static_cast<std::int32_t>(details.num_active_clients));
        response->set_num_expired_clients(static_cast<std::int32_t>(details.num_expired_clients));
        response->set_override_enabled(details.override_enabled);
        response->set_override_expires(static_cast<std::int64_t>(details.override_expires));
        response->set_override_active(static_cast<std::int64_t>(details.override_active));
        response->set_cas_now_nanos(static_cast<std::int64_t>(details.cas_now_nanos));

      } catch (const std::exception& e) {
        response->set_success(false);
        spdlog::info("got {}", e.what());
      } catch (...) {
        response->set_success(false);
        spdlog::error("unknown exception during clientRecordProcess");
      }
      spdlog::trace("clientRecordProcess returning {}", response->DebugString());
      return grpc::Status::OK;
    },
    [&] {
      spdlog::error("clientRecordProcess took too long, aborted");
      return grpc::Status::CANCELLED;
    });
}

grpc::Status
TxnService::transactionStream(
  grpc::ServerContext* /*context*/,
  grpc::ServerReaderWriter<protocol::transactions::TransactionStreamPerformerToDriver,
                           protocol::transactions::TransactionStreamDriverToPerformer>* stream)
{
  protocol::transactions::TransactionCreateRequest request;
  ConnectionPtr conn;
  protocol::transactions::TransactionStreamDriverToPerformer d2p;
  std::list<std::thread> txn_threads;
  spdlog::info("transactionStream called");
  std::atomic<bool> done{ false };
  Counters counters;

  while (stream->Read(&d2p)) {
    if (d2p.has_create()) {
      spdlog::info("got create: {}", d2p.DebugString());
      protocol::transactions::TransactionStreamPerformerToDriver created;
      request = d2p.create();
      conn = getConn(request.cluster_connection_id());
      if (!conn) {
        return { ::grpc::CANCELLED,
                 fmt::format("no connection with ID={}", request.cluster_connection_id()) };
      }
      conn->should_read(true);
      created.set_allocated_created(
        protocol::transactions::TransactionCreated::default_instance().New());
      stream->Write(created);
      conn->set_latch_callback([&](const std::string& name) {
        protocol::transactions::TransactionStreamPerformerToDriver broadcast;
        auto b =
          protocol::transactions::BroadcastToOtherConcurrentTransactionsRequest::default_instance()
            .New();
        auto l = protocol::transactions::CommandSetLatch::default_instance().New();
        l->set_latch_name(name);
        b->set_allocated_latch_set(l);
        broadcast.set_allocated_broadcast(b);
        stream->Write(broadcast);
      });
    } else if (d2p.has_start()) {
      spdlog::info("{} got start, creating txn in thread... {}", request.name(), d2p.DebugString());
      conn->should_read(false);
      txn_threads.emplace_back([&]() {
        auto result = protocol::transactions::TransactionResult::default_instance().New();
        protocol::transactions::TransactionStreamPerformerToDriver finalresult;
        conn->run_with_timeout<grpc::Status>(
          std::chrono::seconds(TXN_TIMEOUT),
          [&]() {
            return startTxn(conn, &request, result, done, counters);
          },
          [&]() -> grpc::Status {
            done = true;
            return { grpc::StatusCode::DEADLINE_EXCEEDED, "startTxn in stream timed out!" };
          });
        finalresult.set_allocated_final_result(result);
        stream->Write(finalresult);
        spdlog::trace("{} is done, wrote results {}", request.name(), finalresult.DebugString());
        done = true;
      });
    } else if (d2p.has_broadcast()) {
      spdlog::info("{} got broadcast {}", request.name(), d2p.DebugString());
      if (d2p.broadcast().has_latch_set()) {
        auto latch_name = d2p.broadcast().latch_set().latch_name();
        // we may be about to stop waiting, so other thread will enable a read if needed
        conn->should_read(false);
        conn->get_latch(latch_name).set();
      } else {
        spdlog::error("broadcast has no latchSet message {}", d2p.broadcast().DebugString());
        throw performer_exception::internal("unexpected message in broadcast");
      }
    } else {
      spdlog::error("transactionStream unexpected value (not create, start, broadcast) {}",
                    d2p.DebugString());
      throw performer_exception::internal("unexpected value in DriverToPerformer");
    }
    while (!conn->should_read()) {
      spdlog::trace("{} should_read is false, will sleep", request.name());
      if (!done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        spdlog::trace("{} woke up", request.name());
      } else {
        break;
      }
    }
    spdlog::trace("{} checking if done", request.name());
    if (done.load()) {
      spdlog::trace("{} done, stopping read loop", request.name());
      break;
    }
    spdlog::trace("{} will block to read", request.name());
  }
  spdlog::trace("read complete, joining {} threads", txn_threads.size());
  for (auto& t : txn_threads) {
    if (t.joinable()) {
      t.join();
    }
  }
  spdlog::info("{} threads joined", txn_threads.size());
  conn->clear_latches();
  return grpc::Status::OK;
}

grpc::Status
TxnService::performerCapsFetch(grpc::ServerContext* /*context*/,
                               const protocol::performer::PerformerCapsFetchRequest* request,
                               protocol::performer::PerformerCapsFetchResponse* response)
{
  spdlog::info("performerCapsFetch called with {}", request->DebugString());
  auto info = couchbase::core::meta::sdk_build_info();
  response->set_transactions_protocol_version(info["txns_forward_compat_protocol_version"]);
  response->set_library_version(couchbase::core::meta::sdk_semver());
  std::stringstream s_stream(info["txns_forward_compat_extensions"]);
  while (s_stream.good()) {
    std::string cap;
    getline(s_stream, cap, ','); // get first string delimited by comma
    response->add_transaction_implementations_caps(
      TxnSvcUtils::convert_extension_to_performer_caps(cap));
  }
  response->set_performer_user_agent("cpp");

  response->add_performer_caps(protocol::performer::Caps::KV_SUPPORT_1);
  response->add_performer_caps(protocol::performer::Caps::TRANSACTIONS_SUPPORT_1);
  response->add_performer_caps(protocol::performer::Caps::CLUSTER_CONFIG_CERT);
  response->add_performer_caps(protocol::performer::Caps::CLUSTER_CONFIG_INSECURE);
  response->add_performer_caps(protocol::performer::Caps::CUSTOM_SERIALIZATION_SUPPORT_FOR_SEARCH);
  response->add_performer_caps(protocol::performer::Caps::CONTENT_AS_PERFORMER_VALIDATION);
#ifdef COUCHBASE_CXX_CLIENT_STABLE_OTEL_CONVENTIONS
  response->add_performer_caps(protocol::performer::Caps::OBSERVABILITY_1);
#endif

  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_KV);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_QUERY);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_LOOKUP_IN);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_QUERY_INDEX_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_BUCKET_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_COLLECTION_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_MANAGEMENT_HISTORY_RETENTION);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_COLLECTION_QUERY_INDEX_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_KV_RANGE_SCAN);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_QUERY_READ_FROM_REPLICA);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_LOOKUP_IN_REPLICAS);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_SEARCH_INDEX_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_DOCUMENT_NOT_LOCKED);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_SEARCH);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_VECTOR_SEARCH);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_SCOPE_SEARCH_INDEX_MANAGEMENT);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_SCOPE_SEARCH);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_INDEX_MANAGEMENT_RFC_REVISION_25);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_VECTOR_SEARCH_BASE64);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_ZONE_AWARE_READ_FROM_REPLICA);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SUPPORTS_AUTHENTICATOR);
#ifdef COUCHBASE_CXX_CLIENT_SUPPORTS_APP_TELEMETRY
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_APP_TELEMETRY);
#endif
#ifdef COUCHBASE_CXX_CLIENT_HAS_BUCKET_SETTINGS_NUM_VBUCKETS
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_BUCKET_SETTINGS_NUM_VBUCKETS);
#endif
#ifdef COUCHBASE_CXX_CLIENT_QUERY_SUPPORTS_BOTH_POSITIONAL_NAMED_PARAMETERS
  response->add_sdk_implementation_caps(
    protocol::sdk::Caps::SDK_QUERY_BOTH_POSITIONAL_AND_NAMED_PARAMETERS);
#endif
#ifdef COUCHBASE_CXX_CLIENT_HAS_VECTOR_SEARCH_PREFILTER
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_PREFILTER_VECTOR_SEARCH);
#endif
#ifdef COUCHBASE_CXX_CLIENT_HAS_SET_AUTHENTICATOR
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_SET_AUTHENTICATOR);
#endif
#ifdef COUCHBASE_CXX_CLIENT_STABLE_OTEL_CONVENTIONS
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_OBSERVABILITY_CLUSTER_LABELS);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_OBSERVABILITY_RFC_REV_24);
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS);
  response->add_sdk_implementation_caps(
    protocol::sdk::Caps::SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS_EMITTED_BY_DEFAULT);
#endif
#ifdef COUCHBASE_CXX_CLIENT_HAS_JWT_AUTHENTICATOR
  response->add_sdk_implementation_caps(protocol::sdk::Caps::SDK_JWT);
#endif

  response->add_supported_apis(protocol::shared::API::DEFAULT);

  spdlog::info("performerCapsFetch responding with {}", response->DebugString());
  return grpc::Status::OK;
}

grpc::Status
TxnService::clusterConnectionCreate(grpc::ServerContext* /*context*/,
                                    const protocol::shared::ClusterConnectionCreateRequest* request,
                                    protocol::shared::ClusterConnectionCreateResponse* response)
{
  spdlog::info("clusterConnectionCreate called with {}", request->DebugString());
  std::lock_guard<std::mutex> lock(mutex_);
  ConnectionPtr conn{};
  conn = std::make_shared<fit_cxx::Connection>(request);
  if (!conn || !conn->cluster()) {
    return grpc::Status().CANCELLED;
  }
  connections_[request->cluster_connection_id()] = conn;
  conn->fixup_hooks();
  response->set_cluster_connection_count(static_cast<std::int32_t>(connections_.size()));
  std::stringstream stream;
  for (auto& c : connections_) {
    stream << c.first << ",";
  }
  spdlog::info("clusterConnectionCreate total connections {}, ({}), responding with {}",
               connections_.size(),
               stream.str(),
               response->DebugString());
  return grpc::Status::OK;
}

TxnService::TxnService()
{
  std::random_device rd;
  generator_ = std::mt19937(rd());
}

grpc::Status
TxnService::clusterConnectionClose(grpc::ServerContext* /*context*/,
                                   const protocol::shared::ClusterConnectionCloseRequest* request,
                                   protocol::shared::ClusterConnectionCloseResponse* response)
{
  spdlog::info("clusterConnectionClose called with {}", request->DebugString());
  std::lock_guard<std::mutex> lock(mutex_);
  if (connections_.find(request->cluster_connection_id()) == connections_.end()) {
    spdlog::error("clusterConnectionClose called with connection_id {}, which is not registered",
                  request->cluster_connection_id());
    return grpc::Status::CANCELLED;
  }
  {
    auto cluster = connections_.at(request->cluster_connection_id())->cluster();
    cluster->close().get();
  }
  connections_.erase(request->cluster_connection_id());
  response->set_cluster_connection_count(static_cast<std::int32_t>(connections_.size()));
  spdlog::info("clusterConnectionClose total connections {}, responding with {}",
               connections_.size(),
               response->DebugString());
  return grpc::Status::OK;
}

grpc::Status
TxnService::cleanupSetFetch(grpc::ServerContext* /*context*/,
                            const protocol::transactions::CleanupSetFetchRequest* request,
                            protocol::transactions::CleanupSetFetchResponse* response)
{
  spdlog::info("cleanupSetFetch called with {}", request->DebugString());
  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }
  auto txn = std::dynamic_pointer_cast<couchbase::core::transactions::transactions>(
    conn->cluster()->transactions());
  auto collections = txn->cleanup().collections();
  for (auto& c : collections) {
    auto set = response->mutable_cleanup_set()->add_cleanup_set();
    set->set_bucket_name(c.bucket);
    set->set_scope_name(c.scope);
    set->set_collection_name(c.collection);
  }
  return grpc::Status::OK;
}

grpc::Status
TxnService::echo(grpc::ServerContext* /*context*/,
                 const protocol::shared::EchoRequest* request,
                 protocol::shared::EchoResponse* /*response*/)
{
  spdlog::info("========== {} : ========== {}", request->testname(), request->message());
  return grpc::Status::OK;
}

grpc::Status
TxnService::run(grpc::ServerContext* /*context*/,
                const protocol::run::Request* request,
                grpc::ServerWriter<::protocol::run::Result>* writer)
{
  spdlog::info("got run with {}", request->DebugString());
  std::string run_id{ couchbase::core::uuid::to_string(couchbase::core::uuid::random()) };
  constexpr auto cmd_timeout = std::chrono::seconds(60);
  fit_cxx::Bounds workload_bounds;
  auto conn = getConn(request->workloads().cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}",
                         request->workloads().cluster_connection_id()) };
  }

  bool metrics_enabled = (request->has_config() && request->config().has_streaming_config() &&
                          request->config().streaming_config().enable_metrics());
  fit_cxx::concurrent_queue<protocol::run::Result> result_queue;
  auto batcher = fit_cxx::Batcher::build_batcher(writer, &result_queue, request, metrics_enabled);
  batcher->start();

  std::shared_ptr<fit_cxx::metrics_reporter> reporter;
  if (metrics_enabled) {
    auto this_pid = ::getpid();
    reporter = std::make_shared<fit_cxx::metrics_reporter>(conn->ctx(), this_pid, batcher, run_id);
    reporter->start();
  }

  // now, loop over the workloads...
  std::list<std::future<void>> workload_futures, stream_futures;
  std::mutex stream_mut;
  Counters counters;
  try {
    for (auto& horiz : request->workloads().horizontal_scaling()) {
      for (auto& work : horiz.workloads()) {
        workload_futures.push_back(std::async(std::launch::async, [&]() -> void {
          if (work.has_sdk()) {
            int cmd_count = work.sdk().command_size();
            std::string bounds_key;
            if (work.has_transaction()) {
              bounds_key = configure_bounds(
                workload_bounds, work.transaction().has_bounds(), work.transaction().bounds());
            } else {
              bounds_key =
                configure_bounds(workload_bounds, work.sdk().has_bounds(), work.sdk().bounds());
            }

            int counter = 0;
            while (workload_bounds.can_run(bounds_key, counter, cmd_count)) {
              auto& cmd = work.sdk().command(counter % cmd_count);
              auto [cmd_fut, stream_fut] = execute_sdk_command(conn, cmd, batcher, counters);
              if (stream_fut) {
                std::lock_guard<std::mutex> lock(stream_mut);
                stream_futures.push_back(std::move(stream_fut.value()));
              }
              if (cmd_fut.wait_for(cmd_timeout) != std::future_status::ready) {
                spdlog::error(
                  "command {} timed out after {} seconds", cmd.DebugString(), cmd_timeout.count());
                return;
              }
              counter++;
            }
          } else if (work.has_transaction()) {
            int cmd_count = work.transaction().command_size();
            std::string bounds_key = configure_bounds(
              workload_bounds, work.transaction().has_bounds(), work.transaction().bounds());

            int counter = 0;
            while (workload_bounds.can_run(bounds_key, counter, cmd_count)) {
              auto& cmd = work.transaction().command(counter % cmd_count);
              std::atomic<bool> tim852ed_out{ false };
              conn->run_with_timeout<int>(
                std::chrono::seconds(TXN_TIMEOUT),
                [&]() {
                  protocol::run::Result res;
                  std::atomic<bool> timed_out{ false };
                  auto start = std::chrono::high_resolution_clock::now();
                  res.mutable_initiated()->CopyFrom(
                    google::protobuf::util::TimeUtil::GetCurrentTime());
                  auto result =
                    startTxn(conn, &cmd, res.mutable_transaction(), timed_out, counters);
                  spdlog::debug("transaction finished with result {} {}",
                                result.error_message(),
                                res.DebugString());
                  auto end = std::chrono::high_resolution_clock::now();
                  res.set_elapsednanos(
                    std::chrono::duration<int64_t, std::nano>(end - start).count());
                  batcher->send_result(res);
                  return 0;
                },
                [&]() -> int {
                  spdlog::error("command {} timed out", cmd.DebugString());
                  return 0;
                });
              counter++;
            }
          } else {
            throw performer_exception::unimplemented("Unknown operation");
          }
        }));
      }
    }
    // first wait for all the horizontal workloads to finish...
    for (auto& f : workload_futures) {
      f.get();
    }
    // then wait on the streams produced by them to finish...
    for (auto& f : stream_futures) {
      f.get();
    }
    spdlog::info("Inform batcher that workloads are complete");
    batcher->set_workloads_complete();
    batcher->wait();
    spdlog::info("Sending OK status");
    return grpc::Status::OK;
  } catch (const performer_exception& err) {
    spdlog::error("got performer exception with message '{}' during run", err.what());
    batcher->set_workloads_complete();
    return err.to_grpc_status();
  } catch (const std::exception& err) {
    spdlog::error("got exception with message '{}' during run", err.what());
    batcher->set_workloads_complete();
    return grpc::Status(grpc::StatusCode::UNKNOWN, err.what());
  }
}

std::string
TxnService::configure_bounds(fit_cxx::Bounds& workload_bounds,
                             bool has_bounds,
                             const protocol::shared::Bounds& bounds)
{
  std::string bounds_key;
  if (has_bounds) {
    if (bounds.has_counter()) {
      bounds_key = bounds.counter().counter_id();
      workload_bounds.add_counter(bounds_key, bounds.counter().global().count());
    } else if (bounds.has_for_time()) {
      workload_bounds.add_timer(bounds.for_time().seconds());
    }
  }
  return bounds_key;
}

CmdFutures
TxnService::execute_sdk_command(ConnectionPtr conn,
                                const protocol::sdk::Command cmd,
                                std::shared_ptr<fit_cxx::Batcher> batcher,
                                Counters& counters)
{
  auto barrier = std::make_shared<std::promise<void>>();
  auto cmd_fut = barrier->get_future();
  std::optional<std::future<void>> stream_fut;
  google::protobuf::Timestamp initiated = google::protobuf::util::TimeUtil::GetCurrentTime();
  std::string stream_id;
  bool return_result = cmd.return_result();
  if (cmd.has_get()) {
    auto coll = to_collection(conn, cmd.get().location());
    auto key = to_key(cmd.get().location(), counters);
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ key,
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_command(cmd.get(), cmd_args);
    batcher->send_result(res);
  }
  if (cmd.has_replace()) {
    auto coll = to_collection(conn, cmd.replace().location());
    auto key = to_key(cmd.replace().location(), counters);
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ key,
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_command(cmd.replace(), cmd_args);
    batcher->send_result(res);
  }
  if (cmd.has_insert()) {
    auto coll = to_collection(conn, cmd.insert().location());
    auto key = to_key(cmd.insert().location(), counters);
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ key,
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_command(cmd.insert(), cmd_args);
    batcher->send_result(res);
  }

  if (cmd.has_upsert()) {
    auto coll = to_collection(conn, cmd.upsert().location());
    auto key = to_key(cmd.upsert().location(), counters);
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ key,
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_command(cmd.upsert(), cmd_args);
    batcher->send_result(res);
  }
  if (cmd.has_remove()) {
    auto coll = to_collection(conn, cmd.remove().location());
    auto key = to_key(cmd.remove().location(), counters);
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ key,
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_command(cmd.remove(), cmd_args);
    batcher->send_result(res);
  }
  if (cmd.has_range_scan()) {
    auto coll = to_collection(conn, cmd.range_scan().collection());
    auto cmd_args = fit_cxx::commands::key_value::command_args{
      /* .collection = */ coll,
      /* .key = */ {},
      /* .spans = */ &spans_,
      /* .return_result = */ return_result,
    };
    auto res = fit_cxx::commands::key_value::execute_streaming_command(cmd.range_scan(), cmd_args);
    if (std::holds_alternative<fit_cxx::next_function>(res)) {
      auto stream_config = cmd.range_scan().stream_config();
      auto& stream =
        streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
      auto cleanup_fn = [this](std::string id) {
        streams_.remove(id);
      };
      auto next_fn = std::get<fit_cxx::next_function>(res);
      stream_fut = stream.begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
    } else {
      batcher->send_result(std::get<protocol::run::Result>(res));
    }
  }
  if (cmd.has_cluster_command()) {
    auto cluster_cmd = cmd.cluster_command();
    auto cluster = conn->cluster();
    if (cluster_cmd.has_query()) {
      auto cmd_args = fit_cxx::commands::query::command_args{
        /* .cluster = */ cluster,
        /* .scope = */ {},
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::query::execute_command(cluster_cmd.query(), cmd_args);
      batcher->send_result(res);
    } else if (cluster_cmd.has_query_index_manager()) {
      auto cmd_args = fit_cxx::commands::query_index_management::command_args{
        /* .cluster = */ cluster,
        /* .collection = */ {},
        /* .bucket_name = */ {},
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::query_index_management::execute_cluster_command(
        cluster_cmd.query_index_manager(), cmd_args);
      batcher->send_result(res);
    } else if (cluster_cmd.has_bucket_manager()) {
      auto cmd_args = fit_cxx::commands::bucket_management::command_args{
        /* .cluster = */ cluster,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::bucket_management::execute_command(cluster_cmd.bucket_manager(),
                                                                       cmd_args);
      batcher->send_result(res);
    } else if (cluster_cmd.has_search_index_manager()) {
      auto cmd_args = fit_cxx::commands::search_index_management::command_args{
        /* .cluster = */ cluster,
        /* .scope = */ {},
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::search_index_management::execute_cluster_command(
        cluster_cmd.search_index_manager(), cmd_args);
      batcher->send_result(res);
    } else if (cluster_cmd.has_search()) {
      throw performer_exception::unimplemented(
        "The C++ SDK does not support the `search_query()` API");
    } else if (cluster_cmd.has_search_v2()) {
      auto cmd_args = fit_cxx::commands::search::command_args{
        /* .cluster = */ cluster,
        /* .scope = */ {},
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::search::execute_streaming_command(cluster_cmd.search_v2(), cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = cluster_cmd.search_v2().stream_config();
        auto& stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream.begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
      } else {
        batcher->send_result(std::get<protocol::run::Result>(res));
      }
#ifdef COUCHBASE_CXX_CLIENT_HAS_SET_AUTHENTICATOR
    } else if (cluster_cmd.has_authenticator()) {
      const auto auth_cmd = cluster_cmd.authenticator();
      auto res = fit_cxx::commands::common::create_new_result();
      couchbase::error err{};
      switch (auth_cmd.authenticator_case()) {
        case protocol::shared::Authenticator::AuthenticatorCase::kPasswordAuth: {
          err = conn->cluster()->set_authenticator(couchbase::password_authenticator{
            auth_cmd.password_auth().username(),
            auth_cmd.password_auth().password(),
          });
          break;
        }
        case protocol::shared::Authenticator::AuthenticatorCase::kCertificateAuth: {
          const auto cert_auth =
            conn->create_certificate_authenticator(auth_cmd.certificate_auth());
          err = conn->cluster()->set_authenticator(cert_auth);
          break;
        }
#ifdef COUCHBASE_CXX_CLIENT_HAS_JWT_AUTHENTICATOR
        case protocol::shared::Authenticator::AuthenticatorCase::kJwtAuth: {
          err = conn->cluster()->set_authenticator(
            couchbase::jwt_authenticator{ auth_cmd.jwt_auth().jwt() });
          break;
        }
#endif
        default:
          throw performer_exception::invalid_argument("Unknown authenticator type");
      }
      if (err) {
        fit_cxx::commands::common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
      batcher->send_result(res);
#endif
    }
  }
  if (cmd.has_bucket_command()) {
    auto bucket_cmd = cmd.bucket_command();
    auto bucket = to_bucket(conn, bucket_cmd.bucket_name());
    if (bucket_cmd.has_collection_manager()) {
      auto cmd_args = fit_cxx::commands::collection_management::command_args{
        /* .bucket = */ bucket,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::collection_management::execute_command(
        bucket_cmd.collection_manager(), cmd_args);
      batcher->send_result(res);
    }
  }
  if (cmd.has_scope_command()) {
    auto scope_cmd = cmd.scope_command();
    auto scope = to_scope(conn, scope_cmd.scope());
    if (scope_cmd.has_query()) {
      auto cmd_args = fit_cxx::commands::query::command_args{
        /* .cluster = */ {},
        /* .scope = */ scope,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::query::execute_command(scope_cmd.query(), cmd_args);
      batcher->send_result(res);
    } else if (scope_cmd.has_search_v2()) {
      auto cmd_args = fit_cxx::commands::search::command_args{
        /* .cluster = */ {},
        /* .scope = */ scope,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::search::execute_streaming_command(scope_cmd.search_v2(), cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = scope_cmd.search_v2().stream_config();
        auto& stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream.begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
      } else {
        batcher->send_result(std::get<protocol::run::Result>(res));
      }
    } else if (scope_cmd.has_search_index_manager()) {
      auto cmd_args = fit_cxx::commands::search_index_management::command_args{
        /* .cluster = */ {},
        /* .scope = */ scope,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::search_index_management::execute_scope_command(
        scope_cmd.search_index_manager(), cmd_args);
      batcher->send_result(res);
    }
  }
  if (cmd.has_collection_command()) {
    auto collection_cmd = cmd.collection_command();
    auto collection = to_collection(conn, collection_cmd.collection());

    if (collection_cmd.has_lookup_in()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.lookup_in().location()),
        /* .key = */ to_key(collection_cmd.lookup_in().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.lookup_in(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_and_lock()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_and_lock().location()),
        /* .key = */ to_key(collection_cmd.get_and_lock().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_and_lock(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_unlock()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.unlock().location()),
        /* .key = */ to_key(collection_cmd.unlock().location(), counters),
        /* .spans = */ &spans_,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.unlock(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_and_touch()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_and_touch().location()),
        /* .key = */ to_key(collection_cmd.get_and_touch().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_and_touch(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_exists()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.exists().location()),
        /* .key = */ to_key(collection_cmd.exists().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.exists(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_touch()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.touch().location()),
        /* .key = */ to_key(collection_cmd.touch().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.touch(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_mutate_in()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.mutate_in().location()),
        /* .key = */ to_key(collection_cmd.mutate_in().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.mutate_in(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_any_replica()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_any_replica().location()),
        /* .key = */ to_key(collection_cmd.get_any_replica().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_any_replica(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_all_replicas()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_all_replicas().location()),
        /* .key = */ to_key(collection_cmd.get_all_replicas().location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_streaming_command(
        collection_cmd.get_all_replicas(), cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = collection_cmd.get_all_replicas().stream_config();
        auto& stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream.begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
      } else {
        batcher->send_result(std::get<protocol::run::Result>(res));
      }
    } else if (collection_cmd.has_binary()) {
      auto binary_cmd = collection_cmd.binary();
      if (binary_cmd.has_append()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.append().location()),
          /* .key = */ to_key(binary_cmd.append().location(), counters),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.append(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_prepend()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.prepend().location()),
          /* .key = */ to_key(binary_cmd.prepend().location(), counters),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.prepend(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_increment()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.increment().location()),
          /* .key = */ to_key(binary_cmd.increment().location(), counters),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.increment(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_decrement()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.decrement().location()),
          /* .key = */ to_key(binary_cmd.decrement().location(), counters),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.decrement(), cmd_args);
        batcher->send_result(res);
      }
    } else if (collection_cmd.has_lookup_in_any_replica()) {
      auto lookup_in_cmd = collection_cmd.lookup_in_any_replica();
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, lookup_in_cmd.location()),
        /* .key = */ to_key(lookup_in_cmd.location(), counters),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(lookup_in_cmd, cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_lookup_in_all_replicas()) {
      auto lookup_in_cmd = collection_cmd.lookup_in_all_replicas();
      collection = to_collection(conn, lookup_in_cmd.location());
      auto key = to_key(lookup_in_cmd.location(), counters);
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ collection,
        /* .key = */ key,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_streaming_command(lookup_in_cmd, cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = lookup_in_cmd.stream_config();
        auto& stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream.begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
      } else {
        batcher->send_result(std::get<protocol::run::Result>(res));
      }
    } else if (collection_cmd.has_query_index_manager()) {
      auto cmd_args = fit_cxx::commands::query_index_management::command_args{
        /* .cluster = */ {},
        /* .collection = */ collection,
        /* .bucket_name = */ {},
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::query_index_management::execute_collection_command(
        collection_cmd.query_index_manager(), cmd_args);
      batcher->send_result(res);
    }
  }

  return std::make_pair<std::future<void>, std::optional<std::future<void>>>(std::move(cmd_fut),
                                                                             std::move(stream_fut));
}

grpc::Status
TxnService::streamRequestItems(grpc::ServerContext* /*context*/,
                               const protocol::streams::RequestItemsRequest* request,
                               protocol::streams::RequestItemsResponse* /*response*/)
{
  spdlog::info("got streamRequestItems with {}", request->DebugString());
  try {
    streams_.find(request->stream_id())
      .set_num_to_write(static_cast<std::size_t>(request->num_items()));
    return grpc::Status::OK;
  } catch (...) {
    return grpc::Status(grpc::StatusCode::CANCELLED,
                        fmt::format("no stream with id {}", request->stream_id()));
  }
}
grpc::Status
TxnService::streamCancel(grpc::ServerContext* /*context*/,
                         const protocol::streams::CancelRequest* request,
                         protocol::streams::CancelResponse* /*response*/)
{
  spdlog::info("got streamCancel with {}", request->DebugString());
  try {
    streams_.find(request->stream_id()).cancel();
    return grpc::Status::OK;
  } catch (...) {
    return grpc::Status(grpc::StatusCode::CANCELLED,
                        fmt::format("no stream with id {}", request->stream_id()));
  }
}

grpc::Status
TxnService::spanCreate(grpc::ServerContext* /*context*/,
                       const protocol::observability::SpanCreateRequest* request,
                       protocol::observability::SpanCreateResponse* /*response*/)
{
  spdlog::info("spanCreate called with {}", request->DebugString());

  ConnectionPtr conn;
  try {
    conn = connections_.at(request->cluster_connection_id());
  } catch (const std::out_of_range&) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        fmt::format("No connection with id {}", request->cluster_connection_id()));
  }
  auto tracer = conn->tracer();
  if (!tracer) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        fmt::format("Tracer has not been set up for connection with id {}",
                                    request->cluster_connection_id()));
  }

  try {
    spans_.create_span(tracer, request);
  } catch (const performer_exception& e) {
    return e.to_grpc_status();
  }

  return grpc::Status::OK;
}

grpc::Status
TxnService::spanFinish(grpc::ServerContext* /*context*/,
                       const protocol::observability::SpanFinishRequest* request,
                       protocol::observability::SpanFinishResponse* /*response*/)
{
  spdlog::info("spanFinish called with {}", request->DebugString());

  try {
    spans_.finish_span(request);
  } catch (const performer_exception& e) {
    return e.to_grpc_status();
  }

  return grpc::Status::OK;
}
