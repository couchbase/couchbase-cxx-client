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

#include <algorithm>
#include <array>
#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <variant>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

constexpr auto TXN_TIMEOUT = std::chrono::seconds(
  120); // a txn over 2 minutes will terminate, return, and the tests can continue.
constexpr auto CLIENT_RECORD_PROCESS_TIMEOUT =
  std::chrono::seconds(30); // a get_active_clients() call that takes over 30 seconds should abort.

grpc::Status
TxnService::disconnectConnections(grpc::ServerContext* /*context*/,
                                  const protocol::shared::DisconnectConnectionsRequest* /*request*/,
                                  protocol::shared::DisconnectConnectionsResponse* /*response*/)
{
  // Move the connections out from under the lock: close() blocks on network I/O, and holding
  // mutex_ across it would stall every other RPC. close() can also throw, and an exception escaping
  // a gRPC handler would terminate the server, so each close is guarded individually.
  decltype(connections_) connections;
  {
    std::scoped_lock<std::mutex> lock(mutex_);
    connections.swap(connections_);
  }
  spdlog::info("clearing {} connections...", connections.size());
  for (const auto& [key, conn] : connections) {
    spdlog::trace("closing connection {}", key);
    try {
      conn->cluster()->close().get();
    } catch (const std::exception& e) {
      spdlog::warn("failed to close connection {}: {}", key, e.what());
    }
  }
  return grpc::Status::OK;
}

grpc::Status
TxnService::startTxn(ConnectionPtr conn,
                     const protocol::transactions::TransactionCreateRequest* request,
                     protocol::transactions::TransactionResult* response,
                     std::atomic<bool>& timed_out,
                     fit_cxx::transaction_context& txn_ctx)
{
  // Guard against an empty attempts list: the per-attempt index below is computed as
  // min(attempt_count, attempts_size() - 1), which underflows to -1 when attempts_size() == 0, and
  // request->attempts(-1) is an out-of-bounds protobuf access (UB / crash under NDEBUG, where the
  // ABSL_DCHECK bound check is compiled out) that no surrounding try/catch can convert to a status.
  if (request->attempts_size() == 0) {
    return { grpc::StatusCode::INVALID_ARGUMENT, "transaction request has no attempts" };
  }

  int attempt_count = 0;
  std::list<fit_cxx::TxnSvcHook> hooks;
  couchbase::transactions::transaction_options opts;
  grpc::Status grpc_status{};

  auto start_time = std::chrono::steady_clock::now();
  if (request->has_options()) {
    TxnSvcUtils::update_common_options(request->options(), opts, conn, hooks);
  }
  // Latches are registered on txn_ctx by the caller before the transaction starts (and, for the
  // streaming path, before the worker signals "created"), so that incoming set-latch broadcasts can
  // be delivered while this transaction runs.

  auto expiry =
    couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions())
      ->config()
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
          auto op_err = execute_command(conn, ctx, command, request->name(), txn_ctx);
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
    couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions()),
    response);
  spdlog::trace("startTxn responding with {}", response->DebugString());
  return grpc::Status::OK;
}

grpc::Status
TxnService::transactionCreate(grpc::ServerContext* /*context*/,
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
  // Unary transactions have no concurrent peers, so the context has no broadcaster; latches still
  // get registered for parity with the streaming path.
  fit_cxx::transaction_context txn_ctx;
  for (const auto& l : request->latches()) {
    txn_ctx.latches().add(l.name(), static_cast<std::uint32_t>(l.initial_count()));
  }
  return conn->run_with_timeout<grpc::Status>(
    std::chrono::seconds(TXN_TIMEOUT),
    [&]() {
      return startTxn(conn, request, response, timed_out, txn_ctx);
    },
    [&]() -> grpc::Status {
      timed_out = true;
      // Unblock any in-progress wait_on_latch so startTxn can unwind instead of blocking past the
      // timeout (a latch with no peer to set it would otherwise never wake).
      txn_ctx.latches().cancel_all();
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
TxnService::to_key(const protocol::shared::DocLocation& loc)
{
  if (loc.has_specific()) {
    return loc.specific().id();
  }
  if (loc.has_uuid()) {
    return couchbase::core::uuid::to_string(couchbase::core::uuid::random());
  }
  if (loc.has_pool()) {
    std::int64_t pool_size = loc.pool().pool_size();
    if (pool_size <= 0) {
      throw performer_exception::invalid_argument("pool_size must be positive");
    }
    std::string id_preface = loc.pool().id_preface();
    if (loc.pool().has_random()) {
      std::uniform_int_distribution<std::int64_t> distribution(0, pool_size - 1);
      std::int64_t random_number = 0;
      {
        // generator_ (std::mt19937) is shared across worker threads and is not thread-safe.
        const std::scoped_lock<std::mutex> lock(rng_mutex_);
        random_number = distribution(generator_);
      }
      return id_preface + std::to_string(random_number);
    }
    if (loc.pool().has_counter()) {
      const auto counter_value =
        counters_.get_counter(loc.pool().counter().counter())->increment() -
        1; // Using the pre-increment value, ensuring that the first key always has the initial
           // value as suffix.
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
TxnService::transactionCleanup(grpc::ServerContext* /*context*/,
                               const protocol::transactions::TransactionCleanupRequest* request,
                               protocol::transactions::TransactionCleanupAttempt* response)
{
  spdlog::info("transactionCleanup called with {}", request->DebugString());
  couchbase::transactions::transactions_config config;
  std::list<fit_cxx::TxnSvcHook> hooks;

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }

  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  try {
    auto txn =
      couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions());
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
  grpc::ServerContext* /*context*/,
  const protocol::transactions::TransactionCleanupATRRequest* request,
  protocol::transactions::TransactionCleanupATRResult* response)
{
  // we must make our own factory (no transactionfactoryref in the request)
  spdlog::info("transactionCleanupATR called with {}", request->DebugString());
  std::list<fit_cxx::TxnSvcHook> hooks;

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }
  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  try {
    auto txn =
      couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions());
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
  // Validate the operation's content/result BEFORE deciding whether to propagate the error. A
  // successful op that carries a content_as_validation must still be checked even when
  // do_not_propagate_error is set; otherwise a wrong-content success would be reported as a pass.
  // This matches the reference Java performer, which always validates content on the success path.
  verify_op_result(cmd, result);
  if (do_not_propagate_error) {
    return {};
  }
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
                               const protocol::transactions::TransactionCommand& cmd)
  -> std::vector<couchbase::transactions::transaction_get_multi_spec>
{
  std::vector<couchbase::transactions::transaction_get_multi_spec> specs;
  specs.reserve(static_cast<std::size_t>(cmd.get_multi().specs_size()));

  for (const auto& spec : cmd.get_multi().specs()) {
    specs.emplace_back(to_collection(conn, spec.location()), to_key(spec.location()));
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
  return options;
}

auto
TxnService::to_get_multi_replicas_from_preferred_server_group_specs(
  ConnectionPtr conn,
  const protocol::transactions::TransactionCommand& cmd)
  -> std::vector<
    couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_spec>
{
  std::vector<
    couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_spec>
    specs;
  specs.reserve(static_cast<std::size_t>(cmd.get_multi().specs_size()));

  for (const auto& spec : cmd.get_multi().specs()) {
    specs.emplace_back(to_collection(conn, spec.location()), to_key(spec.location()));
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
  return options;
}

couchbase::error
TxnService::execute_command(ConnectionPtr conn,
                            std::shared_ptr<couchbase::transactions::attempt_context> ctx,
                            const protocol::transactions::TransactionCommand& cmd,
                            const std::string& logger_prefix,
                            fit_cxx::transaction_context& txn_ctx,
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
    return execute_op(cmd.get(),
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, result] = c->get(coll, id);
                        if (!err.ec()) {
                          set_stashed_result(result);
                          if (cmd.get().has_stash_in_slot()) {
                            set_stashed_slot(static_cast<std::uint32_t>(cmd.get().stash_in_slot()),
                                             result);
                          }
                        }
                        return std::pair{ err, result };
                      });
  }
  if (cmd.has_get_from_preferred_server_group()) {
    auto coll = to_collection(cmd.get_from_preferred_server_group().doc_id(), conn->cluster());
    auto id = cmd.get_from_preferred_server_group().doc_id().doc_id();
    return execute_op(
      cmd.get_from_preferred_server_group(),
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        auto [err, result] = c->get_replica_from_preferred_server_group(coll, id);
        if (!err.ec()) {
          set_stashed_result(result);
          if (cmd.get_from_preferred_server_group().has_stash_in_slot()) {
            set_stashed_slot(
              static_cast<std::uint32_t>(cmd.get_from_preferred_server_group().stash_in_slot()),
              result);
          }
        }
        spdlog::debug("get_from_preferred_server_group: {}, {}", err.message(), err.ec().message());
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
            to_get_multi_replicas_from_preferred_server_group_specs(conn, cmd),
            to_get_multi_replicas_from_preferred_server_group_options(cmd));
          spdlog::debug("get_multi_replicas: err.message: {}, err.ec.message: {}",
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
                        auto [err, result] =
                          c->get_multi(to_get_multi_specs(conn, cmd), to_get_multi_options(cmd));
                        spdlog::debug("get_multi: err.message: {}, err.ec.message: {}",
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
                          set_stashed_result(result);
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
          if (auto stashed_result = get_stashed_result(); stashed_result) {
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
                stashed_result.value(),
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
                          if (auto stashed = get_stashed_result(); stashed) {
                            return std::pair{ c->remove(stashed.value()),
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
    auto id = to_key(req.location());
    return execute_op(req,
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        auto [err, result] = c->get(coll, id);
                        if (!err.ec()) {
                          set_stashed_result(result);
                          // get_v2 is the active oneof member here, so the slot must come from req
                          // (not cmd.get(), the inactive v1 member, which would always report
                          // has_stash_in_slot() == false).
                          if (req.has_stash_in_slot()) {
                            set_stashed_slot(static_cast<std::uint32_t>(req.stash_in_slot()),
                                             result);
                          }
                        }
                        return std::pair{ err, result };
                      });
  }
  if (cmd.has_replace_v2()) {
    auto req = cmd.replace_v2();
    auto coll = to_collection(conn, req.location());
    auto id = to_key(req.location());
    auto content_str = content_as_string(req.content());
    auto content = couchbase::core::utils::json::parse(std::string_view{ content_str });
    return execute_op(req,
                      ctx,
                      cmd.do_not_propagate_error(),
                      logger_prefix,
                      false,
                      false,
                      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
                        if (req.has_use_stashed_slot()) {
                          auto stashed =
                            get_stashed_slot(static_cast<std::uint32_t>(req.use_stashed_slot()));
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
    auto id = to_key(req.location());
    return execute_op(
      req,
      ctx,
      cmd.do_not_propagate_error(),
      logger_prefix,
      false,
      false,
      [&](std::shared_ptr<couchbase::transactions::attempt_context> c) {
        if (req.has_use_stashed_slot()) {
          auto stashed = get_stashed_slot(static_cast<std::uint32_t>(req.use_stashed_slot()));
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
    auto id = to_key(req.location());
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
    // counting_latch::wait() throws a plain std::runtime_error (deliberately NOT a
    // performer_exception) if the latch is cancelled because the transaction timed out or its
    // stream was torn down. Like the "timeout hack" in startTxn, a non-performer_exception
    // propagates out of the SDK run() lambda so the transaction is rolled back without a retry.
    // Converting it to a performer_exception would instead route it through startTxn's handler,
    // which reports the error but returns success - committing a transaction that was aborted.
    txn_ctx.latches().get(cmd.wait_on_latch().latch_name())->wait();
  } else if (cmd.has_set_latch()) {
    // Count down our own copy of the latch and tell the driver (and thereby other concurrent
    // transactions) to do the same. Mirrors the Java performer's handleSetLatch.
    txn_ctx.latches().get(cmd.set_latch().latch_name())->set();
    txn_ctx.broadcast_set_latch(cmd.set_latch().latch_name());
  } else if (cmd.has_parallelize()) {
    // parallelism is a proto int32; a zero or negative value would launch no worker threads and
    // silently skip every sub-command. Fail fast so the driver sees the configuration error.
    const std::int32_t parallelism = cmd.parallelize().parallelism();
    if (parallelism <= 0) {
      throw performer_exception::invalid_argument(
        "parallelize requires a parallelism of at least 1, got " + std::to_string(parallelism));
    }
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
        try {
          execute_command(conn, ctx, c, logger_prefix, txn_ctx, true);
        } catch (...) {
          // Release the in-flight slot and wake a waiter even on exception; otherwise the other
          // workers can block forever in cv.wait() and f.get() below would deadlock. The exception
          // is re-thrown and surfaced by f.get().
          lock.lock();
          in_flight--;
          lock.unlock();
          cv.notify_one();
          throw;
        }
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
  std::list<fit_cxx::TxnSvcHook> hooks;

  auto conn = getConn(request->cluster_connection_id());
  if (!conn) {
    return { ::grpc::CANCELLED,
             fmt::format("no connection with ID={}", request->cluster_connection_id()) };
  }

  auto hook_pair = fit_cxx::TxnSvcHook::convert_hooks(request->hook(), hooks, conn);
  auto txn = couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions());
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
      return grpc::Status{ grpc::StatusCode::CANCELLED, "clientRecordProcess timed out" };
    });
}

grpc::Status
TxnService::transactionStream(
  grpc::ServerContext* /*context*/,
  grpc::ServerReaderWriter<protocol::transactions::TransactionStreamPerformerToDriver,
                           protocol::transactions::TransactionStreamDriverToPerformer>* stream)
{
  spdlog::info("transactionStream called");

  protocol::transactions::TransactionCreateRequest request;
  ConnectionPtr conn;

  // Coordinates the read loop (this thread) with the single worker thread that runs the
  // transaction, mirroring the Java TwoWayTransactionMarshaller. The read loop never writes to the
  // stream; the worker performs every write (the created message, set-latch broadcasts, and the
  // final result), so there is never more than one concurrent writer.
  std::mutex start_mutex;
  std::condition_variable start_cv;
  bool ready_to_start = false;
  bool aborted = false;
  std::thread worker;
  auto txn_ctx = std::make_shared<fit_cxx::transaction_context>();
  // All writes to the stream are serialized through this mutex. The worker is normally the only
  // writer, but a parallelize() command runs sub-commands on several threads, any of which may
  // execute set_latch and invoke the broadcaster below. gRPC permits only one concurrent writer, so
  // every stream->Write must hold this lock.
  auto write_mutex = std::make_shared<std::mutex>();

  grpc::Status status = grpc::Status::OK;
  protocol::transactions::TransactionStreamDriverToPerformer d2p;
  try {
    while (stream->Read(&d2p)) {
      if (d2p.has_create()) {
        spdlog::info("got create: {}", d2p.DebugString());
        if (worker.joinable()) {
          spdlog::error("received a second create on the same stream, ignoring");
          continue;
        }
        request = d2p.create();
        conn = getConn(request.cluster_connection_id());
        if (!conn) {
          status = { ::grpc::CANCELLED,
                     fmt::format("no connection with ID={}", request.cluster_connection_id()) };
          break;
        }
        // The set-latch broadcaster writes to the stream, but it is only ever invoked from within
        // the transaction (i.e. on the worker thread), keeping the worker the sole writer.
        txn_ctx = std::make_shared<fit_cxx::transaction_context>(
          [stream, write_mutex](const std::string& name) {
            protocol::transactions::TransactionStreamPerformerToDriver broadcast;
            auto* b = protocol::transactions::BroadcastToOtherConcurrentTransactionsRequest::
                        default_instance()
                          .New();
            auto* l = protocol::transactions::CommandSetLatch::default_instance().New();
            l->set_latch_name(name);
            b->set_allocated_latch_set(l);
            broadcast.set_allocated_broadcast(b);
            const std::scoped_lock<std::mutex> lock(*write_mutex);
            stream->Write(broadcast);
          });
        // Register latches before the worker signals "created", so an incoming set-latch broadcast
        // (handled on this read thread) always finds its latch.
        for (const auto& l : request.latches()) {
          txn_ctx->latches().add(l.name(), static_cast<std::uint32_t>(l.initial_count()));
        }

        auto worker_request = request;
        auto worker_conn = conn;
        worker = std::thread([this,
                              stream,
                              worker_request,
                              worker_conn,
                              txn_ctx,
                              write_mutex,
                              &start_mutex,
                              &start_cv,
                              &ready_to_start,
                              &aborted]() {
          protocol::transactions::TransactionStreamPerformerToDriver created;
          created.set_allocated_created(
            protocol::transactions::TransactionCreated::default_instance().New());
          {
            const std::scoped_lock<std::mutex> lock(*write_mutex);
            stream->Write(created);
          }
          spdlog::info("{} created, waiting until told to start", worker_request.name());

          {
            std::unique_lock<std::mutex> lock(start_mutex);
            start_cv.wait(lock, [&]() -> bool {
              return ready_to_start || aborted;
            });
            if (aborted) {
              spdlog::info("{} stream torn down before start, worker exiting",
                           worker_request.name());
              return;
            }
          }

          spdlog::info("{} starting", worker_request.name());
          auto* result = protocol::transactions::TransactionResult::default_instance().New();
          std::atomic<bool> timed_out{ false };
          try {
            worker_conn->run_with_timeout<grpc::Status>(
              std::chrono::seconds(TXN_TIMEOUT),
              [&]() {
                return startTxn(worker_conn, &worker_request, result, timed_out, *txn_ctx);
              },
              [&]() -> grpc::Status {
                timed_out = true;
                // Unblock any in-progress wait_on_latch so the worker can unwind on timeout.
                txn_ctx->latches().cancel_all();
                return { grpc::StatusCode::DEADLINE_EXCEEDED, "startTxn in stream timed out!" };
              });
          } catch (const std::exception& e) {
            spdlog::error(
              "{} transaction worker caught exception: {}", worker_request.name(), e.what());
          } catch (...) {
            spdlog::error("{} transaction worker caught unknown exception", worker_request.name());
          }
          protocol::transactions::TransactionStreamPerformerToDriver final_result;
          final_result.set_allocated_final_result(result);
          {
            const std::scoped_lock<std::mutex> lock(*write_mutex);
            stream->Write(final_result);
          }
          spdlog::trace("{} done, wrote final result", worker_request.name());
        });
      } else if (d2p.has_start()) {
        spdlog::info("{} got start", request.name());
        {
          const std::scoped_lock<std::mutex> lock(start_mutex);
          ready_to_start = true;
        }
        start_cv.notify_all();
      } else if (d2p.has_broadcast()) {
        if (d2p.broadcast().has_latch_set()) {
          const auto& latch_name = d2p.broadcast().latch_set().latch_name();
          spdlog::info("{} got broadcast, setting latch {}", request.name(), latch_name);
          txn_ctx->latches().get(latch_name)->set();
        } else {
          spdlog::error("broadcast has no latchSet message {}", d2p.broadcast().DebugString());
          throw performer_exception::internal("unexpected message in broadcast");
        }
      } else {
        spdlog::error("transactionStream unexpected value (not create, start, broadcast) {}",
                      d2p.DebugString());
        throw performer_exception::internal("unexpected value in DriverToPerformer");
      }
    }
  } catch (const std::exception& e) {
    spdlog::error("transactionStream read loop error: {}", e.what());
    status = { grpc::StatusCode::CANCELLED, e.what() };
  }

  // Unblock the worker if it is still waiting for "start" (e.g. the driver disconnected early),
  // then join it before returning so the stream stays valid for all of the worker's writes.
  {
    const std::scoped_lock<std::mutex> lock(start_mutex);
    aborted = true;
  }
  start_cv.notify_all();
  // Also unblock any in-progress wait_on_latch: if the driver disconnected, broadcast the wrong
  // latch name, or never broadcast at all, the worker would otherwise block in the latch forever
  // and worker.join() below would never return.
  txn_ctx->latches().cancel_all();
  if (worker.joinable()) {
    worker.join();
  }
  spdlog::info("transactionStream complete");
  return status;
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
  try {
    while (s_stream.good()) {
      std::string cap;
      getline(s_stream, cap, ','); // get first string delimited by comma
      response->add_transaction_implementations_caps(
        TxnSvcUtils::convert_extension_to_performer_caps(cap));
    }
  } catch (const performer_exception& e) {
    return e.to_grpc_status();
  } catch (const std::exception& e) {
    return { ::grpc::CANCELLED, e.what() };
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
  // Redact secret material before logging: the request carries the cluster password, JWT, and the
  // client certificate/private-key PEM, and DebugString() would otherwise emit them verbatim at
  // info level into logs that CI commonly aggregates.
  auto redacted = *request;
  redacted.clear_cluster_password();
  if (redacted.has_authenticator()) {
    auto* auth = redacted.mutable_authenticator();
    if (auth->has_password_auth()) {
      auth->mutable_password_auth()->clear_password();
    }
    if (auth->has_jwt_auth()) {
      auth->mutable_jwt_auth()->clear_jwt();
    }
    if (auth->has_certificate_auth()) {
      auth->mutable_certificate_auth()->clear_cert();
      auth->mutable_certificate_auth()->clear_key();
    }
  }
  spdlog::info("clusterConnectionCreate called with {}", redacted.DebugString());
  // Construct the connection (which can block while it retries connecting) BEFORE taking the lock,
  // so connection setup does not stall every other RPC that needs mutex_/getConn().
  ConnectionPtr conn;
  try {
    conn = std::make_shared<fit_cxx::Connection>(request);
  } catch (const std::exception& e) {
    return { grpc::StatusCode::CANCELLED,
             fmt::format("failed to create connection: {}", e.what()) };
  }
  conn->fixup_hooks();

  std::scoped_lock<std::mutex> lock(mutex_);
  connections_[request->cluster_connection_id()] = conn;
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
  ConnectionPtr conn;
  std::size_t remaining = 0;
  {
    // Remove the connection from the map under the lock, but close it outside the lock: close()
    // blocks on network I/O (holding mutex_ across it would stall every other RPC) and can throw
    // (an exception escaping this gRPC handler would terminate the server). Mirrors
    // disconnectConnections().
    std::scoped_lock<std::mutex> lock(mutex_);
    auto it = connections_.find(request->cluster_connection_id());
    if (it == connections_.end()) {
      spdlog::error("clusterConnectionClose called with connection_id {}, which is not registered",
                    request->cluster_connection_id());
      return { ::grpc::CANCELLED,
               fmt::format("no connection with ID={}", request->cluster_connection_id()) };
    }
    conn = it->second;
    connections_.erase(it);
    remaining = connections_.size();
  }
  try {
    conn->cluster()->close().get();
  } catch (const std::exception& e) {
    spdlog::warn("failed to close connection {}: {}", request->cluster_connection_id(), e.what());
  }
  response->set_cluster_connection_count(static_cast<std::int32_t>(remaining));
  spdlog::info("clusterConnectionClose total connections {}, responding with {}",
               remaining,
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
  // get_core_transactions()/collections() can throw (e.g. std::bad_alloc copying the set under its
  // lock). Guard them so the exception is mapped to a status instead of escaping this gRPC handler
  // and terminating the server, consistent with the other cleanup-family handlers.
  try {
    auto txn =
      couchbase::core::transactions::get_core_transactions(conn->cluster()->transactions());
    auto collections = txn->cleanup().collections();
    for (auto& c : collections) {
      auto set = response->mutable_cleanup_set()->add_cleanup_set();
      set->set_bucket_name(c.bucket);
      set->set_scope_name(c.scope);
      set->set_collection_name(c.collection);
    }
  } catch (const std::exception& e) {
    return grpc::Status(::grpc::CANCELLED, e.what());
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
#ifdef _WIN32
    auto this_pid = ::_getpid();
#else
    auto this_pid = ::getpid();
#endif
    reporter = std::make_shared<fit_cxx::metrics_reporter>(conn->ctx(), this_pid, batcher, run_id);
    reporter->start();
  }

  // now, loop over the workloads...
  // Declaration order matters because, on the exception path, the catch blocks below return without
  // joining still-running tasks, so the joins happen in the ~future destructors. Locals destruct in
  // reverse declaration order, so:
  //   * stream_mut is declared FIRST (destruct last): workload/stream tasks capture
  //     them by reference, so they must outlive every task join.
  //   * stream_futures is declared before workload_futures so that workload_futures destructs
  //   FIRST.
  //     Workload tasks push into stream_futures (under stream_mut), so all workloads must be joined
  //     before stream_futures is destroyed, otherwise a mid-flight workload could touch a
  //     half-destroyed stream_futures.
  // Without this ordering, a sibling task still running when another task throws would read and
  // lock already-destroyed objects (use-after-free / data race).
  std::mutex stream_mut;
  std::list<std::future<void>> stream_futures;
  std::list<std::future<void>> workload_futures;
  try {
    for (const auto& horiz : request->workloads().horizontal_scaling()) {
      for (const auto& work : horiz.workloads()) {
        // Capture `work` by value: it is a per-iteration loop variable, so capturing it by
        // reference would let the async task observe a later iteration's workload (or a destroyed
        // reference). Everything else captured by reference outlives these futures.
        workload_futures.push_back(std::async(std::launch::async, [&, work]() -> void {
          if (work.has_sdk()) {
            const auto cmd_count = work.sdk().command_size();
            if (cmd_count == 0) {
              throw performer_exception::invalid_argument("The SDK workload has no commands");
            }
            const auto bounds = fit_cxx::bounds::from_sdk_workload(counters_, work.sdk());
            int counter = 0;
            while (bounds->can_execute()) {
              const auto& cmd = work.sdk().command(counter % cmd_count);
              auto [cmd_fut, stream_fut] = execute_sdk_command(conn, cmd, batcher);
              if (stream_fut) {
                const std::scoped_lock<std::mutex> lock(stream_mut);
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
            const auto cmd_count = work.transaction().command_size();
            if (cmd_count == 0) {
              throw performer_exception::invalid_argument(
                "The transaction workload has no commands");
            }
            const auto bounds =
              fit_cxx::bounds::from_transactions_workload(counters_, work.transaction());

            int counter = 0;
            while (bounds->can_execute()) {
              const auto& cmd = work.transaction().command(counter % cmd_count);
              // Declared in the loop scope so the timeout handler below can set it and startTxn()
              // (which polls it) can observe the timeout and short-circuit cooperatively.
              std::atomic<bool> timed_out{ false };
              fit_cxx::transaction_context txn_ctx;
              for (const auto& l : cmd.latches()) {
                txn_ctx.latches().add(l.name(), static_cast<std::uint32_t>(l.initial_count()));
              }
              conn->run_with_timeout<int>(
                std::chrono::seconds(TXN_TIMEOUT),
                [&]() {
                  protocol::run::Result res;
                  auto start = std::chrono::high_resolution_clock::now();
                  res.mutable_initiated()->CopyFrom(
                    google::protobuf::util::TimeUtil::GetCurrentTime());
                  auto result = startTxn(conn, &cmd, res.mutable_transaction(), timed_out, txn_ctx);
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
                  timed_out = true;
                  // Unblock any in-progress wait_on_latch so startTxn can unwind on timeout.
                  txn_ctx.latches().cancel_all();
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
    // Join the batcher before returning: it holds raw pointers to this frame's `result_queue` and
    // to `writer`, so returning while its async thread is still draining would be a use-after-free.
    batcher->set_workloads_complete();
    batcher->wait();
    return err.to_grpc_status();
  } catch (const std::exception& err) {
    spdlog::error("got exception with message '{}' during run", err.what());
    batcher->set_workloads_complete();
    batcher->wait();
    return grpc::Status(grpc::StatusCode::UNKNOWN, err.what());
  }
}

CmdFutures
TxnService::execute_sdk_command(ConnectionPtr conn,
                                const protocol::sdk::Command cmd,
                                std::shared_ptr<fit_cxx::Batcher> batcher)
{
  auto barrier = std::make_shared<std::promise<void>>();
  auto cmd_fut = barrier->get_future();
  std::optional<std::future<void>> stream_fut;
  google::protobuf::Timestamp initiated = google::protobuf::util::TimeUtil::GetCurrentTime();
  std::string stream_id;
  bool return_result = cmd.return_result();
  // Fail fast if the command oneof is empty: otherwise none of the has_*() branches below run and
  // the performer silently reports success for a command it never executed.
  if (cmd.command_case() == protocol::sdk::Command::COMMAND_NOT_SET) {
    throw performer_exception::invalid_argument("sdk command has no operation set: " +
                                                cmd.ShortDebugString());
  }
  if (cmd.has_get()) {
    auto coll = to_collection(conn, cmd.get().location());
    auto key = to_key(cmd.get().location());
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
    auto key = to_key(cmd.replace().location());
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
    auto key = to_key(cmd.insert().location());
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
    auto key = to_key(cmd.upsert().location());
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
    auto key = to_key(cmd.remove().location());
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
      auto stream =
        streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
      auto cleanup_fn = [this](std::string id) {
        streams_.remove(id);
      };
      auto next_fn = std::get<fit_cxx::next_function>(res);
      stream_fut = stream->begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
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
        auto stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream->begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
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
    } else {
      throw performer_exception::invalid_argument("unrecognized cluster command: " +
                                                  cluster_cmd.ShortDebugString());
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
    } else {
      throw performer_exception::invalid_argument("unrecognized bucket command: " +
                                                  bucket_cmd.ShortDebugString());
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
        auto stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream->begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
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
    } else {
      throw performer_exception::invalid_argument("unrecognized scope command: " +
                                                  scope_cmd.ShortDebugString());
    }
  }
  if (cmd.has_collection_command()) {
    auto collection_cmd = cmd.collection_command();
    auto collection = to_collection(conn, collection_cmd.collection());

    if (collection_cmd.has_lookup_in()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.lookup_in().location()),
        /* .key = */ to_key(collection_cmd.lookup_in().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.lookup_in(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_and_lock()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_and_lock().location()),
        /* .key = */ to_key(collection_cmd.get_and_lock().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_and_lock(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_unlock()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.unlock().location()),
        /* .key = */ to_key(collection_cmd.unlock().location()),
        /* .spans = */ &spans_,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.unlock(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_and_touch()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_and_touch().location()),
        /* .key = */ to_key(collection_cmd.get_and_touch().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_and_touch(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_exists()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.exists().location()),
        /* .key = */ to_key(collection_cmd.exists().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.exists(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_touch()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.touch().location()),
        /* .key = */ to_key(collection_cmd.touch().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(collection_cmd.touch(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_mutate_in()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.mutate_in().location()),
        /* .key = */ to_key(collection_cmd.mutate_in().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.mutate_in(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_any_replica()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_any_replica().location()),
        /* .key = */ to_key(collection_cmd.get_any_replica().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res =
        fit_cxx::commands::key_value::execute_command(collection_cmd.get_any_replica(), cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_get_all_replicas()) {
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ to_collection(conn, collection_cmd.get_all_replicas().location()),
        /* .key = */ to_key(collection_cmd.get_all_replicas().location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_streaming_command(
        collection_cmd.get_all_replicas(), cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = collection_cmd.get_all_replicas().stream_config();
        auto stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream->begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
      } else {
        batcher->send_result(std::get<protocol::run::Result>(res));
      }
    } else if (collection_cmd.has_binary()) {
      auto binary_cmd = collection_cmd.binary();
      if (binary_cmd.has_append()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.append().location()),
          /* .key = */ to_key(binary_cmd.append().location()),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.append(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_prepend()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.prepend().location()),
          /* .key = */ to_key(binary_cmd.prepend().location()),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.prepend(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_increment()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.increment().location()),
          /* .key = */ to_key(binary_cmd.increment().location()),
          /* .spans = */ &spans_,
          /* .return_result = */ return_result,
        };
        auto res = fit_cxx::commands::key_value::execute_command(binary_cmd.increment(), cmd_args);
        batcher->send_result(res);
      } else if (binary_cmd.has_decrement()) {
        auto cmd_args = fit_cxx::commands::key_value::command_args{
          /* .collection = */ to_collection(conn, binary_cmd.decrement().location()),
          /* .key = */ to_key(binary_cmd.decrement().location()),
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
        /* .key = */ to_key(lookup_in_cmd.location()),
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_command(lookup_in_cmd, cmd_args);
      batcher->send_result(res);
    } else if (collection_cmd.has_lookup_in_all_replicas()) {
      auto lookup_in_cmd = collection_cmd.lookup_in_all_replicas();
      collection = to_collection(conn, lookup_in_cmd.location());
      auto key = to_key(lookup_in_cmd.location());
      auto cmd_args = fit_cxx::commands::key_value::command_args{
        /* .collection = */ collection,
        /* .key = */ key,
        /* .spans = */ &spans_,
        /* .return_result = */ return_result,
      };
      auto res = fit_cxx::commands::key_value::execute_streaming_command(lookup_in_cmd, cmd_args);
      if (std::holds_alternative<fit_cxx::next_function>(res)) {
        auto stream_config = lookup_in_cmd.stream_config();
        auto stream =
          streams_.insert(stream_config.stream_id(), batcher, stream_config.has_automatically());
        auto cleanup_fn = [this](std::string id) {
          streams_.remove(id);
        };
        auto next_fn = std::get<fit_cxx::next_function>(res);
        stream_fut = stream->begin(stream_config.stream_id(), std::move(next_fn), cleanup_fn);
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
    } else {
      throw performer_exception::invalid_argument("unrecognized collection command: " +
                                                  collection_cmd.ShortDebugString());
    }
  }

  // The command above runs synchronously, so signal completion now. Without this the promise would
  // be destroyed unfulfilled and cmd_fut would only become ready via broken_promise (and would
  // throw on get()).
  barrier->set_value();
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
      ->set_num_to_write(static_cast<std::size_t>(request->num_items()));
    return grpc::Status::OK;
  } catch (...) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
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
    streams_.find(request->stream_id())->cancel();
    return grpc::Status::OK;
  } catch (...) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        fmt::format("no stream with id {}", request->stream_id()));
  }
}

grpc::Status
TxnService::spanCreate(grpc::ServerContext* /*context*/,
                       const protocol::observability::SpanCreateRequest* request,
                       protocol::observability::SpanCreateResponse* /*response*/)
{
  spdlog::info("spanCreate called with {}", request->DebugString());

  ConnectionPtr conn = getConn(request->cluster_connection_id());
  if (!conn) {
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
  } catch (const std::exception& e) {
    return { ::grpc::CANCELLED, e.what() };
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
  } catch (const std::exception& e) {
    return { ::grpc::CANCELLED, e.what() };
  }

  return grpc::Status::OK;
}

grpc::Status
TxnService::setCounter(grpc::ServerContext* /*context*/,
                       const protocol::shared::Counter* request,
                       protocol::shared::SetCounterResponse* /*response*/)
{
  spdlog::info("setCounter called with {}", request->DebugString());

  try {
    counters_.set_counter_value(*request);
  } catch (const performer_exception& e) {
    return e.to_grpc_status();
  }
  return grpc::Status::OK;
}

grpc::Status
TxnService::clearAllCounters(grpc::ServerContext* /*context*/,
                             const protocol::shared::ClearAllCountersRequest* /*request*/,
                             protocol::shared::ClearAllCountersResponse* /*response*/)
{
  spdlog::info("clearAllCounters called");

  counters_.clear();
  return grpc::Status::OK;
}
