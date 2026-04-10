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

#include "batcher.hxx"
#include "bounds.hxx"
#include "connection.hxx"
#include "counters.hxx"
#include "exceptions.hxx"
#include "observability/span_owner.hxx"
#include "performer.grpc.pb.h"
#include "performer.pb.h"
#include "stream.hxx"

#include <core/transactions.hxx>

#include <couchbase/error_context.hxx>

#include <grpc++/grpc++.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

using ConnectionPtr = std::shared_ptr<fit_cxx::Connection>;
using CmdFutures = std::pair<std::future<void>, std::optional<std::future<void>>>;

class TxnService final : public protocol::PerformerService::Service
{
private:
  std::mutex mutex_;
  std::map<std::string, ConnectionPtr> connections_;
  std::optional<couchbase::transactions::transaction_get_result> stashed_result_{};
  std::map<std::uint32_t, couchbase::transactions::transaction_get_result> stashed_result_slots_{};
  fit_cxx::run_result_streams streams_;
  std::mt19937 generator_;
  fit_cxx::observability::span_owner spans_{};

  ConnectionPtr getConn(const std::string& id)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = connections_.find(id);
    if (it == connections_.end()) {
      spdlog::error("no connection with id={} in connections", id);
      return nullptr;
    }
    return it->second;
  }

  CmdFutures execute_sdk_command(ConnectionPtr conn,
                                 protocol::sdk::Command cmd,
                                 std::shared_ptr<fit_cxx::Batcher> batcher,
                                 Counters& counters);

  couchbase::error execute_command(ConnectionPtr conn,
                                   std::shared_ptr<couchbase::transactions::attempt_context> ctx,
                                   const protocol::transactions::TransactionCommand& cmd,
                                   const std::string& logger_prefix,
                                   Counters& counters,
                                   bool is_batch = false);

  grpc::Status startTxn(ConnectionPtr conn,
                        const protocol::transactions::TransactionCreateRequest* request,
                        protocol::transactions::TransactionResult* response,
                        std::atomic<bool>& timed_out,
                        Counters& counters);

  std::string configure_bounds(fit_cxx::Bounds& workload_bounds,
                               bool has_bounds,
                               const protocol::shared::Bounds& bounds);

  std::string to_key(const protocol::shared::DocLocation& loc, Counters& counters);

  auto to_get_multi_specs(ConnectionPtr conn,
                          const protocol::transactions::TransactionCommand& cmd,
                          Counters& counters)
    -> std::vector<couchbase::transactions::transaction_get_multi_spec>;
  auto to_get_multi_options(const protocol::transactions::TransactionCommand& cmd)
    -> couchbase::transactions::transaction_get_multi_options;
  auto to_get_multi_replicas_from_preferred_server_group_specs(
    ConnectionPtr conn,
    const protocol::transactions::TransactionCommand& cmd,
    Counters& counters)
    -> std::vector<
      couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_spec>;
  auto to_get_multi_replicas_from_preferred_server_group_options(
    const protocol::transactions::TransactionCommand& cmd)
    -> couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_options;

public:
  TxnService();

  grpc::Status clusterConnectionClose(
    grpc::ServerContext* context,
    const protocol::shared::ClusterConnectionCloseRequest* request,
    protocol::shared::ClusterConnectionCloseResponse* response) override;
  grpc::Status clusterConnectionCreate(
    grpc::ServerContext* context,
    const protocol::shared::ClusterConnectionCreateRequest* request,
    protocol::shared::ClusterConnectionCreateResponse* response) override;
  grpc::Status performerCapsFetch(
    grpc::ServerContext* context,
    const protocol::performer::PerformerCapsFetchRequest* request,
    protocol::performer::PerformerCapsFetchResponse* response) override;

  grpc::Status disconnectConnections(
    grpc::ServerContext* context,
    const ::protocol::shared::DisconnectConnectionsRequest* request,
    ::protocol::shared::DisconnectConnectionsResponse* response) override;

  grpc::Status transactionStream(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<protocol::transactions::TransactionStreamPerformerToDriver,
                             protocol::transactions::TransactionStreamDriverToPerformer>* stream)
    override;

  grpc::Status transactionCreate(grpc::ServerContext* context,
                                 const protocol::transactions::TransactionCreateRequest* request,
                                 protocol::transactions::TransactionResult* response) override;

  grpc::Status transactionCleanup(
    grpc::ServerContext* context,
    const protocol::transactions::TransactionCleanupRequest* request,
    protocol::transactions::TransactionCleanupAttempt* response) override;

  grpc::Status transactionCleanupATR(
    grpc::ServerContext* context,
    const protocol::transactions::TransactionCleanupATRRequest* request,
    protocol::transactions::TransactionCleanupATRResult* response) override;

  grpc::Status clientRecordProcess(
    grpc::ServerContext* context,
    const protocol::transactions::ClientRecordProcessRequest* request,
    protocol::transactions::ClientRecordProcessResponse* response) override;

  grpc::Status cleanupSetFetch(grpc::ServerContext* context,
                               const protocol::transactions::CleanupSetFetchRequest* request,
                               protocol::transactions::CleanupSetFetchResponse* response) override;

  grpc::Status echo(grpc::ServerContext* context,
                    const protocol::shared::EchoRequest* request,
                    protocol::shared::EchoResponse* response) override;

  grpc::Status run(grpc::ServerContext* context,
                   const protocol::run::Request* request,
                   grpc::ServerWriter<::protocol::run::Result>* writer) override;

  grpc::Status streamRequestItems(grpc::ServerContext* context,
                                  const protocol::streams::RequestItemsRequest* request,
                                  protocol::streams::RequestItemsResponse* response) override;
  grpc::Status streamCancel(grpc::ServerContext* context,
                            const protocol::streams::CancelRequest* request,
                            protocol::streams::CancelResponse* response) override;

  grpc::Status spanCreate(grpc::ServerContext* context,
                          const protocol::observability::SpanCreateRequest* request,
                          protocol::observability::SpanCreateResponse* response) override;

  grpc::Status spanFinish(grpc::ServerContext* context,
                          const protocol::observability::SpanFinishRequest* request,
                          protocol::observability::SpanFinishResponse* response) override;
};
