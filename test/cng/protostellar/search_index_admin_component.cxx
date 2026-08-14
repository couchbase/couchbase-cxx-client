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

// The search index management overloads against an in-process admin.search.v1 server that records
// what it was asked (CXXCBC-901). Env-agnostic; no cluster.
//
// This is where the two properties a live gateway cannot show are checked.
//
// Which RPC an operation picks: a real gateway answers CreateIndex and UpdateIndex through the
// same PUT, and the paired control RPCs through endpoints whose effect this transport cannot read
// back, so from the outside the two branches of each pair are indistinguishable. Here the server
// records the method name.
//
// And whether a refusal happened locally: the gateway also answers an empty index name with
// InvalidArgument, so the error code alone does not say whether anything was sent. The recorded
// call count does.

#include "framework/test_runner.hxx"

#include "callback_queue_keepalive.hxx"

#include "core/cluster_credentials.hxx"
#include "core/protostellar/component.hxx"

#include <couchbase/error_codes.hxx>

#include <couchbase/admin/search/v1/search.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace v1 = ::couchbase::admin::search::v1;
namespace ops = ::couchbase::core::operations;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using namespace std::chrono_literals;

class recording_search_admin_service final : public v1::SearchAdminService::Service
{
public:
  std::vector<std::string> calls{};
  v1::CreateIndexRequest last_create{};
  v1::UpdateIndexRequest last_update{};
  // What GetIndex answers with. Left unset so the default is a response with no index, which is
  // the shape the review asks to be reported as index_not_found.
  v1::GetIndexResponse get_response{};

  auto CreateIndex(grpc::ServerContext* /* ctx */,
                   const v1::CreateIndexRequest* request,
                   v1::CreateIndexResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("CreateIndex");
    last_create = *request;
    return grpc::Status::OK;
  }

  auto UpdateIndex(grpc::ServerContext* /* ctx */,
                   const v1::UpdateIndexRequest* request,
                   v1::UpdateIndexResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("UpdateIndex");
    last_update = *request;
    return grpc::Status::OK;
  }

  auto GetIndex(grpc::ServerContext* /* ctx */,
                const v1::GetIndexRequest* /* request */,
                v1::GetIndexResponse* response) -> grpc::Status override
  {
    calls.emplace_back("GetIndex");
    *response = get_response;
    return grpc::Status::OK;
  }

  auto DeleteIndex(grpc::ServerContext* /* ctx */,
                   const v1::DeleteIndexRequest* /* request */,
                   v1::DeleteIndexResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("DeleteIndex");
    return grpc::Status::OK;
  }

  auto AnalyzeDocument(grpc::ServerContext* /* ctx */,
                       const v1::AnalyzeDocumentRequest* /* request */,
                       v1::AnalyzeDocumentResponse* response) -> grpc::Status override
  {
    calls.emplace_back("AnalyzeDocument");
    response->set_status("ok");
    response->set_analyzed("[]");
    return grpc::Status::OK;
  }

  auto PauseIndexIngest(grpc::ServerContext* /* ctx */,
                        const v1::PauseIndexIngestRequest* /* request */,
                        v1::PauseIndexIngestResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("PauseIndexIngest");
    return grpc::Status::OK;
  }

  auto ResumeIndexIngest(grpc::ServerContext* /* ctx */,
                         const v1::ResumeIndexIngestRequest* /* request */,
                         v1::ResumeIndexIngestResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("ResumeIndexIngest");
    return grpc::Status::OK;
  }

  auto AllowIndexQuerying(grpc::ServerContext* /* ctx */,
                          const v1::AllowIndexQueryingRequest* /* request */,
                          v1::AllowIndexQueryingResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("AllowIndexQuerying");
    return grpc::Status::OK;
  }

  auto DisallowIndexQuerying(grpc::ServerContext* /* ctx */,
                             const v1::DisallowIndexQueryingRequest* /* request */,
                             v1::DisallowIndexQueryingResponse* /* response */)
    -> grpc::Status override
  {
    calls.emplace_back("DisallowIndexQuerying");
    return grpc::Status::OK;
  }

  auto FreezeIndexPlan(grpc::ServerContext* /* ctx */,
                       const v1::FreezeIndexPlanRequest* /* request */,
                       v1::FreezeIndexPlanResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("FreezeIndexPlan");
    return grpc::Status::OK;
  }

  auto UnfreezeIndexPlan(grpc::ServerContext* /* ctx */,
                         const v1::UnfreezeIndexPlanRequest* /* request */,
                         v1::UnfreezeIndexPlanResponse* /* response */) -> grpc::Status override
  {
    calls.emplace_back("UnfreezeIndexPlan");
    return grpc::Status::OK;
  }
};

class in_process_server
{
public:
  in_process_server()
  {
    // Pin gRPC's process-global callback completion queue for the lifetime of this binary, for the
    // reason component.cxx gives: destroying the last channel between cases otherwise races
    // gRPC's polling threads and aborts the process (CXXCBC-919).
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
  [[nodiscard]] auto service() -> recording_search_admin_service&
  {
    return service_;
  }

private:
  recording_search_admin_service service_;
  std::unique_ptr<grpc::Server> server_;
};

// Runs one request to completion on its own io_context and hands back the response.
template<typename Request>
[[nodiscard]] auto
run(in_process_server& server, Request request) -> typename Request::response_type
{
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  component comp{ io, component_config{ server.channel(), cluster_credentials{}, { 5000ms } } };

  typename Request::response_type outcome;
  comp.execute(std::move(request), [&](typename Request::response_type response) {
    outcome = std::move(response);
    work.reset();
  });
  io.run();
  return outcome;
}

[[nodiscard]] auto
definition(std::string name) -> couchbase::core::management::search::index
{
  couchbase::core::management::search::index index;
  index.name = std::move(name);
  index.type = "fulltext-index";
  index.source_type = "couchbase";
  index.source_name = "travel";
  return index;
}

void
an_upsert_without_a_uuid_calls_create_index([[maybe_unused]] context& ctx)
{
  in_process_server server;

  ops::management::search_index_upsert_request request{};
  request.index = definition("idx");
  const auto response = run(server, std::move(request));

  assert_false(static_cast<bool>(response.ctx.ec), "the upsert succeeds");
  assert_eq(server.service().calls,
            std::vector<std::string>{ "CreateIndex" },
            "an upsert with no uuid goes to CreateIndex");
  assert_false(server.service().last_create.has_prev_index_uuid(),
               "and carries no prev_index_uuid");
}

void
an_upsert_with_a_uuid_calls_update_index([[maybe_unused]] context& ctx)
{
  in_process_server server;

  auto index = definition("idx");
  index.uuid = "existing-uuid";
  ops::management::search_index_upsert_request request{};
  request.index = std::move(index);
  const auto response = run(server, std::move(request));

  assert_false(static_cast<bool>(response.ctx.ec), "the upsert succeeds");
  assert_eq(server.service().calls,
            std::vector<std::string>{ "UpdateIndex" },
            "an upsert carrying a uuid goes to UpdateIndex");
  assert_eq(server.service().last_update.index().uuid(),
            std::string{ "existing-uuid" },
            "and the uuid reaches Index.uuid, which the gateway requires");
  assert_eq(server.service().last_update.index().name(),
            std::string{ "idx" },
            "the rest of the definition travels with it");
}

void
the_control_operations_select_their_paired_rpc([[maybe_unused]] context& ctx)
{
  {
    in_process_server server;
    ops::management::search_index_control_ingest_request request{};
    request.index_name = "idx";
    request.pause = true;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "pause succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "PauseIndexIngest" },
              "pause selects PauseIndexIngest");
  }
  {
    in_process_server server;
    ops::management::search_index_control_ingest_request request{};
    request.index_name = "idx";
    request.pause = false;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "resume succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "ResumeIndexIngest" },
              "resume selects ResumeIndexIngest");
  }
  {
    in_process_server server;
    ops::management::search_index_control_query_request request{};
    request.index_name = "idx";
    request.allow = true;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "allow succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "AllowIndexQuerying" },
              "allow selects AllowIndexQuerying");
  }
  {
    in_process_server server;
    ops::management::search_index_control_query_request request{};
    request.index_name = "idx";
    request.allow = false;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "disallow succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "DisallowIndexQuerying" },
              "disallow selects DisallowIndexQuerying");
  }
  {
    in_process_server server;
    ops::management::search_index_control_plan_freeze_request request{};
    request.index_name = "idx";
    request.freeze = true;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "freeze succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "FreezeIndexPlan" },
              "freeze selects FreezeIndexPlan");
  }
  {
    in_process_server server;
    ops::management::search_index_control_plan_freeze_request request{};
    request.index_name = "idx";
    request.freeze = false;
    assert_false(static_cast<bool>(run(server, std::move(request)).ctx.ec), "unfreeze succeeds");
    assert_eq(server.service().calls,
              std::vector<std::string>{ "UnfreezeIndexPlan" },
              "unfreeze selects UnfreezeIndexPlan");
  }
}

void
an_empty_index_name_is_refused_without_a_round_trip([[maybe_unused]] context& ctx)
{
  // The gateway answers an empty name with InvalidArgument too, so the error code on its own does
  // not distinguish a local refusal from a round trip. The empty call list is what does.
  const auto refused_locally = [](in_process_server& server, std::error_code ec, const char* what) {
    assert_eq(ec, std::error_code{ couchbase::errc::common::invalid_argument }, what);
    assert_true(server.service().calls.empty(), "and nothing was sent");
  };

  {
    in_process_server server;
    ops::management::search_index_upsert_request request{};
    request.index = definition("");
    refused_locally(server, run(server, std::move(request)).ctx.ec, "upsert refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_get_request request{};
    refused_locally(server, run(server, std::move(request)).ctx.ec, "get refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_drop_request request{};
    refused_locally(server, run(server, std::move(request)).ctx.ec, "drop refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_analyze_document_request request{};
    request.encoded_document = R"({"a":1})";
    refused_locally(
      server, run(server, std::move(request)).ctx.ec, "analyze refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_control_ingest_request request{};
    request.pause = true;
    refused_locally(
      server, run(server, std::move(request)).ctx.ec, "control ingest refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_control_query_request request{};
    request.allow = true;
    refused_locally(
      server, run(server, std::move(request)).ctx.ec, "control query refuses an empty name");
  }
  {
    in_process_server server;
    ops::management::search_index_control_plan_freeze_request request{};
    request.freeze = true;
    refused_locally(
      server, run(server, std::move(request)).ctx.ec, "control plan freeze refuses an empty name");
  }
}

void
a_definition_that_cannot_be_represented_is_not_sent([[maybe_unused]] context& ctx)
{
  in_process_server server;

  auto index = definition("idx");
  index.params_json = R"(["not","an","object"])";
  ops::management::search_index_upsert_request request{};
  request.index = std::move(index);
  const auto response = run(server, std::move(request));

  assert_eq(response.ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "a params blob with no representation is reported as invalid_argument");
  assert_true(server.service().calls.empty(),
              "and no index was created from the part that did convert");
}

void
a_get_response_without_an_index_reports_index_not_found([[maybe_unused]] context& ctx)
{
  in_process_server server;

  ops::management::search_index_get_request request{};
  request.index_name = "idx";
  const auto response = run(server, std::move(request));

  assert_eq(response.ctx.ec,
            std::error_code{ couchbase::errc::common::index_not_found },
            "a success carrying no index is index_not_found, not an empty definition");
  assert_true(response.index.name.empty(), "and no definition is handed back");
}

void
an_index_that_cannot_be_decoded_reports_parsing_failure([[maybe_unused]] context& ctx)
{
  in_process_server server;
  (*server.service().get_response.mutable_index()->mutable_params())["mapping"] = "not json";
  server.service().get_response.mutable_index()->set_name("idx");

  ops::management::search_index_get_request request{};
  request.index_name = "idx";
  const auto response = run(server, std::move(request));

  assert_eq(response.ctx.ec,
            std::error_code{ couchbase::errc::common::parsing_failure },
            "a definition that cannot be rebuilt is an error, not a truncated definition");
  assert_true(response.index.params_json.empty(), "and no partial definition is handed back");
}

void
a_successful_call_reports_status_ok([[maybe_unused]] context& ctx)
{
  // The field the classic path fills from FTS's {"status":"ok"}; admin.search.v1 answers with
  // empty messages, so a successful RPC is what stands in for it.
  {
    in_process_server server;
    ops::management::search_index_drop_request request{};
    request.index_name = "idx";
    const auto response = run(server, std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec), "drop succeeds");
    assert_eq(response.status, std::string{ "ok" }, "drop reports status ok");
  }
  {
    in_process_server server;
    server.service().get_response.mutable_index()->set_name("idx");
    ops::management::search_index_get_request request{};
    request.index_name = "idx";
    const auto response = run(server, std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec), "get succeeds");
    assert_eq(response.status, std::string{ "ok" }, "get reports status ok");
  }
  {
    in_process_server server;
    ops::management::search_index_analyze_document_request request{};
    request.index_name = "idx";
    request.encoded_document = R"({"a":1})";
    const auto response = run(server, std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec), "analyze succeeds");
    assert_eq(response.status, std::string{ "ok" }, "analyze relays the gateway's status");
    assert_eq(response.analysis, std::string{ "[]" }, "and the analysis");
  }
}

void
a_failed_call_reports_no_status([[maybe_unused]] context& ctx)
{
  // A status the server never reported would say the operation succeeded; the error code is what
  // describes a failure.
  in_process_server server;
  ops::management::search_index_get_request request{};
  request.index_name = "idx";
  const auto response = run(server, std::move(request));
  assert_true(static_cast<bool>(response.ctx.ec), "the default GetIndexResponse fails the call");
  assert_true(response.status.empty(), "and leaves status unset");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_search_index_admin_component",
    {
      { "an_upsert_without_a_uuid_calls_create_index",
        an_upsert_without_a_uuid_calls_create_index },
      { "an_upsert_with_a_uuid_calls_update_index", an_upsert_with_a_uuid_calls_update_index },
      { "the_control_operations_select_their_paired_rpc",
        the_control_operations_select_their_paired_rpc },
      { "an_empty_index_name_is_refused_without_a_round_trip",
        an_empty_index_name_is_refused_without_a_round_trip },
      { "a_definition_that_cannot_be_represented_is_not_sent",
        a_definition_that_cannot_be_represented_is_not_sent },
      { "a_get_response_without_an_index_reports_index_not_found",
        a_get_response_without_an_index_reports_index_not_found },
      { "an_index_that_cannot_be_decoded_reports_parsing_failure",
        an_index_that_cannot_be_decoded_reports_parsing_failure },
      { "a_successful_call_reports_status_ok", a_successful_call_reports_status_ok },
      { "a_failed_call_reports_no_status", a_failed_call_reports_no_status },
    },
  };
}

} // namespace couchbase::test
