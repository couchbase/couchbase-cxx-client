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

#include "search_index_management.hxx"

#include "../exceptions.hxx"
#include "common.hxx"
#include "run.top_level.pb.h"

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "core/meta/features.hxx"
#include "core/utils/json.hxx"

namespace fit_cxx::commands::search_index_management
{
couchbase::get_search_index_options
to_get_index_options(const protocol::sdk::search::index_manager::GetIndex& cmd,
                     observability::span_owner* spans)
{
  couchbase::get_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_all_search_indexes_options
to_get_all_indexes_options(const protocol::sdk::search::index_manager::GetAllIndexes& cmd,
                           observability::span_owner* spans)
{
  couchbase::get_all_search_indexes_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::upsert_search_index_options
to_upsert_index_options(const protocol::sdk::search::index_manager::UpsertIndex& cmd,
                        observability::span_owner* spans)
{
  couchbase::upsert_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::drop_search_index_options
to_drop_index_options(const protocol::sdk::search::index_manager::DropIndex& cmd,
                      observability::span_owner* spans)
{
  couchbase::drop_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_indexed_search_index_options
to_get_indexed_documents_count_options(
  const protocol::sdk::search::index_manager::GetIndexedDocumentsCount& cmd,
  observability::span_owner* spans)
{
  couchbase::get_indexed_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::pause_ingest_search_index_options
to_pause_ingest_options(const protocol::sdk::search::index_manager::PauseIngest& cmd,
                        observability::span_owner* spans)
{
  couchbase::pause_ingest_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::resume_ingest_search_index_options
to_resume_ingest_options(const protocol::sdk::search::index_manager::ResumeIngest& cmd,
                         observability::span_owner* spans)
{
  couchbase::resume_ingest_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::allow_querying_search_index_options
to_allow_querying_options(const protocol::sdk::search::index_manager::AllowQuerying& cmd,
                          observability::span_owner* spans)
{
  couchbase::allow_querying_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::disallow_querying_search_index_options
to_disallow_querying_options(const protocol::sdk::search::index_manager::DisallowQuerying& cmd,
                             observability::span_owner* spans)
{
  couchbase::disallow_querying_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::freeze_plan_search_index_options
to_freeze_plan_options(const protocol::sdk::search::index_manager::FreezePlan& cmd,
                       observability::span_owner* spans)
{
  couchbase::freeze_plan_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::unfreeze_plan_search_index_options
to_unfreeze_plan_options(const protocol::sdk::search::index_manager::UnfreezePlan& cmd,
                         observability::span_owner* spans)
{
  couchbase::unfreeze_plan_search_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::analyze_document_options
to_analyze_document_options(const protocol::sdk::search::index_manager::AnalyzeDocument& cmd,
                            observability::span_owner* spans)
{
  couchbase::analyze_document_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::management::search::index
to_search_index(std::string definition)
{
  couchbase::management::search::index index{};

  auto parsed = couchbase::core::utils::json::parse(definition);
  auto object = parsed.get_object();

  for (const auto& [key, val] : object) {
    if (key == "name") {
      index.name = val.as<std::string>();
    } else if (key == "type") {
      index.type = val.as<std::string>();
    } else if (key == "sourceName") {
      index.source_name = val.as<std::string>();
    } else if (key == "sourceType") {
      index.source_type = val.as<std::string>();
    } else if (key == "uuid") {
      index.uuid = val.as<std::string>();
    } else if (key == "params") {
      index.params_json = couchbase::core::utils::json::generate(val);
    } else if (key == "sourceUUID") {
      index.source_uuid = val.as<std::string>();
    } else if (key == "sourceParams") {
      index.source_params_json = couchbase::core::utils::json::generate(val);
    } else if (key == "planParams") {
      index.plan_params_json = couchbase::core::utils::json::generate(val);
    } else if (key == "id") {
      spdlog::warn("'id' field in search index definition being ignored");
    } else {
      throw performer_exception::unimplemented(fmt::format("Unknown search index key: {}", key));
    }
  }

  return index;
}

void
from_get_search_index_result(const couchbase::management::search::index& result,
                             protocol::sdk::search::index_manager::SearchIndex* res)
{
  res->set_name(result.name);
  res->set_type(result.type);
  res->set_source_type(result.source_type);
  if (result.uuid.has_value()) {
    res->set_uuid(result.uuid.value());
  }
  if (result.source_uuid.has_value()) {
    res->set_source_uuid(result.source_uuid.value());
  }
  if (result.params_json.has_value()) {
    res->set_params(result.params_json.value());
  }
  if (result.source_params_json.has_value()) {
    res->set_source_params(result.source_params_json.value());
  }
  if (result.plan_params_json.has_value()) {
    res->set_plan_params(result.plan_params_json.value());
  }
}

void
from_get_all_search_indexes_result(const std::vector<couchbase::management::search::index>& result,
                                   protocol::sdk::search::index_manager::SearchIndexes* res)
{
  for (const auto& index : result) {
    auto* idx = res->add_indexes();
    from_get_search_index_result(index, idx);
  }
}

void
from_analyze_document_result(std::vector<std::string> analyses,
                             protocol::sdk::search::index_manager::AnalyzeDocumentResult* res)
{
  for (const auto& analysis : analyses)
    res->add_results(analysis);
}

template<typename SearchIndexManager>
protocol::run::Result
execute_shared_command(const protocol::sdk::search::index_manager::Command& cmd,
                       command_args& args,
                       SearchIndexManager manager)
{
  protocol::run::Result res;

  if (cmd.has_get_index()) {
    const auto& get_index = cmd.get_index();
    const auto opts = to_get_index_options(get_index, args.spans);
    auto [err, result] = manager.get_index(get_index.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_get_search_index_result(
          result, res.mutable_sdk()->mutable_search_index_manager_result()->mutable_index());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else if (cmd.has_get_all_indexes()) {
    const auto& get_all_indexes = cmd.get_all_indexes();
    const auto opts = to_get_all_indexes_options(get_all_indexes, args.spans);
    auto [err, result] = manager.get_all_indexes(opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_get_all_search_indexes_result(
          result, res.mutable_sdk()->mutable_search_index_manager_result()->mutable_indexes());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else if (cmd.has_upsert_index()) {
    const auto& upsert_index = cmd.upsert_index();
    const auto opts = to_upsert_index_options(upsert_index, args.spans);
    const auto index = to_search_index(upsert_index.index_definition());
    auto err = manager.upsert_index(index, opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_drop_index()) {
    const auto& drop_index = cmd.drop_index();
    const auto opts = to_drop_index_options(drop_index, args.spans);
    auto err = manager.drop_index(drop_index.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_get_indexed_documents_count()) {
    const auto& get_indexed_documents_count = cmd.get_indexed_documents_count();
    const auto opts =
      to_get_indexed_documents_count_options(get_indexed_documents_count, args.spans);
    auto [err, result] =
      manager.get_indexed_documents_count(get_indexed_documents_count.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        res.mutable_sdk()->mutable_search_index_manager_result()->set_indexed_document_counts(
          static_cast<std::int32_t>(result));
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else if (cmd.has_pause_ingest()) {
    const auto& pause_ingest = cmd.pause_ingest();
    const auto opts = to_pause_ingest_options(pause_ingest, args.spans);
    auto err = manager.pause_ingest(pause_ingest.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_resume_ingest()) {
    const auto& resume_ingest = cmd.resume_ingest();
    const auto opts = to_resume_ingest_options(resume_ingest, args.spans);
    auto err = manager.resume_ingest(resume_ingest.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_allow_querying()) {
    const auto& allow_querying = cmd.allow_querying();
    const auto opts = to_allow_querying_options(allow_querying, args.spans);
    auto err = manager.allow_querying(allow_querying.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_disallow_querying()) {
    const auto& disallow_querying = cmd.disallow_querying();
    const auto opts = to_disallow_querying_options(disallow_querying, args.spans);
    auto err = manager.disallow_querying(disallow_querying.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_freeze_plan()) {
    const auto& freeze_plan = cmd.freeze_plan();
    const auto opts = to_freeze_plan_options(freeze_plan, args.spans);
    auto err = manager.freeze_plan(freeze_plan.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_unfreeze_plan()) {
    const auto& unfreeze_plan = cmd.unfreeze_plan();
    const auto opts = to_unfreeze_plan_options(unfreeze_plan, args.spans);
    auto err = manager.unfreeze_plan(unfreeze_plan.index_name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_analyze_document()) {
    const auto& analyze_document = cmd.analyze_document();
    const auto opts = to_analyze_document_options(analyze_document, args.spans);
    auto document = couchbase::core::utils::json::parse(analyze_document.document());
    auto [err, result] =
      manager.analyze_document(analyze_document.index_name(), document, opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_analyze_document_result(
          result,
          res.mutable_sdk()->mutable_search_index_manager_result()->mutable_analyze_document());
      }
    }
  }
  return res;
}

protocol::run::Result
execute_cluster_command(const protocol::sdk::cluster::search::index_manager::Command& cmd,
                        command_args& args)
{
  auto manager = args.cluster->search_indexes();
  return execute_shared_command<couchbase::search_index_manager>(cmd.shared(), args, manager);
}

protocol::run::Result
execute_scope_command(const protocol::sdk::scope::search::index_manager::Command& cmd,
                      command_args& args)
{
  auto manager = args.scope.value().search_indexes();
  return execute_shared_command<couchbase::scope_search_index_manager>(cmd.shared(), args, manager);
}
} // namespace fit_cxx::commands::search_index_management
