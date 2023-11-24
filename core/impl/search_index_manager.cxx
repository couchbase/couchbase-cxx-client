/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/cluster.hxx"

#include "core/operations/management/search_index_analyze_document.hxx"
#include "core/operations/management/search_index_control_ingest.hxx"
#include "core/operations/management/search_index_control_plan_freeze.hxx"
#include "core/operations/management/search_index_control_query.hxx"
#include "core/operations/management/search_index_drop.hxx"
#include "core/operations/management/search_index_get.hxx"
#include "core/operations/management/search_index_get_all.hxx"
#include "core/operations/management/search_index_get_documents_count.hxx"
#include "core/operations/management/search_index_upsert.hxx"
#include "core/utils/json.hxx"
#include "internal_manager_error_context.hxx"

#include <couchbase/search_index_manager.hxx>

#include <memory>
#include <utility>

namespace couchbase
{
namespace
{
template<typename Response>
manager_error_context
build_context(Response& resp)
{
    return manager_error_context(internal_manager_error_context{ resp.ctx.ec,
                                                                 resp.ctx.last_dispatched_to,
                                                                 resp.ctx.last_dispatched_from,
                                                                 resp.ctx.retry_attempts,
                                                                 std::move(resp.ctx.retry_reasons),
                                                                 std::move(resp.ctx.client_context_id),
                                                                 resp.ctx.http_status,
                                                                 std::move(resp.ctx.http_body),
                                                                 std::move(resp.ctx.path) });
}

couchbase::management::search::index
map_search_index(const couchbase::core::management::search::index& index)
{
    couchbase::management::search::index public_index{};
    public_index.uuid = index.uuid;
    public_index.name = index.name;
    public_index.type = index.type;
    public_index.params_json = index.params_json;

    public_index.source_name = index.source_name;
    public_index.source_type = index.source_type;
    public_index.source_uuid = index.source_uuid;
    public_index.source_params_json = index.source_params_json;

    public_index.plan_params_json = index.plan_params_json;

    return public_index;
}

std::vector<couchbase::management::search::index>
map_all_search_indexes(const std::vector<couchbase::core::management::search::index>& indexes)
{
    std::vector<couchbase::management::search::index> search_indexes{};
    for (const auto& index : indexes) {
        auto converted_index = map_search_index(index);
        search_indexes.emplace_back(converted_index);
    }
    return search_indexes;
}

couchbase::core::management::search::index
map_search_index(const couchbase::management::search::index& index)
{
    couchbase::core::management::search::index search_index{};
    search_index.name = index.name;
    search_index.type = index.type;
    search_index.source_type = index.source_type;

    if (index.uuid.has_value()) {
        search_index.uuid = index.uuid.value();
    }
    if (index.params_json.has_value()) {
        search_index.params_json = index.params_json.value();
    }
    // Below marked as omitempty
    if (!index.source_name.empty()) {
        search_index.source_name = index.source_name;
    }
    if (index.source_uuid.has_value()) {
        if (!index.source_uuid.value().empty()) {
            search_index.source_uuid = index.source_uuid.value();
        }
    }
    if (index.source_params_json.has_value()) {
        if (index.source_params_json.value() != "{}" && !index.source_params_json.value().empty()) {
            search_index.source_params_json = index.source_params_json.value();
        }
    }
    if (index.plan_params_json.has_value()) {
        if (index.plan_params_json.value() != "{}" && !index.plan_params_json->empty()) {
            search_index.plan_params_json = index.plan_params_json.value();
        }
    }
    return search_index;
}

std::vector<std::string>
convert_analysis(const std::string& analysis)
{
    std::vector<std::string> result;

    if (analysis.empty()) {
        return result;
    }

    auto parsed = core::utils::json::parse(analysis);
    auto analyses = parsed.get_array();
    for (auto const& object : analyses) {
        result.emplace_back(core::utils::json::generate(object));
    }
    return result;
}
} // namespace

class search_index_manager_impl
{
  public:
    explicit search_index_manager_impl(core::cluster core)
      : core_{ std::move(core) }
    {
    }

    void get_index(std::string index_name,
                   const couchbase::get_search_index_options::built& options,
                   get_search_index_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_get_request{ std::move(index_name), {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), map_search_index(resp.index)); });
    }

    void get_all_indexes(const get_all_search_indexes_options::built& options, get_all_search_indexes_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_get_all_request{ {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), map_all_search_indexes(resp.indexes)); });
    }

    void upsert_index(const management::search::index& search_index,
                      const upsert_search_index_options::built& options,
                      upsert_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_upsert_request{ map_search_index(search_index), {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void drop_index(std::string index_name, const drop_search_index_options::built& options, drop_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_drop_request{ std::move(index_name), {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void get_indexed_documents_count(std::string index_name,
                                     const get_indexed_search_index_options::built& options,
                                     get_indexed_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_get_documents_count_request{ std::move(index_name), {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), resp.count); });
    }

    void pause_ingest(std::string index_name,
                      const pause_ingest_search_index_options::built& options,
                      pause_ingest_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_control_ingest_request{ std::move(index_name), true, {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void resume_ingest(std::string index_name,
                       const resume_ingest_search_index_options::built& options,
                       resume_ingest_search_index_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_control_ingest_request{ std::move(index_name), false, {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void allow_querying(std::string index_name,
                        const allow_querying_search_index_options::built& options,
                        allow_querying_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_control_query_request{ std::move(index_name), true, {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void disallow_querying(std::string index_name,
                           const disallow_querying_search_index_options::built& options,
                           disallow_querying_search_index_handler&& handler) const
    {
        core_.execute(core::operations::management::search_index_control_query_request{ std::move(index_name), false, {}, options.timeout },
                      [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void freeze_plan(std::string index_name,
                     const freeze_plan_search_index_options::built& options,
                     freeze_plan_search_index_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_control_plan_freeze_request{ std::move(index_name), true, {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void unfreeze_plan(std::string index_name,
                       const unfreeze_plan_search_index_options::built& options,
                       unfreeze_plan_search_index_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_control_plan_freeze_request{ std::move(index_name), false, {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void analyze_document(std::string index_name,
                          std::string document,
                          const analyze_document_options::built& options,
                          analyze_document_handler&& handler) const
    {
        core_.execute(
          core::operations::management::search_index_analyze_document_request{
            std::move(index_name), std::move(document), {}, options.timeout },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), convert_analysis(resp.analysis)); });
    }

  private:
    core::cluster core_;
};

search_index_manager::search_index_manager(core::cluster core)
  : impl_(std::make_shared<search_index_manager_impl>(std::move(core)))
{
}

void
search_index_manager::get_index(std::string index_name,
                                const couchbase::get_search_index_options& options,
                                get_search_index_handler&& handler) const
{
    return impl_->get_index(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::get_index(std::string index_name, const couchbase::get_search_index_options& options) const
  -> std::future<std::pair<manager_error_context, management::search::index>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, management::search::index>>>();
    get_index(std::move(index_name), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

void
search_index_manager::get_all_indexes(const couchbase::get_all_search_indexes_options& options,
                                      get_all_search_indexes_handler&& handler) const
{
    return impl_->get_all_indexes(options.build(), std::move(handler));
}

auto
search_index_manager::get_all_indexes(const couchbase::get_all_search_indexes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::search::index>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::search::index>>>>();
    get_all_indexes(options,
                    [barrier](auto ctx, auto result) mutable { barrier->set_value(std::make_pair(std::move(ctx), std::move(result))); });
    return barrier->get_future();
}

void
search_index_manager::upsert_index(const management::search::index& search_index,
                                   const couchbase::upsert_search_index_options& options,
                                   couchbase::upsert_search_index_handler&& handler) const
{
    return impl_->upsert_index(search_index, options.build(), std::move(handler));
}

auto
search_index_manager::upsert_index(const management::search::index& search_index,
                                   const couchbase::upsert_search_index_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    upsert_index(search_index, options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::drop_index(std::string index_name,
                                 const couchbase::drop_search_index_options& options,
                                 couchbase::drop_search_index_handler&& handler) const
{
    return impl_->drop_index(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::drop_index(std::string index_name, const couchbase::drop_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    drop_index(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::get_indexed_documents_count(std::string index_name,
                                                  const couchbase::get_indexed_search_index_options& options,
                                                  couchbase::get_indexed_search_index_handler&& handler) const
{
    return impl_->get_indexed_documents_count(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::get_indexed_documents_count(std::string index_name, const get_indexed_search_index_options& options) const
  -> std::future<std::pair<manager_error_context, std::uint64_t>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::uint64_t>>>();
    get_indexed_documents_count(std::move(index_name), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

void
search_index_manager::pause_ingest(std::string index_name,
                                   const couchbase::pause_ingest_search_index_options& options,
                                   couchbase::pause_ingest_search_index_handler&& handler) const
{
    return impl_->pause_ingest(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::pause_ingest(std::string index_name, const couchbase::pause_ingest_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    pause_ingest(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::resume_ingest(std::string index_name,
                                    const couchbase::resume_ingest_search_index_options& options,
                                    couchbase::resume_ingest_search_index_handler&& handler) const
{
    return impl_->resume_ingest(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::resume_ingest(std::string index_name, const couchbase::resume_ingest_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    resume_ingest(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::allow_querying(std::string index_name,
                                     const couchbase::allow_querying_search_index_options& options,
                                     couchbase::allow_querying_search_index_handler&& handler) const
{
    return impl_->allow_querying(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::allow_querying(std::string index_name, const couchbase::allow_querying_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    allow_querying(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::disallow_querying(std::string index_name,
                                        const couchbase::disallow_querying_search_index_options& options,
                                        couchbase::disallow_querying_search_index_handler&& handler) const
{
    return impl_->disallow_querying(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::disallow_querying(std::string index_name, const couchbase::disallow_querying_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    disallow_querying(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::freeze_plan(std::string index_name,
                                  const couchbase::freeze_plan_search_index_options& options,
                                  couchbase::freeze_plan_search_index_handler&& handler) const
{
    return impl_->freeze_plan(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::freeze_plan(std::string index_name, const couchbase::freeze_plan_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    freeze_plan(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::unfreeze_plan(std::string index_name,
                                    const couchbase::unfreeze_plan_search_index_options& options,
                                    couchbase::unfreeze_plan_search_index_handler&& handler) const
{
    return impl_->unfreeze_plan(std::move(index_name), options.build(), std::move(handler));
}

auto
search_index_manager::unfreeze_plan(std::string index_name, const couchbase::unfreeze_plan_search_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    unfreeze_plan(std::move(index_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
search_index_manager::analyze_document(std::string index_name,
                                       std::string document,
                                       const couchbase::analyze_document_options& options,
                                       couchbase::analyze_document_handler&& handler) const
{
    return impl_->analyze_document(std::move(index_name), std::move(document), options.build(), std::move(handler));
}

auto
search_index_manager::analyze_document(std::string index_name,
                                       std::string document,
                                       const couchbase::analyze_document_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<std::string>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<std::string>>>>();
    analyze_document(std::move(index_name), std::move(document), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}
} // namespace couchbase
