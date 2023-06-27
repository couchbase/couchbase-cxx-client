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

#include <couchbase/manager_error_context.hxx>
#include <couchbase/query_index_manager.hxx>
#include <utility>

#include "core/cluster.hxx"
#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_get_all.hxx"

namespace couchbase
{
template<typename Response>
static manager_error_context
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

static core::operations::management::query_index_get_all_request
build_get_all_indexes_request(std::string bucket_name, const get_all_query_indexes_options::built& options)
{
    core::operations::management::query_index_get_all_request request{ std::move(bucket_name), "", "", {}, {}, options.timeout };
    return request;
}

static core::operations::management::query_index_get_all_request
build_get_all_indexes_request(std::string bucket_name,
                              std::string scope_name,
                              std::string collection_name,
                              const get_all_query_indexes_options::built& options)
{
    core::operations::management::query_index_get_all_request request{
        "", "", std::move(collection_name), core::query_context(std::move(bucket_name), std::move(scope_name)), {}, options.timeout
    };
    return request;
}

void
query_index_manager::get_all_indexes(std::string bucket_name,
                                     const couchbase::get_all_query_indexes_options& options,
                                     couchbase::get_all_query_indexes_handler&& handler) const
{
    auto request = build_get_all_indexes_request(std::move(bucket_name), options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_get_all_response resp) mutable {
                       return handler(build_context(resp), resp.indexes);
                   });
}

auto
query_index_manager::get_all_indexes(std::string bucket_name, const couchbase::get_all_query_indexes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::query::index>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::query::index>>>>();
    get_all_indexes(std::move(bucket_name), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

void
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options, get_all_query_indexes_handler&& handler) const
{
    auto request = build_get_all_indexes_request(bucket_name_, scope_name_, collection_name_, options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_get_all_response resp) mutable {
                       return handler(build_context(resp), resp.indexes);
                   });
}

auto
collection_query_index_manager::get_all_indexes(const couchbase::get_all_query_indexes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<couchbase::management::query::index>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::query::index>>>>();
    get_all_indexes(options,
                    [barrier](auto ctx, auto result) mutable { barrier->set_value(std::make_pair(std::move(ctx), std::move(result))); });
    return barrier->get_future();
}
} // namespace couchbase
