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

#include "core/cluster.hxx"
#include "core/operations/management/scope_get_all.hxx"
#include "couchbase/collection_manager.hxx"

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

static core::operations::management::scope_get_all_request
build_get_all_scopes_request(std::string bucket_name, const get_all_scopes_options::built& options)
{
    core::operations::management::scope_get_all_request request{ std::move(bucket_name), {}, options.timeout };
    return request;
}

static management::bucket::collection_spec
map_collection(std::string scope_name, const core::topology::collections_manifest::collection& collection)
{
    management::bucket::collection_spec spec{};
    spec.name = collection.name;
    spec.scope_name = std::move(scope_name);
    spec.max_expiry = collection.max_expiry;
    spec.history = collection.history;
    return spec;
}

static std::vector<couchbase::management::bucket::scope_spec>
map_scope_specs(core::topology::collections_manifest& manifest)
{
    std::vector<couchbase::management::bucket::scope_spec> scope_specs{};
    for (const auto& scope : manifest.scopes) {
        couchbase::management::bucket::scope_spec converted_scope{};
        converted_scope.name = scope.name;
        for (const auto& collection : scope.collections) {
            auto collection_spec = map_collection(scope.name, collection);
            converted_scope.collections.emplace_back(collection_spec);
        }
        scope_specs.emplace_back(converted_scope);
    }
    return scope_specs;
}

void
collection_manager::get_all_scopes(const get_all_scopes_options& options, get_all_scopes_handler&& handler) const
{
    auto request = build_get_all_scopes_request(bucket_name_, options.build());

    core_->execute(std::move(request), [handler = std::move(handler)](core::operations::management::scope_get_all_response resp) mutable {
        return handler(build_context(resp), map_scope_specs(resp.manifest));
    });
}

auto
collection_manager::get_all_scopes(const couchbase::get_all_scopes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::bucket::scope_spec>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::bucket::scope_spec>>>>();
    get_all_scopes(options,
                   [barrier](auto ctx, auto result) mutable { barrier->set_value(std::make_pair(std::move(ctx), std::move(result))); });
    return barrier->get_future();
}
} // namespace couchbase
