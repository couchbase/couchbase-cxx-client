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
#include <utility>

#include "core/cluster.hxx"
#include "core/operations/management/scope_create.hxx"
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

static core::operations::management::scope_create_request
build_scope_create_request(std::string bucket_name, std::string scope_name, const create_scope_options::built& options)
{
    core::operations::management::scope_create_request request{ std::move(bucket_name), std::move(scope_name), {}, options.timeout };
    return request;
}

void
collection_manager::create_scope(std::string scope_name,
                                 const couchbase::create_scope_options& options,
                                 couchbase::create_scope_handler&& handler) const
{
    auto request = build_scope_create_request(bucket_name_, std::move(scope_name), options.build());

    core_->execute(std::move(request), [handler = std::move(handler)](core::operations::management::scope_create_response resp) mutable {
        return handler(build_context(resp));
    });
}

auto
collection_manager::create_scope(std::string scope_name, const couchbase::create_scope_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_scope(std::move(scope_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
