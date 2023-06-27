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
#include "core/operations/management/bucket_flush.hxx"
#include "couchbase/bucket_manager.hxx"

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

static core::operations::management::bucket_flush_request
build_flush_bucket_request(std::string bucket_name, const flush_bucket_options::built& options)
{
    core::operations::management::bucket_flush_request request{ std::move(bucket_name), {}, options.timeout };
    return request;
}

void
bucket_manager::flush_bucket(std::string bucket_name, const flush_bucket_options& options, flush_bucket_handler&& handler) const
{
    auto request = build_flush_bucket_request(std::move(bucket_name), options.build());

    core_->execute(std::move(request), [handler = std::move(handler)](core::operations::management::bucket_flush_response resp) mutable {
        return handler(build_context(resp));
    });
}

auto
bucket_manager::flush_bucket(std::string bucket_name, const flush_bucket_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    flush_bucket(std::move(bucket_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
