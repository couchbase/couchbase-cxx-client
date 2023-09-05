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
#include "core/operations/management/collection_update.hxx"
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

static core::operations::management::collection_update_request
build_collection_update_request(std::string bucket_name,
                                std::string scope_name,
                                std::string collection_name,
                                const update_collection_settings& settings,
                                const update_collection_options::built& options)
{
    core::operations::management::collection_update_request request{
        std::move(bucket_name), std::move(scope_name), std::move(collection_name), settings.max_expiry, settings.history, {},
        options.timeout
    };
    return request;
}

void
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::update_collection_settings& settings,
                                      const couchbase::update_collection_options& options,
                                      couchbase::update_collection_handler&& handler) const
{
    auto request =
      build_collection_update_request(bucket_name_, std::move(scope_name), std::move(collection_name), settings, options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::collection_update_response resp) mutable {
                       return handler(build_context(resp));
                   });
}

auto
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::update_collection_settings& settings,
                                      const couchbase::update_collection_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    update_collection(std::move(scope_name), std::move(collection_name), settings, options, [barrier](auto ctx) mutable {
        barrier->set_value(std::move(ctx));
    });
    return barrier->get_future();
}
} // namespace couchbase
