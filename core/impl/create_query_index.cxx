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

#include <couchbase/error_codes.hxx>
#include <couchbase/query_index_manager.hxx>

#include "core/cluster.hxx"
#include "core/operations/management/query_index_create.hxx"

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

static core::operations::management::query_index_create_request
build_create_index_request(std::string bucket_name,
                           std::string index_name,
                           std::vector<std::string> fields,
                           create_query_index_options::built options)
{
    core::operations::management::query_index_create_request request{
        std::move(bucket_name),
        "",
        "",
        std::move(index_name),
        std::move(fields),
        {},
        false,
        options.ignore_if_exists,
        options.condition,
        options.deferred,
        options.num_replicas,
        {},
        options.timeout,
    };
    return request;
}

static core::operations::management::query_index_create_request
build_create_primary_index_request(std::string bucket_name, create_primary_query_index_options::built options)
{
    core::operations::management::query_index_create_request request{
        std::move(bucket_name),
        "",
        "",
        options.index_name.value_or(""),
        {},
        {},
        true,
        options.ignore_if_exists,
        {},
        options.deferred,
        options.num_replicas,
        {},
        options.timeout,
    };
    return request;
}

static core::operations::management::query_index_create_request
build_create_index_request(std::string bucket_name,
                           std::string scope_name,
                           std::string collection_name,
                           std::string index_name,
                           std::vector<std::string> fields,
                           create_query_index_options::built options)
{
    core::operations::management::query_index_create_request request{
        "",
        "",
        std::move(collection_name),
        std::move(index_name),
        std::move(fields),
        core::query_context(std::move(bucket_name), std::move(scope_name)),
        false,
        options.ignore_if_exists,
        options.condition,
        options.deferred,
        options.num_replicas,
        {},
        options.timeout,
    };
    return request;
}

static core::operations::management::query_index_create_request
build_create_primary_index_request(std::string bucket_name,
                                   std::string scope_name,
                                   std::string collection_name,
                                   create_primary_query_index_options::built options)
{
    core::operations::management::query_index_create_request request{
        "",
        "",
        std::move(collection_name),
        options.index_name.value_or(""),
        {},
        core::query_context(std::move(bucket_name), std::move(scope_name)),
        true,
        options.ignore_if_exists,
        {},
        options.deferred,
        options.num_replicas,
        {},
        options.timeout,
    };
    return request;
}

void
query_index_manager::create_index(std::string bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> fields,
                                  const couchbase::create_query_index_options& options,
                                  couchbase::create_primary_query_index_handler&& handler) const
{
    auto request = build_create_index_request(std::move(bucket_name), std::move(index_name), std::move(fields), options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_create_response resp) mutable {
                       return handler(build_context(resp));
                   });
}

auto
query_index_manager::create_index(std::string bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> fields,
                                  const couchbase::create_query_index_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_index(std::move(bucket_name), std::move(index_name), std::move(fields), options, [barrier](auto ctx) mutable {
        barrier->set_value(std::move(ctx));
    });
    return barrier->get_future();
}

void
query_index_manager::create_primary_index(std::string bucket_name,
                                          const create_primary_query_index_options& options,
                                          create_query_index_handler&& handler)
{
    auto request = build_create_primary_index_request(std::move(bucket_name), options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_create_response resp) mutable {
                       return handler(build_context(resp));
                   });
}

auto
query_index_manager::create_primary_index(std::string bucket_name, const create_primary_query_index_options& options)
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_primary_index(std::move(bucket_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> fields,
                                             const create_query_index_options& options,
                                             create_query_index_handler&& handler) const
{
    auto request =
      build_create_index_request(bucket_name_, scope_name_, collection_name_, std::move(index_name), std::move(fields), options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_create_response resp) mutable {
                       return handler(build_context(resp));
                   });
}

auto
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> fields,
                                             const couchbase::create_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_index(std::move(index_name), std::move(fields), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_query_index_manager::create_primary_index(const create_primary_query_index_options& options,
                                                     create_query_index_handler&& handler) const
{
    auto request = build_create_primary_index_request(bucket_name_, scope_name_, collection_name_, options.build());

    core_->execute(std::move(request),
                   [handler = std::move(handler)](core::operations::management::query_index_create_response resp) mutable {
                       return handler(build_context(resp));
                   });
}

auto
collection_query_index_manager::create_primary_index(const couchbase::create_primary_query_index_options& options)
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_primary_index(options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
