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
#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_build_deferred.hxx"

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

static core::operations::management::query_index_build_request
build_build_index_request(std::string bucket_name,
                          core::operations::management::query_index_get_all_deferred_response list_resp,
                          const build_query_index_options::built& options)
{
    core::operations::management::query_index_build_request request{
        std::move(bucket_name), {}, {}, {}, std::move(list_resp.index_names), {}, options.timeout
    };
    return request;
}

static core::operations::management::query_index_get_all_deferred_request
build_get_all_request(std::string bucket_name, const build_query_index_options::built& options)
{
    core::operations::management::query_index_get_all_deferred_request request{ std::move(bucket_name), {}, {}, {}, {}, options.timeout };
    return request;
}

static core::operations::management::query_index_build_request
build_build_index_request(std::string bucket_name,
                          std::string scope_name,
                          std::string collection_name,
                          core::operations::management::query_index_get_all_deferred_response list_resp,
                          const build_query_index_options::built& options)
{
    core::operations::management::query_index_build_request request{ "",
                                                                     "",
                                                                     std::move(collection_name),
                                                                     core::query_context{ std::move(bucket_name), std::move(scope_name) },
                                                                     std::move(list_resp.index_names),
                                                                     {},
                                                                     options.timeout };
    return request;
}

static core::operations::management::query_index_get_all_deferred_request
build_get_all_request(std::string bucket_name,
                      std::string scope_name,
                      std::string collection_name,
                      const build_query_index_options::built& options)
{
    core::operations::management::query_index_get_all_deferred_request request{
        "", "", std::move(collection_name), core::query_context{ std::move(bucket_name), std::move(scope_name) }, {}, options.timeout
    };
    return request;
}

void
query_index_manager::build_deferred_indexes(std::string bucket_name,
                                            const couchbase::build_query_index_options& options,
                                            couchbase::build_deferred_query_indexes_handler&& handler) const
{

    auto get_all_request = build_get_all_request(bucket_name, options.build());
    core_->execute(std::move(get_all_request),
                   [handler = std::move(handler), this, bucket_name, options](
                     core::operations::management::query_index_get_all_deferred_response resp1) mutable {
                       auto list_resp = std::move(resp1);
                       if (list_resp.ctx.ec) {
                           return handler(build_context(list_resp));
                       }
                       if (list_resp.index_names.empty()) {
                           return handler(build_context(list_resp));
                       }
                       auto build_request = build_build_index_request(std::move(bucket_name), list_resp, options.build());
                       core_->execute(
                         std::move(build_request),
                         [handler = std::move(handler)](core::operations::management::query_index_build_response resp2) mutable {
                             auto build_resp = std::move(resp2);
                             return handler(build_context(build_resp));
                         });
                   });
}

auto
query_index_manager::build_deferred_indexes(std::string bucket_name, const couchbase::build_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    build_deferred_indexes(std::move(bucket_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_query_index_manager::build_deferred_indexes(const build_query_index_options& options,
                                                       build_deferred_query_indexes_handler&& handler) const
{
    auto get_all_request = build_get_all_request(bucket_name_, scope_name_, collection_name_, options.build());
    core_->execute(
      std::move(get_all_request),
      [handler = std::move(handler), this, options](core::operations::management::query_index_get_all_deferred_response resp1) mutable {
          auto list_resp = std::move(resp1);
          if (list_resp.ctx.ec) {
              return handler(build_context(list_resp));
          }
          if (list_resp.index_names.empty()) {
              return handler(build_context(list_resp));
          }
          auto build_request = build_build_index_request(bucket_name_, scope_name_, collection_name_, list_resp, options.build());
          core_->execute(std::move(build_request),
                         [handler = std::move(handler)](core::operations::management::query_index_build_response resp2) mutable {
                             auto build_resp = std::move(resp2);
                             return handler(build_context(build_resp));
                         });
      });
}

auto
collection_query_index_manager::build_deferred_indexes(const couchbase::build_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    build_deferred_indexes(options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
