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
#include "core/operations/management/query_index_get_all_deferred.hxx"
#include "core/utils/json.hxx"

namespace couchbase::core::impl
{
template<typename Response>
static manager_error_context
build_context(Response& resp)
{
    return { resp.ctx.ec,
             resp.ctx.last_dispatched_to,
             resp.ctx.last_dispatched_from,
             resp.ctx.retry_attempts,
             std::move(resp.ctx.retry_reasons),
             std::move(resp.ctx.client_context_id),
             resp.ctx.http_status,
             std::move(resp.ctx.http_body),
             std::move(resp.ctx.path) };
}
void
initiate_build_deferred_indexes(std::shared_ptr<couchbase::core::cluster> core,
                                std::string bucket_name,
                                build_query_index_options::built options,
                                query_context query_ctx,
                                std::string collection_name,
                                build_deferred_query_indexes_handler&& handler)
{
    core->execute(
      operations::management::query_index_get_all_deferred_request{
        bucket_name,
        "",
        collection_name,
        query_ctx,
        {},
        options.timeout,
      },
      [core, bucket_name, collection_name, options = std::move(options), query_ctx, handler = std::move(handler)](
        operations::management::query_index_get_all_deferred_response resp1) mutable {
          auto list_resp = std::move(resp1);
          if (list_resp.ctx.ec) {
              return handler(build_context(list_resp));
          }
          if (list_resp.index_names.empty()) {
              return handler(build_context(list_resp));
          }
          core->execute(
            operations::management::query_index_build_request{
              std::move(bucket_name),
              "",
              collection_name,
              query_ctx,
              std::move(list_resp.index_names),
              {},
              options.timeout,
            },
            [handler = std::move(handler)](operations::management::query_index_build_response resp2) {
                auto build_resp = std::move(resp2);
                return handler(build_context(build_resp));
            });
      });
}
void
initiate_build_deferred_indexes(std::shared_ptr<couchbase::core::cluster> core,
                                std::string bucket_name,
                                build_query_index_options::built options,
                                build_deferred_query_indexes_handler&& handler)
{
    return initiate_build_deferred_indexes(core, std::move(bucket_name), options, {}, "", std::move(handler));
}
} // namespace couchbase::core::impl
