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

#include "core/cluster.hxx"
#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_get_all.hxx"

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
initiate_get_all_query_indexes(std::shared_ptr<couchbase::core::cluster> core,
                               std::string bucket_name,
                               couchbase::get_all_query_indexes_options::built options,
                               query_context query_ctx,
                               std::string collection_name,
                               get_all_query_indexes_handler&& handler)
{
    core->execute(
      operations::management::query_index_get_all_request{
        bucket_name,
        "",
        collection_name,
        query_ctx,
        {},
        options.timeout,
      },
      [handler = std::move(handler)](operations::management::query_index_get_all_response resp) {
          if (resp.ctx.ec) {
              return handler(build_context(resp), {});
          }
          handler(build_context(resp), resp.indexes);
      });
}

void
initiate_get_all_query_indexes(std::shared_ptr<couchbase::core::cluster> core,
                               std::string bucket_name,
                               couchbase::get_all_query_indexes_options::built options,
                               get_all_query_indexes_handler&& handler)
{
    initiate_get_all_query_indexes(core, std::move(bucket_name), options, {}, "", std::move(handler));
}

} // namespace couchbase::core::impl
