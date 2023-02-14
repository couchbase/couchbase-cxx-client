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

#include "couchbase/error_codes.hxx"
#include "couchbase/query_index_manager.hxx"

#include "core/cluster.hxx"
#include "core/operations/management/query_index_drop.hxx"

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
initiate_drop_query_index(std::shared_ptr<couchbase::core::cluster> core,
                          std::string bucket_name,
                          std::string index_name,
                          drop_query_index_options::built options,
                          query_context query_context,
                          std::string collection_name,
                          drop_query_index_handler&& handler)
{
    core->execute(
      operations::management::query_index_drop_request{
        bucket_name,
        "",
        collection_name,
        index_name,
        query_context,
        false,
        options.ignore_if_not_exists,
        {},
        options.timeout,
      },
      [core, bucket_name, options = std::move(options), handler = std::move(handler)](
        operations::management::query_index_drop_response resp) { handler(build_context(resp)); });
}

void
initiate_drop_query_index(std::shared_ptr<couchbase::core::cluster> core,
                          std::string bucket_name,
                          std::string index_name,
                          couchbase::drop_query_index_options::built options,
                          drop_query_index_handler&& handler)
{
    initiate_drop_query_index(core, std::move(bucket_name), std::move(index_name), options, {}, "", std::move(handler));
}

void
initiate_drop_primary_query_index(std::shared_ptr<couchbase::core::cluster> core,
                                  std::string bucket_name,
                                  couchbase::drop_primary_query_index_options::built options,
                                  query_context query_ctx,
                                  std::string collection_name,
                                  drop_primary_query_index_handler&& handler)
{
    core->execute(
      operations::management::query_index_drop_request{
        bucket_name,
        "",
        collection_name,
        options.index_name.value_or(""),
        query_ctx,
        true,
        options.ignore_if_not_exists,
        {},
        options.timeout,
      },
      [core, bucket_name, options = std::move(options), handler = std::move(handler)](
        operations::management::query_index_drop_response resp) { handler(build_context(resp)); });
}
void
initiate_drop_primary_query_index(std::shared_ptr<couchbase::core::cluster> core,
                                  std::string bucket_name,
                                  couchbase::drop_primary_query_index_options::built options,
                                  drop_primary_query_index_handler&& handler)
{
    return initiate_drop_primary_query_index(core, std::move(bucket_name), options, {}, "", std::move(handler));
}

} // namespace couchbase::core::impl