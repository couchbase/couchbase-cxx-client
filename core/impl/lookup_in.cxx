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

#include "core/cluster.hxx"
#include "subdoc/command_bundle.hxx"

#include <couchbase/lookup_in_options.hxx>

namespace couchbase::core::impl
{
void
initiate_lookup_in_operation(std::shared_ptr<couchbase::core::cluster> core,
                             std::string bucket_name,
                             std::string scope_name,
                             std::string collection_name,
                             std::string document_key,
                             const std::vector<couchbase::core::impl::subdoc::command>& specs,
                             lookup_in_options::built options,
                             lookup_in_handler&& handler)
{
    return core->execute(
      operations::lookup_in_request{
        document_id{
          std::move(bucket_name),
          std::move(scope_name),
          std::move(collection_name),
          std::move(document_key),
        },
        {},
        {},
        options.access_deleted,
        specs,
        options.timeout,
        { options.retry_strategy },
      },
      [handler = std::move(handler)](operations::lookup_in_response&& resp) mutable {
          if (resp.ctx.ec()) {
              return handler(std::move(resp.ctx), lookup_in_result{});
          }

          std::vector<lookup_in_result::entry> entries{};
          entries.reserve(resp.fields.size());
          for (auto& entry : resp.fields) {
              entries.emplace_back(lookup_in_result::entry{
                std::move(entry.path),
                std::move(entry.value),
                entry.original_index,
                entry.exists,
              });
          }
          return handler(std::move(resp.ctx), lookup_in_result{ resp.cas, std::move(entries), resp.deleted });
      });
}
} // namespace couchbase::core::impl
