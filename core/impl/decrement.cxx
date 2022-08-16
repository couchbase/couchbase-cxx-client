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
#include "core/error_context/key_value.hxx"
#include "core/operations/document_decrement.hxx"

#include <couchbase/decrement_options.hxx>

namespace couchbase::core::impl
{
void
initiate_decrement_operation(std::shared_ptr<couchbase::core::cluster> core,
                             std::string bucket_name,
                             std::string scope_name,
                             std::string collection_name,
                             std::string document_key,
                             decrement_options::built options,
                             decrement_handler&& handler)
{
    core->execute(
      operations::decrement_request{
        document_id{ std::move(bucket_name), std::move(scope_name), std::move(collection_name), std::move(document_key) },
        {},
        {},
        options.expiry,
        options.delta,
        options.initial_value,
        options.durability_level,
        options.timeout },
      [handler = std::move(handler)](operations::decrement_response&& resp) mutable {
          if (resp.ctx.ec()) {
              return handler(std::move(resp.ctx), counter_result{});
          }
          return handler(std::move(resp.ctx), counter_result{ resp.cas, std::move(resp.token), resp.content });
      });
}
} // namespace couchbase::core::impl
