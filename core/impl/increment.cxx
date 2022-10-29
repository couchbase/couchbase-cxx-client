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
#include "core/impl/observe_poll.hxx"
#include "core/impl/observe_seqno.hxx"
#include "core/operations/document_increment.hxx"

#include <couchbase/increment_options.hxx>

namespace couchbase::core::impl
{
void
initiate_increment_operation(std::shared_ptr<couchbase::core::cluster> core,
                             std::string bucket_name,
                             std::string scope_name,
                             std::string collection_name,
                             std::string document_key,
                             increment_options::built options,
                             increment_handler&& handler)
{
    auto id = document_id{
        std::move(bucket_name),
        std::move(scope_name),
        std::move(collection_name),
        std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
        return core->execute(
          operations::increment_request{
            std::move(id),
            {},
            {},
            options.expiry,
            options.delta,
            options.initial_value,
            options.durability_level,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](operations::increment_response&& resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), counter_result{});
              }
              return handler(std::move(resp.ctx), counter_result{ resp.cas, std::move(resp.token), resp.content });
          });
    }

    operations::increment_request request{
        id,
        {},
        {},
        options.expiry,
        options.delta,
        options.initial_value,
        durability_level::none,
        options.timeout,
        { options.retry_strategy },
    };
    return core->execute(
      std::move(request), [core, id = std::move(id), options, handler = std::move(handler)](operations::increment_response&& resp) mutable {
          if (resp.ctx.ec()) {
              return handler(std::move(resp.ctx), counter_result{ resp.cas, std::move(resp.token), resp.content });
          }

          initiate_observe_poll(core,
                                std::move(id),
                                resp.token,
                                options.timeout,
                                options.persist_to,
                                options.replicate_to,
                                [resp = std::move(resp), handler = std::move(handler)](std::error_code ec) mutable {
                                    if (ec) {
                                        resp.ctx.override_ec(ec);
                                        return handler(std::move(resp.ctx), counter_result{});
                                    }
                                    return handler(std::move(resp.ctx), counter_result{ resp.cas, std::move(resp.token), resp.content });
                                });
      });
}
} // namespace couchbase::core::impl
