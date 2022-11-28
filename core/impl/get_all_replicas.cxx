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

#include "get_all_replicas.hxx"
#include "get_replica.hxx"

#include "core/cluster.hxx"
#include "core/error_context/key_value.hxx"
#include "core/operations/document_get.hxx"
#include "core/topology/configuration.hxx"

namespace couchbase::core::impl
{

void
initiate_get_all_replicas_operation(std::shared_ptr<cluster> core,
                                    const std::string& bucket_name,
                                    const std::string& scope_name,
                                    const std::string& collection_name,
                                    std::string document_key,
                                    get_all_replicas_options::built options,
                                    get_all_replicas_handler&& handler)
{
    return initiate_get_all_replicas_operation(std::move(core),
                                               bucket_name,
                                               scope_name,
                                               collection_name,
                                               std::move(document_key),
                                               options.timeout,
                                               movable_get_all_replicas_handler{ std::move(handler) });
}

void
initiate_get_all_replicas_operation(std::shared_ptr<cluster> core,
                                    const std::string& bucket_name,
                                    const std::string& scope_name,
                                    const std::string& collection_name,
                                    std::string document_key,
                                    std::optional<std::chrono::milliseconds> timeout,
                                    movable_get_all_replicas_handler&& handler)
{
    auto request = std::make_shared<couchbase::core::impl::get_all_replicas_request>(
      bucket_name, scope_name, collection_name, std::move(document_key), timeout);
    core->with_bucket_configuration(
      bucket_name,
      [core, r = std::move(request), h = std::move(handler)](std::error_code ec, const core::topology::configuration& config) mutable {
          if (ec) {
              return h(make_key_value_error_context(ec, r->id()), get_all_replicas_result{});
          }
          struct replica_context {
              replica_context(movable_get_all_replicas_handler handler, std::uint32_t expected_responses)
                : handler_(std::move(handler))
                , expected_responses_(expected_responses)
              {
              }

              movable_get_all_replicas_handler handler_;
              std::uint32_t expected_responses_;
              bool done_{ false };
              std::mutex mutex_{};
              get_all_replicas_result result_{};
          };
          auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

          for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
              document_id replica_id{ r->id() };
              replica_id.node_index(idx);
              core->execute(impl::get_replica_request{ std::move(replica_id), r->timeout() }, [ctx](impl::get_replica_response&& resp) {
                  movable_get_all_replicas_handler local_handler{};
                  {
                      std::scoped_lock lock(ctx->mutex_);
                      if (ctx->done_) {
                          return;
                      }
                      --ctx->expected_responses_;
                      if (resp.ctx.ec()) {
                          if (ctx->expected_responses_ > 0) {
                              // just ignore the response
                              return;
                          }
                      } else {
                          ctx->result_.emplace_back(
                            get_replica_result{ resp.cas, true /* replica */, { std::move(resp.value), resp.flags } });
                      }
                      if (ctx->expected_responses_ == 0) {
                          ctx->done_ = true;
                          std::swap(local_handler, ctx->handler_);
                      }
                  }
                  if (local_handler) {
                      if (!ctx->result_.empty()) {
                          resp.ctx.override_ec({});
                      }
                      return local_handler(std::move(resp.ctx), std::move(ctx->result_));
                  }
              });
          }

          core::operations::get_request active{ document_id{ r->id() } };
          active.timeout = r->timeout();
          core->execute(active, [ctx](core::operations::get_response&& resp) {
              movable_get_all_replicas_handler local_handler{};
              {
                  std::scoped_lock lock(ctx->mutex_);
                  if (ctx->done_) {
                      return;
                  }
                  --ctx->expected_responses_;
                  if (resp.ctx.ec()) {
                      if (ctx->expected_responses_ > 0) {
                          // just ignore the response
                          return;
                      }
                  } else {
                      ctx->result_.emplace_back(get_replica_result{ resp.cas, false /* active */, { std::move(resp.value), resp.flags } });
                  }
                  if (ctx->expected_responses_ == 0) {
                      ctx->done_ = true;
                      std::swap(local_handler, ctx->handler_);
                  }
              }
              if (local_handler) {
                  if (!ctx->result_.empty()) {
                      resp.ctx.override_ec({});
                  }
                  return local_handler(std::move(resp.ctx), std::move(ctx->result_));
              }
          });
      });
}
} // namespace couchbase::core::impl
