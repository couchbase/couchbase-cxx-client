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

#include "lookup_in_all_replicas.hxx"
#include "lookup_in_replica.hxx"

#include "core/cluster.hxx"
#include "core/error_context/key_value.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/topology/configuration.hxx"

namespace couchbase::core::impl
{

void
initiate_lookup_in_all_replicas_operation(std::shared_ptr<cluster> core,
                                          const std::string& bucket_name,
                                          const std::string& scope_name,
                                          const std::string& collection_name,
                                          std::string document_key,
                                          const std::vector<subdoc::command>& specs,
                                          lookup_in_all_replicas_options::built options,
                                          lookup_in_all_replicas_handler&& handler)
{
    return initiate_lookup_in_all_replicas_operation(std::move(core),
                                                     bucket_name,
                                                     scope_name,
                                                     collection_name,
                                                     std::move(document_key),
                                                     specs,
                                                     options.timeout,
                                                     movable_lookup_in_all_replicas_handler{ std::move(handler) });
}

void
initiate_lookup_in_all_replicas_operation(std::shared_ptr<cluster> core,
                                          const std::string& bucket_name,
                                          const std::string& scope_name,
                                          const std::string& collection_name,
                                          std::string document_key,
                                          const std::vector<subdoc::command>& specs,
                                          std::optional<std::chrono::milliseconds> timeout,
                                          movable_lookup_in_all_replicas_handler&& handler)
{
    auto request = std::make_shared<couchbase::core::impl::lookup_in_all_replicas_request>(
      bucket_name, scope_name, collection_name, std::move(document_key), specs, timeout);
    core->with_bucket_configuration(
      bucket_name,
      [core, r = std::move(request), h = std::move(handler)](std::error_code ec, const core::topology::configuration& config) mutable {
          if (!config.supports_subdoc_read_replica()) {
              ec = errc::common::feature_not_available;
          }

          if (ec) {
              std::optional<std::string> first_error_path{};
              std::optional<std::size_t> first_error_index{};
              return h(
                make_subdocument_error_context(make_key_value_error_context(ec, r->id()), ec, first_error_path, first_error_index, false),
                lookup_in_all_replicas_result{});
          }
          struct replica_context {
              replica_context(movable_lookup_in_all_replicas_handler handler, std::uint32_t expected_responses)
                : handler_(std::move(handler))
                , expected_responses_(expected_responses)
              {
              }

              movable_lookup_in_all_replicas_handler handler_;
              std::uint32_t expected_responses_;
              bool done_{ false };
              std::mutex mutex_{};
              lookup_in_all_replicas_result result_{};
          };
          auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

          for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
              document_id replica_id{ r->id() };
              replica_id.node_index(idx);
              core->execute(
                impl::lookup_in_replica_request{ std::move(replica_id), r->specs(), r->timeout() },
                [ctx](impl::lookup_in_replica_response&& resp) {
                    movable_lookup_in_all_replicas_handler local_handler{};
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
                            std::vector<lookup_in_replica_result::entry> entries{};
                            for (auto& field : resp.fields) {
                                lookup_in_replica_result::entry lookup_in_entry{};
                                lookup_in_entry.path = field.path;
                                lookup_in_entry.value = field.value;
                                lookup_in_entry.exists = field.exists;
                                lookup_in_entry.original_index = field.original_index;
                                lookup_in_entry.ec = field.ec;
                                entries.emplace_back(lookup_in_entry);
                            }
                            ctx->result_.emplace_back(lookup_in_replica_result{ resp.cas, entries, resp.deleted, true /* replica */ });
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

          core::operations::lookup_in_request active{ document_id{ r->id() } };
          active.specs = r->specs();
          active.timeout = r->timeout();
          core->execute(active, [ctx](core::operations::lookup_in_response&& resp) {
              movable_lookup_in_all_replicas_handler local_handler{};
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
                      std::vector<lookup_in_replica_result::entry> entries{};
                      for (auto& field : resp.fields) {
                          lookup_in_replica_result::entry lookup_in_entry{};
                          lookup_in_entry.path = field.path;
                          lookup_in_entry.value = field.value;
                          lookup_in_entry.exists = field.exists;
                          lookup_in_entry.original_index = field.original_index;
                          lookup_in_entry.ec = field.ec;
                          entries.emplace_back(lookup_in_entry);
                      }
                      ctx->result_.emplace_back(lookup_in_replica_result{ resp.cas, entries, resp.deleted, false /* active */ });
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
