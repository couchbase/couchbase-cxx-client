/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#pragma once

#include "core/error_context/key_value.hxx"
#include "core/impl/lookup_in_replica.hxx"
#include "core/impl/subdoc/command.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/utils/movable_function.hxx"
#include "couchbase/codec/encoded_value.hxx"
#include "couchbase/error_codes.hxx"

#include <functional>
#include <memory>
#include <mutex>

namespace couchbase::core::operations
{
struct lookup_in_all_replicas_response {
    struct entry {
        struct lookup_in_entry {
            std::string path;
            codec::binary value;
            std::size_t original_index;
            bool exists;
            protocol::subdoc_opcode opcode;
            key_value_status_code status;
            std::error_code ec{};
        };
        std::vector<lookup_in_entry> fields{};
        couchbase::cas cas{};
        bool deleted{ false };
        bool is_replica{ true };
    };
    subdocument_error_context ctx{};
    std::vector<entry> entries{};
};

struct lookup_in_all_replicas_request {
    using response_type = lookup_in_all_replicas_response;
    using encoded_request_type = core::protocol::client_request<core::protocol::lookup_in_replica_request_body>;
    using encoded_response_type = core::protocol::client_response<core::protocol::lookup_in_replica_response_body>;

    core::document_id id;
    std::vector<couchbase::core::impl::subdoc::command> specs{};
    std::optional<std::chrono::milliseconds> timeout{};
    std::shared_ptr<couchbase::tracing::request_span> parent_span{ nullptr };

    template<typename Core, typename Handler>
    void execute(Core core, Handler handler)
    {
        core->with_bucket_configuration(
          id.bucket(),
          [core, id = id, timeout = timeout, specs = specs, parent_span = parent_span, h = std::forward<Handler>(handler)](
            std::error_code ec, const topology::configuration& config) mutable {
              if (!config.supports_subdoc_read_replica()) {
                  ec = errc::common::feature_not_available;
              }

              if (ec) {
                  std::optional<std::string> first_error_path{};
                  std::optional<std::size_t> first_error_index{};
                  return h(response_type{
                    make_subdocument_error_context(make_key_value_error_context(ec, id), ec, first_error_path, first_error_index, false) });
              }
              using handler_type = utils::movable_function<void(response_type)>;

              struct replica_context {
                  replica_context(handler_type handler, std::uint32_t expected_responses)
                    : handler_(std::move(handler))
                    , expected_responses_(expected_responses)
                  {
                  }

                  handler_type handler_;
                  std::uint32_t expected_responses_;
                  bool done_{ false };
                  std::mutex mutex_{};
                  std::vector<lookup_in_all_replicas_response::entry> result_{};
              };
              auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

              for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                  document_id replica_id{ id };
                  replica_id.node_index(idx);
                  core->execute(impl::lookup_in_replica_request{ std::move(replica_id), specs, timeout, parent_span },
                                [ctx](impl::lookup_in_replica_response&& resp) {
                                    handler_type local_handler{};
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
                                            lookup_in_all_replicas_response::entry top_entry{};
                                            top_entry.cas = resp.cas;
                                            top_entry.deleted = resp.deleted;
                                            top_entry.is_replica = true;
                                            for (auto& field : resp.fields) {
                                                lookup_in_all_replicas_response::entry::lookup_in_entry lookup_in_entry{};
                                                lookup_in_entry.path = field.path;
                                                lookup_in_entry.value = field.value;
                                                lookup_in_entry.status = field.status;
                                                lookup_in_entry.ec = field.ec;
                                                lookup_in_entry.exists = field.exists;
                                                lookup_in_entry.original_index = field.original_index;
                                                lookup_in_entry.opcode = field.opcode;
                                                top_entry.fields.emplace_back(lookup_in_entry);
                                            }
                                            ctx->result_.emplace_back(lookup_in_all_replicas_response::entry{ top_entry });
                                        }
                                        if (ctx->expected_responses_ == 0) {
                                            ctx->done_ = true;
                                            std::swap(local_handler, ctx->handler_);
                                        }
                                    }
                                    if (local_handler) {
                                        return local_handler({ std::move(resp.ctx), std::move(ctx->result_) });
                                    }
                                });
              }

              core->execute(lookup_in_request{ document_id{ id }, {}, {}, false, specs, timeout }, [ctx](lookup_in_response&& resp) {
                  handler_type local_handler{};
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
                          lookup_in_all_replicas_response::entry top_entry{};
                          top_entry.cas = resp.cas;
                          top_entry.deleted = resp.deleted;
                          top_entry.is_replica = false;
                          for (auto& field : resp.fields) {
                              lookup_in_all_replicas_response::entry::lookup_in_entry lookup_in_entry{};
                              lookup_in_entry.path = field.path;
                              lookup_in_entry.value = field.value;
                              lookup_in_entry.status = field.status;
                              lookup_in_entry.ec = field.ec;
                              lookup_in_entry.exists = field.exists;
                              lookup_in_entry.original_index = field.original_index;
                              lookup_in_entry.opcode = field.opcode;
                              top_entry.fields.emplace_back(lookup_in_entry);
                          }
                          ctx->result_.emplace_back(lookup_in_all_replicas_response::entry{ top_entry });
                      }
                      if (ctx->expected_responses_ == 0) {
                          ctx->done_ = true;
                          std::swap(local_handler, ctx->handler_);
                      }
                  }
                  if (local_handler) {
                      return local_handler({ std::move(resp.ctx), std::move(ctx->result_) });
                  }
              });
          });
    }
};

template<>
struct is_compound_operation<lookup_in_all_replicas_request> : public std::true_type {
};
} // namespace couchbase::core::operations
