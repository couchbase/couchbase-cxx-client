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
#include "core/impl/get_replica.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/utils/movable_function.hxx"
#include "couchbase/error_codes.hxx"

#include <functional>
#include <memory>
#include <mutex>

namespace couchbase::core::operations
{
struct get_any_replica_response {
    key_value_error_context ctx{};
    std::vector<std::byte> value{};
    couchbase::cas cas{};
    std::uint32_t flags{};
    bool replica{ true };
};

struct get_any_replica_request {
    using response_type = get_any_replica_response;
    using encoded_request_type = core::protocol::client_request<core::protocol::get_replica_request_body>;
    using encoded_response_type = core::protocol::client_response<core::protocol::get_replica_response_body>;

    core::document_id id;
    std::optional<std::chrono::milliseconds> timeout{};

    template<typename Core, typename Handler>
    void execute(Core core, Handler handler)
    {
        core->with_bucket_configuration(
          id.bucket(),
          [core, id = id, timeout = timeout, h = std::forward<Handler>(handler)](std::error_code ec,
                                                                                 const topology::configuration& config) mutable {
              if (ec) {
                  return h(response_type{ make_key_value_error_context(ec, id) });
              }
              using handler_type = utils::movable_function<void(response_type)>;

              struct replica_context {
                  replica_context(handler_type&& handler, std::uint32_t expected_responses)
                    : handler_(std::move(handler))
                    , expected_responses_(expected_responses)
                  {
                  }

                  handler_type handler_;
                  std::uint32_t expected_responses_;
                  bool done_{ false };
                  std::mutex mutex_{};
              };
              auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

              for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                  document_id replica_id{ id };
                  replica_id.node_index(idx);
                  core->execute(impl::get_replica_request{ std::move(replica_id), timeout }, [ctx](impl::get_replica_response&& resp) {
                      handler_type local_handler;
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
                              // consider document irretrievable and give up
                              resp.ctx.override_ec(errc::key_value::document_irretrievable);
                          }
                          ctx->done_ = true;
                          std::swap(local_handler, ctx->handler_);
                      }
                      if (local_handler) {
                          return local_handler(response_type{ std::move(resp.ctx), std::move(resp.value), resp.cas, resp.flags, true });
                      }
                  });
              }

              core->execute(get_request{ id, {}, {}, timeout }, [ctx](get_response&& resp) {
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
                          // consider document irretrievable and give up
                          resp.ctx.override_ec(errc::key_value::document_irretrievable);
                      }
                      ctx->done_ = true;
                      std::swap(local_handler, ctx->handler_);
                  }
                  if (local_handler) {
                      return local_handler(response_type{ std::move(resp.ctx), std::move(resp.value), resp.cas, resp.flags, false });
                  }
              });
          });
    }
};

template<>
struct is_compound_operation<get_any_replica_request> : public std::true_type {
};
} // namespace couchbase::core::operations
