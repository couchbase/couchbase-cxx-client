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
#include "core/impl/replica_utils.hxx"
#include "core/impl/with_cancellation.hxx"
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
  using encoded_request_type =
    core::protocol::client_request<core::protocol::get_replica_request_body>;
  using encoded_response_type =
    core::protocol::client_response<core::protocol::get_replica_response_body>;

  static const inline std::string observability_identifier = "get_any_replica";

  core::document_id id;
  std::optional<std::chrono::milliseconds> timeout{};
  couchbase::read_preference read_preference{ couchbase::read_preference::no_preference };
  std::shared_ptr<couchbase::tracing::request_span> parent_span{ nullptr };

  template<typename Core, typename Handler>
  void execute(Core core, Handler handler)
  {
    core->with_bucket_configuration(
      id.bucket(),
      [core,
       id = id,
       timeout = timeout,
       read_preference = read_preference,
       parent_span = std::move(parent_span),
       h = std::forward<Handler>(handler)](
        std::error_code ec, std::shared_ptr<topology::configuration> config) mutable {
        const auto [e, origin] = core->origin();
        if (e && !ec) {
          ec = e;
        }

        auto nodes =
          impl::effective_nodes(id, config, read_preference, origin.options().server_group);
        if (nodes.empty()) {
          CB_LOG_DEBUG(
            "Unable to retrieve replicas for \"{}\", server_group={}, number_of_replicas={}",
            id,
            origin.options().server_group,
            config->num_replicas.value_or(0));
          ec = errc::key_value::document_irretrievable;
        }

        if (ec) {
          return h(response_type{ make_key_value_error_context(ec, id) });
        }
        using handler_type = utils::movable_function<void(response_type)>;

        struct replica_context {
          replica_context(handler_type&& handler, std::size_t expected_responses)
            : handler_(std::move(handler))
            , expected_responses_(expected_responses)
          {
          }

          void add_cancellation_token(std::shared_ptr<impl::cancellation_token> token)
          {
            std::scoped_lock<std::mutex> lock(cancel_tokens_mutex_);
            cancel_tokens_.emplace_back(std::move(token));
          }

          auto get_cancellation_tokens() -> std::vector<std::shared_ptr<impl::cancellation_token>>
          {
            std::vector<std::shared_ptr<impl::cancellation_token>> tokens{};
            {
              std::scoped_lock<std::mutex> lock(cancel_tokens_mutex_);
              std::swap(tokens, cancel_tokens_);
            }
            return tokens;
          }

          handler_type handler_;
          std::size_t expected_responses_;
          bool done_{ false };
          std::mutex mutex_{};
          std::vector<std::shared_ptr<impl::cancellation_token>> cancel_tokens_{};
          std::mutex cancel_tokens_mutex_{};
        };
        auto ctx = std::make_shared<replica_context>(std::move(h), nodes.size());

        for (const auto& node : nodes) {
          auto subop_span = core->tracer()->create_span(
            node.is_replica ? tracing::operation::mcbp_get_replica : tracing::operation::mcbp_get,
            parent_span);

          if (subop_span->uses_tags()) {
            subop_span->add_tag(tracing::attributes::op::service, tracing::service::key_value);
            subop_span->add_tag(tracing::attributes::op::operation_name,
                                node.is_replica ? tracing::operation::mcbp_get_replica
                                                : tracing::operation::mcbp_get);
            subop_span->add_tag(tracing::attributes::op::bucket_name, id.bucket());
            subop_span->add_tag(tracing::attributes::op::scope_name, id.scope());
            subop_span->add_tag(tracing::attributes::op::collection_name, id.collection());
          }

          if (node.is_replica) {
            document_id replica_id{ id };
            replica_id.node_index(node.index);
            impl::with_cancellation<impl::get_replica_request> req{
              {
                std::move(replica_id),
                timeout,
                {},
                {},
                {},
                subop_span,
              },
            };
            ctx->add_cancellation_token(req.cancel_token);
            core->execute(
              std::move(req), [ctx, subop_span](auto&& resp) {
                {
                  if (subop_span->uses_tags() && resp.ctx.retry_attempts() > 0) {
                    subop_span->add_tag(tracing::attributes::op::retry_count,
                                        resp.ctx.retry_attempts());
                  }
                  subop_span->end();
                }
                handler_type local_handler;
                std::vector<std::shared_ptr<impl::cancellation_token>> cancel_tokens;
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
                  cancel_tokens = ctx->get_cancellation_tokens();
                }
                for (const auto& token : cancel_tokens) {
                  token->cancel();
                }
                if (local_handler) {
                  return local_handler(response_type{
                    std::move(resp.ctx), std::move(resp.value), resp.cas, resp.flags, true });
                }
              });
          } else {
            impl::with_cancellation<get_request> req{
              {
                id,
                {},
                {},
                timeout,
                {},
                subop_span,
              },
            };
            ctx->add_cancellation_token(req.cancel_token);
            core->execute(std::move(req), [ctx, subop_span](auto&& resp) {
              {
                if (subop_span->uses_tags() && resp.ctx.retry_attempts() > 0) {
                  subop_span->add_tag(tracing::attributes::op::retry_count,
                                      resp.ctx.retry_attempts());
                }
                subop_span->end();
              }
              handler_type local_handler{};
              std::vector<std::shared_ptr<impl::cancellation_token>> cancel_tokens;
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
                cancel_tokens = ctx->get_cancellation_tokens();
              }
              for (const auto& token : cancel_tokens) {
                token->cancel();
              }
              if (local_handler) {
                return local_handler(response_type{
                  std::move(resp.ctx), std::move(resp.value), resp.cas, resp.flags, false });
              }
            });
          }
        }
      });
  }
};

template<>
struct is_compound_operation<get_any_replica_request> : public std::true_type {
};
} // namespace couchbase::core::operations
