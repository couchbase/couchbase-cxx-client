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

#include <couchbase/api/get_replica_result.hxx>
#include <couchbase/impl/get_all_replicas.hxx>
#include <couchbase/impl/get_any_replica.hxx>
#include <couchbase/impl/get_replica.hxx>

#include <memory>

namespace couchbase
{
class cluster;

template<typename Request, typename Handler>
void
execute(std::shared_ptr<couchbase::cluster> c, Request request, Handler&& handler)
{
    c->execute(std::move(request), std::forward<Handler>(handler));
}

template<typename Handler>
void
execute(std::shared_ptr<couchbase::cluster> c, std::shared_ptr<impl::get_any_replica_request> request, Handler&& handler)
{
    using response_type = typename impl::get_any_replica_request::response_type;
    auto bucket_name = request->id().bucket();
    c->with_bucket_configuration(
      bucket_name,
      [c, r = std::move(request), h = std::forward<Handler>(handler)](std::error_code ec, const topology::configuration& config) mutable {
          if (ec) {
              return h(error_context::key_value{ couchbase::document_id{ r->id() }, ec }, response_type{});
          }
          struct replica_context {
              replica_context(Handler handler, std::uint32_t expected_responses)
                : handler_(std::move(handler))
                , expected_responses_(expected_responses)
              {
              }

              Handler handler_;
              std::uint32_t expected_responses_;
              bool done_{ false };
              std::mutex mutex_{};
          };
          auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

          for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
              couchbase::document_id replica_id{ r->id() };
              replica_id.node_index(idx);
              c->execute(impl::get_replica_request{ std::move(replica_id), r->timeout() }, [ctx](impl::get_replica_response&& resp) {
                  std::scoped_lock lock(ctx->mutex_);
                  if (ctx->done_) {
                      return;
                  }
                  --ctx->expected_responses_;
                  if (resp.ctx.ec) {
                      if (ctx->expected_responses_ == 0) {
                          auto err = std::move(resp.ctx);
                          // consider document irretrievable and give up
                          err.ec = error::key_value_errc::document_irretrievable;
                          return ctx->handler_(std::move(err), response_type{});
                      }
                      // just ignore the response
                      return;
                  }
                  ctx->done_ = true;
                  return ctx->handler_(std::move(resp.ctx),
                                       response_type{ resp.cas, true /* replica */, std::move(resp.value), resp.flags });
              });
          }

          operations::get_request active{ couchbase::document_id{ r->id() } };
          active.timeout = r->timeout();
          c->execute(active, [ctx](operations::get_response&& resp) {
              std::scoped_lock lock(ctx->mutex_);
              if (ctx->done_) {
                  return;
              }
              --ctx->expected_responses_;
              if (resp.ctx.ec) {
                  if (ctx->expected_responses_ == 0) {
                      auto err = std::move(resp.ctx);
                      // consider document irretrievable and give up
                      err.ec = error::key_value_errc::document_irretrievable;
                      return ctx->handler_(std::move(err), response_type{});
                  }
                  // just ignore the response
                  return;
              }
              ctx->done_ = true;
              return ctx->handler_(std::move(resp.ctx), response_type{ resp.cas, false /* active */, std::move(resp.value), resp.flags });
          });
      });
}

template<typename Handler>
void
execute(std::shared_ptr<couchbase::cluster> c, std::shared_ptr<impl::get_all_replicas_request> request, Handler&& handler)
{
    using response_type = typename impl::get_all_replicas_request::response_type;
    auto bucket_name = request->id().bucket();
    c->with_bucket_configuration(
      bucket_name,
      [c, r = std::move(request), h = std::forward<Handler>(handler)](std::error_code ec, const topology::configuration& config) mutable {
          if (ec) {
              return h(error_context::key_value{ couchbase::document_id{ r->id() }, ec }, response_type{});
          }
          struct replica_context {
              replica_context(Handler handler, std::uint32_t expected_responses)
                : handler_(std::move(handler))
                , expected_responses_(expected_responses)
              {
              }

              Handler handler_;
              std::uint32_t expected_responses_;
              bool done_{ false };
              std::mutex mutex_{};
              response_type result_{};
          };
          auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

          for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
              couchbase::document_id replica_id{ r->id() };
              replica_id.node_index(idx);
              c->execute(impl::get_replica_request{ std::move(replica_id), r->timeout() }, [ctx](impl::get_replica_response&& resp) {
                  std::scoped_lock lock(ctx->mutex_);
                  if (ctx->done_) {
                      return;
                  }
                  --ctx->expected_responses_;
                  if (resp.ctx.ec) {
                      // just ignore the response
                      return;
                  }
                  ctx->result_.emplace_back(response_type::value_type{ resp.cas, true /* replica */, std::move(resp.value), resp.flags });
                  if (ctx->expected_responses_ == 0) {
                      ctx->done_ = true;
                      return ctx->handler_(std::move(resp.ctx), std::move(ctx->result_));
                  }
              });
          }

          operations::get_request active{ couchbase::document_id{ r->id() } };
          active.timeout = r->timeout();
          c->execute(active, [ctx](operations::get_response&& resp) {
              std::scoped_lock lock(ctx->mutex_);
              if (ctx->done_) {
                  return;
              }
              --ctx->expected_responses_;
              if (resp.ctx.ec) {
                  // just ignore the response
                  return;
              }
              ctx->result_.emplace_back(response_type::value_type{ resp.cas, false /* active */, std::move(resp.value), resp.flags });
              if (ctx->expected_responses_ == 0) {
                  ctx->done_ = true;
                  return ctx->handler_(std::move(resp.ctx), std::move(ctx->result_));
              }
          });
      });
}
} // namespace couchbase
