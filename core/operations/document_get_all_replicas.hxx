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
#include "core/logger/logger.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/utils/movable_function.hxx"
#include "couchbase/error_codes.hxx"

#include <memory>
#include <mutex>

namespace couchbase::core::operations
{
struct get_all_replicas_response {
  struct entry {
    std::vector<std::byte> value{};
    couchbase::cas cas{};
    std::uint32_t flags{};
    bool replica{ true };
  };
  key_value_error_context ctx{};
  std::vector<entry> entries{};
};

struct get_all_replicas_request {
  using response_type = get_all_replicas_response;
  using encoded_request_type =
    core::protocol::client_request<core::protocol::get_replica_request_body>;
  using encoded_response_type =
    core::protocol::client_response<core::protocol::get_replica_response_body>;

  static const inline std::string observability_identifier = "get_all_replicas";

  core::document_id id;
  std::optional<std::chrono::milliseconds> timeout{};
  couchbase::read_preference read_preference{ couchbase::read_preference::no_preference };

  template<typename Core, typename Handler>
  void execute(Core core, Handler handler)
  {
    core->with_bucket_configuration(
      id.bucket(),
      [core,
       id = id,
       timeout = timeout,
       read_preference = read_preference,
       h = std::forward<Handler>(handler)](
        std::error_code ec, std::shared_ptr<topology::configuration> config) mutable {
        if (ec) {
          return h(response_type{ make_key_value_error_context(ec, id) });
        }
        const auto [e, origin] = core->origin();
        if (e) {
          return h(response_type{ make_key_value_error_context(e, id) });
        }

        auto nodes =
          impl::effective_nodes(id, config, read_preference, origin.options().server_group);
        if (nodes.empty()) {
          CB_LOG_DEBUG(
            "Unable to retrieve replicas for \"{}\", server_group={}, number_of_replicas={}",
            id,
            origin.options().server_group,
            config->num_replicas.value_or(0));
          return h(response_type{
            make_key_value_error_context(errc::key_value::document_irretrievable, id) });
        }

        using handler_type = utils::movable_function<void(response_type)>;

        struct replica_context {
          replica_context(handler_type handler, std::size_t expected_responses)
            : handler_(std::move(handler))
            , expected_responses_(expected_responses)
          {
          }

          handler_type handler_;
          std::size_t expected_responses_;
          bool done_{ false };
          std::mutex mutex_{};
          std::vector<get_all_replicas_response::entry> result_{};
        };
        auto ctx = std::make_shared<replica_context>(std::move(h), nodes.size());

        for (const auto& node : nodes) {
          if (node.is_replica) {
            document_id replica_id{ id };
            replica_id.node_index(node.index);
            core->execute(
              impl::get_replica_request{ std::move(replica_id), timeout }, [ctx](auto&& resp) {
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
                    ctx->result_.emplace_back(get_all_replicas_response::entry{
                      std::move(resp.value), resp.cas, resp.flags, true /* replica */ });
                  }
                  if (ctx->expected_responses_ == 0) {
                    ctx->done_ = true;
                    std::swap(local_handler, ctx->handler_);
                  }
                }
                if (local_handler) {
                  if (ctx->result_.empty()) {
                    // Return an error only when we have no results from any replica.
                    return local_handler({ std::move(resp.ctx), {} });
                  }
                  return local_handler({ {}, std::move(ctx->result_) });
                }
              });
          } else {
            core->execute(get_request{ document_id{ id }, {}, {}, timeout }, [ctx](auto&& resp) {
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
                  ctx->result_.emplace_back(get_all_replicas_response::entry{
                    std::move(resp.value), resp.cas, resp.flags, false /* active */ });
                }
                if (ctx->expected_responses_ == 0) {
                  ctx->done_ = true;
                  std::swap(local_handler, ctx->handler_);
                }
              }
              if (local_handler) {
                if (ctx->result_.empty()) {
                  // Return an error only when we have no results from any replica.
                  return local_handler({ std::move(resp.ctx), {} });
                }
                return local_handler({ {}, std::move(ctx->result_) });
              }
            });
          }
        }
      });
  }
};

template<>
struct is_compound_operation<get_all_replicas_request> : public std::true_type {
};
} // namespace couchbase::core::operations
