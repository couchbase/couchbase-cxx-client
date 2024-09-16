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
#include "core/impl/replica_utils.hxx"
#include "core/impl/subdoc/command.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/utils/movable_function.hxx"

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/read_preference.hxx>

#include <functional>
#include <memory>
#include <mutex>

namespace couchbase::core::operations
{
struct lookup_in_any_replica_response {
  struct entry {
    std::string path;
    codec::binary value;
    std::size_t original_index;
    bool exists;
    protocol::subdoc_opcode opcode;
    key_value_status_code status;
    std::error_code ec{};
  };
  subdocument_error_context ctx{};
  couchbase::cas cas{};
  std::vector<entry> fields{};
  bool deleted{ false };
  bool is_replica{ true };
};

struct lookup_in_any_replica_request {
  using response_type = lookup_in_any_replica_response;
  using encoded_request_type =
    core::protocol::client_request<core::protocol::lookup_in_replica_request_body>;
  using encoded_response_type =
    core::protocol::client_response<core::protocol::lookup_in_replica_response_body>;

  core::document_id id;
  std::vector<couchbase::core::impl::subdoc::command> specs{};
  std::optional<std::chrono::milliseconds> timeout{};
  std::shared_ptr<couchbase::tracing::request_span> parent_span{ nullptr };
  couchbase::read_preference read_preference{ couchbase::read_preference::no_preference };

  template<typename Core, typename Handler>
  void execute(Core core, Handler handler)
  {
    core->open_bucket(
      id.bucket(),
      [core,
       id = id,
       timeout = timeout,
       specs = specs,
       parent_span = parent_span,
       read_preference = read_preference,
       h = std::forward<Handler>(handler)](std::error_code ec) mutable {
        if (ec) {
          std::optional<std::string> first_error_path{};
          std::optional<std::size_t> first_error_index{};
          h(response_type{ make_subdocument_error_context(make_key_value_error_context(ec, id),
                                                          ec,
                                                          first_error_path,
                                                          first_error_index,
                                                          false) });
          return;
        }
        return core->with_bucket_configuration(
          id.bucket(),
          [core, id, timeout, specs, parent_span, read_preference, h = std::forward<Handler>(h)](
            std::error_code ec, const topology::configuration& config) mutable {
            if (!config.capabilities.supports_subdoc_read_replica()) {
              ec = errc::common::feature_not_available;
            }
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
                config.num_replicas.value_or(0));
              ec = errc::key_value::document_irretrievable;
            }

            if (ec) {
              return h(response_type{ make_subdocument_error_context(
                make_key_value_error_context(ec, id), ec, {}, {}, false) });
            }

            using handler_type = utils::movable_function<void(response_type)>;

            struct replica_context {
              replica_context(handler_type&& handler, std::size_t expected_responses)
                : handler_(std::move(handler))
                , expected_responses_(expected_responses)
              {
              }

              handler_type handler_;
              std::size_t expected_responses_;
              bool done_{ false };
              std::mutex mutex_{};
            };
            auto ctx = std::make_shared<replica_context>(std::move(h), nodes.size());

            for (const auto& node : nodes) {
              if (node.is_replica) {
                document_id replica_id{ id };
                replica_id.node_index(node.index);
                core->execute(
                  impl::lookup_in_replica_request{
                    std::move(replica_id), specs, timeout, parent_span },
                  [ctx](auto&& resp) {
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
                      response_type res{};
                      res.ctx = resp.ctx;
                      res.cas = resp.cas;
                      res.deleted = resp.deleted;
                      res.is_replica = true;
                      for (auto& field : resp.fields) {
                        auto lookup_in_entry = lookup_in_any_replica_response::entry{};
                        lookup_in_entry.path = field.path;
                        lookup_in_entry.value = field.value;
                        lookup_in_entry.status = field.status;
                        lookup_in_entry.ec = field.ec;
                        lookup_in_entry.exists = field.exists;
                        lookup_in_entry.original_index = field.original_index;
                        lookup_in_entry.opcode = field.opcode;
                        res.fields.emplace_back(lookup_in_entry);
                      }
                      return local_handler(res);
                    }
                  });
              } else {
                core->execute(lookup_in_request{ id, {}, {}, false, specs, timeout },
                              [ctx](auto&& resp) {
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
                                  auto res = response_type{};
                                  res.ctx = resp.ctx;
                                  res.cas = resp.cas;
                                  res.deleted = resp.deleted;
                                  res.is_replica = false;
                                  for (auto& field : resp.fields) {
                                    auto lookup_in_entry = lookup_in_any_replica_response::entry{};
                                    lookup_in_entry.path = field.path;
                                    lookup_in_entry.value = field.value;
                                    lookup_in_entry.status = field.status;
                                    lookup_in_entry.ec = field.ec;
                                    lookup_in_entry.exists = field.exists;
                                    lookup_in_entry.original_index = field.original_index;
                                    lookup_in_entry.opcode = field.opcode;
                                    res.fields.emplace_back(lookup_in_entry);
                                  }
                                  return local_handler(res);
                                }
                              });
              }
            }
          });
      });
  }
};

template<>
struct is_compound_operation<lookup_in_any_replica_request> : public std::true_type {
};
} // namespace couchbase::core::operations
