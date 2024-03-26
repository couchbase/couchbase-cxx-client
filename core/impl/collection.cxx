/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/agent_group.hxx"
#include "core/cluster.hxx"
#include "core/logger/logger.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_exists.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_and_lock.hxx"
#include "core/operations/document_get_and_touch.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_touch.hxx"
#include "core/operations/document_unlock.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/range_scan_options.hxx"
#include "core/range_scan_orchestrator.hxx"
#include "get_all_replicas.hxx"
#include "get_any_replica.hxx"
#include "get_replica.hxx"
#include "internal_scan_result.hxx"
#include "lookup_in_all_replicas.hxx"
#include "lookup_in_any_replica.hxx"
#include "lookup_in_replica.hxx"
#include "observe_poll.hxx"

#include <couchbase/collection.hxx>

#include <memory>

namespace couchbase
{
class collection_impl : public std::enable_shared_from_this<collection_impl>
{
  public:
    collection_impl(core::cluster core, std::string_view bucket_name, std::string_view scope_name, std::string_view name)
      : core_{ std::move(core) }
      , bucket_name_{ bucket_name }
      , scope_name_{ scope_name }
      , name_{ name }
    {
    }

    [[nodiscard]] auto bucket_name() const -> const std::string&
    {
        return bucket_name_;
    }

    [[nodiscard]] auto scope_name() const -> const std::string&
    {
        return scope_name_;
    }

    [[nodiscard]] auto name() const -> const std::string&
    {
        return name_;
    }

    [[nodiscard]] auto core() const -> const core::cluster&
    {
        return core_;
    }

    void get(std::string document_key, get_options::built options, get_handler&& handler) const
    {
        if (!options.with_expiry && options.projections.empty()) {
            return core_.execute(
              core::operations::get_request{
                core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
                {},
                {},
                options.timeout,
                { options.retry_strategy },
              },
              [handler = std::move(handler)](auto resp) mutable {
                  return handler(std::move(resp.ctx), get_result{ resp.cas, { std::move(resp.value), resp.flags }, {} });
              });
        }
        return core_.execute(
          core::operations::get_projected_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            options.projections,
            options.with_expiry,
            {},
            false,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto resp) mutable {
              std::optional<std::chrono::system_clock::time_point> expiry_time{};
              if (resp.expiry && resp.expiry.value() > 0) {
                  expiry_time.emplace(std::chrono::seconds{ resp.expiry.value() });
              }
              return handler(std::move(resp.ctx), get_result{ resp.cas, { std::move(resp.value), resp.flags }, expiry_time });
          });
    }

    void get_and_touch(std::string document_key,
                       std::uint32_t expiry,
                       get_and_touch_options::built options,
                       get_and_touch_handler&& handler) const
    {
        return core_.execute(
          core::operations::get_and_touch_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            expiry,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto resp) mutable {
              return handler(std::move(resp.ctx), get_result{ resp.cas, { std::move(resp.value), resp.flags }, {} });
          });
    }

    void touch(std::string document_key, std::uint32_t expiry, touch_options::built options, touch_handler&& handler) const
    {
        return core_.execute(
          core::operations::touch_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            expiry,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(std::move(resp.ctx), result{ resp.cas }); });
    }

    void get_any_replica(std::string document_key,
                         const get_any_replica_options::built& options,
                         core::impl::movable_get_any_replica_handler&& handler) const
    {
        auto request =
          std::make_shared<core::impl::get_any_replica_request>(bucket_name_, scope_name_, name_, std::move(document_key), options.timeout);
        core_.with_bucket_configuration(
          bucket_name_,
          [core = core_, r = std::move(request), h = std::move(handler)](std::error_code ec,
                                                                         const core::topology::configuration& config) mutable {
              if (ec) {
                  return h(make_key_value_error_context(ec, r->id()), get_replica_result{});
              }
              struct replica_context {
                  replica_context(core::impl::movable_get_any_replica_handler&& handler, std::uint32_t expected_responses)
                    : handler_(std::move(handler))
                    , expected_responses_(expected_responses)
                  {
                  }

                  core::impl::movable_get_any_replica_handler handler_;
                  std::uint32_t expected_responses_;
                  bool done_{ false };
                  std::mutex mutex_{};
              };
              auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

              for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                  core::document_id replica_id{ r->id() };
                  replica_id.node_index(idx);
                  core.execute(core::impl::get_replica_request{ std::move(replica_id), r->timeout() }, [ctx](auto&& resp) {
                      core::impl::movable_get_any_replica_handler local_handler;
                      {
                          const std::scoped_lock lock(ctx->mutex_);
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
                          return local_handler(std::move(resp.ctx),
                                               get_replica_result{ resp.cas, true /* replica */, { std::move(resp.value), resp.flags } });
                      }
                  });
              }

              core::operations::get_request active{ core::document_id{ r->id() } };
              active.timeout = r->timeout();
              core.execute(active, [ctx](auto resp) {
                  core::impl::movable_get_any_replica_handler local_handler{};
                  {
                      const std::scoped_lock lock(ctx->mutex_);
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
                      return local_handler(std::move(resp.ctx),
                                           get_replica_result{ resp.cas, false /* active */, { std::move(resp.value), resp.flags } });
                  }
              });
          });
    }

    void get_all_replicas(std::string document_key,
                          const get_all_replicas_options::built& options,
                          core::impl::movable_get_all_replicas_handler&& handler) const
    {
        auto request = std::make_shared<core::impl::get_all_replicas_request>(
          bucket_name_, scope_name_, name_, std::move(document_key), options.timeout);
        core_.with_bucket_configuration(
          bucket_name_,
          [core = core_, r = std::move(request), h = std::move(handler)](std::error_code ec,
                                                                         const core::topology::configuration& config) mutable {
              if (ec) {
                  return h(make_key_value_error_context(ec, r->id()), get_all_replicas_result{});
              }
              struct replica_context {
                  replica_context(core::impl::movable_get_all_replicas_handler handler, std::uint32_t expected_responses)
                    : handler_(std::move(handler))
                    , expected_responses_(expected_responses)
                  {
                  }

                  core::impl::movable_get_all_replicas_handler handler_;
                  std::uint32_t expected_responses_;
                  bool done_{ false };
                  std::mutex mutex_{};
                  get_all_replicas_result result_{};
              };
              auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

              for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                  core::document_id replica_id{ r->id() };
                  replica_id.node_index(idx);
                  core.execute(core::impl::get_replica_request{ std::move(replica_id), r->timeout() }, [ctx](auto resp) {
                      core::impl::movable_get_all_replicas_handler local_handler{};
                      {
                          const std::scoped_lock lock(ctx->mutex_);
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

              core::operations::get_request active{ core::document_id{ r->id() } };
              active.timeout = r->timeout();
              core.execute(active, [ctx](auto resp) {
                  core::impl::movable_get_all_replicas_handler local_handler{};
                  {
                      const std::scoped_lock lock(ctx->mutex_);
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
                            get_replica_result{ resp.cas, false /* active */, { std::move(resp.value), resp.flags } });
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

    void remove(std::string document_key, remove_options::built options, remove_handler&& handler) const
    {
        auto id = core::document_id{
            bucket_name_,
            scope_name_,
            name_,
            std::move(document_key),
        };
        if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
            return core_.execute(
              core::operations::remove_request{
                std::move(id),
                {},
                {},
                options.cas,
                options.durability_level,
                options.timeout,
                { options.retry_strategy },
              },
              [handler = std::move(handler)](auto resp) mutable {
                  if (resp.ctx.ec()) {
                      return handler(std::move(resp.ctx), mutation_result{});
                  }
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              });
        }

        core::operations::remove_request request{
            id, {}, {}, options.cas, durability_level::none, options.timeout, { options.retry_strategy },
        };
        return core_.execute(
          std::move(request), [core = core_, id = std::move(id), options, handler = std::move(handler)](auto&& resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              }
              auto token = resp.token;
              core::impl::initiate_observe_poll(core,
                                                std::move(id),
                                                token,
                                                options.timeout,
                                                options.persist_to,
                                                options.replicate_to,
                                                [resp, handler = std::move(handler)](std::error_code ec) mutable {
                                                    if (ec) {
                                                        resp.ctx.override_ec(ec);
                                                        return handler(std::move(resp.ctx), mutation_result{});
                                                    }
                                                    return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
                                                });
          });
    }

    void get_and_lock(std::string document_key,
                      std::chrono::seconds lock_duration,
                      get_and_lock_options::built options,
                      get_and_lock_handler&& handler) const
    {
        core_.execute(
          core::operations::get_and_lock_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            static_cast<uint32_t>(lock_duration.count()),
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto&& resp) mutable {
              return handler(std::move(resp.ctx), get_result{ resp.cas, { std::move(resp.value), resp.flags }, {} });
          });
    }

    void unlock(std::string document_key, couchbase::cas cas, unlock_options::built options, unlock_handler&& handler) const
    {
        core_.execute(
          core::operations::unlock_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            cas,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto&& resp) mutable { return handler(std::move(resp.ctx)); });
    }

    void exists(std::string document_key, exists_options::built options, exists_handler&& handler) const
    {
        core_.execute(
          core::operations::exists_request{
            core::document_id{ bucket_name_, scope_name_, name_, std::move(document_key) },
            {},
            {},
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto&& resp) mutable {
              return handler(std::move(resp.ctx), exists_result{ resp.cas, resp.exists() });
          });
    }

    void lookup_in(std::string document_key,
                   const std::vector<core::impl::subdoc::command>& specs,
                   lookup_in_options::built options,
                   lookup_in_handler&& handler) const
    {
        return core_.execute(
          core::operations::lookup_in_request{
            core::document_id{
              bucket_name_,
              scope_name_,
              name_,
              std::move(document_key),
            },
            {},
            {},
            options.access_deleted,
            specs,
            options.timeout,
            { options.retry_strategy },
          },
          [handler = std::move(handler)](auto resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), lookup_in_result{});
              }

              std::vector<lookup_in_result::entry> entries{};
              entries.reserve(resp.fields.size());
              for (auto& entry : resp.fields) {
                  entries.emplace_back(lookup_in_result::entry{
                    std::move(entry.path),
                    std::move(entry.value),
                    entry.original_index,
                    entry.exists,
                    entry.ec,
                  });
              }
              return handler(std::move(resp.ctx), lookup_in_result{ resp.cas, std::move(entries), resp.deleted });
          });
    }

    void lookup_in_all_replicas(std::string document_key,
                                const std::vector<core::impl::subdoc::command>& specs,
                                const lookup_in_all_replicas_options::built& options,
                                lookup_in_all_replicas_handler&& handler) const
    {
        auto request = std::make_shared<couchbase::core::impl::lookup_in_all_replicas_request>(
          bucket_name_, scope_name_, name_, std::move(document_key), specs, options.timeout);
        core_.open_bucket(
          bucket_name_,
          [core = core_, bucket_name = bucket_name_, r = std::move(request), h = std::move(handler)](std::error_code ec) mutable {
              if (ec) {
                  h(core::make_subdocument_error_context(make_key_value_error_context(ec, r->id()), ec, {}, {}, false),
                    lookup_in_all_replicas_result{});
                  return;
              }

              return core.with_bucket_configuration(
                bucket_name,
                [core = core, r = std::move(r), h = std::move(h)](std::error_code ec, const core::topology::configuration& config) mutable {
                    if (!config.capabilities.supports_subdoc_read_replica()) {
                        ec = errc::common::feature_not_available;
                    }

                    if (ec) {
                        return h(core::make_subdocument_error_context(make_key_value_error_context(ec, r->id()), ec, {}, {}, false),
                                 lookup_in_all_replicas_result{});
                    }
                    struct replica_context {
                        replica_context(core::impl::movable_lookup_in_all_replicas_handler handler, std::uint32_t expected_responses)
                          : handler_(std::move(handler))
                          , expected_responses_(expected_responses)
                        {
                        }

                        core::impl::movable_lookup_in_all_replicas_handler handler_;
                        std::uint32_t expected_responses_;
                        bool done_{ false };
                        std::mutex mutex_{};
                        lookup_in_all_replicas_result result_{};
                    };
                    auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

                    for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                        core::document_id replica_id{ r->id() };
                        replica_id.node_index(idx);
                        core.execute(core::impl::lookup_in_replica_request{ std::move(replica_id), r->specs(), r->timeout() },
                                     [ctx](core::impl::lookup_in_replica_response&& resp) {
                                         core::impl::movable_lookup_in_all_replicas_handler local_handler{};
                                         {
                                             const std::scoped_lock lock(ctx->mutex_);
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
                                                 for (const auto& field : resp.fields) {
                                                     lookup_in_replica_result::entry lookup_in_entry{};
                                                     lookup_in_entry.path = field.path;
                                                     lookup_in_entry.value = field.value;
                                                     lookup_in_entry.exists = field.exists;
                                                     lookup_in_entry.original_index = field.original_index;
                                                     lookup_in_entry.ec = field.ec;
                                                     entries.emplace_back(lookup_in_entry);
                                                 }
                                                 ctx->result_.emplace_back(resp.cas, entries, resp.deleted, true /* replica */);
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

                    core::operations::lookup_in_request active{ core::document_id{ r->id() } };
                    active.specs = r->specs();
                    active.timeout = r->timeout();
                    core.execute(active, [ctx](core::operations::lookup_in_response&& resp) {
                        core::impl::movable_lookup_in_all_replicas_handler local_handler{};
                        {
                            const std::scoped_lock lock(ctx->mutex_);
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
                                for (const auto& field : resp.fields) {
                                    lookup_in_replica_result::entry lookup_in_entry{};
                                    lookup_in_entry.path = field.path;
                                    lookup_in_entry.value = field.value;
                                    lookup_in_entry.exists = field.exists;
                                    lookup_in_entry.original_index = field.original_index;
                                    lookup_in_entry.ec = field.ec;
                                    entries.emplace_back(lookup_in_entry);
                                }
                                ctx->result_.emplace_back(resp.cas, entries, resp.deleted, false /* active */);
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
          });
    };

    void lookup_in_any_replica(std::string document_key,
                               const std::vector<core::impl::subdoc::command>& specs,
                               const lookup_in_any_replica_options::built& options,
                               lookup_in_any_replica_handler&& handler) const
    {
        auto request = std::make_shared<couchbase::core::impl::lookup_in_any_replica_request>(
          bucket_name_, scope_name_, name_, std::move(document_key), specs, options.timeout);
        core_.open_bucket(
          bucket_name_,
          [core = core_, bucket_name = bucket_name_, r = std::move(request), h = std::move(handler)](std::error_code ec) mutable {
              if (ec) {
                  h(core::make_subdocument_error_context(make_key_value_error_context(ec, r->id()), ec, {}, {}, false),
                    lookup_in_replica_result{});
                  return;
              }

              return core.with_bucket_configuration(
                bucket_name,
                [core = core, r = std::move(r), h = std::move(h)](std::error_code ec, const core::topology::configuration& config) mutable {
                    if (!config.capabilities.supports_subdoc_read_replica()) {
                        ec = errc::common::feature_not_available;
                    }
                    if (ec) {
                        return h(core::make_subdocument_error_context(make_key_value_error_context(ec, r->id()), ec, {}, {}, false),
                                 lookup_in_replica_result{});
                    }
                    struct replica_context {
                        replica_context(core::impl::movable_lookup_in_any_replica_handler handler, std::uint32_t expected_responses)
                          : handler_(std::move(handler))
                          , expected_responses_(expected_responses)
                        {
                        }

                        core::impl::movable_lookup_in_any_replica_handler handler_;
                        std::uint32_t expected_responses_;
                        bool done_{ false };
                        std::mutex mutex_{};
                    };
                    auto ctx = std::make_shared<replica_context>(std::move(h), config.num_replicas.value_or(0U) + 1U);

                    for (std::size_t idx = 1U; idx <= config.num_replicas.value_or(0U); ++idx) {
                        core::document_id replica_id{ r->id() };
                        replica_id.node_index(idx);
                        core.execute(core::impl::lookup_in_replica_request{ std::move(replica_id), r->specs(), r->timeout() },
                                     [ctx](core::impl::lookup_in_replica_response&& resp) {
                                         core::impl::movable_lookup_in_any_replica_handler local_handler;
                                         {
                                             const std::scoped_lock lock(ctx->mutex_);
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
                                             std::vector<lookup_in_replica_result::entry> entries;
                                             for (const auto& field : resp.fields) {
                                                 lookup_in_replica_result::entry entry{};
                                                 entry.path = field.path;
                                                 entry.original_index = field.original_index;
                                                 entry.exists = field.exists;
                                                 entry.value = field.value;
                                                 entry.ec = field.ec;
                                                 entries.emplace_back(entry);
                                             }
                                             return local_handler(
                                               std::move(resp.ctx),
                                               lookup_in_replica_result{ resp.cas, entries, resp.deleted, true /* replica */ });
                                         }
                                     });
                    }

                    core::operations::lookup_in_request active{ core::document_id{ r->id() } };
                    active.specs = r->specs();
                    active.timeout = r->timeout();
                    core.execute(active, [ctx](core::operations::lookup_in_response&& resp) {
                        core::impl::movable_lookup_in_any_replica_handler local_handler{};
                        {
                            const std::scoped_lock lock(ctx->mutex_);
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
                            std::vector<lookup_in_replica_result::entry> entries;
                            for (const auto& field : resp.fields) {
                                lookup_in_replica_result::entry entry{};
                                entry.path = field.path;
                                entry.original_index = field.original_index;
                                entry.exists = field.exists;
                                entry.value = field.value;
                                entry.ec = field.ec;
                                entries.emplace_back(entry);
                            }
                            return local_handler(std::move(resp.ctx),
                                                 lookup_in_replica_result{ resp.cas, entries, resp.deleted, false /* active */ });
                        }
                    });
                });
          });
    };

    void mutate_in(std::string document_key,
                   const std::vector<core::impl::subdoc::command>& specs,
                   mutate_in_options::built options,
                   mutate_in_handler&& handler) const
    {
        auto id = core::document_id{
            bucket_name_,
            scope_name_,
            name_,
            std::move(document_key),
        };
        if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
            return core_.execute(
              core::operations::mutate_in_request{
                std::move(id),
                {},
                {},
                options.cas,
                options.access_deleted,
                options.create_as_deleted,
                options.expiry,
                options.store_semantics,
                specs,
                options.durability_level,
                options.timeout,
                { options.retry_strategy },
                options.preserve_expiry,
              },
              [handler = std::move(handler)](auto resp) mutable {
                  if (resp.ctx.ec()) {
                      return handler(std::move(resp.ctx), mutate_in_result{});
                  }
                  std::vector<mutate_in_result::entry> entries{};
                  entries.reserve(resp.fields.size());
                  for (auto& entry : resp.fields) {
                      entries.emplace_back(mutate_in_result::entry{
                        std::move(entry.path),
                        std::move(entry.value),
                        entry.original_index,
                      });
                  }
                  return handler(std::move(resp.ctx),
                                 mutate_in_result{ resp.cas, std::move(resp.token), std::move(entries), resp.deleted });
              });
        }

        core::operations::mutate_in_request request{
            id,
            {},
            {},
            options.cas,
            options.access_deleted,
            options.create_as_deleted,
            options.expiry,
            options.store_semantics,
            specs,
            durability_level::none,
            options.timeout,
            { options.retry_strategy },
            options.preserve_expiry,
        };
        return core_.execute(
          std::move(request), [core = core_, id = std::move(id), options, handler = std::move(handler)](auto&& resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), mutate_in_result{});
              }

              auto token = resp.token;
              core::impl::initiate_observe_poll(
                core,
                std::move(id),
                token,
                options.timeout,
                options.persist_to,
                options.replicate_to,
                [resp, handler = std::move(handler)](std::error_code ec) mutable {
                    if (ec) {
                        resp.ctx.override_ec(ec);
                        return handler(std::move(resp.ctx), mutate_in_result{});
                    }
                    std::vector<mutate_in_result::entry> entries{};
                    entries.reserve(resp.fields.size());
                    for (auto& entry : resp.fields) {
                        entries.emplace_back(mutate_in_result::entry{
                          std::move(entry.path),
                          std::move(entry.value),
                          entry.original_index,
                        });
                    }
                    return handler(std::move(resp.ctx),
                                   mutate_in_result{ resp.cas, std::move(resp.token), std::move(entries), resp.deleted });
                });
          });
    }

    void upsert(std::string document_key, codec::encoded_value encoded, upsert_options::built options, upsert_handler&& handler) const
    {
        auto value = std::move(encoded);
        auto id = core::document_id{
            bucket_name_,
            scope_name_,
            name_,
            std::move(document_key),
        };
        if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
            return core_.execute(
              core::operations::upsert_request{
                std::move(id),
                std::move(value.data),
                {},
                {},
                value.flags,
                options.expiry,
                options.durability_level,
                options.timeout,
                { options.retry_strategy },
                options.preserve_expiry,
              },
              [handler = std::move(handler)](auto resp) mutable {
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              });
        }

        core::operations::upsert_request request{
            id,
            std::move(value.data),
            {},
            {},
            value.flags,
            options.expiry,
            durability_level::none,
            options.timeout,
            { options.retry_strategy },
            options.preserve_expiry,
        };
        return core_.execute(
          std::move(request), [core = core_, id = std::move(id), options, handler = std::move(handler)](auto resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              }

              auto token = resp.token;
              core::impl::initiate_observe_poll(core,
                                                std::move(id),
                                                token,
                                                options.timeout,
                                                options.persist_to,
                                                options.replicate_to,
                                                [resp, handler = std::move(handler)](std::error_code ec) mutable {
                                                    if (ec) {
                                                        resp.ctx.override_ec(ec);
                                                        return handler(std::move(resp.ctx), mutation_result{});
                                                    }
                                                    return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
                                                });
          });
    }

    void insert(std::string document_key, codec::encoded_value encoded, insert_options::built options, insert_handler&& handler) const
    {
        auto value = std::move(encoded);
        auto id = core::document_id{
            bucket_name_,
            scope_name_,
            name_,
            std::move(document_key),
        };
        if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
            return core_.execute(
              core::operations::insert_request{
                std::move(id),
                std::move(value.data),
                {},
                {},
                value.flags,
                options.expiry,
                options.durability_level,
                options.timeout,
                { options.retry_strategy },
              },
              [handler = std::move(handler)](auto&& resp) mutable {
                  if (resp.ctx.ec()) {
                      return handler(std::move(resp.ctx), mutation_result{});
                  }
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              });
        }

        core::operations::insert_request request{
            id,
            std::move(value.data),
            {},
            {},
            value.flags,
            options.expiry,
            durability_level::none,
            options.timeout,
            { options.retry_strategy },
        };
        return core_.execute(
          std::move(request), [core = core_, id = std::move(id), options, handler = std::move(handler)](auto resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              }

              auto token = resp.token;
              core::impl::initiate_observe_poll(core,
                                                std::move(id),
                                                token,
                                                options.timeout,
                                                options.persist_to,
                                                options.replicate_to,
                                                [resp, handler = std::move(handler)](std::error_code ec) mutable {
                                                    if (ec) {
                                                        resp.ctx.override_ec(ec);
                                                        return handler(std::move(resp.ctx), mutation_result{});
                                                    }
                                                    return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
                                                });
          });
    }
    void replace(std::string document_key, codec::encoded_value encoded, replace_options::built options, replace_handler&& handler) const
    {
        auto value = std::move(encoded);
        auto id = core::document_id{
            bucket_name_,
            scope_name_,
            name_,
            std::move(document_key),
        };
        if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
            return core_.execute(
              core::operations::replace_request{
                std::move(id),
                std::move(value.data),
                {},
                {},
                value.flags,
                options.expiry,
                options.cas,
                options.durability_level,
                options.timeout,
                { options.retry_strategy },
                options.preserve_expiry,
              },
              [handler = std::move(handler)](auto resp) mutable {
                  if (resp.ctx.ec()) {
                      return handler(std::move(resp.ctx), mutation_result{});
                  }
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              });
        }

        core::operations::replace_request request{
            id,
            std::move(value.data),
            {},
            {},
            value.flags,
            options.expiry,
            options.cas,
            durability_level::none,
            options.timeout,
            { options.retry_strategy },
            options.preserve_expiry,
        };
        return core_.execute(
          std::move(request), [core = core_, id = std::move(id), options, handler = std::move(handler)](auto&& resp) mutable {
              if (resp.ctx.ec()) {
                  return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
              }

              auto token = resp.token;
              core::impl::initiate_observe_poll(core,
                                                std::move(id),
                                                token,
                                                options.timeout,
                                                options.persist_to,
                                                options.replicate_to,
                                                [resp, handler = std::move(handler)](std::error_code ec) mutable {
                                                    if (ec) {
                                                        resp.ctx.override_ec(ec);
                                                        return handler(std::move(resp.ctx), mutation_result{});
                                                    }
                                                    return handler(std::move(resp.ctx), mutation_result{ resp.cas, std::move(resp.token) });
                                                });
          });
    }

    void scan(scan_type::built scan_type, scan_options::built options, scan_handler&& handler) const
    {
        core::range_scan_orchestrator_options orchestrator_opts{ options.ids_only };
        if (!options.mutation_state.empty()) {
            orchestrator_opts.consistent_with = core::mutation_state{ options.mutation_state };
        }
        if (options.batch_item_limit.has_value()) {
            orchestrator_opts.batch_item_limit = options.batch_item_limit.value();
        }
        if (options.batch_byte_limit.has_value()) {
            orchestrator_opts.batch_byte_limit = options.batch_byte_limit.value();
        }
        if (options.concurrency.has_value()) {
            orchestrator_opts.concurrency = options.concurrency.value();
        }
        if (options.timeout.has_value()) {
            orchestrator_opts.timeout = options.timeout.value();
        }

        std::variant<std::monostate, core::range_scan, core::prefix_scan, core::sampling_scan> core_scan_type{};
        switch (scan_type.type) {
            case scan_type::built::prefix_scan:
                core_scan_type = core::prefix_scan{
                    scan_type.prefix,
                };
                break;
            case scan_type::built::range_scan:
                core_scan_type = core::range_scan{
                    (scan_type.from) ? std::make_optional(core::scan_term{ scan_type.from->term, scan_type.from->exclusive })
                                     : std::nullopt,
                    (scan_type.to) ? std::make_optional(core::scan_term{ scan_type.to->term, scan_type.to->exclusive }) : std::nullopt,
                };
                break;
            case scan_type::built::sampling_scan:
                core_scan_type = core::sampling_scan{
                    scan_type.limit,
                    scan_type.seed,
                };
                break;
        }

        return core_.open_bucket(
          bucket_name_, [this, handler = std::move(handler), orchestrator_opts, core_scan_type](std::error_code ec) mutable {
              if (ec) {
                  return handler(ec, {});
              }
              return core_.with_bucket_configuration(
                bucket_name_,
                [this, handler = std::move(handler), orchestrator_opts, core_scan_type](
                  std::error_code ec, const core::topology::configuration& config) mutable {
                    if (ec) {
                        return handler(ec, {});
                    }
                    if (!config.capabilities.supports_range_scan()) {
                        return handler(errc::common::feature_not_available, {});
                    }
                    auto agent_group = core::agent_group(core_.io_context(), core::agent_group_config{ { core_ } });
                    ec = agent_group.open_bucket(bucket_name_);
                    if (ec) {
                        return handler(ec, {});
                    }
                    auto agent = agent_group.get_agent(bucket_name_);
                    if (!agent.has_value()) {
                        return handler(agent.error(), {});
                    }
                    if (!config.vbmap.has_value() || config.vbmap->empty()) {
                        CB_LOG_WARNING("Unable to get vbucket map for `{}` - cannot perform scan operation", bucket_name_);
                        return handler(errc::common::request_canceled, {});
                    }

                    auto orchestrator = core::range_scan_orchestrator(
                      core_.io_context(), agent.value(), config.vbmap.value(), scope_name_, name_, core_scan_type, orchestrator_opts);
                    return orchestrator.scan([handler = std::move(handler)](auto ec, auto core_scan_result) mutable {
                        if (ec) {
                            return handler(ec, {});
                        }
                        auto internal_result = std::make_shared<internal_scan_result>(std::move(core_scan_result));
                        scan_result result{ internal_result };
                        return handler({}, result);
                    });
                });
          });
    }

  private:
    core::cluster core_;
    std::string bucket_name_;
    std::string scope_name_;
    std::string name_;
};

collection::collection(core::cluster core, std::string_view bucket_name, std::string_view scope_name, std::string_view name)
  : impl_(std::make_shared<collection_impl>(std::move(core), bucket_name, scope_name, name))
{
}

auto
collection::bucket_name() const -> const std::string&
{
    return impl_->bucket_name();
}

auto
collection::scope_name() const -> const std::string&
{
    return impl_->scope_name();
}

auto
collection::name() const -> const std::string&
{
    return impl_->name();
}

auto
collection::query_indexes() const -> collection_query_index_manager
{
    return { impl_->core(), impl_->bucket_name(), impl_->scope_name(), impl_->name() };
}

auto
collection::binary() const -> binary_collection
{
    return { impl_->core(), impl_->bucket_name(), impl_->scope_name(), impl_->name() };
}

void
collection::get(std::string document_id, const get_options& options, get_handler&& handler) const
{
    return impl_->get(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get(std::string document_id, const get_options& options) const -> std::future<std::pair<key_value_error_context, get_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
    auto future = barrier->get_future();
    get(std::move(document_id), options, [barrier](auto ctx, auto result) { barrier->set_value({ std::move(ctx), std::move(result) }); });
    return future;
}

void
collection::get_and_touch(std::string document_id,
                          std::chrono::seconds duration,
                          const get_and_touch_options& options,
                          get_and_touch_handler&& handler) const
{
    return impl_->get_and_touch(std::move(document_id), core::impl::expiry_relative(duration), options.build(), std::move(handler));
}

auto
collection::get_and_touch(std::string document_id, std::chrono::seconds duration, const get_and_touch_options& options) const
  -> std::future<std::pair<key_value_error_context, get_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
    auto future = barrier->get_future();
    get_and_touch(std::move(document_id), duration, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::get_and_touch(std::string document_id,
                          std::chrono::system_clock::time_point time_point,
                          const get_and_touch_options& options,
                          get_and_touch_handler&& handler) const
{
    return impl_->get_and_touch(std::move(document_id), core::impl::expiry_absolute(time_point), options.build(), std::move(handler));
}

auto
collection::get_and_touch(std::string document_id,
                          std::chrono::system_clock::time_point time_point,
                          const get_and_touch_options& options) const -> std::future<std::pair<key_value_error_context, get_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
    auto future = barrier->get_future();
    get_and_touch(std::move(document_id), time_point, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::touch(std::string document_id, std::chrono::seconds duration, const touch_options& options, touch_handler&& handler) const
{
    return impl_->touch(std::move(document_id), core::impl::expiry_relative(duration), options.build(), std::move(handler));
}

auto
collection::touch(std::string document_id, std::chrono::seconds duration, const touch_options& options) const
  -> std::future<std::pair<key_value_error_context, result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, result>>>();
    auto future = barrier->get_future();
    touch(std::move(document_id), duration, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::touch(std::string document_id,
                  std::chrono::system_clock::time_point time_point,
                  const touch_options& options,
                  touch_handler&& handler) const
{
    return impl_->touch(std::move(document_id), core::impl::expiry_absolute(time_point), options.build(), std::move(handler));
}

auto
collection::touch(std::string document_id, std::chrono::system_clock::time_point time_point, const touch_options& options) const
  -> std::future<std::pair<key_value_error_context, result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, result>>>();
    auto future = barrier->get_future();
    touch(std::move(document_id), time_point, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::get_any_replica(std::string document_id, const get_any_replica_options& options, get_any_replica_handler&& handler) const
{
    return impl_->get_any_replica(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get_any_replica(std::string document_id, const get_any_replica_options& options) const
  -> std::future<std::pair<key_value_error_context, get_replica_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_replica_result>>>();
    auto future = barrier->get_future();
    get_any_replica(std::move(document_id), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::get_all_replicas(std::string document_id, const get_all_replicas_options& options, get_all_replicas_handler&& handler) const
{
    return impl_->get_all_replicas(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get_all_replicas(std::string document_id, const get_all_replicas_options& options) const
  -> std::future<std::pair<key_value_error_context, get_all_replicas_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_all_replicas_result>>>();
    auto future = barrier->get_future();
    get_all_replicas(std::move(document_id), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::remove(std::string document_id, const remove_options& options, remove_handler&& handler) const
{
    return impl_->remove(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::remove(std::string document_id, const remove_options& options) const
  -> std::future<std::pair<key_value_error_context, mutation_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
    auto future = barrier->get_future();
    remove(std::move(document_id), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::mutate_in(std::string document_id,
                      const mutate_in_specs& specs,
                      const mutate_in_options& options,
                      mutate_in_handler&& handler) const
{
    return impl_->mutate_in(std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::mutate_in(std::string document_id, const mutate_in_specs& specs, const mutate_in_options& options) const
  -> std::future<std::pair<subdocument_error_context, mutate_in_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, mutate_in_result>>>();
    auto future = barrier->get_future();
    mutate_in(std::move(document_id), specs, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::lookup_in(std::string document_id,
                      const lookup_in_specs& specs,
                      const lookup_in_options& options,
                      lookup_in_handler&& handler) const
{
    return impl_->lookup_in(std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in(std::string document_id, const lookup_in_specs& specs, const lookup_in_options& options) const
  -> std::future<std::pair<subdocument_error_context, lookup_in_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, lookup_in_result>>>();
    auto future = barrier->get_future();
    lookup_in(std::move(document_id), specs, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::lookup_in_all_replicas(std::string document_id,
                                   const lookup_in_specs& specs,
                                   const lookup_in_all_replicas_options& options,
                                   lookup_in_all_replicas_handler&& handler) const
{
    return impl_->lookup_in_all_replicas(std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in_all_replicas(std::string document_id,
                                   const lookup_in_specs& specs,
                                   const lookup_in_all_replicas_options& options) const
  -> std::future<std::pair<subdocument_error_context, lookup_in_all_replicas_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, lookup_in_all_replicas_result>>>();
    auto future = barrier->get_future();
    lookup_in_all_replicas(std::move(document_id), specs, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::lookup_in_any_replica(std::string document_id,
                                  const lookup_in_specs& specs,
                                  const lookup_in_any_replica_options& options,
                                  lookup_in_any_replica_handler&& handler) const
{
    return impl_->lookup_in_any_replica(std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in_any_replica(std::string document_id, const lookup_in_specs& specs, const lookup_in_any_replica_options& options) const
  -> std::future<std::pair<subdocument_error_context, lookup_in_replica_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, lookup_in_replica_result>>>();
    auto future = barrier->get_future();
    lookup_in_any_replica(std::move(document_id), specs, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::get_and_lock(std::string document_id,
                         std::chrono::seconds lock_duration,
                         const get_and_lock_options& options,
                         get_and_lock_handler&& handler) const
{
    return impl_->get_and_lock(std::move(document_id), lock_duration, options.build(), std::move(handler));
}

auto
collection::get_and_lock(std::string document_id, std::chrono::seconds lock_duration, const get_and_lock_options& options) const
  -> std::future<std::pair<key_value_error_context, get_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
    auto future = barrier->get_future();
    get_and_lock(std::move(document_id), lock_duration, options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::unlock(std::string document_id, couchbase::cas cas, const unlock_options& options, unlock_handler&& handler) const
{
    return impl_->unlock(std::move(document_id), cas, options.build(), std::move(handler));
}

auto
collection::unlock(std::string document_id, couchbase::cas cas, const unlock_options& options) const -> std::future<key_value_error_context>
{
    auto barrier = std::make_shared<std::promise<key_value_error_context>>();
    auto future = barrier->get_future();
    unlock(std::move(document_id), cas, options, [barrier](auto ctx) { barrier->set_value({ std::move(ctx) }); });
    return future;
}

void
collection::exists(std::string document_id, const exists_options& options, exists_handler&& handler) const
{
    return impl_->exists(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::exists(std::string document_id, const exists_options& options) const
  -> std::future<std::pair<key_value_error_context, exists_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, exists_result>>>();
    auto future = barrier->get_future();
    exists(std::move(document_id), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::upsert(std::string document_id, codec::encoded_value document, const upsert_options& options, upsert_handler&& handler) const
{
    return impl_->upsert(std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::upsert(std::string document_id, codec::encoded_value document, const upsert_options& options) const
  -> std::future<std::pair<key_value_error_context, mutation_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
    auto future = barrier->get_future();
    upsert(std::move(document_id), std::move(document), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::insert(std::string document_id, codec::encoded_value document, const insert_options& options, insert_handler&& handler) const
{
    return impl_->insert(std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::insert(std::string document_id, codec::encoded_value document, const insert_options& options) const
  -> std::future<std::pair<key_value_error_context, mutation_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
    auto future = barrier->get_future();
    insert(std::move(document_id), std::move(document), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::replace(std::string document_id, codec::encoded_value document, const replace_options& options, replace_handler&& handler) const
{
    return impl_->replace(std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::replace(std::string document_id, codec::encoded_value document, const replace_options& options) const
  -> std::future<std::pair<key_value_error_context, mutation_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
    auto future = barrier->get_future();
    replace(std::move(document_id), std::move(document), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
collection::scan(const couchbase::scan_type& scan_type, const couchbase::scan_options& options, couchbase::scan_handler&& handler) const
{
    return impl_->scan(scan_type.build(), options.build(), std::move(handler));
}

auto
collection::scan(const couchbase::scan_type& scan_type, const couchbase::scan_options& options) const
  -> std::future<std::pair<std::error_code, scan_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<std::error_code, scan_result>>>();
    auto future = barrier->get_future();
    scan(scan_type, options, [barrier](auto ec, auto result) { barrier->set_value({ ec, std::move(result) }); });
    return future;
}
} // namespace couchbase
