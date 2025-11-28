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

#include <couchbase/collection.hxx>

#include "error.hxx"
#include "get_all_replicas.hxx"
#include "get_any_replica.hxx"
#include "internal_scan_result.hxx"
#include "observability_recorder.hxx"
#include "observe_poll.hxx"

#include "core/agent_group.hxx"
#include "core/agent_group_config.hxx"
#include "core/cluster.hxx"
#include "core/impl/subdoc/command.hxx"
#include "core/logger/logger.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_exists.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_all_replicas.hxx"
#include "core/operations/document_get_and_lock.hxx"
#include "core/operations/document_get_and_touch.hxx"
#include "core/operations/document_get_any_replica.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_lookup_in_all_replicas.hxx"
#include "core/operations/document_lookup_in_any_replica.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_touch.hxx"
#include "core/operations/document_unlock.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/range_scan_options.hxx"
#include "core/range_scan_orchestrator.hxx"
#include "core/range_scan_orchestrator_options.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/binary_collection.hxx>
#include <couchbase/cas.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/collection_query_index_manager.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/error.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/exists_options.hxx>
#include <couchbase/exists_result.hxx>
#include <couchbase/expiry.hxx>
#include <couchbase/get_all_replicas_options.hxx>
#include <couchbase/get_and_lock_options.hxx>
#include <couchbase/get_and_touch_options.hxx>
#include <couchbase/get_any_replica_options.hxx>
#include <couchbase/get_options.hxx>
#include <couchbase/get_replica_result.hxx>
#include <couchbase/get_result.hxx>
#include <couchbase/insert_options.hxx>
#include <couchbase/lookup_in_all_replicas_options.hxx>
#include <couchbase/lookup_in_any_replica_options.hxx>
#include <couchbase/lookup_in_options.hxx>
#include <couchbase/lookup_in_replica_result.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_options.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/remove_options.hxx>
#include <couchbase/replace_options.hxx>
#include <couchbase/replicate_to.hxx>
#include <couchbase/result.hxx>
#include <couchbase/scan_options.hxx>
#include <couchbase/scan_type.hxx>
#include <couchbase/touch_options.hxx>
#include <couchbase/unlock_options.hxx>
#include <couchbase/upsert_options.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include "core/tracing/attribute_helpers.hxx"

namespace couchbase
{
class collection_impl : public std::enable_shared_from_this<collection_impl>
{
public:
  collection_impl(core::cluster core,
                  std::string_view bucket_name,
                  std::string_view scope_name,
                  std::string_view name,
                  std::shared_ptr<crypto::manager> crypto_manager)
    : core_{ std::move(core) }
    , bucket_name_{ bucket_name }
    , scope_name_{ scope_name }
    , name_{ name }
    , crypto_manager_{ std::move(crypto_manager) }
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

  [[nodiscard]] auto crypto_manager() const -> const std::shared_ptr<crypto::manager>&
  {
    return crypto_manager_;
  }

  void get(std::string document_key, get_options::built options, get_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_get, options.parent_span);

    if (!options.with_expiry && options.projections.empty()) {
      core::operations::get_request request{
        core::document_id{
          bucket_name_,
          scope_name_,
          name_,
          std::move(document_key),
        },
        {},
        {},
        options.timeout,
        { options.retry_strategy },
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec),
         crypto_manager = crypto_manager_,
         handler = std::move(handler)](auto resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(
            core::impl::make_error(std::move(resp.ctx)),
            get_result{
              resp.cas, { std::move(resp.value), resp.flags }, {}, std::move(crypto_manager) });
        });
    }
    core::operations::get_projected_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      options.projections,
      options.with_expiry,
      {},
      false,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       crypto_manager = crypto_manager_,
       handler = std::move(handler)](auto resp) mutable {
        std::optional<std::chrono::system_clock::time_point> expiry_time{};
        if (resp.expiry && resp.expiry.value() > 0) {
          expiry_time.emplace(std::chrono::seconds{ resp.expiry.value() });
        }
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)),
                       get_result{ resp.cas,
                                   { std::move(resp.value), resp.flags },
                                   expiry_time,
                                   std::move(crypto_manager) });
      });
  }

  void get_and_touch(std::string document_key,
                     std::uint32_t expiry,
                     get_and_touch_options::built options,
                     get_and_touch_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(core::tracing::operation::mcbp_get_and_touch,
                                                 options.parent_span);

    core::operations::get_and_touch_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      expiry,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };

    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       crypto_manager = crypto_manager_,
       handler = std::move(handler)](auto resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(
          core::impl::make_error(std::move(resp.ctx)),
          get_result{
            resp.cas, { std::move(resp.value), resp.flags }, {}, std::move(crypto_manager) });
      });
  }

  void touch(std::string document_key,
             std::uint32_t expiry,
             touch_options::built options,
             touch_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_touch, options.parent_span);

    core::operations::touch_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      expiry,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](const auto& resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)), result{ resp.cas });
      });
  }

  void get_any_replica(std::string document_key,
                       const get_any_replica_options::built& options,
                       core::impl::movable_get_any_replica_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(core::tracing::operation::mcbp_get_any_replica,
                                                 options.parent_span);

    core::operations::get_any_replica_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      options.timeout,
      options.read_preference,
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       crypto_manager = crypto_manager_,
       handler = std::move(handler)](auto resp) mutable {
        obs_rec->finish(resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)),
                       get_replica_result{
                         resp.cas,
                         resp.replica,
                         { std::move(resp.value), resp.flags },
                         std::move(crypto_manager),
                       });
      });
  }

  void get_all_replicas(std::string document_key,
                        const get_all_replicas_options::built& options,
                        core::impl::movable_get_all_replicas_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(core::tracing::operation::mcbp_get_all_replicas,
                                                 options.parent_span);

    core::operations::get_all_replicas_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      options.timeout,
      options.read_preference,
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       crypto_manager = crypto_manager_,
       handler = std::move(handler)](auto resp) mutable {
        get_all_replicas_result result{};
        for (auto& entry : resp.entries) {
          result.emplace_back(get_replica_result{
            entry.cas,
            entry.replica,
            { std::move(entry.value), entry.flags },
            crypto_manager,
          });
        }
        obs_rec->finish(resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)), std::move(result));
      });
  }

  void remove(std::string document_key,
              remove_options::built options,
              remove_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_remove, options.parent_span);

    auto id = core::document_id{
      bucket_name_,
      scope_name_,
      name_,
      std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
      core::operations::remove_request request{
        std::move(id),
        {},
        {},
        options.cas,
        options.durability_level,
        options.timeout,
        { options.retry_strategy },
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          if (resp.ctx.ec()) {
            return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
          }
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        });
    }

    core::operations::remove_request request{
      id,
      {},
      {},
      options.cas,
      durability_level::none,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       core = core_,
       id = std::move(id),
       options,
       handler = std::move(handler)](auto&& resp) mutable {
        if (resp.ctx.ec()) {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        }
        auto token = resp.token;
        core::impl::initiate_observe_poll(
          core,
          std::move(id),
          token,
          options.timeout,
          options.persist_to,
          options.replicate_to,
          [obs_rec = std::move(obs_rec), resp, handler = std::move(handler)](
            std::error_code ec) mutable {
            obs_rec->finish(resp.ctx.retry_attempts(), ec);
            if (ec) {
              resp.ctx.override_ec(ec);
              return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
            }
            return handler(core::impl::make_error(std::move(resp.ctx)),
                           mutation_result{ resp.cas, std::move(resp.token) });
          });
      });
  }

  void get_and_lock(std::string document_key,
                    std::chrono::seconds lock_duration,
                    get_and_lock_options::built options,
                    get_and_lock_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(core::tracing::operation::mcbp_get_and_lock,
                                                 options.parent_span);

    core::operations::get_and_lock_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      static_cast<uint32_t>(lock_duration.count()),
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       crypto_manager = crypto_manager_,
       handler = std::move(handler)](auto&& resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(
          core::impl::make_error(std::move(resp.ctx)),
          get_result{
            resp.cas, { std::move(resp.value), resp.flags }, {}, std::move(crypto_manager) });
      });
  }

  void unlock(std::string document_key,
              couchbase::cas cas,
              unlock_options::built options,
              unlock_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_unlock, options.parent_span);

    core::operations::unlock_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      cas,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto&& resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)));
      });
  }

  void exists(std::string document_key,
              exists_options::built options,
              exists_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_exists, options.parent_span);

    core::operations::exists_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      {},
      {},
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto&& resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)),
                       exists_result{ resp.cas, resp.exists() });
      });
  }

  void lookup_in(std::string document_key,
                 const std::vector<core::impl::subdoc::command>& specs,
                 lookup_in_options::built options,
                 lookup_in_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_lookup_in, options.parent_span);

    core::operations::lookup_in_request request{
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
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
        obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());

        if (resp.ctx.ec()) {
          return handler(core::impl::make_error(std::move(resp.ctx)), lookup_in_result{});
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
        return handler(core::impl::make_error(std::move(resp.ctx)),
                       lookup_in_result{ resp.cas, std::move(entries), resp.deleted });
      });
  }

  void lookup_in_all_replicas(std::string document_key,
                              const std::vector<core::impl::subdoc::command>& specs,
                              const lookup_in_all_replicas_options::built& options,
                              lookup_in_all_replicas_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_lookup_in_all_replicas, options.parent_span);

    core::operations::lookup_in_all_replicas_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      specs,
      options.timeout,
      obs_rec->operation_span(),
      options.read_preference,
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
        lookup_in_all_replicas_result result{};
        for (auto& res : resp.entries) {
          std::vector<lookup_in_result::entry> entries;
          entries.reserve(res.fields.size());
          for (auto& field : res.fields) {
            entries.emplace_back(lookup_in_result::entry{
              std::move(field.path),
              std::move(field.value),
              field.original_index,
              field.exists,
              field.ec,
            });
          }
          result.emplace_back(lookup_in_replica_result{
            res.cas,
            std::move(entries),
            res.deleted,
            res.is_replica,
          });
        }
        obs_rec->finish(resp.ctx.ec());
        return handler(core::impl::make_error(std::move(resp.ctx)), result);
      });
  }

  void lookup_in_any_replica(std::string document_key,
                             const std::vector<core::impl::subdoc::command>& specs,
                             const lookup_in_any_replica_options::built& options,
                             lookup_in_any_replica_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_lookup_in_any_replica, options.parent_span);

    core::operations::lookup_in_any_replica_request request{
      core::document_id{
        bucket_name_,
        scope_name_,
        name_,
        std::move(document_key),
      },
      specs,
      options.timeout,
      obs_rec->operation_span(),
      options.read_preference,
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
        std::vector<lookup_in_result::entry> entries;
        entries.reserve(resp.fields.size());
        for (auto& field : resp.fields) {
          entries.emplace_back(lookup_in_result::entry{
            std::move(field.path),
            std::move(field.value),
            field.original_index,
            field.exists,
            field.ec,
          });
        }
        obs_rec->finish(resp.ctx.ec());
        return handler(
          core::impl::make_error(std::move(resp.ctx)),
          lookup_in_replica_result{ resp.cas, std::move(entries), resp.deleted, resp.is_replica });
      });
  }

  void mutate_in(std::string document_key,
                 const std::vector<core::impl::subdoc::command>& specs,
                 mutate_in_options::built options,
                 mutate_in_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_mutate_in, options.parent_span, options.durability_level);

    auto id = core::document_id{
      bucket_name_,
      scope_name_,
      name_,
      std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
      core::operations::mutate_in_request request{
        std::move(id),
        {},
        {},
        options.cas,
        options.access_deleted,
        options.create_as_deleted,
        false,
        options.expiry,
        options.store_semantics,
        specs,
        options.durability_level,
        options.timeout,
        { options.retry_strategy },
        options.preserve_expiry,
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          if (resp.ctx.ec()) {
            return handler(core::impl::make_error(std::move(resp.ctx)), mutate_in_result{});
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
          return handler(
            core::impl::make_error(std::move(resp.ctx)),
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
      false,
      options.expiry,
      options.store_semantics,
      specs,
      durability_level::none,
      options.timeout,
      { options.retry_strategy },
      options.preserve_expiry,
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       core = core_,
       id = std::move(id),
       options,
       handler = std::move(handler)](auto&& resp) mutable {
        if (resp.ctx.ec()) {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(core::impl::make_error(std::move(resp.ctx)), mutate_in_result{});
        }

        auto token = resp.token;
        core::impl::initiate_observe_poll(
          core,
          std::move(id),
          token,
          options.timeout,
          options.persist_to,
          options.replicate_to,
          [obs_rec = std::move(obs_rec), resp, handler = std::move(handler)](
            std::error_code ec) mutable {
            obs_rec->finish(resp.ctx.retry_attempts(), ec);
            if (ec) {
              resp.ctx.override_ec(ec);
              return handler(core::impl::make_error(std::move(resp.ctx)), mutate_in_result{});
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
            return handler(core::impl::make_error(std::move(resp.ctx)),
                           mutate_in_result{
                             resp.cas, std::move(resp.token), std::move(entries), resp.deleted });
          });
      });
  }

  void upsert(std::string document_key,
              std::variant<codec::encoded_value, std::function<codec::encoded_value()>> value,
              upsert_options::built options,
              upsert_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_upsert, options.parent_span, options.durability_level);

    auto [data, flags] = get_encoded_value(std::move(value), obs_rec);
    auto id = core::document_id{
      bucket_name_,
      scope_name_,
      name_,
      std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
      core::operations::upsert_request request{
        std::move(id),
        std::move(data),
        {},
        {},
        flags,
        options.expiry,
        options.durability_level,
        options.timeout,
        { options.retry_strategy },
        options.preserve_expiry,
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        });
    }

    core::operations::upsert_request request{
      id,
      std::move(data),
      {},
      {},
      flags,
      options.expiry,
      durability_level::none,
      options.timeout,
      { options.retry_strategy },
      options.preserve_expiry,
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       core = core_,
       id = std::move(id),
       options,
       handler = std::move(handler)](auto resp) mutable {
        if (resp.ctx.ec()) {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        }

        auto token = resp.token;
        core::impl::initiate_observe_poll(
          core,
          std::move(id),
          token,
          options.timeout,
          options.persist_to,
          options.replicate_to,
          [obs_rec = std::move(obs_rec), resp, handler = std::move(handler)](
            std::error_code ec) mutable {
            obs_rec->finish(resp.ctx.retry_attempts(), ec);
            if (ec) {
              resp.ctx.override_ec(ec);
              return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
            }
            return handler(core::impl::make_error(std::move(resp.ctx)),
                           mutation_result{ resp.cas, std::move(resp.token) });
          });
      });
  }

  void insert(std::string document_key,
              std::variant<codec::encoded_value, std::function<codec::encoded_value()>> value,
              insert_options::built options,
              insert_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_insert, options.parent_span, options.durability_level);

    auto [data, flags] = get_encoded_value(std::move(value), obs_rec);
    auto id = core::document_id{
      bucket_name_,
      scope_name_,
      name_,
      std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
      core::operations::insert_request request{
        std::move(id),
        std::move(data),
        {},
        {},
        flags,
        options.expiry,
        options.durability_level,
        options.timeout,
        { options.retry_strategy },
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto&& resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          if (resp.ctx.ec()) {
            return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
          }
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        });
    }

    core::operations::insert_request request{
      id,
      std::move(data),
      {},
      {},
      flags,
      options.expiry,
      durability_level::none,
      options.timeout,
      { options.retry_strategy },
      obs_rec->operation_span(),
    };
    return core_.execute(
      std::move(request),
      [obs_rec = std::move(obs_rec),
       core = core_,
       id = std::move(id),
       options,
       handler = std::move(handler)](auto resp) mutable {
        if (resp.ctx.ec()) {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        }

        auto token = resp.token;
        core::impl::initiate_observe_poll(
          core,
          std::move(id),
          token,
          options.timeout,
          options.persist_to,
          options.replicate_to,
          [obs_rec = std::move(obs_rec), resp, handler = std::move(handler)](
            std::error_code ec) mutable {
            obs_rec->finish(resp.ctx.retry_attempts(), ec);
            if (ec) {
              resp.ctx.override_ec(ec);
              return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
            }
            return handler(core::impl::make_error(std::move(resp.ctx)),
                           mutation_result{ resp.cas, std::move(resp.token) });
          });
      });
  }

  void replace(std::string document_key,
               std::variant<codec::encoded_value, std::function<codec::encoded_value()>> value,
               replace_options::built options,
               replace_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mcbp_replace, options.parent_span, options.durability_level);

    auto [data, flags] = get_encoded_value(std::move(value), obs_rec);

    auto id = core::document_id{
      bucket_name_,
      scope_name_,
      name_,
      std::move(document_key),
    };
    if (options.persist_to == persist_to::none && options.replicate_to == replicate_to::none) {
      core::operations::replace_request request{
        std::move(id),
        std::move(data),
        {},
        {},
        flags,
        options.expiry,
        options.cas,
        options.durability_level,
        options.timeout,
        { options.retry_strategy },
        options.preserve_expiry,
        obs_rec->operation_span(),
      };
      return core_.execute(
        std::move(request),
        [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable {
          obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
          if (resp.ctx.ec()) {
            return handler(core::impl::make_error(std::move(resp.ctx)), mutation_result{});
          }
          return handler(core::impl::make_error(std::move(resp.ctx)),
                         mutation_result{ resp.cas, std::move(resp.token) });
        });
    }

    core::operations::replace_request request{
      id,
      std::move(data),
      {},
      {},
      flags,
      options.expiry,
      options.cas,
      durability_level::none,
      options.timeout,
      { options.retry_strategy },
      options.preserve_expiry,
      obs_rec->operation_span(),
    };
    return core_.execute(std::move(request),
                         [obs_rec = std::move(obs_rec),
                          core = core_,
                          id = std::move(id),
                          options,
                          handler = std::move(handler)](auto&& resp) mutable {
                           if (resp.ctx.ec()) {
                             obs_rec->finish(resp.ctx.retry_attempts(), resp.ctx.ec());
                             return handler(core::impl::make_error(std::move(resp.ctx)),
                                            mutation_result{ resp.cas, std::move(resp.token) });
                           }

                           auto token = resp.token;
                           core::impl::initiate_observe_poll(
                             core,
                             std::move(id),
                             token,
                             options.timeout,
                             options.persist_to,
                             options.replicate_to,
                             [obs_rec = std::move(obs_rec), resp, handler = std::move(handler)](
                               std::error_code ec) mutable {
                               obs_rec->finish(resp.ctx.retry_attempts(), ec);
                               if (ec) {
                                 resp.ctx.override_ec(ec);
                                 return handler(core::impl::make_error(std::move(resp.ctx)),
                                                mutation_result{});
                               }
                               return handler(core::impl::make_error(std::move(resp.ctx)),
                                              mutation_result{ resp.cas, std::move(resp.token) });
                             });
                         });
  }

  void scan(scan_type::built scan_type, scan_options::built options, scan_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mcbp_scan, options.parent_span);

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
    } else {
      auto [ec, origin] = core_.origin();
      if (ec) {
        handler({ ec }, {});
        return;
      }
      orchestrator_opts.timeout = origin.options().key_value_scan_timeout;
    }

    std::variant<std::monostate, core::range_scan, core::prefix_scan, core::sampling_scan>
      core_scan_type{};
    switch (scan_type.type) {
      case scan_type::built::prefix_scan:
        core_scan_type = core::prefix_scan{
          scan_type.prefix,
        };
        break;
      case scan_type::built::range_scan:
        core_scan_type = core::range_scan{
          (scan_type.from)
            ? std::make_optional(core::scan_term{ scan_type.from->term, scan_type.from->exclusive })
            : std::nullopt,
          (scan_type.to)
            ? std::make_optional(core::scan_term{ scan_type.to->term, scan_type.to->exclusive })
            : std::nullopt,
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
      bucket_name_,
      [this,
       obs_rec = std::move(obs_rec),
       handler = std::move(handler),
       orchestrator_opts,
       core_scan_type](std::error_code ec) mutable {
        if (ec) {
          obs_rec->finish(ec);
          return handler(error(ec), {});
        }
        return core_.with_bucket_configuration(
          bucket_name_,
          [this,
           obs_rec = std::move(obs_rec),
           handler = std::move(handler),
           orchestrator_opts,
           core_scan_type](std::error_code ec,
                           const std::shared_ptr<core::topology::configuration>& config) mutable {
            if (ec) {
              obs_rec->finish(ec);
              return handler(
                error(ec, "An error occurred when attempting to fetch the bucket configuration."),
                {});
            }
            if (!config->capabilities.supports_range_scan()) {
              obs_rec->finish(ec);
              return handler(error(errc::common::feature_not_available,
                                   "This bucket does not support range scan."),
                             {});
            }
            auto agent_group =
              core::agent_group(core_.io_context(), core::agent_group_config{ { core_ } });
            ec = agent_group.open_bucket(bucket_name_);
            if (ec) {
              obs_rec->finish(ec);
              return handler(error(ec,
                                   fmt::format("An error occurred while opening the `{}` bucket.",
                                               bucket_name_)),
                             {});
            }
            auto agent = agent_group.get_agent(bucket_name_);
            if (!agent.has_value()) {
              obs_rec->finish(ec);
              return handler(
                error(agent.error(),
                      fmt::format(
                        "An error occurred while getting an operation agent for the `{}` bucket",
                        bucket_name_)),
                {});
            }
            if (!config->vbmap.has_value() || config->vbmap->empty()) {
              CB_LOG_WARNING("Unable to get vbucket map for `{}` - cannot perform scan operation",
                             bucket_name_);
              obs_rec->finish(ec);
              return handler(error(errc::common::request_canceled,
                                   "No vbucket map included with the bucket config"),
                             {});
            }

            auto orchestrator = core::range_scan_orchestrator(core_.io_context(),
                                                              agent.value(),
                                                              config->vbmap.value(),
                                                              scope_name_,
                                                              name_,
                                                              core_scan_type,
                                                              orchestrator_opts);
            return orchestrator.scan(
              [obs_rec = std::move(obs_rec),
               crypto_manager = crypto_manager_,
               handler = std::move(handler)](auto ec, auto core_scan_result) mutable {
                obs_rec->finish(ec);
                if (ec) {
                  return handler(error(ec, "Error while starting the range scan"), {});
                }
                auto internal_result = std::make_shared<internal_scan_result>(
                  std::move(core_scan_result), std::move(crypto_manager));
                return handler({}, scan_result{ internal_result });
              });
          });
      });
  }

private:
  static auto get_encoded_value(
    std::variant<codec::encoded_value, std::function<codec::encoded_value()>> value,
    const std::unique_ptr<core::impl::observability_recorder>& obs_rec) -> codec::encoded_value
  {
    if (std::holds_alternative<codec::encoded_value>(value)) {
      return std::get<codec::encoded_value>(value);
    }
    const auto request_encoding_span = obs_rec->create_request_encoding_span();
    auto encoded = std::get<std::function<codec::encoded_value()>>(value)();
    request_encoding_span->end();
    return encoded;
  }

  [[nodiscard]] auto create_observability_recorder(
    const std::string& operation_name,
    const std::shared_ptr<tracing::request_span>& parent_span,
    const std::optional<durability_level> durability = {}) const
    -> std::unique_ptr<core::impl::observability_recorder>
  {
    auto rec = core::impl::observability_recorder::create(
      operation_name, parent_span, core_.tracer(), core_.meter());

    rec->with_service(core::tracing::service::key_value);
    rec->with_bucket_name(bucket_name_);
    rec->with_scope_name(scope_name_);
    rec->with_collection_name(name_);
    if (durability.has_value()) {
      rec->with_durability(durability.value());
    }

    return rec;
  }

  core::cluster core_;
  std::string bucket_name_;
  std::string scope_name_;
  std::string name_;
  std::shared_ptr<crypto::manager> crypto_manager_;
};

collection::collection(core::cluster core,
                       std::string_view bucket_name,
                       std::string_view scope_name,
                       std::string_view name,
                       std::shared_ptr<crypto::manager> crypto_manager)
  : impl_(std::make_shared<collection_impl>(std::move(core),
                                            bucket_name,
                                            scope_name,
                                            name,
                                            std::move(crypto_manager)))
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

auto
collection::crypto_manager() const -> const std::shared_ptr<crypto::manager>&
{
  return impl_->crypto_manager();
}

void
collection::get(std::string document_id, const get_options& options, get_handler&& handler) const
{
  return impl_->get(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get(std::string document_id, const get_options& options) const
  -> std::future<std::pair<error, get_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_result>>>();
  auto future = barrier->get_future();
  get(std::move(document_id), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::get_and_touch(std::string document_id,
                          std::chrono::seconds duration,
                          const get_and_touch_options& options,
                          get_and_touch_handler&& handler) const
{
  return impl_->get_and_touch(std::move(document_id),
                              core::impl::expiry_relative(duration),
                              options.build(),
                              std::move(handler));
}

auto
collection::get_and_touch(std::string document_id,
                          std::chrono::seconds duration,
                          const get_and_touch_options& options) const
  -> std::future<std::pair<error, get_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_result>>>();
  auto future = barrier->get_future();
  get_and_touch(std::move(document_id), duration, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::get_and_touch(std::string document_id,
                          std::chrono::system_clock::time_point time_point,
                          const get_and_touch_options& options,
                          get_and_touch_handler&& handler) const
{
  return impl_->get_and_touch(std::move(document_id),
                              core::impl::expiry_absolute(time_point),
                              options.build(),
                              std::move(handler));
}

auto
collection::get_and_touch(std::string document_id,
                          std::chrono::system_clock::time_point time_point,
                          const get_and_touch_options& options) const
  -> std::future<std::pair<error, get_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_result>>>();
  auto future = barrier->get_future();
  get_and_touch(std::move(document_id), time_point, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::touch(std::string document_id,
                  std::chrono::seconds duration,
                  const touch_options& options,
                  touch_handler&& handler) const
{
  return impl_->touch(std::move(document_id),
                      core::impl::expiry_relative(duration),
                      options.build(),
                      std::move(handler));
}

auto
collection::touch(std::string document_id,
                  std::chrono::seconds duration,
                  const touch_options& options) const -> std::future<std::pair<error, result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, result>>>();
  auto future = barrier->get_future();
  touch(std::move(document_id), duration, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::touch(std::string document_id,
                  std::chrono::system_clock::time_point time_point,
                  const touch_options& options,
                  touch_handler&& handler) const
{
  return impl_->touch(std::move(document_id),
                      core::impl::expiry_absolute(time_point),
                      options.build(),
                      std::move(handler));
}

auto
collection::touch(std::string document_id,
                  std::chrono::system_clock::time_point time_point,
                  const touch_options& options) const -> std::future<std::pair<error, result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, result>>>();
  auto future = barrier->get_future();
  touch(std::move(document_id), time_point, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::get_any_replica(std::string document_id,
                            const get_any_replica_options& options,
                            get_any_replica_handler&& handler) const
{
  return impl_->get_any_replica(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get_any_replica(std::string document_id, const get_any_replica_options& options) const
  -> std::future<std::pair<error, get_replica_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_replica_result>>>();
  auto future = barrier->get_future();
  get_any_replica(std::move(document_id), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::get_all_replicas(std::string document_id,
                             const get_all_replicas_options& options,
                             get_all_replicas_handler&& handler) const
{
  return impl_->get_all_replicas(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::get_all_replicas(std::string document_id, const get_all_replicas_options& options) const
  -> std::future<std::pair<error, get_all_replicas_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_all_replicas_result>>>();
  auto future = barrier->get_future();
  get_all_replicas(std::move(document_id), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::remove(std::string document_id,
                   const remove_options& options,
                   remove_handler&& handler) const
{
  return impl_->remove(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::remove(std::string document_id, const remove_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  remove(std::move(document_id), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::mutate_in(std::string document_id,
                      const mutate_in_specs& specs,
                      const mutate_in_options& options,
                      mutate_in_handler&& handler) const
{
  return impl_->mutate_in(
    std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::mutate_in(std::string document_id,
                      const mutate_in_specs& specs,
                      const mutate_in_options& options) const
  -> std::future<std::pair<error, mutate_in_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutate_in_result>>>();
  auto future = barrier->get_future();
  mutate_in(std::move(document_id), specs, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::lookup_in(std::string document_id,
                      const lookup_in_specs& specs,
                      const lookup_in_options& options,
                      lookup_in_handler&& handler) const
{
  return impl_->lookup_in(
    std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in(std::string document_id,
                      const lookup_in_specs& specs,
                      const lookup_in_options& options) const
  -> std::future<std::pair<error, lookup_in_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, lookup_in_result>>>();
  auto future = barrier->get_future();
  lookup_in(std::move(document_id), specs, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::lookup_in_all_replicas(std::string document_id,
                                   const lookup_in_specs& specs,
                                   const lookup_in_all_replicas_options& options,
                                   lookup_in_all_replicas_handler&& handler) const
{
  return impl_->lookup_in_all_replicas(
    std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in_all_replicas(std::string document_id,
                                   const lookup_in_specs& specs,
                                   const lookup_in_all_replicas_options& options) const
  -> std::future<std::pair<error, lookup_in_all_replicas_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, lookup_in_all_replicas_result>>>();
  auto future = barrier->get_future();
  lookup_in_all_replicas(std::move(document_id), specs, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::lookup_in_any_replica(std::string document_id,
                                  const lookup_in_specs& specs,
                                  const lookup_in_any_replica_options& options,
                                  lookup_in_any_replica_handler&& handler) const
{
  return impl_->lookup_in_any_replica(
    std::move(document_id), specs.specs(), options.build(), std::move(handler));
}

auto
collection::lookup_in_any_replica(std::string document_id,
                                  const lookup_in_specs& specs,
                                  const lookup_in_any_replica_options& options) const
  -> std::future<std::pair<error, lookup_in_replica_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, lookup_in_replica_result>>>();
  auto future = barrier->get_future();
  lookup_in_any_replica(std::move(document_id), specs, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::get_and_lock(std::string document_id,
                         std::chrono::seconds lock_duration,
                         const get_and_lock_options& options,
                         get_and_lock_handler&& handler) const
{
  return impl_->get_and_lock(
    std::move(document_id), lock_duration, options.build(), std::move(handler));
}

auto
collection::get_and_lock(std::string document_id,
                         std::chrono::seconds lock_duration,
                         const get_and_lock_options& options) const
  -> std::future<std::pair<error, get_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, get_result>>>();
  auto future = barrier->get_future();
  get_and_lock(std::move(document_id), lock_duration, options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::unlock(std::string document_id,
                   couchbase::cas cas,
                   const unlock_options& options,
                   unlock_handler&& handler) const
{
  return impl_->unlock(std::move(document_id), cas, options.build(), std::move(handler));
}

auto
collection::unlock(std::string document_id, couchbase::cas cas, const unlock_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  unlock(std::move(document_id), cas, options, [barrier](auto err) {
    barrier->set_value({ std::move(err) });
  });
  return future;
}

void
collection::exists(std::string document_id,
                   const exists_options& options,
                   exists_handler&& handler) const
{
  return impl_->exists(std::move(document_id), options.build(), std::move(handler));
}

auto
collection::exists(std::string document_id, const exists_options& options) const
  -> std::future<std::pair<error, exists_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, exists_result>>>();
  auto future = barrier->get_future();
  exists(std::move(document_id), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::upsert(std::string document_id,
                   codec::encoded_value document,
                   const upsert_options& options,
                   upsert_handler&& handler) const
{
  return impl_->upsert(
    std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::upsert(std::string document_id,
                   codec::encoded_value document,
                   const upsert_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  upsert(std::move(document_id), std::move(document), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

auto
collection::upsert(std::string document_id,
                   std::function<codec::encoded_value()> document_fn,
                   const upsert_options& options,
                   upsert_handler&& handler) const -> void
{
  impl_->upsert(
    std::move(document_id), std::move(document_fn), options.build(), std::move(handler));
}

auto
collection::upsert(std::string document_id,
                   std::function<codec::encoded_value()> document_fn,
                   const upsert_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  upsert(std::move(document_id), std::move(document_fn), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::insert(std::string document_id,
                   codec::encoded_value document,
                   const insert_options& options,
                   insert_handler&& handler) const
{
  return impl_->insert(
    std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::insert(std::string document_id,
                   codec::encoded_value document,
                   const insert_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  insert(std::move(document_id), std::move(document), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

auto
collection::insert(std::string document_id,
                   std::function<codec::encoded_value()> document_fn,
                   const insert_options& options,
                   insert_handler&& handler) const -> void
{
  impl_->insert(
    std::move(document_id), std::move(document_fn), options.build(), std::move(handler));
}

auto
collection::insert(std::string document_id,
                   std::function<codec::encoded_value()> document_fn,
                   const insert_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  insert(std::move(document_id), std::move(document_fn), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::replace(std::string document_id,
                    codec::encoded_value document,
                    const replace_options& options,
                    replace_handler&& handler) const
{
  return impl_->replace(
    std::move(document_id), std::move(document), options.build(), std::move(handler));
}

auto
collection::replace(std::string document_id,
                    codec::encoded_value document,
                    const replace_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  replace(std::move(document_id), std::move(document), options, [barrier](auto err, auto result) {
    barrier->set_value({ std::move(err), std::move(result) });
  });
  return future;
}

void
collection::replace(std::string document_id,
                    std::function<codec::encoded_value()> document_fn,
                    const replace_options& options,
                    replace_handler&& handler) const
{
  impl_->replace(
    std::move(document_id), std::move(document_fn), options.build(), std::move(handler));
}

auto
collection::replace(std::string document_id,
                    std::function<codec::encoded_value()> document_fn,
                    const replace_options& options) const
  -> std::future<std::pair<error, mutation_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, mutation_result>>>();
  auto future = barrier->get_future();
  replace(
    std::move(document_id), std::move(document_fn), options, [barrier](auto err, auto result) {
      barrier->set_value({ std::move(err), std::move(result) });
    });
  return future;
}

void
collection::scan(const couchbase::scan_type& scan_type,
                 const couchbase::scan_options& options,
                 couchbase::scan_handler&& handler) const
{
  return impl_->scan(scan_type.build(), options.build(), std::move(handler));
}

auto
collection::scan(const couchbase::scan_type& scan_type,
                 const couchbase::scan_options& options) const
  -> std::future<std::pair<error, scan_result>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, scan_result>>>();
  auto future = barrier->get_future();
  scan(scan_type, options, [barrier](const auto& err, auto result) {
    barrier->set_value({ err, std::move(result) });
  });
  return future;
}
} // namespace couchbase
