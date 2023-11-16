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

#include "core/cluster.hxx"

#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_drop.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"
#include "core/operations/management/scope_get_all.hxx"
#include "internal_manager_error_context.hxx"

#include <couchbase/collection_manager.hxx>

#include <memory>

namespace couchbase
{
namespace
{
template<typename Response>
manager_error_context
build_context(Response& resp)
{
    return manager_error_context(internal_manager_error_context{ resp.ctx.ec,
                                                                 resp.ctx.last_dispatched_to,
                                                                 resp.ctx.last_dispatched_from,
                                                                 resp.ctx.retry_attempts,
                                                                 std::move(resp.ctx.retry_reasons),
                                                                 std::move(resp.ctx.client_context_id),
                                                                 resp.ctx.http_status,
                                                                 std::move(resp.ctx.http_body),
                                                                 std::move(resp.ctx.path) });
}

management::bucket::collection_spec
map_collection(std::string scope_name, const core::topology::collections_manifest::collection& collection)
{
    management::bucket::collection_spec spec{};
    spec.name = collection.name;
    spec.scope_name = std::move(scope_name);
    spec.max_expiry = collection.max_expiry;
    spec.history = collection.history;
    return spec;
}

std::vector<couchbase::management::bucket::scope_spec>
map_scope_specs(core::topology::collections_manifest& manifest)
{
    std::vector<couchbase::management::bucket::scope_spec> scope_specs{};
    for (const auto& scope : manifest.scopes) {
        couchbase::management::bucket::scope_spec converted_scope{};
        converted_scope.name = scope.name;
        for (const auto& collection : scope.collections) {
            auto collection_spec = map_collection(scope.name, collection);
            converted_scope.collections.emplace_back(collection_spec);
        }
        scope_specs.emplace_back(converted_scope);
    }
    return scope_specs;
}
} // namespace

class collection_manager_impl
{
  public:
    collection_manager_impl(core::cluster core, std::string_view bucket_name)
      : core_{ std::move(core) }
      , bucket_name_{ bucket_name }
    {
    }

    void drop_collection(std::string scope_name,
                         std::string collection_name,
                         const couchbase::drop_collection_options::built& options,
                         couchbase::drop_collection_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::collection_drop_request{
            bucket_name_,
            std::move(scope_name),
            std::move(collection_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void update_collection(std::string scope_name,
                           std::string collection_name,
                           const couchbase::update_collection_settings& settings,
                           const couchbase::update_collection_options::built& options,
                           couchbase::update_collection_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::collection_update_request{
            bucket_name_,
            std::move(scope_name),
            std::move(collection_name),
            settings.max_expiry,
            settings.history,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void create_collection(std::string scope_name,
                           std::string collection_name,
                           const couchbase::create_collection_settings& settings,
                           const couchbase::create_collection_options::built& options,
                           couchbase::update_collection_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::collection_create_request{
            bucket_name_,
            std::move(scope_name),
            std::move(collection_name),
            settings.max_expiry,
            settings.history,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void get_all_scopes(const get_all_scopes_options::built& options, get_all_scopes_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::scope_get_all_request{
            bucket_name_,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), map_scope_specs(resp.manifest)); });
    }

    void create_scope(std::string scope_name,
                      const couchbase::create_scope_options::built& options,
                      couchbase::create_scope_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::scope_create_request{
            bucket_name_,
            std::move(scope_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void drop_scope(std::string scope_name,
                    const couchbase::drop_scope_options::built& options,
                    couchbase::drop_scope_handler&& handler) const
    {
        core_.execute(
          core::operations::management::scope_drop_request{
            bucket_name_,
            std::move(scope_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

  private:
    core::cluster core_;
    std::string bucket_name_;
};

collection_manager::collection_manager(core::cluster core, std::string_view bucket_name)
  : impl_(std::make_shared<collection_manager_impl>(std::move(core), bucket_name))
{
}

void
collection_manager::drop_collection(std::string scope_name,
                                    std::string collection_name,
                                    const couchbase::drop_collection_options& options,
                                    couchbase::drop_collection_handler&& handler) const
{
    return impl_->drop_collection(std::move(scope_name), std::move(collection_name), options.build(), std::move(handler));
}

auto
collection_manager::drop_collection(std::string scope_name,
                                    std::string collection_name,
                                    const couchbase::drop_collection_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    drop_collection(
      std::move(scope_name), std::move(collection_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const update_collection_settings& settings,
                                      const update_collection_options& options,
                                      update_collection_handler&& handler) const
{
    return impl_->update_collection(std::move(scope_name), std::move(collection_name), settings, options.build(), std::move(handler));
}

auto
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::update_collection_settings& settings,
                                      const couchbase::update_collection_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    update_collection(std::move(scope_name), std::move(collection_name), settings, options, [barrier](auto ctx) mutable {
        barrier->set_value(std::move(ctx));
    });
    return barrier->get_future();
}

void
collection_manager::create_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::create_collection_settings& settings,
                                      const couchbase::create_collection_options& options,
                                      couchbase::create_collection_handler&& handler) const
{
    return impl_->create_collection(std::move(scope_name), std::move(collection_name), settings, options.build(), std::move(handler));
}

auto
collection_manager::create_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::create_collection_settings& settings,
                                      const couchbase::create_collection_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_collection(std::move(scope_name), std::move(collection_name), settings, options, [barrier](auto ctx) mutable {
        barrier->set_value(std::move(ctx));
    });
    return barrier->get_future();
}

void
collection_manager::get_all_scopes(const get_all_scopes_options& options, get_all_scopes_handler&& handler) const
{
    return impl_->get_all_scopes(options.build(), std::move(handler));
}

auto
collection_manager::get_all_scopes(const couchbase::get_all_scopes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::bucket::scope_spec>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::bucket::scope_spec>>>>();
    get_all_scopes(options,
                   [barrier](auto ctx, auto result) mutable { barrier->set_value(std::make_pair(std::move(ctx), std::move(result))); });
    return barrier->get_future();
}

void
collection_manager::create_scope(std::string scope_name,
                                 const couchbase::create_scope_options& options,
                                 couchbase::create_scope_handler&& handler) const
{
    return impl_->create_scope(std::move(scope_name), options.build(), std::move(handler));
}

auto
collection_manager::create_scope(std::string scope_name, const couchbase::create_scope_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_scope(std::move(scope_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_manager::drop_scope(std::string scope_name,
                               const couchbase::drop_scope_options& options,
                               couchbase::drop_scope_handler&& handler) const
{
    return impl_->drop_scope(std::move(scope_name), options.build(), std::move(handler));
}

auto
collection_manager::drop_scope(std::string scope_name, const couchbase::drop_scope_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    drop_scope(std::move(scope_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
