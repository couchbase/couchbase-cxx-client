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
#include "core/cluster_options.hxx"
#include "core/core_sdk_shim.hxx"
#include "core/http_component.hxx"
#include "core/impl/error.hxx"
#include "core/impl/observability_recorder.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/io/query_cache.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_drop.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"
#include "core/operations/management/scope_get_all.hxx"
#include "core/platform/uuid.h"
#include "core/timeout_defaults.hxx"
#include "core/topology/collections_manifest.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/collection_manager.hxx>
#include <couchbase/create_collection_options.hxx>
#include <couchbase/create_scope_options.hxx>
#include <couchbase/drop_collection_options.hxx>
#include <couchbase/drop_scope_options.hxx>
#include <couchbase/error.hxx>
#include <couchbase/get_all_scopes_options.hxx>
#include <couchbase/management/collection_spec.hxx>
#include <couchbase/management/scope_spec.hxx>
#include <couchbase/update_collection_options.hxx>

#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace couchbase
{
namespace
{
auto
map_collection(std::string scope_name,
               const core::topology::collections_manifest::collection& collection)
  -> management::bucket::collection_spec
{
  management::bucket::collection_spec spec{};
  spec.name = collection.name;
  spec.scope_name = std::move(scope_name);
  spec.max_expiry = collection.max_expiry;
  spec.history = collection.history;
  return spec;
}

auto
map_scope_specs(core::topology::collections_manifest& manifest)
  -> std::vector<couchbase::management::bucket::scope_spec>
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
  collection_manager_impl(const core::cluster& core, std::string_view bucket_name)
    : http_{ core.io_context(), core::core_sdk_shim{ core } }
    , tracer_{ core.tracer() }
    , meter_{ core.meter() }
    , bucket_name_{ bucket_name }
  {
  }

  void drop_collection(std::string scope_name,
                       std::string collection_name,
                       const couchbase::drop_collection_options::built& options,
                       couchbase::drop_collection_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mgr_collections_drop_collection,
                                    scope_name,
                                    collection_name,
                                    options.parent_span);
    core::operations::management::collection_drop_request request{
      bucket_name_, std::move(scope_name), std::move(collection_name),
      {},           options.timeout,       obs_rec->operation_span(),
    };
    execute_http(std::move(request),
                 [obs_rec = std::move(obs_rec),
                  handler = std::move(handler)](const auto& resp) mutable -> void {
                   obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
                   handler(core::impl::make_error(resp.ctx));
                 });
  }

  void update_collection(std::string scope_name,
                         std::string collection_name,
                         const couchbase::update_collection_settings& settings,
                         const couchbase::update_collection_options::built& options,
                         couchbase::update_collection_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mgr_collections_update_collection,
                                    scope_name,
                                    collection_name,
                                    options.parent_span);
    core::operations::management::collection_update_request request{
      bucket_name_,        std::move(scope_name),     std::move(collection_name),
      settings.max_expiry, settings.history,          {},
      options.timeout,     obs_rec->operation_span(),
    };
    execute_http(std::move(request),
                 [obs_rec = std::move(obs_rec),
                  handler = std::move(handler)](const auto& resp) mutable -> void {
                   obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
                   handler(core::impl::make_error(resp.ctx));
                 });
  }

  void create_collection(std::string scope_name,
                         std::string collection_name,
                         const couchbase::create_collection_settings& settings,
                         const couchbase::create_collection_options::built& options,
                         couchbase::update_collection_handler&& handler) const
  {
    auto obs_rec =
      create_observability_recorder(core::tracing::operation::mgr_collections_create_collection,
                                    scope_name,
                                    collection_name,
                                    options.parent_span);
    core::operations::management::collection_create_request request{
      bucket_name_,        std::move(scope_name),     std::move(collection_name),
      settings.max_expiry, settings.history,          {},
      options.timeout,     obs_rec->operation_span(),
    };
    execute_http(std::move(request),
                 [obs_rec = std::move(obs_rec),
                  handler = std::move(handler)](const auto& resp) mutable -> void {
                   obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
                   handler(core::impl::make_error(resp.ctx));
                 });
  }

  void get_all_scopes(const get_all_scopes_options::built& options,
                      get_all_scopes_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mgr_collections_get_all_scopes, {}, {}, options.parent_span);
    core::operations::management::scope_get_all_request request{
      bucket_name_,
      {},
      options.timeout,
      obs_rec->operation_span(),
    };
    execute_http(
      std::move(request),
      [obs_rec = std::move(obs_rec), handler = std::move(handler)](auto resp) mutable -> void {
        obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
        handler(core::impl::make_error(resp.ctx), map_scope_specs(resp.manifest));
      });
  }

  void create_scope(std::string scope_name,
                    const couchbase::create_scope_options::built& options,
                    couchbase::create_scope_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mgr_collections_create_scope, scope_name, {}, options.parent_span);
    core::operations::management::scope_create_request request{
      bucket_name_, std::move(scope_name), {}, options.timeout, obs_rec->operation_span(),
    };
    execute_http(std::move(request),
                 [obs_rec = std::move(obs_rec),
                  handler = std::move(handler)](const auto& resp) mutable -> void {
                   obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
                   handler(core::impl::make_error(resp.ctx));
                 });
  }

  void drop_scope(std::string scope_name,
                  const couchbase::drop_scope_options::built& options,
                  couchbase::drop_scope_handler&& handler) const
  {
    auto obs_rec = create_observability_recorder(
      core::tracing::operation::mgr_collections_drop_scope, scope_name, {}, options.parent_span);
    core::operations::management::scope_drop_request request{
      bucket_name_, std::move(scope_name), {}, options.timeout, obs_rec->operation_span(),
    };
    execute_http(std::move(request),
                 [obs_rec = std::move(obs_rec),
                  handler = std::move(handler)](const auto& resp) mutable -> void {
                   obs_rec->finish(resp.ctx.retry_attempts, resp.ctx.ec);
                   handler(core::impl::make_error(resp.ctx));
                 });
  }

private:
  [[nodiscard]] auto create_observability_recorder(
    const std::string& operation_name,
    const std::optional<std::string>& scope_name,
    const std::optional<std::string>& collection_name,
    const std::shared_ptr<tracing::request_span>& parent_span) const
    -> std::unique_ptr<core::impl::observability_recorder>
  {
    auto obs_rec =
      core::impl::observability_recorder::create(operation_name, parent_span, tracer_, meter_);
    obs_rec->with_service(core::tracing::service::management);
    obs_rec->with_bucket_name(bucket_name_);
    if (scope_name.has_value()) {
      obs_rec->with_scope_name(scope_name.value());
    }
    if (collection_name.has_value()) {
      obs_rec->with_collection_name(collection_name.value());
    }
    return obs_rec;
  }

  template<typename Request, typename Handler>
  void execute_http(Request request, Handler handler) const
  {
    // Build io::http_request by calling encode_to. All management request types used here ignore
    // the http_context parameter, so we supply dummy objects.
    core::topology::configuration dummy_config{};
    core::cluster_options dummy_options{};
    core::query_cache dummy_cache{};
    core::http_context dummy_ctx{ dummy_config, dummy_options, dummy_cache, {}, 0, {}, 0 };

    core::io::http_request io_req{};
    if (auto ec = request.encode_to(io_req, dummy_ctx); ec) {
      // Encoding failed (e.g. invalid argument). Invoke the handler synchronously, exactly once.
      typename Request::response_type resp{};
      resp.ctx.ec = ec;
      handler(std::move(resp));
      return;
    }

    auto client_context_id =
      request.client_context_id.value_or(core::uuid::to_string(core::uuid::random()));
    auto timeout = request.timeout.value_or(core::timeout_defaults::management_timeout);

    core::http_request http_req{};
    http_req.service = core::service_type::management;
    http_req.method = io_req.method;
    http_req.path = io_req.path;
    http_req.headers = io_req.headers;
    http_req.body = io_req.body;
    http_req.client_context_id = client_context_id;
    http_req.timeout = timeout;
    http_req.parent_span = request.parent_span;

    // Capture the typed Request so we can call make_response in the callback.
    auto shared_request = std::make_shared<Request>(std::move(request));
    auto shared_handler = std::make_shared<Handler>(std::move(handler));

    auto result = http_.do_http_request_buffered(
      http_req,
      [shared_request,
       shared_handler,
       captured_method = io_req.method,
       captured_path = io_req.path,
       captured_client_context_id = std::move(client_context_id)](
        core::buffered_http_response resp, std::error_code ec) mutable -> void {
        core::io::http_response io_resp{};
        io_resp.status_code = resp.status_code();
        io_resp.body.append(resp.body());

        core::error_context::http ctx{};
        ctx.ec = ec;
        ctx.client_context_id = captured_client_context_id;
        ctx.method = captured_method;
        ctx.path = captured_path;
        ctx.http_status = io_resp.status_code;
        ctx.http_body = io_resp.body.data();

        auto typed_resp = shared_request->make_response(std::move(ctx), io_resp);
        (*shared_handler)(std::move(typed_resp));
      });

    if (!result) {
      // do_http_request_buffered returned an error without consuming the callback.
      // Invoke the handler here, exactly once.
      typename Request::response_type error_resp{};
      error_resp.ctx.ec = result.error();
      error_resp.ctx.client_context_id = http_req.client_context_id;
      error_resp.ctx.method = http_req.method;
      error_resp.ctx.path = http_req.path;
      (*shared_handler)(std::move(error_resp));
    }
  }

  mutable core::http_component http_;
  std::weak_ptr<core::tracing::tracer_wrapper> tracer_;
  std::weak_ptr<core::metrics::meter_wrapper> meter_;
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
  return impl_->drop_collection(
    std::move(scope_name), std::move(collection_name), options.build(), std::move(handler));
}

auto
collection_manager::drop_collection(std::string scope_name,
                                    std::string collection_name,
                                    const couchbase::drop_collection_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  drop_collection(std::move(scope_name),
                  std::move(collection_name),
                  options,
                  [barrier](auto err) mutable -> void {
                    barrier->set_value(std::move(err));
                  });
  return barrier->get_future();
}

void
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const update_collection_settings& settings,
                                      const update_collection_options& options,
                                      update_collection_handler&& handler) const
{
  return impl_->update_collection(std::move(scope_name),
                                  std::move(collection_name),
                                  settings,
                                  options.build(),
                                  std::move(handler));
}

auto
collection_manager::update_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::update_collection_settings& settings,
                                      const couchbase::update_collection_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  update_collection(std::move(scope_name),
                    std::move(collection_name),
                    settings,
                    options,
                    [barrier](auto err) mutable -> void {
                      barrier->set_value(std::move(err));
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
  return impl_->create_collection(std::move(scope_name),
                                  std::move(collection_name),
                                  settings,
                                  options.build(),
                                  std::move(handler));
}

auto
collection_manager::create_collection(std::string scope_name,
                                      std::string collection_name,
                                      const couchbase::create_collection_settings& settings,
                                      const couchbase::create_collection_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  create_collection(std::move(scope_name),
                    std::move(collection_name),
                    settings,
                    options,
                    [barrier](auto err) mutable -> void {
                      barrier->set_value(std::move(err));
                    });
  return barrier->get_future();
}

void
collection_manager::get_all_scopes(const get_all_scopes_options& options,
                                   get_all_scopes_handler&& handler) const
{
  return impl_->get_all_scopes(options.build(), std::move(handler));
}

auto
collection_manager::get_all_scopes(const couchbase::get_all_scopes_options& options) const
  -> std::future<std::pair<error, std::vector<management::bucket::scope_spec>>>
{
  auto barrier =
    std::make_shared<std::promise<std::pair<error, std::vector<management::bucket::scope_spec>>>>();
  get_all_scopes(options, [barrier](auto err, auto result) mutable -> void {
    barrier->set_value(std::make_pair(std::move(err), std::move(result)));
  });
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
collection_manager::create_scope(std::string scope_name,
                                 const couchbase::create_scope_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  create_scope(std::move(scope_name), options, [barrier](auto err) mutable -> void {
    barrier->set_value(std::move(err));
  });
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
collection_manager::drop_scope(std::string scope_name,
                               const couchbase::drop_scope_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  drop_scope(std::move(scope_name), options, [barrier](auto err) mutable -> void {
    barrier->set_value(std::move(err));
  });
  return barrier->get_future();
}
} // namespace couchbase
