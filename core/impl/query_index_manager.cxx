/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/operations/management/query_index_build_deferred.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all.hxx"
#include "internal_manager_error_context.hxx"

#include "core/logger/logger.hxx"

#include <couchbase/collection_query_index_manager.hxx>
#include <couchbase/query_index_manager.hxx>

#include <asio/steady_timer.hpp>

#include <algorithm>
#include <utility>

namespace couchbase
{
namespace
{
template<typename Response>
manager_error_context
build_context(Response& resp)
{
    return manager_error_context{ internal_manager_error_context{ resp.ctx.ec,
                                                                  resp.ctx.last_dispatched_to,
                                                                  resp.ctx.last_dispatched_from,
                                                                  resp.ctx.retry_attempts,
                                                                  std::move(resp.ctx.retry_reasons),
                                                                  std::move(resp.ctx.client_context_id),
                                                                  resp.ctx.http_status,
                                                                  std::move(resp.ctx.http_body),
                                                                  std::move(resp.ctx.path) } };
}

class watch_context : public std::enable_shared_from_this<watch_context>
{

  public:
    watch_context(couchbase::core::cluster core,
                  std::string bucket_name,
                  std::vector<std::string> index_names,
                  couchbase::watch_query_indexes_options::built options,
                  std::string scope_name,
                  std::string collection_name,
                  watch_query_indexes_handler&& handler)
      : core_(std::move(core))
      , bucket_name_(std::move(bucket_name))
      , index_names_(std::move(index_names))
      , options_(std::move(options))
      , scope_name_(std::move(scope_name))
      , collection_name_(std::move(collection_name))
      , handler_(std::move(handler))
    {
    }

    watch_context(watch_context&& other) noexcept
      : core_(std::move(other.core_))
      , bucket_name_(std::move(other.bucket_name_))
      , index_names_(std::move(other.index_names_))
      , options_(std::move(other.options_))
      , scope_name_(std::move(other.scope_name_))
      , collection_name_(std::move(other.collection_name_))
      , handler_(std::move(other.handler_))
      , timer_(std::move(other.timer_))
      , start_time_(other.start_time_)
      , timeout_(other.timeout_)
      , attempts_(other.attempts_.load())
    {
    }

    void execute()
    {
        return core_.execute(
          core::operations::management::query_index_get_all_request{
            bucket_name_,
            scope_name_,
            collection_name_,
            {},
            {},
            remaining(),
          },
          [ctx = shared_from_this()](auto resp) {
              if (ctx->check(resp)) {
                  ctx->finish(resp);
              } else if (ctx->remaining().count() <= 0) {
                  ctx->finish(resp, couchbase::errc::common::ambiguous_timeout);
              } else {
                  ctx->poll();
              }
          });
    }

  private:
    template<typename Response>
    void finish(Response& resp, std::optional<std::error_code> ec = {})
    {
        watch_query_indexes_handler handler{};
        std::swap(handler, handler_);
        if (handler) {
            handler({ manager_error_context(internal_manager_error_context{ ec.value_or(resp.ctx.ec),
                                                                            resp.ctx.last_dispatched_to,
                                                                            resp.ctx.last_dispatched_from,
                                                                            resp.ctx.retry_attempts,
                                                                            std::move(resp.ctx.retry_reasons),
                                                                            std::move(resp.ctx.client_context_id),
                                                                            resp.ctx.http_status,
                                                                            std::move(resp.ctx.http_body),
                                                                            std::move(resp.ctx.path) }) });
            timer_.cancel();
        }
    }

    std::chrono::milliseconds remaining() const
    {
        return timeout_ - std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_);
    }

    bool check(const couchbase::core::operations::management::query_index_get_all_response& resp)
    {
        if (resp.ctx.ec == couchbase::errc::common::ambiguous_timeout) {
            return false;
        }
        bool complete = true;

        for (const auto& name : index_names_) {
            const auto it = std::find_if(resp.indexes.begin(), resp.indexes.end(), [&](const couchbase::management::query_index& index) {
                return index.name == name;
            });
            if (it == resp.indexes.end()) {
                finish(resp, couchbase::errc::common::index_not_found);
                return true;
            }
            complete &= (it != resp.indexes.end() && it->state == "online");
        }
        if (options_.watch_primary) {
            const auto it = std::find_if(
              resp.indexes.begin(), resp.indexes.end(), [&](const couchbase::management::query_index& index) { return index.is_primary; });
            complete &= it != resp.indexes.end() && it->state == "online";
        }
        return complete;
    }

    void poll()
    {
        timer_.expires_after(options_.polling_interval);
        timer_.async_wait([ctx = shared_from_this()](asio::error_code) { ctx->execute(); });
    }

    couchbase::core::cluster core_;
    std::string bucket_name_;
    std::vector<std::string> index_names_;
    couchbase::watch_query_indexes_options::built options_;
    std::string scope_name_;
    std::string collection_name_;
    watch_query_indexes_handler handler_;
    asio::steady_timer timer_{ core_.io_context() };
    std::chrono::steady_clock::time_point start_time_{ std::chrono::steady_clock::now() };
    std::chrono::milliseconds timeout_{ options_.timeout.value_or(core_.origin().second.options().query_timeout) };
    std::atomic<size_t> attempts_{ 0 };
};

} // namespace

class query_index_manager_impl : public std::enable_shared_from_this<query_index_manager_impl>
{
  public:
    explicit query_index_manager_impl(core::cluster core)
      : core_{ std::move(core) }
    {
    }

    void get_all_indexes(const std::string& bucket_name,
                         const std::string& scope_name,
                         const std::string& collection_name,
                         const get_all_query_indexes_options::built& options,
                         get_all_query_indexes_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::query_index_get_all_request{
            bucket_name,
            scope_name,
            collection_name,
            {},
            {},
            options.timeout,
          },
          [handler = std::move(handler)](core::operations::management::query_index_get_all_response resp) {
              if (resp.ctx.ec) {
                  return handler(build_context(resp), {});
              }
              handler(build_context(resp), resp.indexes);
          });
    }

    void create_index(const std::string& bucket_name,
                      const std::string& scope_name,
                      const std::string& collection_name,
                      std::string index_name,
                      std::vector<std::string> keys,
                      const create_query_index_options::built& options,
                      create_query_index_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::query_index_create_request{
            bucket_name,
            scope_name,
            collection_name,
            std::move(index_name),
            std::move(keys),
            {},
            false /* is_primary */,
            options.ignore_if_exists,
            options.condition,
            options.deferred,
            options.num_replicas,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) { handler(build_context(resp)); });
    }

    void create_primary_index(const std::string& bucket_name,
                              const std::string& scope_name,
                              const std::string& collection_name,
                              const create_primary_query_index_options::built& options,
                              create_primary_query_index_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::query_index_create_request{
            bucket_name,
            scope_name,
            collection_name,
            options.index_name.value_or(""),
            {},
            {},
            true /* is_primary */,
            options.ignore_if_exists,
            {},
            options.deferred,
            options.num_replicas,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) { handler(build_context(resp)); });
    }

    void drop_index(const std::string& bucket_name,
                    const std::string& scope_name,
                    const std::string& collection_name,
                    std::string index_name,
                    const drop_query_index_options::built& options,
                    drop_query_index_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::query_index_drop_request{
            bucket_name,
            scope_name,
            collection_name,
            std::move(index_name),
            {},
            false /* is_primary */,
            options.ignore_if_not_exists,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) { handler(build_context(resp)); });
    }

    void drop_primary_index(const std::string& bucket_name,
                            const std::string& scope_name,
                            const std::string& collection_name,
                            const drop_primary_query_index_options::built& options,
                            drop_primary_query_index_handler&& handler) const
    {
        return core_.execute(
          core::operations::management::query_index_drop_request{
            bucket_name,
            scope_name,
            collection_name,
            options.index_name.value_or(""),
            {},
            true,
            options.ignore_if_not_exists,
            {},
            options.timeout,
          },
          [handler = std::move(handler)](core::operations::management::query_index_drop_response resp) { handler(build_context(resp)); });
    }

    void build_deferred_indexes(const std::string& bucket_name,
                                const std::string& scope_name,
                                const std::string& collection_name,
                                const build_query_index_options::built& options,
                                build_deferred_query_indexes_handler&& handler) const
    {
        auto timeout = options.timeout;
        return core_.execute(
          core::operations::management::query_index_get_all_deferred_request{ bucket_name, scope_name, collection_name, {}, {}, timeout },
          [self = shared_from_this(),
           bucket = bucket_name,
           scope = scope_name,
           collection = collection_name,
           timeout,
           handler = std::move(handler)](auto list_resp) mutable {
              if (list_resp.ctx.ec) {
                  return handler(build_context(list_resp));
              }
              if (list_resp.index_names.empty()) {
                  return handler(build_context(list_resp));
              }
              self->core_.execute(
                core::operations::management::query_index_build_request{
                  std::move(bucket), scope, collection, {}, std::move(list_resp.index_names), {}, timeout },
                [handler = std::move(handler)](auto build_resp) { return handler(build_context(build_resp)); });
          });
    }

    void watch_indexes(const std::string& bucket_name,
                       const std::string& scope_name,
                       const std::string& collection_name,
                       std::vector<std::string> index_names,
                       watch_query_indexes_options::built options,
                       watch_query_indexes_handler&& handler) const
    {
        auto ctx = std::make_shared<watch_context>(
          core_, bucket_name, std::move(index_names), std::move(options), scope_name, collection_name, std::move(handler));
        return ctx->execute();
    }

  private:
    core::cluster core_;
};

query_index_manager::query_index_manager(core::cluster core)
  : impl_(std::make_shared<query_index_manager_impl>(std::move(core)))
{
}

void
query_index_manager::get_all_indexes(std::string bucket_name,
                                     const get_all_query_indexes_options& options,
                                     get_all_query_indexes_handler&& handler) const
{
    return impl_->get_all_indexes(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::get_all_indexes(std::string bucket_name, const get_all_query_indexes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::query_index>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::query_index>>>>();
    auto future = barrier->get_future();
    get_all_indexes(std::move(bucket_name), options, [barrier](auto ctx, auto resp) mutable {
        barrier->set_value({ std::move(ctx), resp });
    });
    return future;
}

void
query_index_manager::create_index(std::string bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> fields,
                                  const create_query_index_options& options,
                                  create_query_index_handler&& handler) const
{
    return impl_->create_index(bucket_name, {}, {}, std::move(index_name), std::move(fields), options.build(), std::move(handler));
}

auto
query_index_manager::create_index(std::string bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> fields,
                                  const create_query_index_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    create_index(std::move(bucket_name), std::move(index_name), std::move(fields), options, [barrier](auto ctx) {
        barrier->set_value(std::move(ctx));
    });
    return future;
}

void
query_index_manager::create_primary_index(std::string bucket_name,
                                          const create_primary_query_index_options& options,
                                          create_query_index_handler&& handler) const
{
    return impl_->create_primary_index(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::create_primary_index(std::string bucket_name, const create_primary_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    create_primary_index(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
query_index_manager::drop_primary_index(std::string bucket_name,
                                        const drop_primary_query_index_options& options,
                                        drop_query_index_handler&& handler) const
{
    return impl_->drop_primary_index(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::drop_primary_index(std::string bucket_name, const drop_primary_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    drop_primary_index(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
query_index_manager::drop_index(std::string bucket_name,
                                std::string index_name,
                                const drop_query_index_options& options,
                                drop_query_index_handler&& handler) const
{
    return impl_->drop_index(bucket_name, {}, {}, std::move(index_name), options.build(), std::move(handler));
}

auto
query_index_manager::drop_index(std::string bucket_name, std::string index_name, const drop_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    drop_index(std::move(bucket_name), std::move(index_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
query_index_manager::build_deferred_indexes(std::string bucket_name,
                                            const build_query_index_options& options,
                                            build_deferred_query_indexes_handler&& handler) const
{
    return impl_->build_deferred_indexes(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::build_deferred_indexes(std::string bucket_name, const build_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    build_deferred_indexes(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
query_index_manager::watch_indexes(std::string bucket_name,
                                   std::vector<std::string> index_names,
                                   const watch_query_indexes_options& options,
                                   watch_query_indexes_handler&& handler) const
{
    return impl_->watch_indexes(bucket_name, {}, {}, std::move(index_names), options.build(), std::move(handler));
}

auto
query_index_manager::watch_indexes(std::string bucket_name,
                                   std::vector<std::string> index_names,
                                   const watch_query_indexes_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    watch_indexes(std::move(bucket_name), std::move(index_names), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

collection_query_index_manager::collection_query_index_manager(core::cluster core,
                                                               std::string bucket_name,
                                                               std::string scope_name,
                                                               std::string collection_name)
  : impl_(std::make_shared<query_index_manager_impl>(std::move(core)))
  , bucket_name_(std::move(bucket_name))
  , scope_name_(std::move(scope_name))
  , collection_name_(std::move(collection_name))
{
}

void
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options, get_all_query_indexes_handler&& handler) const
{
    return impl_->get_all_indexes(bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::query_index>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::query_index>>>>();
    auto future = barrier->get_future();
    get_all_indexes(options, [barrier](auto ctx, auto resp) mutable { barrier->set_value({ std::move(ctx), resp }); });
    return future;
}

void
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> keys,
                                             const create_query_index_options& options,
                                             create_query_index_handler&& handler) const
{
    return impl_->create_index(
      bucket_name_, scope_name_, collection_name_, std::move(index_name), std::move(keys), options.build(), std::move(handler));
}

auto
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> keys,
                                             const create_query_index_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    create_index(std::move(index_name), std::move(keys), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
collection_query_index_manager::create_primary_index(const create_primary_query_index_options& options,
                                                     create_query_index_handler&& handler) const
{
    return impl_->create_primary_index(bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::create_primary_index(const create_primary_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    create_primary_index(options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
collection_query_index_manager::drop_primary_index(const drop_primary_query_index_options& options,
                                                   drop_query_index_handler&& handler) const
{
    return impl_->drop_primary_index(bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::drop_primary_index(const drop_primary_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    drop_primary_index(options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
collection_query_index_manager::drop_index(std::string index_name,
                                           const drop_query_index_options& options,
                                           drop_query_index_handler&& handler) const
{
    return impl_->drop_index(bucket_name_, scope_name_, collection_name_, std::move(index_name), options.build(), std::move(handler));
}

auto
collection_query_index_manager::drop_index(std::string index_name, const drop_query_index_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    drop_index(std::move(index_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
collection_query_index_manager::build_deferred_indexes(const build_query_index_options& options,
                                                       build_deferred_query_indexes_handler&& handler) const
{
    return impl_->build_deferred_indexes(bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::build_deferred_indexes(const build_query_index_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    build_deferred_indexes(options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}

void
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names,
                                              const watch_query_indexes_options& options,
                                              watch_query_indexes_handler&& handler) const
{
    return impl_->watch_indexes(bucket_name_, scope_name_, collection_name_, std::move(index_names), options.build(), std::move(handler));
}

auto
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names, const watch_query_indexes_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    auto future = barrier->get_future();
    watch_indexes(std::move(index_names), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
    return future;
}
} // namespace couchbase
