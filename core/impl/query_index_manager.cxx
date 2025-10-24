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

#include "core/impl/error.hxx"
#include "core/logger/logger.hxx"
#include "core/operations/management/query_index_build_deferred.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/collection_query_index_manager.hxx>
#include <couchbase/query_index_manager.hxx>

#include <asio/steady_timer.hpp>

#include <algorithm>
#include <utility>

#include "couchbase/bucket.hxx"

namespace couchbase
{
namespace
{
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
    , span_(create_span(core::tracing::operation::mgr_query_watch_indexes, options_.parent_span))
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
    , span_(std::move(other.span_))
    , timer_(std::move(other.timer_))
    , start_time_(other.start_time_)
    , timeout_(other.timeout_)
    , attempts_(other.attempts_.load())
  {
  }

  watch_context(const watch_context&) = delete;
  auto operator=(const watch_context&) -> watch_context& = delete;
  auto operator=(watch_context&&) -> watch_context& = delete;
  ~watch_context() = default;

  void execute()
  {
    auto get_all_span = create_span(core::tracing::operation::mgr_query_get_all_indexes, span_);
    core::operations::management::query_index_get_all_request get_all_request{
      bucket_name_, scope_name_, collection_name_, {}, {}, remaining(), get_all_span,
    };
    return core_.execute(
      std::move(get_all_request),
      [ctx = shared_from_this(), get_all_span = std::move(get_all_span)](auto resp) {
        if (auto retries = resp.ctx.retry_attempts; get_all_span->uses_tags() && retries > 0) {
          get_all_span->add_tag(core::tracing::attributes::op::retry_count, retries);
        }
        get_all_span->end();
        if (ctx->check(resp)) {
          ctx->finish(resp, {});
        } else if (ctx->remaining().count() <= 0) {
          ctx->finish(resp, couchbase::errc::common::ambiguous_timeout);
        } else {
          ctx->poll();
        }
      });
  }

private:
  template<typename Response>
  void finish(Response& resp, std::optional<std::error_code> ec)
  {
    watch_query_indexes_handler handler{};
    std::swap(handler, handler_);
    if (handler) {
      if (ec.has_value()) {
        resp.ctx.ec = ec.value();
      }
      span_->end();
      handler(core::impl::make_error(resp.ctx));
      timer_.cancel();
    }
  }

  auto remaining() const -> std::chrono::milliseconds
  {
    return timeout_ - std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_time_);
  }

  auto check(couchbase::core::operations::management::query_index_get_all_response& resp) -> bool
  {
    if (resp.ctx.ec == couchbase::errc::common::ambiguous_timeout) {
      return false;
    }
    bool complete = true;

    for (const auto& name : index_names_) {
      const auto it = std::find_if(resp.indexes.begin(),
                                   resp.indexes.end(),
                                   [&](const couchbase::management::query_index& index) {
                                     return index.name == name;
                                   });
      if (it == resp.indexes.end()) {
        finish(resp, couchbase::errc::common::index_not_found);
        return true;
      }
      complete &= it->state == "online";
    }
    if (options_.watch_primary) {
      const auto it = std::find_if(resp.indexes.begin(),
                                   resp.indexes.end(),
                                   [&](const couchbase::management::query_index& index) {
                                     return index.is_primary;
                                   });
      complete &= it != resp.indexes.end() && it->state == "online";
    }
    return complete;
  }

  void poll()
  {
    timer_.expires_after(options_.polling_interval);
    timer_.async_wait([ctx = shared_from_this()](asio::error_code) {
      ctx->execute();
    });
  }

  [[nodiscard]] auto create_span(const std::string& operation_name,
                                 const std::shared_ptr<tracing::request_span>& parent_span) const
    -> std::shared_ptr<tracing::request_span>
  {
    auto span = core_.tracer()->create_span(operation_name, parent_span);
    if (span->uses_tags()) {
      span->add_tag(core::tracing::attributes::op::service, core::tracing::service::query);
      span->add_tag(core::tracing::attributes::op::bucket_name, bucket_name_);
      if (!collection_name_.empty()) {
        span->add_tag(core::tracing::attributes::op::scope_name, scope_name_);
        span->add_tag(core::tracing::attributes::op::collection_name, collection_name_);
      }
      span->add_tag(core::tracing::attributes::op::operation_name, operation_name);
    }
    return span;
  }

  couchbase::core::cluster core_;
  std::string bucket_name_;
  std::vector<std::string> index_names_;
  couchbase::watch_query_indexes_options::built options_;
  std::string scope_name_;
  std::string collection_name_;
  watch_query_indexes_handler handler_;
  std::shared_ptr<tracing::request_span> span_;
  asio::steady_timer timer_{ core_.io_context() };
  std::chrono::steady_clock::time_point start_time_{ std::chrono::steady_clock::now() };
  std::chrono::milliseconds timeout_{ options_.timeout.value_or(
    core_.origin().second.options().query_timeout) };
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
    auto span = create_span(core::tracing::operation::mgr_query_get_all_indexes,
                            bucket_name,
                            scope_name,
                            collection_name,
                            options.parent_span);

    core::operations::management::query_index_get_all_request request{
      bucket_name, scope_name, collection_name, {}, {}, options.timeout, span,
    };
    return core_.execute(std::move(request),
                         [span = std::move(span), handler = std::move(handler)](
                           const core::operations::management::query_index_get_all_response& resp) {
                           if (const auto retries = resp.ctx.retry_attempts;
                               span->uses_tags() && retries > 0) {
                             span->add_tag(core::tracing::attributes::op::retry_count, retries);
                           }
                           span->end();
                           if (resp.ctx.ec) {
                             return handler(core::impl::make_error(resp.ctx), {});
                           }
                           handler(core::impl::make_error(resp.ctx), resp.indexes);
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
    auto span = create_span(core::tracing::operation::mgr_query_create_index,
                            bucket_name,
                            scope_name,
                            collection_name,
                            options.parent_span);

    core::operations::management::query_index_create_request request{
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
      span,
    };
    return core_.execute(
      std::move(request), [span = std::move(span), handler = std::move(handler)](const auto& resp) {
        if (const auto retries = resp.ctx.retry_attempts; span->uses_tags() && retries > 0) {
          span->add_tag(core::tracing::attributes::op::retry_count, retries);
        }
        span->end();
        handler(core::impl::make_error(resp.ctx));
      });
  }

  void create_primary_index(const std::string& bucket_name,
                            const std::string& scope_name,
                            const std::string& collection_name,
                            const create_primary_query_index_options::built& options,
                            create_primary_query_index_handler&& handler) const
  {
    auto span = create_span(core::tracing::operation::mgr_query_create_primary_index,
                            bucket_name,
                            scope_name,
                            collection_name,
                            options.parent_span);

    core::operations::management::query_index_create_request request{
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
      span,
    };
    return core_.execute(
      std::move(request), [span = std::move(span), handler = std::move(handler)](const auto& resp) {
        if (const auto retries = resp.ctx.retry_attempts; span->uses_tags() && retries > 0) {
          span->add_tag(core::tracing::attributes::op::retry_count, retries);
        }
        span->end();
        handler(core::impl::make_error(resp.ctx));
      });
  }

  void drop_index(const std::string& bucket_name,
                  const std::string& scope_name,
                  const std::string& collection_name,
                  std::string index_name,
                  const drop_query_index_options::built& options,
                  drop_query_index_handler&& handler) const
  {
    auto span = create_span(core::tracing::operation::mgr_query_drop_index,
                            bucket_name,
                            scope_name,
                            collection_name,
                            options.parent_span);

    core::operations::management::query_index_drop_request request{
      bucket_name,
      scope_name,
      collection_name,
      std::move(index_name),
      {},
      false /* is_primary */,
      options.ignore_if_not_exists,
      {},
      options.timeout,
      span,
    };
    return core_.execute(
      std::move(request), [span = std::move(span), handler = std::move(handler)](const auto& resp) {
        if (const auto retries = resp.ctx.retry_attempts; resp.ctx.ec && retries > 0) {
          span->add_tag(core::tracing::attributes::op::retry_count, retries);
        }
        span->end();
        handler(core::impl::make_error(resp.ctx));
      });
  }

  void drop_primary_index(const std::string& bucket_name,
                          const std::string& scope_name,
                          const std::string& collection_name,
                          const drop_primary_query_index_options::built& options,
                          drop_primary_query_index_handler&& handler) const
  {
    auto span = create_span(core::tracing::operation::mgr_query_drop_primary_index,
                            bucket_name,
                            scope_name,
                            collection_name,
                            options.parent_span);

    core::operations::management::query_index_drop_request request{
      bucket_name,
      scope_name,
      collection_name,
      options.index_name.value_or(""),
      {},
      true,
      options.ignore_if_not_exists,
      {},
      options.timeout,
      span,
    };
    return core_.execute(std::move(request),
                         [span = std::move(span), handler = std::move(handler)](
                           const core::operations::management::query_index_drop_response& resp) {
                           if (const auto retries = resp.ctx.retry_attempts;
                               resp.ctx.ec && retries > 0) {
                             span->add_tag(core::tracing::attributes::op::retry_count, retries);
                           }
                           span->end();
                           handler(core::impl::make_error(resp.ctx));
                         });
  }

  void build_deferred_indexes(const std::string& bucket_name,
                              const std::string& scope_name,
                              const std::string& collection_name,
                              const build_query_index_options::built& options,
                              build_deferred_query_indexes_handler&& handler) const
  {
    auto timeout = options.timeout;

    auto top_span = create_span(core::tracing::operation::mgr_query_build_deferred_indexes,
                                bucket_name,
                                scope_name,
                                collection_name,
                                options.parent_span);

    auto get_all_deferred_span =
      create_span(core::tracing::operation::mgr_query_get_all_deferred_indexes,
                  bucket_name,
                  scope_name,
                  collection_name,
                  top_span);

    core::operations::management::query_index_get_all_deferred_request get_all_deferred_request{
      bucket_name, scope_name, collection_name, {}, {}, options.timeout, get_all_deferred_span,
    };

    return core_.execute(
      std::move(get_all_deferred_request),
      [self = shared_from_this(),
       bucket = bucket_name,
       scope = scope_name,
       collection = collection_name,
       timeout,
       get_all_deferred_span = std::move(get_all_deferred_span),
       top_span = std::move(top_span),
       handler = std::move(handler)](auto list_resp) mutable {
        if (const auto retries = list_resp.ctx.retry_attempts;
            get_all_deferred_span->uses_tags() && retries > 0) {
          get_all_deferred_span->add_tag(core::tracing::attributes::op::retry_count, retries);
        }
        get_all_deferred_span->end();
        if (list_resp.ctx.ec) {
          top_span->end();
          return handler(core::impl::make_error(list_resp.ctx));
        }
        if (list_resp.index_names.empty()) {
          top_span->end();
          return handler(core::impl::make_error(list_resp.ctx));
        }
        auto build_span = self->create_span(
          core::tracing::operation::mgr_query_build_indexes, bucket, scope, collection, top_span);
        core::operations::management::query_index_build_request build_request{
          std::move(bucket), scope,      collection, {}, std::move(list_resp.index_names), {},
          timeout,           build_span,
        };
        self->core_.execute(std::move(build_request),
                            [top_span = std::move(top_span),
                             build_span = std::move(build_span),
                             handler = std::move(handler)](const auto& build_resp) {
                              if (const auto retries = build_resp.ctx.retry_attempts;
                                  build_span->uses_tags() && retries > 0) {
                                build_span->add_tag(core::tracing::attributes::op::retry_count,
                                                    retries);
                              }
                              build_span->end();
                              top_span->end();
                              return handler(core::impl::make_error(build_resp.ctx));
                            });
      });
  }

  void watch_indexes(const std::string& bucket_name,
                     const std::string& scope_name,
                     const std::string& collection_name,
                     std::vector<std::string> index_names,
                     watch_query_indexes_options::built options,
                     watch_query_indexes_handler&& handler) const
  {
    auto ctx = std::make_shared<watch_context>(core_,
                                               bucket_name,
                                               std::move(index_names),
                                               std::move(options),
                                               scope_name,
                                               collection_name,
                                               std::move(handler));
    return ctx->execute();
  }

private:
  [[nodiscard]] auto create_span(const std::string& operation_name,
                                 const std::string& bucket_name,
                                 const std::string& scope_name,
                                 const std::string& collection_name,
                                 const std::shared_ptr<tracing::request_span>& parent_span) const
    -> std::shared_ptr<tracing::request_span>
  {
    auto span = core_.tracer()->create_span(operation_name, parent_span);
    if (span->uses_tags()) {
      span->add_tag(core::tracing::attributes::op::service, core::tracing::service::query);
      span->add_tag(core::tracing::attributes::op::bucket_name, bucket_name);
      if (!collection_name.empty()) {
        span->add_tag(core::tracing::attributes::op::scope_name, scope_name);
        span->add_tag(core::tracing::attributes::op::collection_name, collection_name);
      }
      span->add_tag(core::tracing::attributes::op::operation_name, operation_name);
    }
    return span;
  }

  core::cluster core_;
};

query_index_manager::query_index_manager(core::cluster core)
  : impl_(std::make_shared<query_index_manager_impl>(std::move(core)))
{
}

void
query_index_manager::get_all_indexes(const std::string& bucket_name,
                                     const get_all_query_indexes_options& options,
                                     get_all_query_indexes_handler&& handler) const
{
  return impl_->get_all_indexes(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::get_all_indexes(const std::string& bucket_name,
                                     const get_all_query_indexes_options& options) const
  -> std::future<std::pair<error, std::vector<management::query_index>>>
{
  auto barrier =
    std::make_shared<std::promise<std::pair<error, std::vector<management::query_index>>>>();
  auto future = barrier->get_future();
  get_all_indexes(bucket_name, options, [barrier](auto err, const auto& resp) mutable {
    barrier->set_value({ std::move(err), resp });
  });
  return future;
}

void
query_index_manager::create_index(const std::string& bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> keys,
                                  const create_query_index_options& options,
                                  create_query_index_handler&& handler) const
{
  return impl_->create_index(bucket_name,
                             {},
                             {},
                             std::move(index_name),
                             std::move(keys),
                             options.build(),
                             std::move(handler));
}

auto
query_index_manager::create_index(const std::string& bucket_name,
                                  std::string index_name,
                                  std::vector<std::string> keys,
                                  const create_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  create_index(bucket_name, std::move(index_name), std::move(keys), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
query_index_manager::create_primary_index(const std::string& bucket_name,
                                          const create_primary_query_index_options& options,
                                          create_query_index_handler&& handler) const
{
  return impl_->create_primary_index(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::create_primary_index(const std::string& bucket_name,
                                          const create_primary_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  create_primary_index(bucket_name, options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
query_index_manager::drop_primary_index(const std::string& bucket_name,
                                        const drop_primary_query_index_options& options,
                                        drop_query_index_handler&& handler) const
{
  return impl_->drop_primary_index(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::drop_primary_index(const std::string& bucket_name,
                                        const drop_primary_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  drop_primary_index(bucket_name, options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
query_index_manager::drop_index(const std::string& bucket_name,
                                std::string index_name,
                                const drop_query_index_options& options,
                                drop_query_index_handler&& handler) const
{
  return impl_->drop_index(
    bucket_name, {}, {}, std::move(index_name), options.build(), std::move(handler));
}

auto
query_index_manager::drop_index(const std::string& bucket_name,
                                std::string index_name,
                                const drop_query_index_options& options) const -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  drop_index(bucket_name, std::move(index_name), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
query_index_manager::build_deferred_indexes(const std::string& bucket_name,
                                            const build_query_index_options& options,
                                            build_deferred_query_indexes_handler&& handler) const
{
  return impl_->build_deferred_indexes(bucket_name, {}, {}, options.build(), std::move(handler));
}

auto
query_index_manager::build_deferred_indexes(const std::string& bucket_name,
                                            const build_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  build_deferred_indexes(bucket_name, options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
query_index_manager::watch_indexes(const std::string& bucket_name,
                                   std::vector<std::string> index_names,
                                   const watch_query_indexes_options& options,
                                   watch_query_indexes_handler&& handler) const
{
  return impl_->watch_indexes(
    bucket_name, {}, {}, std::move(index_names), options.build(), std::move(handler));
}

auto
query_index_manager::watch_indexes(const std::string& bucket_name,
                                   std::vector<std::string> index_names,
                                   const watch_query_indexes_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  watch_indexes(bucket_name, std::move(index_names), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
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
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options,
                                                get_all_query_indexes_handler&& handler) const
{
  return impl_->get_all_indexes(
    bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options) const
  -> std::future<std::pair<error, std::vector<management::query_index>>>
{
  auto barrier =
    std::make_shared<std::promise<std::pair<error, std::vector<management::query_index>>>>();
  auto future = barrier->get_future();
  get_all_indexes(options, [barrier](auto err, const auto& resp) mutable {
    barrier->set_value({ std::move(err), resp });
  });
  return future;
}

void
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> keys,
                                             const create_query_index_options& options,
                                             create_query_index_handler&& handler) const
{
  return impl_->create_index(bucket_name_,
                             scope_name_,
                             collection_name_,
                             std::move(index_name),
                             std::move(keys),
                             options.build(),
                             std::move(handler));
}

auto
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> keys,
                                             const create_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  create_index(std::move(index_name), std::move(keys), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
collection_query_index_manager::create_primary_index(
  const create_primary_query_index_options& options,
  create_primary_query_index_handler&& handler) const
{
  return impl_->create_primary_index(
    bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::create_primary_index(
  const create_primary_query_index_options& options) const -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  create_primary_index(options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
collection_query_index_manager::drop_primary_index(const drop_primary_query_index_options& options,
                                                   drop_primary_query_index_handler&& handler) const
{
  return impl_->drop_primary_index(
    bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::drop_primary_index(
  const drop_primary_query_index_options& options) const -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  drop_primary_index(options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
collection_query_index_manager::drop_index(std::string index_name,
                                           const drop_query_index_options& options,
                                           drop_query_index_handler&& handler) const
{
  return impl_->drop_index(bucket_name_,
                           scope_name_,
                           collection_name_,
                           std::move(index_name),
                           options.build(),
                           std::move(handler));
}

auto
collection_query_index_manager::drop_index(std::string index_name,
                                           const drop_query_index_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  drop_index(std::move(index_name), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
collection_query_index_manager::build_deferred_indexes(
  const build_query_index_options& options,
  build_deferred_query_indexes_handler&& handler) const
{
  return impl_->build_deferred_indexes(
    bucket_name_, scope_name_, collection_name_, options.build(), std::move(handler));
}

auto
collection_query_index_manager::build_deferred_indexes(
  const build_query_index_options& options) const -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  build_deferred_indexes(options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}

void
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names,
                                              const watch_query_indexes_options& options,
                                              watch_query_indexes_handler&& handler) const
{
  return impl_->watch_indexes(bucket_name_,
                              scope_name_,
                              collection_name_,
                              std::move(index_names),
                              options.build(),
                              std::move(handler));
}

auto
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names,
                                              const watch_query_indexes_options& options) const
  -> std::future<error>
{
  auto barrier = std::make_shared<std::promise<error>>();
  auto future = barrier->get_future();
  watch_indexes(std::move(index_names), options, [barrier](auto err) {
    barrier->set_value(std::move(err));
  });
  return future;
}
} // namespace couchbase
