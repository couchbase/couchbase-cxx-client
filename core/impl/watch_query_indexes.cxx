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

#include <couchbase/error_codes.hxx>
#include <couchbase/management/query_index.hxx>
#include <couchbase/query_index_manager.hxx>

#include "core/cluster.hxx"
#include "core/operations/management/query_index_get_all.hxx"

namespace couchbase::core::impl
{
class watch_context : public std::enable_shared_from_this<watch_context>
{

  private:
    std::shared_ptr<couchbase::core::cluster> core_;
    std::string bucket_name_;
    std::vector<std::string> index_names_;
    couchbase::watch_query_indexes_options::built options_;
    query_context query_ctx_;
    std::string collection_name_;
    watch_query_indexes_handler handler_;
    asio::steady_timer timer_{ core_->io_context() };
    std::chrono::steady_clock::time_point start_time_{ std::chrono::steady_clock::now() };
    std::chrono::milliseconds timeout_{ options_.timeout.value_or(core_->origin().second.options().query_timeout) };
    std::atomic<size_t> attempts_{ 0 };

    template<typename Response>
    void finish(Response& resp, std::optional<std::error_code> ec = {})
    {
        handler_({ manager_error_context(internal_manager_error_context{ ec ? ec.value() : resp.ctx.ec,
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
    std::chrono::milliseconds remaining()
    {
        return timeout_ - std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_);
    }
    operations::management::query_index_get_all_request make_request()
    {
        return { bucket_name_, "", collection_name_, query_ctx_, {}, remaining() };
    }

    bool check(couchbase::core::operations::management::query_index_get_all_response resp)
    {
        bool complete = true;
        for (auto name : index_names_) {
            auto it = std::find_if(resp.indexes.begin(), resp.indexes.end(), [&](const couchbase::management::query::index& index) {
                return index.name == name;
            });
            if (it == resp.indexes.end()) {
                finish(resp, couchbase::errc::common::index_not_found);
                return complete;
            }
            complete &= (it != resp.indexes.end() && it->state == "online");
        }
        if (options_.watch_primary) {
            auto it = std::find_if(
              resp.indexes.begin(), resp.indexes.end(), [&](const couchbase::management::query::index& index) { return index.is_primary; });
            complete &= it != resp.indexes.end() && it->state == "online";
        }
        if (complete || resp.ctx.ec == couchbase::errc::common::ambiguous_timeout) {
            finish(resp);
        } else if (remaining().count() <= 0) {
            finish(resp, couchbase::errc::common::ambiguous_timeout);
            complete = true;
        }
        return complete;
    }

    void poll()
    {
        timer_.expires_after(options_.polling_interval);
        auto timer_f = [ctx = shared_from_this()](asio::error_code) { ctx->execute(); };
        timer_.async_wait(timer_f);
    }

  public:
    watch_context(std::shared_ptr<couchbase::core::cluster> core,
                  std::string bucket_name,
                  std::vector<std::string> index_names,
                  couchbase::watch_query_indexes_options::built options,
                  query_context query_ctx,
                  std::string collection_name,
                  watch_query_indexes_handler&& handler)
      : core_(core)
      , bucket_name_(bucket_name)
      , index_names_(index_names)
      , options_(options)
      , query_ctx_(query_ctx)
      , collection_name_(collection_name)
      , handler_(std::move(handler))
      , attempts_(0)
    {
    }
    watch_context(watch_context&& other)
      : core_(std::move(other.core_))
      , bucket_name_(std::move(other.bucket_name_))
      , index_names_(std::move(other.index_names_))
      , options_(std::move(other.options_))
      , query_ctx_(std::move(other.query_ctx_))
      , collection_name_(std::move(other.collection_name_))
      , handler_(std::move(other.handler_))
      , timer_(std::move(other.timer_))
      , start_time_(std::move(other.start_time_))
      , timeout_(std::move(other.timeout_))
      , attempts_(other.attempts_.load())
    {
    }

    void execute()
    {
        auto req = make_request();
        CB_LOG_TRACE("watch indexes executing request");
        auto resp_fn = [ctx = shared_from_this()](operations::management::query_index_get_all_response resp) {
            CB_LOG_TRACE("watch indexes got {}", resp.ctx.ec.message());
            if (!ctx->check(resp)) {
                // now we try again
                ctx->poll();
            }
        };
        core_->execute(req, resp_fn);
    }
};
} // namespace couchbase::core::impl

namespace couchbase
{
void
query_index_manager::watch_indexes(std::string bucket_name,
                                   std::vector<std::string> index_names,
                                   const couchbase::watch_query_indexes_options& options,
                                   couchbase::watch_query_indexes_handler&& handler)
{
    auto ctx = std::make_shared<couchbase::core::impl::watch_context>(
      core_, std::move(bucket_name), std::move(index_names), options.build(), core::query_context{}, "", std::move(handler));
    ctx->execute();
}

auto
query_index_manager::watch_indexes(std::string bucket_name,
                                   std::vector<std::string> index_names,
                                   const couchbase::watch_query_indexes_options& options) -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    watch_indexes(
      std::move(bucket_name), std::move(index_names), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names,
                                              const watch_query_indexes_options& options,
                                              watch_query_indexes_handler&& handler) const
{
    auto ctx = std::make_shared<couchbase::core::impl::watch_context>(core_,
                                                                      bucket_name_,
                                                                      std::move(index_names),
                                                                      options.build(),
                                                                      core::query_context(bucket_name_, scope_name_),
                                                                      collection_name_,
                                                                      std::move(handler));
    ctx->execute();
}

auto
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names, const couchbase::watch_query_indexes_options& options)
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    watch_indexes(std::move(index_names), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
