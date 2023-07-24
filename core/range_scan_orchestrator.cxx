/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "range_scan_orchestrator.hxx"

#include "agent.hxx"
#include "core/logger/logger.hxx"
#include "couchbase/error_codes.hxx"

#include <asio/bind_executor.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <asio/experimental/concurrent_channel.hpp>

#include <gsl/narrow>

#include <future>
#include <random>

namespace couchbase::core
{
[[nodiscard]] auto
mutation_state_to_snapshot_requirements(const std::optional<mutation_state>& state)
  -> std::map<std::size_t, std::optional<range_snapshot_requirements>>
{
    if (!state) {
        return {};
    }

    std::map<std::size_t, std::optional<range_snapshot_requirements>> requirements;
    for (const auto& token : state->tokens) {
        auto& requirement = requirements[token.partition_id()];
        if (!requirement.has_value() || requirement->sequence_number < token.sequence_number()) {
            requirement.emplace(range_snapshot_requirements{ token.partition_uuid(), token.sequence_number() });
        }
    }
    return requirements;
}

class range_scan_stream : public std::enable_shared_from_this<range_scan_stream>
{
    struct failed {
        std::error_code ec;
        bool fatal{ true };
    };

    struct not_started {
    };

    struct awaiting_retry {
        std::error_code ec;
    };

    struct running {
        std::vector<std::byte> uuid;
    };

    struct completed {
    };

  public:
    range_scan_stream(asio::io_context& io,
                      agent kv_provider,
                      std::uint16_t vbucket_id,
                      std::int16_t node_id,
                      range_scan_create_options create_options,
                      range_scan_continue_options continue_options,
                      std::shared_ptr<scan_stream_manager> stream_manager)
      : items_{ io, continue_options.batch_item_limit }
      , agent_{ std::move(kv_provider) }
      , vbucket_id_{ vbucket_id }
      , node_id_{ node_id }
      , create_options_{ std::move(create_options) }
      , continue_options_{ std::move(continue_options) }
      , stream_manager_{ std::move(stream_manager) }
    {
    }

    void start()
    {
        // Fail the stream if more time since the timeout has elapsed since the stream was first attempted (if this is a retry)
        if (first_attempt_timestamp_.has_value()) {
            if (std::chrono::steady_clock::now() - first_attempt_timestamp_.value() > create_options_.timeout) {
                CB_LOG_DEBUG("stream for vbucket_id {} cannot be retried any longer because it has exceeded the timeout", vbucket_id_);
                state_ = failed{ errc::common::unambiguous_timeout, !is_sampling_scan() };
                stream_manager_->stream_start_failed(node_id_, error_is_fatal());
                drain_waiting_queue();
                return;
            }
        } else {
            first_attempt_timestamp_ = std::chrono::steady_clock::now();
        }

        CB_LOG_TRACE("starting stream {} in node {}", vbucket_id_, node_id_);
        state_ = std::monostate{};
        if (std::holds_alternative<range_scan>(create_options_.scan_type) && !last_seen_key_.empty()) {
            std::get<range_scan>(create_options_.scan_type).from = scan_term{ last_seen_key_ };
        }

        auto op = agent_.range_scan_create(vbucket_id_, create_options_, [self = shared_from_this()](auto res, auto ec) {
            if (ec) {
                if (ec == errc::key_value::document_not_found) {
                    // Benign error
                    CB_LOG_DEBUG("ignoring vbucket_id {} because no documents exist for it", self->vbucket_id_);
                    CB_LOG_TRACE("setting state for stream {} to FAILED", self->vbucket_id_);
                    self->state_ = failed{ ec, false };
                    self->stream_manager_->stream_start_failed(self->node_id_, self->error_is_fatal());
                } else if (ec == errc::common::temporary_failure) {
                    // Retryable error
                    CB_LOG_DEBUG("received busy status from vbucket with ID {} - reducing concurrency & will retry", self->vbucket_id_);
                    CB_LOG_TRACE("setting state for stream {} to AWAITING_RETRY", self->vbucket_id_);
                    self->state_ = awaiting_retry{ ec };
                    self->stream_manager_->stream_start_failed_awaiting_retry(self->node_id_, self->vbucket_id_);
                } else if (ec == errc::common::internal_server_failure || ec == errc::common::collection_not_found) {
                    // Fatal errors
                    CB_LOG_TRACE("setting state for stream {} to FAILED", self->vbucket_id_);
                    self->state_ = failed{ ec, true };
                    self->stream_manager_->stream_start_failed(self->node_id_, self->error_is_fatal());
                } else {
                    // Unexpected errors
                    CB_LOG_DEBUG(
                      "received unexpected error {} from stream for vbucket {} ({})", ec.value(), self->vbucket_id_, ec.message());
                    CB_LOG_TRACE("setting state for stream {} to FAILED", self->vbucket_id_);
                    self->state_ = failed{ ec, true };
                    self->stream_manager_->stream_start_failed(self->node_id_, self->error_is_fatal());
                }
                self->drain_waiting_queue();
                return;
            }
            self->state_ = running{ std::move(res.scan_uuid) };
            CB_LOG_TRACE("setting state for stream {} to RUNNING", self->vbucket_id_);
            self->drain_waiting_queue();
            self->resume();
        });
    }

    void fail(std::error_code ec)
    {
        if (!is_failed()) {
            if (is_running()) {
                agent_.range_scan_cancel(uuid(), vbucket_id_, {}, [](auto /* res */, auto /* ec */) {});
            }

            items_.cancel();
            items_.close();

            bool fatal{};
            if (ec == errc::key_value::document_not_found || ec == errc::common::authentication_failure ||
                ec == errc::common::collection_not_found || ec == errc::common::request_canceled) {
                // Errors that are fatal unless this is a sampling scan
                fatal = !is_sampling_scan();
            } else if (ec == errc::common::feature_not_available || ec == errc::common::invalid_argument ||
                       ec == errc::common::temporary_failure) {
                // Errors that are always fatal
                fatal = true;
            } else {
                // Unexpected error - always fatal
                CB_LOG_DEBUG("received unexpected error {} from stream for vbucket during range scan continue {} ({})",
                             ec.value(),
                             vbucket_id_,
                             ec.message());
                fatal = true;
            }

            CB_LOG_TRACE("setting state for stream {} to FAILED after range scan continue", vbucket_id_);
            state_ = failed{ ec, fatal };
            stream_manager_->stream_continue_failed(node_id_, fatal);
        }
    }

    void mark_not_started()
    {
        state_ = not_started{};
    }

    void complete()
    {
        if (!is_failed() && !is_completed()) {
            CB_LOG_TRACE("setting state for stream {} to COMPLETED", vbucket_id_);

            stream_manager_->stream_completed(node_id_);
            state_ = completed{};
            drain_waiting_queue();
        }
    }

    void cancel()
    {
        if (!should_cancel_) {
            should_cancel_ = true;
            items_.cancel();
            items_.close();
        }
    }

    template<typename Handler>
    void take(Handler&& handler)
    {
        do_when_ready([self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
            self->take_when_ready(std::forward<Handler>(handler));
        });
    }

    [[nodiscard]] auto node_id() const -> int16_t
    {
        return node_id_;
    }

    [[nodiscard]] auto is_ready() const -> bool
    {
        return !std::holds_alternative<std::monostate>(state_);
    }

    [[nodiscard]] auto is_not_started() const -> bool
    {
        return std::holds_alternative<not_started>(state_);
    }

    [[nodiscard]] auto is_awaiting_retry() const -> bool
    {
        return std::holds_alternative<awaiting_retry>(state_);
    }

    [[nodiscard]] auto is_running() const -> bool
    {
        return std::holds_alternative<running>(state_);
    }

    [[nodiscard]] auto is_failed() const -> bool
    {
        return std::holds_alternative<failed>(state_);
    }

    [[nodiscard]] auto is_completed() const -> bool
    {
        return std::holds_alternative<completed>(state_);
    }

  private:
    template<typename Handler>
    void take_when_ready(Handler&& handler)
    {

        if (is_failed()) {
            if (error_is_fatal()) {
                return handler(std::optional<range_scan_item>{}, false, std::optional<std::error_code>{ error() });
            } else {
                return handler(std::optional<range_scan_item>{}, false, std::optional<std::error_code>{});
            }
        }
        if (is_awaiting_retry() || is_not_started()) {
            return handler(std::optional<range_scan_item>{}, true, std::optional<std::error_code>{});
        }
        if (!items_.ready()) {
            return handler(std::optional<range_scan_item>{}, is_running(), std::optional<std::error_code>{});
        }
        items_.async_receive(
          [self = shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec, range_scan_item item) mutable {
              if (ec) {
                  return handler(std::optional<range_scan_item>{}, false, std::optional<std::error_code>{});
              }
              handler(std::optional<range_scan_item>{ std::move(item) }, true, std::optional<std::error_code>{});
          });
    }

    template<typename Handler>
    void do_when_ready(Handler&& handler)
    {
        if (is_ready()) {
            drain_waiting_queue();
            return handler();
        }
        waiting_queue_.emplace_back(std::forward<Handler>(handler));
    }

    void drain_waiting_queue()
    {
        auto queue = std::move(waiting_queue_);
        for (auto const& waiter : queue) {
            waiter();
        }
    }

    void resume()
    {
        if (!is_running()) {
            return;
        }
        if (should_cancel_) {
            agent_.range_scan_cancel(uuid(), vbucket_id_, {}, [](auto /* res */, auto /* ec */) {});
            items_.close();
            items_.cancel();
            return;
        }

        agent_.range_scan_continue(
          uuid(),
          vbucket_id_,
          continue_options_,
          [self = shared_from_this()](auto item) {
              self->last_seen_key_ = item.key;
              self->items_.async_send({}, std::move(item), [self](std::error_code ec) {
                  if (ec) {
                      self->fail(ec);
                  }
              });
          },
          [self = shared_from_this()](auto res, auto ec) {
              if (ec) {
                  return self->fail(ec);
              }
              if (res.complete) {
                  return self->complete();
              }
              if (res.more) {
                  return self->resume();
              }
          });
    }

    [[nodiscard]] auto uuid() const -> std::vector<std::byte>
    {
        if (is_running()) {
            return std::get<running>(state_).uuid;
        }
        return {};
    }

    [[nodiscard]] auto error() const -> std::error_code
    {
        if (is_failed()) {
            return std::get<failed>(state_).ec;
        }
        return {};
    }

    [[nodiscard]] auto error_is_fatal() const -> bool
    {
        if (is_failed()) {
            return std::get<failed>(state_).fatal;
        }
        return {};
    }

    [[nodiscard]] auto is_sampling_scan() const -> bool
    {
        return std::holds_alternative<sampling_scan>(create_options_.scan_type);
    }

    asio::experimental::concurrent_channel<void(std::error_code, range_scan_item)> items_;
    agent agent_;
    std::uint16_t vbucket_id_;
    std::int16_t node_id_;
    range_scan_create_options create_options_;
    range_scan_continue_options continue_options_;
    std::shared_ptr<scan_stream_manager> stream_manager_;
    std::string last_seen_key_{};
    std::variant<std::monostate, not_started, failed, awaiting_retry, running, completed> state_{};
    bool should_cancel_{ false };
    std::optional<std::chrono::time_point<std::chrono::steady_clock>> first_attempt_timestamp_{};
    std::vector<utils::movable_function<void()>> waiting_queue_{};
};

class range_scan_orchestrator_impl
  : public std::enable_shared_from_this<range_scan_orchestrator_impl>
  , public range_scan_item_iterator
  , public scan_stream_manager
{
  public:
    range_scan_orchestrator_impl(asio::io_context& io,
                                 agent kv_provider,
                                 topology::configuration::vbucket_map vbucket_map,
                                 std::string scope_name,
                                 std::string collection_name,
                                 std::variant<std::monostate, range_scan, prefix_scan, sampling_scan> scan_type,
                                 range_scan_orchestrator_options options)
      : io_{ io }
      , agent_{ std::move(kv_provider) }
      , vbucket_map_{ std::move(vbucket_map) }
      , scope_name_{ std::move(scope_name) }
      , collection_name_{ std::move(collection_name) }
      , scan_type_{ std::move(scan_type) }
      , options_{ std::move(options) }
      , vbucket_to_snapshot_requirements_{ mutation_state_to_snapshot_requirements(options_.consistent_with) }
      , concurrency_{ options_.concurrency }
    {

        if (std::holds_alternative<sampling_scan>(scan_type_)) {
            item_limit_ = std::get<sampling_scan>(scan_type).limit;
        }
    }

    auto scan() -> tl::expected<scan_result, std::error_code>
    {
        if (item_limit_ == 0) {
            return tl::unexpected(errc::common::invalid_argument);
        }
        if (concurrency_ <= 0) {
            return tl::unexpected(errc::common::invalid_argument);
        }

        // Get the collection ID before starting any of the streams
        {
            auto barrier = std::make_shared<std::promise<tl::expected<get_collection_id_result, std::error_code>>>();
            auto f = barrier->get_future();
            get_collection_id_options const get_cid_options{ options_.retry_strategy, options_.timeout, options_.parent_span };
            agent_.get_collection_id(scope_name_, collection_name_, get_cid_options, [barrier](auto result, auto ec) {
                if (ec) {
                    return barrier->set_value(tl::unexpected(ec));
                }
                barrier->set_value(result);
            });
            auto get_cid_res = f.get();
            if (!get_cid_res.has_value()) {
                return tl::unexpected(get_cid_res.error());
            }
            collection_id_ = get_cid_res->collection_id;
        }

        auto batch_time_limit = std::chrono::duration_cast<std::chrono::milliseconds>(0.9 * options_.timeout);
        range_scan_continue_options const continue_options{
            options_.batch_item_limit, options_.batch_byte_limit, batch_time_limit, options_.timeout, options_.retry_strategy,
        };
        for (std::uint16_t vbucket = 0; vbucket < gsl::narrow_cast<std::uint16_t>(vbucket_map_.size()); ++vbucket) {
            const range_scan_create_options create_options{
                scope_name_,       {},
                scan_type_,        options_.timeout,
                collection_id_,    vbucket_to_snapshot_requirements_[vbucket],
                options_.ids_only, options_.retry_strategy,
            };

            // Get the active node for the vbucket (values in vbucket map are the active node id followed by the ids of the replicas)
            auto node_id = vbucket_map_[vbucket][0];

            auto stream = std::make_shared<range_scan_stream>(io_,
                                                              agent_,
                                                              vbucket,
                                                              node_id,
                                                              create_options,
                                                              continue_options,
                                                              std::static_pointer_cast<scan_stream_manager>(shared_from_this()));
            streams_[vbucket] = stream;
            streams_[vbucket]->mark_not_started();
            if (stream_count_per_node_.count(node_id) == 0) {
                stream_count_per_node_[node_id] = 0;
            }
        }
        start_streams(concurrency_);

        return scan_result(shared_from_this());
    }

    void cancel() override
    {
        cancelled_ = true;
        for (const auto& [vbucket_id, stream] : streams_) {
            stream->cancel();
        }
    }

    bool is_cancelled() override
    {
        return cancelled_;
    }

    auto next() -> std::future<tl::expected<range_scan_item, std::error_code>> override
    {
        auto barrier = std::make_shared<std::promise<tl::expected<range_scan_item, std::error_code>>>();
        if (item_limit_ == 0 || item_limit_-- == 0) {
            barrier->set_value(tl::unexpected{ errc::key_value::range_scan_completed });
            cancel();
        } else {
            next_item(streams_.begin(), [barrier](std::optional<range_scan_item> item, std::optional<std::error_code> ec) {
                if (item) {
                    barrier->set_value(std::move(item.value()));
                } else if (ec) {
                    barrier->set_value(tl::unexpected{ ec.value() });
                } else {
                    barrier->set_value(tl::unexpected{ errc::key_value::range_scan_completed });
                }
            });
        }
        return barrier->get_future();
    }

    void next(utils::movable_function<void(range_scan_item, std::error_code)> callback) override
    {
        auto handler = [callback = std::move(callback)](std::optional<range_scan_item> item, std::optional<std::error_code> ec) mutable {
            if (item) {
                callback(std::move(item.value()), {});
            } else if (ec) {
                callback({}, ec.value());
            } else {
                callback({}, errc::key_value::range_scan_completed);
            }
        };
        if (item_limit_ == 0 || item_limit_-- == 0) {
            handler({}, {});
            cancel();
        } else {
            next_item(streams_.begin(), std::move(handler));
        }
    }

    void start_streams(std::uint16_t stream_count)
    {
        std::lock_guard<std::recursive_mutex> const lock(stream_start_mutex_);

        if (cancelled_) {
            CB_LOG_TRACE("scan has been cancelled, do not start another stream");
            return;
        }

        if (stream_count_per_node_.empty()) {
            CB_LOG_TRACE("no more vbuckets to scan");
            return;
        }

        std::uint16_t counter = 0;
        while (counter < stream_count) {
            // Find the node with the least number of active streams from those recorded in stream_count_per_node_
            int16_t least_busy_node{};
            {
                std::lock_guard<std::mutex> const stream_count_lock(stream_count_per_node_mutex_);

                // Pick a random node
                std::random_device rd;
                std::mt19937_64 gen(rd());
                std::uniform_int_distribution<std::size_t> dis(0, stream_count_per_node_.size() - 1);
                auto it = stream_count_per_node_.begin();
                std::advance(it, static_cast<decltype(stream_count_per_node_)::difference_type>(dis(gen)));
                least_busy_node = it->first;

                // If any other node has fewer streams running use that
                for (const auto& [node_id, count] : stream_count_per_node_) {
                    if (count < stream_count_per_node_[least_busy_node]) {
                        least_busy_node = node_id;
                    }
                }
            }

            std::shared_ptr<range_scan_stream> stream{};
            {
                std::lock_guard<std::mutex> const stream_map_lock(stream_map_mutex_);

                for (const auto& [v, s] : streams_) {
                    if ((s->is_not_started() || s->is_awaiting_retry()) && (s->node_id() == least_busy_node)) {
                        CB_LOG_TRACE("selected vbucket {} to scan", v);
                        stream = s;
                        break;
                    }
                }
            }

            if (stream == nullptr) {
                CB_LOG_TRACE("no vbuckets to scan for node {}", least_busy_node);
                {
                    std::lock_guard<std::mutex> const stream_count_lock(stream_count_per_node_mutex_);
                    stream_count_per_node_.erase(least_busy_node);
                }
                return start_streams(static_cast<std::uint16_t>(stream_count - counter));
            }

            auto node_id = stream->node_id();
            active_stream_count_++;
            stream_count_per_node_[node_id]++;
            stream->start();
            counter++;
        }
    }

    void stream_start_failed(std::int16_t node_id, bool fatal) override
    {
        stream_no_longer_running(node_id);
        if (fatal) {
            cancel();
        } else {
            start_streams(1);
        }
    }

    void stream_start_failed_awaiting_retry(std::int16_t node_id, std::uint16_t /* vbucket_id */) override
    {
        {
            std::lock_guard<std::mutex> const stream_count_lock(stream_count_per_node_mutex_);
            if (stream_count_per_node_.count(node_id) == 0) {
                stream_count_per_node_[node_id] = 1;
            }
        }
        stream_no_longer_running(node_id);
        if (active_stream_count_ == 0) {
            start_streams(1);
        }
    }

    void stream_continue_failed(std::int16_t node_id, bool fatal) override
    {
        stream_no_longer_running(node_id);
        if (fatal) {
            cancel();
        } else {
            start_streams(1);
        }
    }

    void stream_completed(std::int16_t node_id) override
    {
        stream_no_longer_running(node_id);
        start_streams(1);
    }

  private:
    void stream_no_longer_running(std::int16_t node_id)
    {
        {
            std::lock_guard<std::mutex> const stream_count_lock(stream_count_per_node_mutex_);
            if (stream_count_per_node_.count(node_id) > 0) {
                stream_count_per_node_[node_id]--;
            }
        }
        active_stream_count_--;
    }

    template<typename Iterator, typename Handler>
    void next_item(Iterator it, Handler&& handler)
    {
        if (streams_.empty() || cancelled_) {
            return handler({}, {});
        }
        auto vbucket_id = it->first;
        auto stream = it->second;
        stream->take([it = std::next(it), vbucket_id, self = shared_from_this(), handler = std::forward<Handler>(handler)](
                       auto item, bool has_more, auto ec) mutable {
            if (ec) {
                // Fatal error
                self->streams_.clear();
                return handler({}, ec);
            }
            if (!has_more) {
                std::lock_guard<std::mutex> const lock(self->stream_map_mutex_);
                self->streams_.erase(vbucket_id);
            }
            if (item) {
                return handler(std::move(item), {});
            }
            if (self->streams_.empty()) {
                return handler({}, {});
            }
            if (it == self->streams_.end()) {
                it = self->streams_.begin();
            }
            return asio::post(asio::bind_executor(self->io_, [it, self, handler = std::forward<Handler>(handler)]() mutable {
                self->next_item(it, std::forward<Handler>(handler));
            }));
        });
    }

    asio::io_context& io_;
    agent agent_;
    topology::configuration::vbucket_map vbucket_map_;
    std::string scope_name_;
    std::string collection_name_;
    std::uint32_t collection_id_;
    std::variant<std::monostate, range_scan, prefix_scan, sampling_scan> scan_type_;
    range_scan_orchestrator_options options_;
    std::map<std::size_t, std::optional<range_snapshot_requirements>> vbucket_to_snapshot_requirements_;
    std::map<std::uint16_t, std::shared_ptr<range_scan_stream>> streams_{};
    std::map<std::int16_t, std::atomic_uint16_t> stream_count_per_node_{};
    std::recursive_mutex stream_start_mutex_{};
    std::mutex stream_map_mutex_{};
    std::mutex stream_count_per_node_mutex_{};
    std::atomic_uint16_t active_stream_count_ = 0;
    std::uint16_t concurrency_ = 1;
    std::size_t item_limit_{ std::numeric_limits<size_t>::max() };
    bool cancelled_{ false };
};

range_scan_orchestrator::range_scan_orchestrator(asio::io_context& io,
                                                 agent kv_provider,
                                                 topology::configuration::vbucket_map vbucket_map,
                                                 std::string scope_name,
                                                 std::string collection_name,
                                                 std::variant<std::monostate, range_scan, prefix_scan, sampling_scan> scan_type,
                                                 range_scan_orchestrator_options options)
  : impl_{ std::make_shared<range_scan_orchestrator_impl>(io,
                                                          std::move(kv_provider),
                                                          std::move(vbucket_map),
                                                          std::move(scope_name),
                                                          std::move(collection_name),
                                                          std::move(scan_type),
                                                          std::move(options)) }
{
}

auto
range_scan_orchestrator::scan() -> tl::expected<scan_result, std::error_code>
{
    return impl_->scan();
}
} // namespace couchbase::core
