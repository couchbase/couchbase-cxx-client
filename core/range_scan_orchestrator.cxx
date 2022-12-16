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

#include <couchbase/retry_strategy.hxx>

#include <asio/bind_executor.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <asio/experimental/concurrent_channel.hpp>

#include <gsl/narrow>

#include <future>

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
                      range_scan_create_options create_options,
                      range_scan_continue_options continue_options)
      : items_{ io, continue_options.batch_item_limit }
      , agent_{ std::move(kv_provider) }
      , vbucket_id_{ vbucket_id }
      , create_options_{ std::move(create_options) }
      , continue_options_{ std::move(continue_options) }
    {
    }

    void start()
    {
        if (std::holds_alternative<range_scan>(create_options_.scan_type) && !last_seen_key_.empty()) {
            std::get<range_scan>(create_options_.scan_type).start_.id = last_seen_key_;
        }

        auto op = agent_.range_scan_create(vbucket_id_, create_options_, [self = shared_from_this()](auto res, auto ec) {
            if (ec) {
                self->state_ = failed{ ec };
                self->drain_waiting_queue();
                if (ec == errc::key_value::document_not_found) {
                    CB_LOG_DEBUG("ignoring vbucket_id {} because no documents exist for it", self->vbucket_id_);
                }
                return;
            }
            self->state_ = running{ std::move(res.scan_uuid) };
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
            state_ = failed{ ec };
            items_.close();
        }
    }

    void complete()
    {
        if (!is_failed() && !is_completed()) {
            state_ = completed{};
        }
    }

    auto pop() -> std::optional<range_scan_item>
    {
        if (peeked_) {
            std::optional<range_scan_item> item{};
            std::swap(peeked_, item);
            return item;
        }
        return peeked_;
    }

    template<typename Handler>
    void peek(Handler&& handler)
    {
        do_when_ready([self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
            self->peek_when_ready(std::forward<Handler>(handler));
        });
    }

    template<typename Handler>
    void take(Handler&& handler)
    {
        do_when_ready([self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
            self->take_when_ready(std::forward<Handler>(handler));
        });
    }

  private:
    template<typename Handler>
    void peek_when_ready(Handler&& handler)
    {
        if (is_failed()) {
            return handler(std::optional<range_scan_item>{});
        }

        if (peeked_) {
            return handler(peeked_);
        }

        if (is_completed() && !items_.ready()) {
            return handler(std::optional<range_scan_item>{});
        }

        items_.async_receive(
          [self = shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec, range_scan_item item) mutable {
              if (ec) {
                  self->peeked_ = {};
              } else {
                  self->peeked_ = std::move(item);
              }
              handler(self->peeked_);
          });
    }

    template<typename Handler>
    void take_when_ready(Handler&& handler)
    {
        if (is_failed()) {
            return handler(std::optional<range_scan_item>{}, false);
        }
        if (!items_.ready()) {
            return handler(std::optional<range_scan_item>{}, is_running());
        }
        items_.async_receive(
          [self = shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec, range_scan_item item) mutable {
              if (ec) {
                  return handler(std::optional<range_scan_item>{}, false);
              }
              handler(std::optional<range_scan_item>{ std::move(item) }, true);
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

    [[nodiscard]] auto is_ready() const -> bool
    {
        return !std::holds_alternative<std::monostate>(state_);
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

    asio::experimental::concurrent_channel<void(std::error_code, range_scan_item)> items_;
    agent agent_;
    std::uint16_t vbucket_id_;
    range_scan_create_options create_options_;
    range_scan_continue_options continue_options_;
    std::vector<std::byte> last_seen_key_{};
    std::variant<std::monostate, failed, running, completed> state_{};
    std::optional<range_scan_item> peeked_{};
    std::vector<utils::movable_function<void()>> waiting_queue_{};
};

struct lowest_item {
    std::uint16_t vbucket_id;
    std::vector<std::byte> key;
};

static auto
less(std::vector<std::byte>& a, std::vector<std::byte>& b) -> bool
{
    auto common_size = std::min(a.size(), b.size());
    for (std::size_t i = 0; i < common_size; ++i) {
        if (a[i] < b[i]) {
            return true;
        }
        if (a[i] > b[i]) {
            return false;
        }
    }
    return a.size() < b.size();
}

class range_scan_orchestrator_impl
  : public std::enable_shared_from_this<range_scan_orchestrator_impl>
  , public range_scan_item_iterator
{
  public:
    range_scan_orchestrator_impl(asio::io_context& io,
                                 agent kv_provider,
                                 std::size_t num_vbuckets,
                                 std::string scope_name,
                                 std::string collection_name,
                                 std::variant<std::monostate, range_scan, sampling_scan> scan_type,
                                 range_scan_orchestrator_options options)
      : io_{ io }
      , agent_{ std::move(kv_provider) }
      , num_vbuckets_{ num_vbuckets }
      , scope_name_{ std::move(scope_name) }
      , collection_name_{ std::move(collection_name) }
      , scan_type_{ std::move(scan_type) }
      , options_{ std::move(options) }
      , vbucket_to_snapshot_requirements_{ mutation_state_to_snapshot_requirements(options_.consistent_with) }
    {
        if (std::holds_alternative<sampling_scan>(scan_type_)) {
            item_limit = std::get<sampling_scan>(scan_type).limit;
        }
    }

    auto scan() -> tl::expected<scan_result, std::error_code>
    {
        if (item_limit == 0) {
            return tl::unexpected(errc::common::invalid_argument);
        }
        range_scan_continue_options continue_options{
            options_.batch_item_limit, options_.batch_byte_limit, options_.batch_time_limit, options_.retry_strategy, options_.ids_only,
        };
        continue_options.batch_time_limit = std::chrono::seconds{ 10 };
        for (std::uint16_t vbucket = 0; vbucket < gsl::narrow_cast<std::uint16_t>(num_vbuckets_); ++vbucket) {
            auto stream = std::make_shared<range_scan_stream>(io_,
                                                              agent_,
                                                              vbucket,
                                                              range_scan_create_options{
                                                                scope_name_,
                                                                collection_name_,
                                                                scan_type_,
                                                                options_.timeout,
                                                                {},
                                                                vbucket_to_snapshot_requirements_[vbucket],
                                                                options_.ids_only,
                                                                options_.retry_strategy,
                                                              },
                                                              continue_options);
            streams_[vbucket] = stream;
            stream->start();
        }

        return scan_result(shared_from_this());
    }

    auto next() -> std::future<std::optional<range_scan_item>> override
    {
        auto barrier = std::make_shared<std::promise<std::optional<range_scan_item>>>();
        if (item_limit == 0 || item_limit-- == 0) {
            barrier->set_value(std::nullopt);
            streams_.clear();
        } else {
            if (options_.sort == scan_sort::none) {
                next_item(streams_.begin(), [barrier](std::optional<range_scan_item> item) { barrier->set_value(std::move(item)); });
            } else {
                next_item_sorted(
                  {}, streams_.begin(), [barrier](std::optional<range_scan_item> item) { barrier->set_value(std::move(item)); });
            }
        }
        return barrier->get_future();
    }

    void next(utils::movable_function<void(range_scan_item, std::error_code)> callback) override
    {
        auto handler = [callback = std::move(callback)](std::optional<range_scan_item> item) mutable {
            if (item) {
                callback(std::move(item.value()), {});
            } else {
                callback({}, errc::key_value::range_scan_completed);
            }
        };
        if (item_limit == 0 || item_limit-- == 0) {
            handler({});
        } else {
            if (options_.sort == scan_sort::none) {
                next_item(streams_.begin(), std::move(handler));
            } else {
                next_item_sorted({}, streams_.begin(), std::move(handler));
            }
        }
    }

  private:
    template<typename Iterator, typename Handler>
    void next_item(Iterator it, Handler&& handler)
    {
        if (streams_.empty()) {
            return handler({});
        }
        auto vbucket_id = it->first;
        auto stream = it->second;
        stream->take([it = std::next(it), vbucket_id, self = shared_from_this(), handler = std::forward<Handler>(handler)](
                       auto item, bool has_more) mutable {
            if (!has_more) {
                self->streams_.erase(vbucket_id);
            }
            if (item) {
                return handler(std::move(item));
            }
            if (self->streams_.empty()) {
                return handler({});
            }
            if (it == self->streams_.end()) {
                it = self->streams_.begin();
            }
            return asio::post(asio::bind_executor(self->io_, [it, self, handler = std::forward<Handler>(handler)]() mutable {
                self->next_item(it, std::forward<Handler>(handler));
            }));
        });
    }

    template<typename Iterator, typename Handler>
    void next_item_sorted(std::optional<lowest_item> lowest, Iterator it, Handler&& handler)
    {
        if (streams_.empty()) {
            return handler({});
        }
        auto vbucket_id = it->first;
        auto stream = it->second;
        stream->peek(
          [lowest = std::move(lowest), it = std::next(it), vbucket_id, self = shared_from_this(), handler = std::forward<Handler>(handler)](
            auto item) mutable {
              if (item) {
                  if (!lowest || less(item->key, lowest->key)) {
                      lowest = { vbucket_id, item->key };
                  }
              } else {
                  self->streams_.erase(vbucket_id);
              }

              if (it != self->streams_.end()) {
                  return asio::post(asio::bind_executor(
                    self->io_, [lowest = std::move(lowest), it, self, handler = std::forward<Handler>(handler)]() mutable {
                        self->next_item_sorted(std::move(lowest), it, std::forward<Handler>(handler));
                    }));
              } else if (lowest) {
                  return handler(self->streams_[lowest->vbucket_id]->pop());
              } else {
                  return handler({});
              }
          });
    }

    asio::io_context& io_;
    agent agent_;
    std::size_t num_vbuckets_;
    std::string scope_name_;
    std::string collection_name_;
    std::variant<std::monostate, range_scan, sampling_scan> scan_type_;
    range_scan_orchestrator_options options_;
    std::map<std::size_t, std::optional<range_snapshot_requirements>> vbucket_to_snapshot_requirements_;
    std::map<std::uint16_t, std::shared_ptr<range_scan_stream>> streams_{};
    std::size_t item_limit{ std::numeric_limits<size_t>::max() };
};

range_scan_orchestrator::range_scan_orchestrator(asio::io_context& io,
                                                 agent kv_provider,
                                                 std::size_t num_vbuckets,
                                                 std::string scope_name,
                                                 std::string collection_name,
                                                 std::variant<std::monostate, range_scan, sampling_scan> scan_type,
                                                 range_scan_orchestrator_options options)
  : impl_{ std::make_shared<range_scan_orchestrator_impl>(io,
                                                          std::move(kv_provider),
                                                          num_vbuckets,
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
