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
#include "collections_options.hxx"
#include "core/platform/base64.h"
#include "core/tracing/attribute_helpers.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "logger/logger.hxx"
#include "range_scan_load_balancer.hxx"
#include "range_scan_options.hxx"

#include "couchbase/collection.hxx"
#include "couchbase/error_codes.hxx"
#include "couchbase/scope.hxx"
#include "utils/movable_function.hxx"

#include <asio/bind_executor.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <gsl/util>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <variant>
#include <vector>

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
      requirement.emplace(
        range_snapshot_requirements{ token.partition_uuid(), token.sequence_number() });
    }
  }
  return requirements;
}

// Sent by the vbucket scan stream when it either completes or fails with a fatal error
struct scan_stream_end_signal {
  std::uint16_t vbucket_id;
  std::optional<std::error_code> error{};
};

range_scan_span_tracker::range_scan_span_tracker(
  utils::movable_function<void(std::error_code)> end_span)
  : end_span_{ std::move(end_span) }
{
}

auto
range_scan_span_tracker::partition_opened() -> bool
{
  const std::scoped_lock lock{ mutex_ };
  if (!end_span_) {
    return false;
  }
  ++open_partitions_;
  return true;
}

void
range_scan_span_tracker::partition_closed()
{
  std::unique_lock lock{ mutex_ };
  --open_partitions_;
  end_if_done(std::move(lock));
}

void
range_scan_span_tracker::finish(std::error_code ec)
{
  std::unique_lock lock{ mutex_ };
  if (!result_) {
    result_ = ec;
  }
  end_if_done(std::move(lock));
}

void
range_scan_span_tracker::end_if_done(std::unique_lock<std::mutex> lock)
{
  if (!result_ || open_partitions_ > 0 || !end_span_) {
    return;
  }
  auto end_span = std::move(end_span_);
  end_span_ = nullptr;
  const auto ec = result_.value();
  lock.unlock();
  end_span(ec);
}

class range_scan_stream : public std::enable_shared_from_this<range_scan_stream>
{
  // The stream has failed and should not be retried
  struct failed {
    std::error_code ec;
    bool fatal{ true };
  };

  // The stream is currently running
  struct running {
    std::vector<std::byte> uuid{};
  };

  // The stream has completed and the items have been retrieved
  struct completed {
  };

public:
  range_scan_stream(asio::io_context& io,
                    agent kv_provider,
                    std::uint16_t vbucket_id,
                    std::int16_t node_id,
                    range_scan_create_options create_options,
                    range_scan_continue_options continue_options,
                    std::weak_ptr<scan_stream_manager> stream_manager,
                    std::shared_ptr<tracing::tracer_wrapper> tracer,
                    std::shared_ptr<couchbase::tracing::request_span> parent_span,
                    std::shared_ptr<range_scan_span_tracker> span_tracker)
    : agent_{ std::move(kv_provider) }
    , io_{ io }
    , vbucket_id_{ vbucket_id }
    , node_id_{ node_id }
    , create_options_{ std::move(create_options) }
    , continue_options_{ std::move(continue_options) }
    , stream_manager_{ std::move(stream_manager) }
    , tracer_{ std::move(tracer) }
    , parent_span_{ std::move(parent_span) }
    , span_tracker_{ std::move(span_tracker) }
  {
  }

  range_scan_stream(const range_scan_stream&) = delete;
  range_scan_stream(range_scan_stream&&) = delete;
  auto operator=(const range_scan_stream&) -> range_scan_stream& = delete;
  auto operator=(range_scan_stream&&) -> range_scan_stream& = delete;

  // A stream abandoned by a cancelled scan before reaching a terminal state ends its span here.
  ~range_scan_stream()
  {
    end_partition_span();
  }

  void start()
  {
    // Fail the stream if more time than the timeout has elapsed since the stream was first
    // attempted (if this is a retry)
    if (first_attempt_timestamp_.has_value()) {
      if (std::chrono::steady_clock::now() - first_attempt_timestamp_.value() >
          create_options_.timeout) {
        CB_LOG_DEBUG(
          "stream for vbucket_id {} cannot be retried because it has exceeded the timeout",
          vbucket_id_);
        state_ = failed{ errc::common::unambiguous_timeout, !is_sampling_scan() };
        end_partition_span();
        if (auto mgr = stream_manager_.lock(); mgr != nullptr) {
          mgr->stream_failed(
            node_id_, vbucket_id_, errc::common::unambiguous_timeout, error_is_fatal());
        }
        return;
      }
    } else {
      first_attempt_timestamp_ = std::chrono::steady_clock::now();
    }

    CB_LOG_TRACE("starting stream for vbucket {} in node {}", vbucket_id_, node_id_);

    if (std::holds_alternative<range_scan>(create_options_.scan_type) && !last_seen_key_.empty()) {
      std::get<range_scan>(create_options_.scan_type).from = scan_term{ last_seen_key_ };
    }

    if (tracer_ && !partition_span_ && (!span_tracker_ || span_tracker_->partition_opened())) {
      partition_span_ =
        tracer_->create_span(tracing::operation::mcbp_range_scan_partition, parent_span_);
      if (partition_span_->uses_tags()) {
        partition_span_->add_tag(tracing::attributes::range_scan::partition_id,
                                 std::uint64_t{ vbucket_id_ });
      }
    }
    auto create_options = create_options_;
    create_options.parent_span = start_create_span();

    agent_.range_scan_create(
      vbucket_id_,
      create_options,
      [self = shared_from_this(), span = create_options.parent_span](auto res, auto ec) {
        end_span(span);
        if (ec) {
          // A busy partition is retried under the same partition span.
          if (ec != errc::common::temporary_failure) {
            self->end_partition_span();
          }
          if (ec == errc::key_value::document_not_found) {
            // Benign error
            CB_LOG_TRACE("ignoring vbucket_id {} because no documents exist for it",
                         self->vbucket_id_);
            self->state_ = failed{ ec, false };
            if (auto mgr = self->stream_manager_.lock(); mgr != nullptr) {
              mgr->stream_failed(self->node_id_, self->vbucket_id_, ec, self->error_is_fatal());
            }
          } else if (ec == errc::common::temporary_failure) {
            // Retryable error - server is overwhelmed, retry after reducing concurrency
            CB_LOG_DEBUG("received busy status during scan from vbucket with ID {} - reducing "
                         "concurrency & retrying",
                         self->vbucket_id_);
            self->state_ = std::monostate{};
            if (auto mgr = self->stream_manager_.lock(); mgr != nullptr) {
              mgr->stream_start_failed_awaiting_retry(self->node_id_, self->vbucket_id_);
            }
          } else if (ec == errc::common::internal_server_failure ||
                     ec == errc::common::collection_not_found) {
            // Fatal errors
            self->state_ = failed{ ec, true };
            if (auto mgr = self->stream_manager_.lock(); mgr != nullptr) {
              mgr->stream_failed(self->node_id_, self->vbucket_id_, ec, self->error_is_fatal());
            }
          } else {
            // Unexpected errors
            CB_LOG_DEBUG("received unexpected error {} from stream for vbucket {} during range "
                         "scan continue ({})",
                         ec.value(),
                         self->vbucket_id_,
                         ec.message());
            self->state_ = failed{ ec, true };
            if (auto mgr = self->stream_manager_.lock(); mgr != nullptr) {
              mgr->stream_failed(self->node_id_, self->vbucket_id_, ec, self->error_is_fatal());
            }
          }
          return;
        }

        self->state_ = running{ std::move(res.scan_uuid) };

        return self->resume();
      });
  }

  void should_cancel()
  {
    should_cancel_ = true;
  }

  [[nodiscard]] auto node_id() const -> std::int16_t
  {
    return node_id_;
  }

private:
  void fail(std::error_code ec)
  {
    if (is_failed()) {
      return;
    }

    bool fatal = false;
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
      CB_LOG_DEBUG(
        "received unexpected error {} from stream for vbucket {} during range scan continue ({})",
        ec.value(),
        vbucket_id_,
        ec.message());
      fatal = true;
    }

    state_ = failed{ ec, fatal };
    end_partition_span();
    if (auto mgr = stream_manager_.lock(); mgr != nullptr) {
      mgr->stream_failed(node_id_, vbucket_id_, ec, fatal);
    }
  }

  void complete()
  {
    if (is_failed() || is_completed()) {
      return;
    }

    end_partition_span();
    if (auto mgr = stream_manager_.lock(); mgr != nullptr) {
      mgr->stream_completed(node_id_, vbucket_id_);
    }
    state_ = completed{};
  }

  void cancel()
  {
    auto scan_uuid = uuid();
    if (scan_uuid.empty()) {
      // The stream is not currently running
      return;
    }

    asio::post(asio::bind_executor(io_, [self = shared_from_this(), scan_uuid]() mutable {
      // A CANCEL with no deadline would hold the partition span, and so the scan span, until a
      // reply.
      range_scan_cancel_options options{};
      options.timeout = self->create_options_.timeout;
      options.parent_span =
        self->scan_uuid_span(tracing::operation::mcbp_range_scan_cancel, scan_uuid);
      self->agent_.range_scan_cancel(
        scan_uuid,
        self->vbucket_id_,
        options,
        [self, span = options.parent_span](auto /* res */, auto /* ec */) {
          end_span(span);
          self->end_partition_span();
        });
    }));
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

  void resume()
  {
    if (!is_running()) {
      return;
    }
    if (should_cancel_) {
      cancel();
      return;
    }

    asio::post(asio::bind_executor(io_, [self = shared_from_this()]() mutable {
      auto scan_uuid = self->uuid();
      auto continue_options = self->continue_options_;
      continue_options.parent_span =
        self->scan_uuid_span(tracing::operation::mcbp_range_scan_continue, scan_uuid);
      if (continue_options.parent_span && continue_options.parent_span->uses_tags()) {
        const auto& span = continue_options.parent_span;
        span->add_tag(tracing::attributes::range_scan::item_limit,
                      std::uint64_t{ continue_options.batch_item_limit });
        span->add_tag(tracing::attributes::range_scan::byte_limit,
                      std::uint64_t{ continue_options.batch_byte_limit });
        span->add_tag(tracing::attributes::range_scan::time_limit,
                      static_cast<std::uint64_t>(continue_options.batch_time_limit.count()));
      }
      self->agent_.range_scan_continue(
        scan_uuid,
        self->vbucket_id_,
        continue_options,
        [self](auto item) {
          // The scan has already been cancelled, no need to send items
          if (self->should_cancel_) {
            return;
          }
          self->last_seen_key_ = item.key;
          if (auto mgr = self->stream_manager_.lock(); mgr != nullptr) {
            mgr->stream_received_item(std::move(item));
          }
        },
        [self, span = continue_options.parent_span](auto res, auto ec) {
          end_span(span);
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
    }));
  }

  static void end_span(const std::shared_ptr<couchbase::tracing::request_span>& span)
  {
    if (span) {
      span->end();
    }
  }

  void end_partition_span()
  {
    if (auto span = std::exchange(partition_span_, nullptr); span) {
      span->end();
      if (span_tracker_) {
        span_tracker_->partition_closed();
      }
    }
  }

  [[nodiscard]] auto start_create_span() const -> std::shared_ptr<couchbase::tracing::request_span>
  {
    if (!tracer_) {
      return nullptr;
    }
    auto span = tracer_->create_span(tracing::operation::mcbp_range_scan_create, partition_span_);
    if (!span->uses_tags()) {
      return span;
    }
    tracing::add_bool_tag(
      span, tracing::attributes::range_scan::without_content, create_options_.ids_only);
    if (const auto range = resolve_range(create_options_.scan_type); range.has_value()) {
      const auto& [from, to] = range.value();
      span->add_tag(tracing::attributes::range_scan::scan_type, "range");
      span->add_tag(tracing::attributes::range_scan::from_term, base64::encode(from.term));
      span->add_tag(tracing::attributes::range_scan::to_term, base64::encode(to.term));
      tracing::add_bool_tag(span, tracing::attributes::range_scan::from_exclusive, from.exclusive);
      tracing::add_bool_tag(span, tracing::attributes::range_scan::to_exclusive, to.exclusive);
    } else if (const auto* sampling = std::get_if<sampling_scan>(&create_options_.scan_type);
               sampling != nullptr) {
      span->add_tag(tracing::attributes::range_scan::scan_type, "sampling");
      span->add_tag(tracing::attributes::range_scan::limit, sampling->limit);
      if (sampling->seed.has_value()) {
        span->add_tag(tracing::attributes::range_scan::seed, sampling->seed.value());
      }
    }
    return span;
  }

  // A CONTINUE or CANCEL span, tagged with the scan UUID as a 0x-prefixed hex string.
  [[nodiscard]] auto scan_uuid_span(const char* name, const std::vector<std::byte>& scan_uuid) const
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
    if (!tracer_) {
      return nullptr;
    }
    auto span = tracer_->create_span(name, partition_span_);
    if (span->uses_tags()) {
      std::string id{ "0x" };
      for (const auto b : scan_uuid) {
        id += fmt::format("{:02x}", std::to_integer<std::uint8_t>(b));
      }
      span->add_tag(tracing::attributes::range_scan::range_scan_id, id);
    }
    return span;
  }

  [[nodiscard]] auto uuid() const -> std::vector<std::byte>
  {
    if (!is_running()) {
      return {};
    }

    auto uuid = std::get<running>(state_).uuid;
    return uuid;
  }

  [[nodiscard]] auto error() const -> std::error_code
  {
    if (!is_failed()) {
      return {};
    }

    return std::get<failed>(state_).ec;
  }

  [[nodiscard]] auto error_is_fatal() const -> bool
  {
    if (!is_failed()) {
      return {};
    }

    return std::get<failed>(state_).fatal;
  }

  [[nodiscard]] auto is_sampling_scan() const -> bool
  {
    return std::holds_alternative<sampling_scan>(create_options_.scan_type);
  }

  agent agent_;
  asio::io_context& io_;
  std::uint16_t vbucket_id_;
  std::int16_t node_id_;
  range_scan_create_options create_options_;
  range_scan_continue_options continue_options_;
  std::weak_ptr<scan_stream_manager> stream_manager_;
  std::shared_ptr<tracing::tracer_wrapper> tracer_;
  std::shared_ptr<couchbase::tracing::request_span> parent_span_;
  std::shared_ptr<range_scan_span_tracker> span_tracker_;
  // Open from the first CREATE of the partition until it completes, fails or is cancelled.
  std::shared_ptr<couchbase::tracing::request_span> partition_span_{};
  std::string last_seen_key_{};
  std::variant<std::monostate, failed, running, completed> state_{};
  std::atomic<bool> should_cancel_{ false };
  std::optional<std::chrono::time_point<std::chrono::steady_clock>> first_attempt_timestamp_{};
};

class range_scan_orchestrator_impl
  : public std::enable_shared_from_this<range_scan_orchestrator_impl>
  , public range_scan_item_iterator
  , public scan_stream_manager
{
public:
  range_scan_orchestrator_impl(
    asio::io_context& io,
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
    , load_balancer_{ vbucket_map_ }
    , items_{ io, 1024 }
    , scan_type_{ std::move(scan_type) }
    , options_{ std::move(options) }
    , vbucket_to_snapshot_requirements_{ mutation_state_to_snapshot_requirements(
        options_.consistent_with) }
    , concurrency_{ options_.concurrency }
  {

    if (std::holds_alternative<sampling_scan>(scan_type_)) {
      auto s = std::get<sampling_scan>(scan_type);
      item_limit_ = s.limit;

      // Set the seed of the load balancer to ensure that if the sampling scan is run multiple times
      // the vbuckets are scanned in the same order when concurrency is 1. This guarantees that the
      // items returned will be the same. We cannot guarantee this when concurrency is greater than
      // 1, as the order of the vbucket scans depends on how long each scan takes and what the load
      // on a node is at any given time.
      if (s.seed.has_value()) {
        load_balancer_.seed(s.seed.value());
      }
    }
  }

  void scan(scan_callback&& cb)
  {
    if (item_limit_ == 0 || concurrency_ == 0) {
      return cb(errc::common::invalid_argument, {});
    }

    // A scan needs an active copy for every vbucket, and every row of the map is
    // indexed below to find one. A row that names no node at all leaves the scan
    // unable to cover its vbucket, which is reported rather than served as a
    // silently short result.
    if (std::any_of(vbucket_map_.begin(), vbucket_map_.end(), [](const auto& chain) {
          return chain.empty();
        })) {
      return cb(errc::network::configuration_not_available, {});
    }

    // The default collection always has ID 0, so it needs no lookup.
    if ((scope_name_.empty() || scope_name_ == scope::default_name) &&
        (collection_name_.empty() || collection_name_ == collection::default_name)) {
      return create_streams(std::move(cb));
    }

    std::shared_ptr<couchbase::tracing::request_span> span{};
    if (options_.tracer) {
      span = options_.tracer->create_span(tracing::operation::mcbp_get_collection_id,
                                          options_.parent_span);
    }
    const get_collection_id_options get_cid_options{ options_.retry_strategy,
                                                     options_.timeout,
                                                     span };
    agent_.get_collection_id(
      scope_name_,
      collection_name_,
      get_cid_options,
      [self = shared_from_this(), span, cb = std::move(cb)](auto get_cid_res, auto ec) mutable {
        if (span) {
          span->end();
        }
        if (ec) {
          return cb(ec, {});
        }
        self->collection_id_ = get_cid_res.collection_id;
        return self->create_streams(std::move(cb));
      });
  }

  void create_streams(scan_callback&& cb)
  {
    auto batch_time_limit =
      std::chrono::duration_cast<std::chrono::milliseconds>(0.9 * options_.timeout);
    const range_scan_continue_options continue_options{
      options_.batch_item_limit, options_.batch_byte_limit, options_.timeout,
      batch_time_limit,          options_.retry_strategy,
    };

    for (std::uint16_t vbucket = 0; vbucket < gsl::narrow_cast<std::uint16_t>(vbucket_map_.size());
         ++vbucket) {
      const range_scan_create_options create_options{
        scope_name_,       collection_name_,        scan_type_,
        options_.timeout,  collection_id_,          vbucket_to_snapshot_requirements_[vbucket],
        options_.ids_only, options_.retry_strategy,
      };

      // Get the active node for the vbucket (values in vbucket map are the active node id
      // followed by the ids of the replicas). scan() has already established that every row
      // names one.
      auto node_id = vbucket_map_[vbucket][0];

      auto stream = std::make_shared<range_scan_stream>(
        io_,
        agent_,
        vbucket,
        node_id,
        create_options,
        continue_options,
        std::static_pointer_cast<scan_stream_manager>(shared_from_this()),
        options_.tracer,
        options_.parent_span,
        options_.span_tracker);
      streams_[vbucket] = stream;
    }
    start_streams(concurrency_);
    // Transferring ownership of the range_scan_orchestrator impl to the scan_result
    return cb({}, scan_result(shared_from_this()));
  }

  void cancel() override
  {
    cancelled_ = true;
    for (const auto& [vbucket_id, stream] : streams_) {
      stream->should_cancel();
    }
  }

  auto is_cancelled() -> bool override
  {
    return cancelled_;
  }

  auto next() -> std::future<tl::expected<range_scan_item, std::error_code>> override
  {
    auto barrier = std::make_shared<std::promise<tl::expected<range_scan_item, std::error_code>>>();
    next([barrier](range_scan_item item, std::error_code ec) mutable {
      if (ec) {
        barrier->set_value(tl::unexpected{ ec });
      } else {
        barrier->set_value(std::move(item));
      }
    });
    return barrier->get_future();
  }

  void next(utils::movable_function<void(range_scan_item, std::error_code)> callback) override
  {
    if (item_limit_ == 0) {
      callback({}, errc::key_value::range_scan_completed);
      cancel();
      return;
    }
    --item_limit_;
    next_item(std::move(callback));
  }

  template<typename Handler>
  void next_item(Handler&& handler)
  {
    if (streams_.empty() || cancelled_) {
      items_.cancel();
      items_.close();
      return handler({}, errc::key_value::range_scan_completed);
    }
    items_.async_receive(
      [self = shared_from_this(), handler = std::forward<Handler>(handler)](
        std::error_code ec, std::variant<range_scan_item, scan_stream_end_signal> it) mutable {
        if (ec) {
          return handler({}, ec);
        }

        if (std::holds_alternative<range_scan_item>(it)) {
          handler(std::get<range_scan_item>(it), {});
        } else {
          auto signal = std::get<scan_stream_end_signal>(it);
          if (signal.error.has_value()) {
            // Fatal error
            handler({}, signal.error.value());
          } else {
            // Empty signal means that stream has completed
            {
              const std::scoped_lock<std::mutex> lock{ self->stream_map_mutex_ };
              self->streams_.erase(signal.vbucket_id);
            }
            return asio::post(asio::bind_executor(
              self->io_, [self, handler = std::forward<Handler>(handler)]() mutable {
                self->next_item(std::forward<Handler>(handler));
              }));
          }
        }
      });
  }

  void start_streams(std::uint16_t stream_count)
  {
    if (cancelled_) {
      CB_LOG_TRACE("scan has been cancelled, do not start another stream");
      return;
    }

    std::uint16_t counter{ 0 };
    while (counter < stream_count) {
      auto vbucket_id = load_balancer_.select_vbucket();
      if (!vbucket_id.has_value()) {
        CB_LOG_TRACE("no more scans, all vbuckets have been scanned");
        return;
      }

      auto v = vbucket_id.value();
      std::shared_ptr<range_scan_stream> stream{};
      {
        const std::scoped_lock<std::mutex> lock{ stream_map_mutex_ };
        stream = streams_.at(v);
      }
      CB_LOG_TRACE("scanning vbucket {} at node {}", vbucket_id.value(), stream->node_id());
      active_stream_count_++;
      counter++;
      asio::post(asio::bind_executor(io_, [stream]() mutable {
        stream->start();
      }));
    }
  }

  void stream_received_item(range_scan_item item) override
  {
    items_.async_send({}, std::move(item), [](std::error_code ec) {
      if (ec && ec != asio::experimental::error::channel_closed &&
          ec != asio::experimental::error::channel_cancelled) {
        CB_LOG_WARNING(
          "unexpected error while sending to scan item channel: {} ({})", ec.value(), ec.message());
      }
    });
  }

  void stream_failed(std::int16_t node_id,
                     std::uint16_t vbucket_id,
                     std::error_code ec,
                     bool fatal) override
  {
    if (!fatal) {
      return stream_completed(node_id, vbucket_id);
    }

    load_balancer_.notify_stream_ended(node_id);
    active_stream_count_--;
    items_.async_send({}, scan_stream_end_signal{ vbucket_id, ec }, [](std::error_code ec) {
      if (ec && ec != asio::experimental::error::channel_closed &&
          ec != asio::experimental::error::channel_cancelled) {
        CB_LOG_WARNING(
          "unexpected error while sending to scan item channel: {} ({})", ec.value(), ec.message());
      }
    });
    return cancel();
  }

  void stream_completed(std::int16_t node_id, std::uint16_t vbucket_id) override
  {
    load_balancer_.notify_stream_ended(node_id);
    active_stream_count_--;
    items_.async_send({}, scan_stream_end_signal{ vbucket_id }, [](std::error_code ec) {
      if (ec && ec != asio::experimental::error::channel_closed &&
          ec != asio::experimental::error::channel_cancelled) {
        CB_LOG_WARNING(
          "unexpected error while sending to scan item channel: {} ({})", ec.value(), ec.message());
      }
    });
    return start_streams(1);
  }

  void stream_start_failed_awaiting_retry(std::int16_t node_id, std::uint16_t vbucket_id) override
  {
    load_balancer_.notify_stream_ended(node_id);
    active_stream_count_--;

    load_balancer_.enqueue_vbucket(node_id, vbucket_id);
    if (active_stream_count_ == 0) {
      return start_streams(1);
    }
  }

private:
  asio::io_context& io_;
  agent agent_;
  topology::configuration::vbucket_map vbucket_map_;
  std::string scope_name_;
  std::string collection_name_;
  range_scan_load_balancer load_balancer_;
  asio::experimental::concurrent_channel<
    void(std::error_code, std::variant<range_scan_item, scan_stream_end_signal>)>
    items_;
  std::uint32_t collection_id_{ 0 };
  std::variant<std::monostate, range_scan, prefix_scan, sampling_scan> scan_type_;
  range_scan_orchestrator_options options_;
  std::map<std::size_t, std::optional<range_snapshot_requirements>>
    vbucket_to_snapshot_requirements_;
  std::map<std::uint16_t, std::shared_ptr<range_scan_stream>> streams_{};
  std::mutex stream_map_mutex_{};
  std::atomic_uint16_t active_stream_count_{ 0 };
  std::uint16_t concurrency_{ 1 };
  std::size_t item_limit_{ std::numeric_limits<std::size_t>::max() };
  std::atomic<bool> cancelled_{ false };
};

range_scan_orchestrator::range_scan_orchestrator(
  asio::io_context& io,
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
  auto barrier = std::make_shared<std::promise<tl::expected<scan_result, std::error_code>>>();
  auto f = barrier->get_future();
  scan([barrier](auto ec, auto res) mutable {
    if (ec) {
      return barrier->set_value(tl::unexpected(ec));
    }
    barrier->set_value(res);
  });
  return f.get();
}

void
range_scan_orchestrator::scan(couchbase::core::scan_callback&& cb)
{
  if (impl_) {
    return impl_->scan(std::move(cb));
  }
  cb(errc::common::request_canceled, {});
}
} // namespace couchbase::core
