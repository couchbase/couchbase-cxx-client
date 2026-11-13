/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include "threshold_logging_tracer.hxx"

#include "couchbase/build_info.hxx"

#include "constants.hxx"
#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/meta/version.hxx"
#include "core/platform/uuid.h"
#include "core/service_type_fmt.hxx"
#include "core/utils/concurrent_fixed_priority_queue.hxx"
#include "core/utils/json.hxx"

#include <asio/steady_timer.hpp>
#include <memory>
#include <tao/json/value.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <utility>

namespace couchbase::core::tracing
{
namespace
{
enum class encoded_service : std::uint8_t {
  unset = 0,
  key_value = 1,
  query = 2,
  view = 3,
  search = 4,
  analytics = 5,
  management = 6,
};

[[nodiscard]] auto
encode_service(const std::string& service_name) -> encoded_service
{
  if (service_name == tracing::service::key_value) {
    return encoded_service::key_value;
  }
  if (service_name == tracing::service::query) {
    return encoded_service::query;
  }
  if (service_name == tracing::service::view) {
    return encoded_service::view;
  }
  if (service_name == tracing::service::search) {
    return encoded_service::search;
  }
  if (service_name == tracing::service::analytics) {
    return encoded_service::analytics;
  }
  if (service_name == tracing::service::management) {
    return encoded_service::management;
  }
  return encoded_service::unset;
}

[[nodiscard]] auto
decode_service(encoded_service value) -> std::optional<service_type>
{
  switch (value) {
    case encoded_service::key_value:
      return service_type::key_value;
    case encoded_service::query:
      return service_type::query;
    case encoded_service::view:
      return service_type::view;
    case encoded_service::search:
      return service_type::search;
    case encoded_service::analytics:
      return service_type::analytics;
    case encoded_service::management:
      return service_type::management;
    case encoded_service::unset:
      break;
  }
  return {};
}
} // namespace
auto
format_dispatch_operation_id(std::uint32_t opaque) -> std::string
{
  // A uint32 in "0x<hex>" form is at most "0x" plus 8 hex digits, so a 12-byte buffer never
  // overflows and the string is built without a heap allocation until the final copy out.
  std::array<char, 12> buffer{};
  const auto result = fmt::format_to_n(buffer.data(), buffer.size(), "0x{:x}", opaque);
  return { buffer.data(), result.out };
}

struct reported_span {
  std::chrono::microseconds duration;
  tao::json::value payload;

  auto operator<(const reported_span& other) const -> bool
  {
    return duration < other.duration;
  }

  auto operator>(const reported_span& other) const -> bool
  {
    return duration > other.duration;
  }
};

class threshold_logging_span
  : public couchbase::tracing::request_span
  , public std::enable_shared_from_this<threshold_logging_span>
{
private:
  static constexpr std::uint64_t rollup_sealed = 1ULL << 63U;
  static constexpr std::uint64_t rollup_report_pending = 1ULL << 62U;
  static constexpr std::uint64_t rollup_folding = 1ULL << 61U;
  static constexpr std::uint64_t rollup_active_mask = rollup_folding - 1ULL;

  struct rollup_node {
    rollup_node* next{ nullptr };
    threshold_logging_span* span{ nullptr };
  };

  static void release_rollup_list(rollup_node* head) noexcept
  {
    while (head != nullptr) {
      auto* const next = head->next;
      head->next = nullptr;
      head->span->self_rollup_.reset();
      head = next;
    }
  }

  void fold_batch(rollup_node* batch) noexcept
  {
    rollup_node* ordered{ nullptr };
    while (batch != nullptr) {
      auto* const next = batch->next;
      batch->next = ordered;
      ordered = batch;
      batch = next;
    }
    while (ordered != nullptr) {
      auto* const next = ordered->next;
      ordered->next = nullptr;
      auto* const child = ordered->span;
      if (child->last_local_id_.has_value()) {
        folded_local_id_ = std::move(child->last_local_id_);
      }
      if (child->operation_id_.has_value()) {
        folded_operation_id_opaque_.reset();
        folded_operation_id_ = std::move(child->operation_id_);
      } else if (child->operation_id_opaque_.has_value()) {
        folded_operation_id_.reset();
        folded_operation_id_opaque_ = child->operation_id_opaque_;
      }
      if (child->peer_hostname_.has_value()) {
        folded_peer_hostname_ = std::move(child->peer_hostname_);
      }
      if (child->peer_port_.has_value()) {
        folded_peer_port_ = child->peer_port_;
      }
      if (child->last_server_duration_us_ > 0) {
        folded_last_server_duration_us_ = child->last_server_duration_us_;
        folded_total_server_duration_us_ += child->last_server_duration_us_;
      }
      child->self_rollup_.reset();
      ordered = next;
    }
  }

  void try_fold_rollup() noexcept
  {
    if ((rollup_state_.fetch_or(rollup_folding, std::memory_order_acq_rel) & rollup_folding) != 0) {
      return;
    }
    for (;;) {
      if (auto* const batch = rollup_head_.exchange(nullptr, std::memory_order_acq_rel);
          batch != nullptr) {
        fold_batch(batch);
      }
      rollup_state_.fetch_and(~rollup_folding, std::memory_order_acq_rel);
      if (rollup_head_.load(std::memory_order_acquire) == nullptr) {
        return;
      }
      if ((rollup_state_.fetch_or(rollup_folding, std::memory_order_acq_rel) & rollup_folding) !=
          0) {
        return;
      }
    }
  }

  void finish_rollup_participant()
  {
    auto cur = rollup_state_.load(std::memory_order_acquire);
    for (;;) {
      const auto remaining_active = (cur & rollup_active_mask) - 1ULL;
      const bool fire_report = remaining_active == 0 && (cur & rollup_report_pending) != 0;
      auto next = (cur & ~rollup_active_mask) | remaining_active;
      if (fire_report) {
        next &= ~rollup_report_pending;
      }
      if (rollup_state_.compare_exchange_weak(
            cur, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (fire_report) {
          try_fold_rollup();
          tracer_->report(shared_from_this());
        }
        return;
      }
    }
  }

  void rollup_child(threshold_logging_span& child)
  {
    if ((rollup_state_.load(std::memory_order_relaxed) & rollup_sealed) != 0) {
      return;
    }
    const auto prev = rollup_state_.fetch_add(1ULL, std::memory_order_acquire);
    if ((prev & rollup_sealed) != 0) {
      finish_rollup_participant();
      return;
    }

    child.self_rollup_ = child.shared_from_this();
    auto* head = rollup_head_.load(std::memory_order_relaxed);
    child.rollup_node_.next = head;
    while (!rollup_head_.compare_exchange_weak(
      head, &child.rollup_node_, std::memory_order_acq_rel, std::memory_order_relaxed)) {
      child.rollup_node_.next = head;
    }

    try_fold_rollup();
    finish_rollup_participant();
  }

  std::chrono::system_clock::time_point start_{ std::chrono::system_clock::now() };
  std::atomic<std::int64_t> total_duration_us_{ 0 };
  std::uint64_t total_server_duration_us_{ 0 };

  std::uint64_t last_server_duration_us_{ 0 };
  std::optional<std::string> operation_id_{};
  std::optional<std::uint32_t> operation_id_opaque_{};
  std::optional<std::string> last_local_id_{};
  std::optional<std::string> peer_hostname_{};
  std::optional<std::uint16_t> peer_port_{};

  std::uint64_t folded_total_server_duration_us_{ 0 };
  std::uint64_t folded_last_server_duration_us_{ 0 };
  std::optional<std::string> folded_operation_id_{};
  std::optional<std::uint32_t> folded_operation_id_opaque_{};
  std::optional<std::string> folded_local_id_{};
  std::optional<std::string> folded_peer_hostname_{};
  std::optional<std::uint16_t> folded_peer_port_{};

  std::shared_ptr<threshold_logging_tracer> tracer_{};
  // Weak reference avoids a parent -> self_rollup_ -> parent cycle while a child is being folded.
  std::weak_ptr<couchbase::tracing::request_span> parent_{};
  const bool is_dispatch_{ false };
  std::atomic<encoded_service> service_{ encoded_service::unset };
  std::atomic_bool ended_{ false };
  rollup_node rollup_node_{ nullptr, this };
  std::shared_ptr<threshold_logging_span> self_rollup_{};
  // Gates child rollup against parent end() and serialises folding into folded_* without locks.
  std::atomic<std::uint64_t> rollup_state_{ 0 };
  std::atomic<rollup_node*> rollup_head_{ nullptr };

public:
  struct rollup_summary {
    std::uint64_t last_server_duration_us{ 0 };
    std::uint64_t total_server_duration_us{ 0 };
    std::optional<std::string> last_operation_id{};
    std::optional<std::string> last_local_id{};
    std::optional<std::string> last_remote_socket{};
  };

  threshold_logging_span(std::string span_name,
                         std::shared_ptr<threshold_logging_tracer> tracer,
                         const std::shared_ptr<request_span>& parent = nullptr)
    : request_span(std::move(span_name))
    , tracer_{ std::move(tracer) }
    , parent_{ parent }
    , is_dispatch_{ name() == tracing::operation::step_dispatch }
  {
  }

  ~threshold_logging_span() override
  {
    release_rollup_list(rollup_head_.exchange(nullptr, std::memory_order_acquire));
  }

  void add_tag(const std::string& tag_name, std::uint64_t value) override
  {
    if (ended_.load(std::memory_order_relaxed)) {
      return;
    }
    if (tag_name == tracing::attributes::dispatch::server_duration) {
      last_server_duration_us_ = value;
      if (!is_dispatch_) {
        total_server_duration_us_ += value;
      }
      return;
    }
    if (tag_name == tracing::attributes::dispatch::peer_port) {
      peer_port_ = static_cast<std::uint16_t>(value);
    }
  }

  void add_tag(const std::string& tag_name, const std::string& value) override
  {
    if (tag_name == tracing::attributes::op::service) {
      service_.store(encode_service(value), std::memory_order_relaxed);
      return;
    }
    if (ended_.load(std::memory_order_relaxed)) {
      return;
    }
    if (tag_name == tracing::attributes::dispatch::local_id) {
      last_local_id_ = value;
      return;
    }
    if (tag_name == tracing::attributes::dispatch::operation_id) {
      operation_id_opaque_.reset();
      operation_id_ = value;
      return;
    }
    if (tag_name == tracing::attributes::dispatch::peer_address) {
      peer_hostname_ = value;
    }
  }

  [[nodiscard]] auto try_set_dispatch_operation_id(std::uint32_t opaque) -> bool override
  {
    // Keep the raw opaque on the hot path; operation_id() formats it only when reported.
    if (!ended_.load(std::memory_order_relaxed)) {
      operation_id_.reset();
      operation_id_opaque_ = opaque;
    }
    return true;
  }

  [[nodiscard]] auto try_set_dispatch_local_id(const std::string& local_id) -> bool override
  {
    if (!ended_.load(std::memory_order_relaxed)) {
      last_local_id_ = local_id;
    }
    return true;
  }

  [[nodiscard]] auto try_set_dispatch_result(std::uint64_t server_duration_us,
                                             const std::string& peer_address,
                                             std::uint16_t peer_port) -> bool override
  {
    if (!ended_.load(std::memory_order_relaxed)) {
      last_server_duration_us_ = server_duration_us;
      peer_hostname_ = peer_address;
      peer_port_ = peer_port;
      if (!is_dispatch_) {
        total_server_duration_us_ += server_duration_us;
      }
    }
    return true;
  }

  void end() override;

  [[nodiscard]] auto parent() const -> std::shared_ptr<request_span> override
  {
    return parent_.lock();
  }

  [[nodiscard]] auto total_duration() const -> std::chrono::microseconds
  {
    return std::chrono::microseconds{ total_duration_us_.load(std::memory_order_relaxed) };
  }

  [[nodiscard]] auto operation_id() const -> std::optional<std::string>
  {
    if (operation_id_.has_value()) {
      return operation_id_;
    }
    if (operation_id_opaque_.has_value()) {
      // Build the string only here, on the reporting path, never on the hot path.
      return format_dispatch_operation_id(operation_id_opaque_.value());
    }
    return {};
  }

  [[nodiscard]] auto summarize_rollup() const -> rollup_summary
  {
    rollup_summary summary{};
    summary.total_server_duration_us = total_server_duration_us_ + folded_total_server_duration_us_;
    summary.last_server_duration_us = folded_last_server_duration_us_ > 0
                                        ? folded_last_server_duration_us_
                                        : last_server_duration_us_;

    if (folded_operation_id_.has_value()) {
      summary.last_operation_id = folded_operation_id_;
    } else if (folded_operation_id_opaque_.has_value()) {
      summary.last_operation_id = format_dispatch_operation_id(folded_operation_id_opaque_.value());
    } else {
      summary.last_operation_id = operation_id();
    }

    summary.last_local_id = folded_local_id_.has_value() ? folded_local_id_ : last_local_id_;

    const auto& peer_hostname =
      folded_peer_hostname_.has_value() ? folded_peer_hostname_ : peer_hostname_;
    const auto peer_port = folded_peer_port_.has_value() ? folded_peer_port_ : peer_port_;
    if (peer_hostname.has_value() && peer_port.has_value()) {
      summary.last_remote_socket = fmt::format("{}:{}", peer_hostname.value(), peer_port.value());
    }
    return summary;
  }

  [[nodiscard]] auto is_key_value() const -> bool
  {
    return service_.load(std::memory_order_relaxed) == encoded_service::key_value;
  }

  [[nodiscard]] auto service() const -> std::optional<service_type>
  {
    return decode_service(service_.load(std::memory_order_relaxed));
  }
};

using fixed_span_queue = utils::concurrent_fixed_priority_queue<reported_span>;

auto
convert(const std::shared_ptr<threshold_logging_span>& span) -> reported_span
{
  const auto rollup = span->summarize_rollup();
  tao::json::value entry{
    { "operation_name", span->name() },
    { "total_duration_us",
      std::chrono::duration_cast<std::chrono::microseconds>(span->total_duration()).count() }
  };
  if (span->is_key_value()) {
    entry["last_server_duration_us"] = rollup.last_server_duration_us;
    entry["total_server_duration_us"] = rollup.total_server_duration_us;
  }

  if (rollup.last_operation_id.has_value()) {
    entry["last_operation_id"] = rollup.last_operation_id.value();
  }

  if (rollup.last_local_id.has_value()) {
    entry["last_local_id"] = rollup.last_local_id.value();
  }

  if (rollup.last_remote_socket.has_value()) {
    entry["last_remote_socket"] = rollup.last_remote_socket.value();
  }

  return { span->total_duration(), std::move(entry) };
}

class threshold_logging_tracer_impl
  : public std::enable_shared_from_this<threshold_logging_tracer_impl>
{
public:
  threshold_logging_tracer_impl(const threshold_logging_options& options, asio::io_context& ctx)
    : options_(options)
    , emit_threshold_report_(ctx)
  {
    threshold_queues_.try_emplace(service_type::key_value, options.threshold_sample_size);
    threshold_queues_.try_emplace(service_type::query, options.threshold_sample_size);
    threshold_queues_.try_emplace(service_type::view, options.threshold_sample_size);
    threshold_queues_.try_emplace(service_type::search, options.threshold_sample_size);
    threshold_queues_.try_emplace(service_type::analytics, options.threshold_sample_size);
    threshold_queues_.try_emplace(service_type::management, options.threshold_sample_size);
  }

  threshold_logging_tracer_impl(const threshold_logging_tracer_impl&) = delete;
  threshold_logging_tracer_impl(threshold_logging_tracer_impl&&) = delete;
  auto operator=(const threshold_logging_tracer_impl&) -> threshold_logging_tracer_impl& = delete;
  auto operator=(threshold_logging_tracer_impl&&) -> threshold_logging_tracer_impl& = delete;

  ~threshold_logging_tracer_impl()
  {
    emit_threshold_report_.cancel();

    log_threshold_report();
  }

  void start()
  {
    rearm_threshold_reporter();
  }

  void stop()
  {
    emit_threshold_report_.cancel();
  }

  void check_threshold(const std::shared_ptr<threshold_logging_span>& span)
  {
    const auto service = span->service();
    if (!service.has_value()) {
      return;
    }
    if (span->total_duration() > options_.threshold_for_service(service.value())) {
      if (const auto queue = threshold_queues_.find(service.value());
          queue != threshold_queues_.end()) {
        queue->second.emplace(convert(span));
      }
    }
  }

  [[nodiscard]] auto flush_and_create_output() -> std::optional<std::string>
  {
    tao::json::value reports = tao::json::empty_object;
    for (auto& [service, threshold_queue] : threshold_queues_) {
      if (threshold_queue.empty()) {
        continue;
      }
      auto [queue, dropped_count] = threshold_queue.steal_data();
      const auto total_count = queue.size() + dropped_count;
      tao::json::value entries = tao::json::empty_array;
      while (!queue.empty()) {
        entries.emplace_back(queue.top().payload);
        queue.pop();
      }
      reports[fmt::format("{}", service)] = tao::json::value{
        { "count", total_count },
        { "top", std::move(entries) },
      };
    }
    if (reports.get_object().empty()) {
      return std::nullopt;
    }
    return utils::json::generate(reports);
  }

private:
  void rearm_threshold_reporter()
  {
    emit_threshold_report_.expires_after(options_.threshold_emit_interval);
    emit_threshold_report_.async_wait([self = shared_from_this()](std::error_code ec) -> void {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->log_threshold_report();
      self->rearm_threshold_reporter();
    });
  }

  void log_threshold_report()
  {
    for (auto& [service, threshold_queue] : threshold_queues_) {
      if (threshold_queue.empty()) {
        continue;
      }
      auto [queue, _] = threshold_queue.steal_data();
      tao::json::value report{
        { "count", queue.size() },
        { "service", fmt::format("{}", service) },
#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
        { "emit_interval_ms", options_.threshold_emit_interval.count() },
        { "sample_size", options_.threshold_sample_size },
        { "threshold_ms",
          std::chrono::duration_cast<std::chrono::microseconds>(
            options_.threshold_for_service(service))
            .count() },
#endif
      };
      tao::json::value entries = tao::json::empty_array;
      while (!queue.empty()) {
        entries.emplace_back(queue.top().payload);
        queue.pop();
      }
      report["top"] = entries;
      CB_LOG_WARNING("Operations over threshold: {}",
                     logger::system_data(utils::json::generate(report)));
    }
  }

  threshold_logging_options options_;

  asio::steady_timer emit_threshold_report_;
  std::map<service_type, fixed_span_queue> threshold_queues_{};
};

auto
threshold_logging_tracer::start_span(std::string name,
                                     std::shared_ptr<couchbase::tracing::request_span> parent)
  -> std::shared_ptr<couchbase::tracing::request_span>
{
  return std::make_shared<threshold_logging_span>(std::move(name), shared_from_this(), parent);
}

void
threshold_logging_tracer::report(const std::shared_ptr<threshold_logging_span>& span)
{
  impl_->check_threshold(span);
}

auto
threshold_logging_tracer::flush_and_create_output() -> std::optional<std::string>
{
  return impl_->flush_and_create_output();
}

threshold_logging_tracer::threshold_logging_tracer(asio::io_context& ctx,
                                                   threshold_logging_options options)
  : options_{ options }
  , impl_(std::make_shared<threshold_logging_tracer_impl>(options_, ctx))
{
}

void
threshold_logging_tracer::start()
{
  impl_->start();
}

void
threshold_logging_tracer::stop()
{
  impl_->stop();
}

void
threshold_logging_span::end()
{
  if (ended_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::system_clock::now() - start_);
  total_duration_us_.store(duration.count(), std::memory_order_relaxed);

  const auto svc = service();
  const bool should_report =
    svc.has_value() && duration > tracer_->threshold_for_service(svc.value());

  auto cur = rollup_state_.load(std::memory_order_acquire);
  for (;;) {
    const bool defer_report = should_report && (cur & rollup_active_mask) != 0;
    const auto next = cur | rollup_sealed | (defer_report ? rollup_report_pending : 0ULL);
    if (rollup_state_.compare_exchange_weak(
          cur, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
      if (should_report && !defer_report) {
        try_fold_rollup();
        tracer_->report(shared_from_this());
      }
      break;
    }
  }

  if (is_dispatch_) {
    if (const auto p = std::dynamic_pointer_cast<threshold_logging_span>(parent()); p != nullptr) {
      p->rollup_child(*this);
    }
  }
}
} // namespace couchbase::core::tracing
