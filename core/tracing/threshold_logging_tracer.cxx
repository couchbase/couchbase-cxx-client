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
#include "core/meta/version.hxx"
#include "core/platform/uuid.h"
#include "core/service_type_fmt.hxx"
#include "core/utils/concurrent_fixed_priority_queue.hxx"
#include "core/utils/json.hxx"

#include <asio/steady_timer.hpp>
#include <memory>
#include <tao/json/value.hpp>

#include <chrono>
#include <utility>

namespace couchbase::core::tracing
{
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
  std::chrono::system_clock::time_point start_{ std::chrono::system_clock::now() };
  std::chrono::microseconds total_duration_{ 0 };

  std::uint64_t last_server_duration_us_{ 0 };
  std::uint64_t total_server_duration_us_{ 0 };
  std::optional<std::string> operation_id_{};
  std::optional<std::string> last_local_id_{};
  std::optional<std::string> service_{};
  std::optional<std::string> peer_hostname_{};
  std::optional<std::uint16_t> peer_port_{};

  // Used by legacy top-level spans. TODO(CXXCBC-738): Remove once HTTP dispatch spans are added.
  std::optional<std::string> last_remote_socket_{};

  std::shared_ptr<threshold_logging_tracer> tracer_{};

public:
  threshold_logging_span(std::string name,
                         std::shared_ptr<threshold_logging_tracer> tracer,
                         std::shared_ptr<request_span> parent = nullptr)
    : request_span(std::move(name), std::move(parent))
    , tracer_{ std::move(tracer) }
  {
  }

  void add_tag(const std::string& tag_name, std::uint64_t value) override
  {
    if (tag_name == tracing::attributes::dispatch::server_duration) {
      last_server_duration_us_ = value;
      if (name() != tracing::operation::step_dispatch) {
        total_server_duration_us_ += value;
      }
    }
    if (tag_name == tracing::attributes::dispatch::peer_port) {
      peer_port_ = static_cast<std::uint16_t>(value);
    }
  }

  void add_tag(const std::string& tag_name, const std::string& value) override
  {
    if (tag_name == tracing::attributes::service) {
      service_ = value;
    }
    if (tag_name == tracing::attributes::remote_socket) {
      last_remote_socket_ = value;
    }
    if (tag_name == tracing::attributes::dispatch::local_id) {
      last_local_id_ = value;
    }
    if (tag_name == tracing::attributes::dispatch::operation_id) {
      operation_id_ = value;
    }
    if (tag_name == tracing::attributes::dispatch::peer_address) {
      peer_hostname_ = value;
    }
  }

  void end() override;

  void set_last_remote_socket(const std::string& socket)
  {
    last_remote_socket_ = socket;
  }

  void set_last_local_id(const std::string& id)
  {
    last_local_id_ = id;
  }

  void set_operation_id(const std::string& id)
  {
    operation_id_ = id;
  }

  void add_server_duration(const std::uint64_t duration_us)
  {
    last_server_duration_us_ = duration_us;
    total_server_duration_us_ += duration_us;
  }

  [[nodiscard]] auto total_duration() const -> std::chrono::microseconds
  {
    return total_duration_;
  }

  [[nodiscard]] auto last_server_duration_us() const -> std::uint64_t
  {
    return last_server_duration_us_;
  }

  [[nodiscard]] auto total_server_duration_us() const -> std::uint64_t
  {
    return total_server_duration_us_;
  }

  [[nodiscard]] auto operation_id() const -> std::optional<std::string>
  {
    return operation_id_;
  }

  [[nodiscard]] auto last_remote_socket() const -> std::optional<std::string>
  {
    return last_remote_socket_;
  }

  [[nodiscard]] auto last_local_id() const -> std::optional<std::string>
  {
    return last_local_id_;
  }

  [[nodiscard]] auto is_key_value() const -> bool
  {
    return service_.has_value() && service_.value() == tracing::service::key_value;
  }

  [[nodiscard]] auto service() const -> std::optional<service_type>
  {
    if (!service_.has_value()) {
      return {};
    }
    const auto& service_name = service_.value();
    if (service_name == tracing::service::key_value) {
      return service_type::key_value;
    }
    if (service_name == tracing::service::query) {
      return service_type::query;
    }
    if (service_name == tracing::service::view) {
      return service_type::view;
    }
    if (service_name == tracing::service::search) {
      return service_type::search;
    }
    if (service_name == tracing::service::analytics) {
      return service_type::analytics;
    }
    if (service_name == tracing::service::management) {
      return service_type::management;
    }
    return {};
  }
};

using fixed_span_queue = utils::concurrent_fixed_priority_queue<reported_span>;

auto
convert(const std::shared_ptr<threshold_logging_span>& span) -> reported_span
{
  tao::json::value entry{
    { "operation_name", span->name() },
    { "total_duration_us",
      std::chrono::duration_cast<std::chrono::microseconds>(span->total_duration()).count() }
  };
  if (span->is_key_value()) {
    entry["last_server_duration_us"] = span->last_server_duration_us();
    entry["total_server_duration_us"] = span->total_server_duration_us();
  }

  if (span->operation_id().has_value()) {
    entry["last_operation_id"] = span->operation_id().value();
  }

  if (span->last_local_id().has_value()) {
    entry["last_local_id"] = span->last_local_id().value();
  }

  if (span->last_remote_socket().has_value()) {
    entry["last_remote_socket"] = span->last_remote_socket().value();
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
      CB_LOG_WARNING("Operations over threshold: {}", utils::json::generate(report));
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
  total_duration_ = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::system_clock::now() - start_);
  if (service_.has_value()) {
    tracer_->report(shared_from_this());
  }
  if (name() == tracing::operation::step_dispatch) {
    // Transfer the relevant attributes to the operation-level span
    if (const auto p = std::dynamic_pointer_cast<threshold_logging_span>(parent()); p) {
      if (last_local_id_.has_value()) {
        p->set_last_local_id(last_local_id_.value());
      }
      if (operation_id_.has_value()) {
        p->set_operation_id(operation_id_.value());
      }
      if (peer_hostname_.has_value() && peer_port_.has_value()) {
        p->set_last_remote_socket(fmt::format("{}:{}", peer_hostname_.value(), peer_port_.value()));
      }
      if (last_server_duration_us_ > 0) {
        p->add_server_duration(last_server_duration_us_);
      }
    }
  }
}
} // namespace couchbase::core::tracing
