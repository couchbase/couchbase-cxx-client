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

#include "file_signal_sink.hxx"

#include "core/metric_measurement.hxx"
#include "core/platform/uuid.h"
#include "signal_attribute.hxx"
#include "signal_bridge.hxx"
#include "trace_span.hxx"

#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_span.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

namespace couchbase::core
{
namespace
{
class file_tracer;

auto
generate_span_id() -> std::string
{
  thread_local std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<std::uint64_t> dis;

  std::uint64_t span_id = dis(gen);

  return fmt::format("{:016x}", span_id);
}

auto
generate_trace_id() -> std::string
{
  thread_local std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<std::uint64_t> dis;

  std::uint64_t high = dis(gen);
  std::uint64_t low = dis(gen);

  return fmt::format("{:016x}{:016x}", high, low);
}

class file_tracer_span
  : public couchbase::tracing::request_span
  , public std::enable_shared_from_this<file_tracer_span>
{
public:
  file_tracer_span(std::shared_ptr<file_signal_sink_impl> sink,
                   std::string name,
                   std::shared_ptr<couchbase::tracing::request_span> parent)
    : couchbase::tracing::request_span(name, std::move(parent))
    , span_{ std::move(name) }
    , sink_{ std::move(sink) }
  {
    span_.context.span_id = generate_span_id();
    if (parent != nullptr) {
      if (auto parent_span = std::dynamic_pointer_cast<file_tracer_span>(parent);
          parent_span != nullptr) {
        span_.parent_id = parent_span->span_.context.span_id;
        span_.context.trace_id = parent_span->span_.context.trace_id;
      }
    } else {
      span_.context.trace_id = generate_trace_id();
    }
    span_.start_time = std::chrono::system_clock::now();
  }

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    span_.attributes.emplace_back(signal_attribute{ name, fmt::format("{}", value) });
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    span_.attributes.emplace_back(signal_attribute{ name, value });
  }

  void end() override;

private:
  trace_span span_{};
  std::shared_ptr<file_signal_sink_impl> sink_{};

  friend class file_signal_sink_impl;
};

struct file_signal_sink_impl_options {
  std::chrono::milliseconds wait_interval{ std::chrono::milliseconds{ 100 } };
  signal_bridge_options bridge_options{};
};

class file_tracer
  : public couchbase::tracing::request_tracer
  , public std::enable_shared_from_this<file_tracer>
{
public:
  explicit file_tracer(std::shared_ptr<file_signal_sink_impl> sink)
    : sink_{ std::move(sink) }
  {
  }

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    return std::make_shared<file_tracer_span>(sink_, std::move(name), std::move(parent));
  }

  void start() override
  {
    // do nothing
  }

  void stop() override
  {
    // do nothing
  }

private:
  std::shared_ptr<file_signal_sink_impl> sink_;
};

class file_value_recorder : public couchbase::metrics::value_recorder
{
public:
  file_value_recorder(std::shared_ptr<file_signal_sink_impl> sink,
                      std::string name,
                      const std::map<std::string, std::string>& tags)
    : sink_{ std::move(sink) }
    , name_{ std::move(name) }
  {
    attributes_.reserve(tags.size());
    for (const auto& [name, value] : tags) {
      attributes_.emplace_back(signal_attribute{ name, value });
    }
  }

  file_value_recorder(const file_value_recorder& other) = delete;
  file_value_recorder(file_value_recorder&& other) = delete;
  auto operator=(const file_value_recorder& other) -> file_value_recorder& = default;
  auto operator=(file_value_recorder&& other) -> file_value_recorder& = default;
  ~file_value_recorder() override = default;

  void record_value(std::int64_t value) override;

private:
  std::shared_ptr<file_signal_sink_impl> sink_;
  std::string name_;
  std::vector<signal_attribute> attributes_;
};

class file_meter
  : public couchbase::metrics::meter
  , public std::enable_shared_from_this<file_meter>
{
public:
  explicit file_meter(std::shared_ptr<file_signal_sink_impl> sink)
    : sink_{ std::move(sink) }
  {
  }

  file_meter(const file_meter&) = delete;
  file_meter(file_meter&&) = delete;
  auto operator=(const file_meter&) -> file_meter& = delete;
  auto operator=(file_meter&&) -> file_meter& = delete;
  ~file_meter() override = default;

  void start() override
  {
    // do nothing
  }

  void stop() override
  {
    // do nothing
  }

  auto get_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags)
    -> std::shared_ptr<couchbase::metrics::value_recorder> override
  {
    return std::make_shared<file_value_recorder>(sink_, name, tags);
  }

private:
  std::shared_ptr<file_signal_sink_impl> sink_;
};

} // namespace

class file_signal_sink_impl : public std::enable_shared_from_this<file_signal_sink_impl>
{
public:
  explicit file_signal_sink_impl(FILE* output, file_signal_sink_impl_options options = {})
    : output_{ output }
    , wait_interval_{ options.wait_interval }
    , bridge_{ options.bridge_options }
  {
  }

  file_signal_sink_impl(const file_signal_sink_impl&) = delete;
  file_signal_sink_impl(file_signal_sink_impl&&) = delete;
  auto operator=(const file_signal_sink_impl&) -> file_signal_sink_impl& = delete;
  auto operator=(file_signal_sink_impl&&) -> file_signal_sink_impl& = delete;
  ~file_signal_sink_impl()
  {
    fflush(output_);
  }

  void commit(trace_span&& span)
  {
    bridge_.emplace(signal_data{ std::move(span) });
  }

  void commit(metric_measurement&& measurement)
  {
    bridge_.emplace(signal_data{ std::move(measurement) });
  }

  void start()
  {
    if (running_.load()) {
      return;
    }

    running_.store(true);

    worker_thread_ = std::thread(&file_signal_sink_impl::worker_loop, this);
  }

  void stop()
  {
    if (!running_.load()) {
      return;
    }

    running_.store(false);

    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
  }

  auto tracer() -> std::shared_ptr<couchbase::tracing::request_tracer>
  {
    std::call_once(tracer_initialized_, [self = shared_from_this()]() -> void {
      self->tracer_ = std::make_shared<file_tracer>(self);
    });
    return tracer_;
  }

  auto meter() -> std::shared_ptr<couchbase::metrics::meter>
  {
    std::call_once(meter_initialized_, [self = shared_from_this()]() -> void {
      self->meter_ = std::make_shared<file_meter>(self);
    });
    return meter_;
  }

private:
  void worker_loop()
  {
    while (running_.load()) {
      if (auto data = bridge_.wait_for_buffer_ready(wait_interval_); data) {
        while (!data->empty()) {
          fmt::println(output_, "{}", to_string(data->front()));
          data->pop();
        }
      }
    }

    auto data = bridge_.take_buffer();
    while (!data.empty()) {
      fmt::println(output_, "{}", to_string(data.front()));
      data.pop();
    }
  }

  FILE* output_;
  std::chrono::milliseconds wait_interval_;
  signal_bridge bridge_;
  std::thread worker_thread_;
  std::atomic<bool> running_{ false };

  std::once_flag tracer_initialized_{};
  std::shared_ptr<file_tracer> tracer_{ nullptr };

  std::once_flag meter_initialized_{};
  std::shared_ptr<file_meter> meter_{ nullptr };
};

namespace
{
void
file_tracer_span::end()
{
  if (auto sink = std::move(sink_); sink) {
    span_.end_time = std::chrono::system_clock::now();
    sink->commit(std::move(span_));
  }
}

void
file_value_recorder::record_value(std::int64_t value)
{
  sink_->commit(metric_measurement{ name_, value, attributes_ });
}
} // namespace

file_signal_sink::file_signal_sink(FILE* output)
  : impl_{ std::make_shared<file_signal_sink_impl>(output) }
{
}

void
file_signal_sink::start()
{
  return impl_->start();
}

void
file_signal_sink::stop()
{
  return impl_->stop();
}

[[nodiscard]] auto
file_signal_sink::tracer() -> std::shared_ptr<couchbase::tracing::request_tracer>
{
  return impl_->tracer();
}

[[nodiscard]] auto
file_signal_sink::meter() -> std::shared_ptr<couchbase::metrics::meter>
{
  return impl_->meter();
}
} // namespace couchbase::core
