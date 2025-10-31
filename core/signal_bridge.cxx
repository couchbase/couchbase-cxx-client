/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Current Couchbase, Inc.
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

#include "signal_bridge.hxx"

#include <condition_variable>
#include <mutex>

namespace couchbase::core
{
class signal_bridge_impl
{
public:
  signal_bridge_impl(std::size_t buffer_limit, double notification_threshold)
    : buffer_limit_{ buffer_limit }
    , notification_threshold_{ notification_threshold }
  {
  }

  signal_bridge_impl(const signal_bridge_impl&) = delete;
  signal_bridge_impl(signal_bridge_impl&&) = delete;
  auto operator=(const signal_bridge_impl&) -> signal_bridge_impl& = delete;
  auto operator=(signal_bridge_impl&&) -> signal_bridge_impl& = delete;
  ~signal_bridge_impl() = default;

  [[nodiscard]] auto wait_for_buffer_ready(std::chrono::milliseconds interval)
    -> std::optional<std::queue<signal_data>>
  {
    std::unique_lock lock(mutex_);
    auto status = buffer_ready_.wait_for(lock, interval);

    if (status == std::cv_status::timeout) {
      // Return empty on timeout to enable implicit batching,
      // even if the buffer contains data below the threshold
      return {};
    }

    // The writer notified us because the threshold has been met
    return std::move(buffer_);
  }

  [[nodiscard]] auto take_buffer() -> std::queue<signal_data>
  {
    std::unique_lock lock(mutex_);
    return std::move(buffer_);
  }

  void emplace(signal_data&& data)
  {
    std::lock_guard lock(mutex_);

    if (buffer_.size() < buffer_limit_) {
      buffer_.emplace(std::move(data));
    }

    if (buffer_.size() >=
        static_cast<std::size_t>(static_cast<double>(buffer_limit_) * notification_threshold_)) {
      buffer_ready_.notify_all();
    }
  }

private:
  std::size_t buffer_limit_;
  double notification_threshold_;
  std::queue<signal_data> buffer_{};
  std::mutex mutex_{};
  std::condition_variable buffer_ready_{};
};

signal_bridge_options::signal_bridge_options(std::size_t buffer_limit,
                                             double notification_threshold)
  : buffer_limit_{ buffer_limit }
  , notification_threshold_{ notification_threshold }
{
}

auto
signal_bridge_options::buffer_limit(std::size_t buffer_limit) -> signal_bridge_options&
{
  buffer_limit_ = buffer_limit;
  return *this;
}

auto
signal_bridge_options::notification_threshold(double notification_threshold)
  -> signal_bridge_options&
{
  notification_threshold_ = notification_threshold;
  return *this;
}

[[nodiscard]] auto
signal_bridge_options::buffer_limit() const -> std::size_t
{
  return buffer_limit_;
}

[[nodiscard]] auto
signal_bridge_options::notification_threshold() const -> double
{
  return notification_threshold_;
}

signal_bridge::~signal_bridge() = default;

signal_bridge::signal_bridge(const signal_bridge_options& options)
  : impl_{ std::make_unique<signal_bridge_impl>(options.buffer_limit(),
                                                options.notification_threshold()) }
{
}

void
signal_bridge::emplace(signal_data&& data)
{
  impl_->emplace(std::move(data));
}

[[nodiscard]] auto
signal_bridge::wait_for_buffer_ready(std::chrono::milliseconds interval)
  -> std::optional<std::queue<signal_data>>
{
  return impl_->wait_for_buffer_ready(interval);
}

[[nodiscard]] auto
signal_bridge::take_buffer() -> std::queue<signal_data>
{
  return impl_->take_buffer();
}
} // namespace couchbase::core
