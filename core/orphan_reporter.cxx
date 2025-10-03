/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025 Couchbase, Inc.
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

#include "orphan_reporter.hxx"

#include <couchbase/build_info.hxx>

#include "logger/logger.hxx"
#include "utils/concurrent_fixed_priority_queue.hxx"
#include "utils/json.hxx"

#include <asio/steady_timer.hpp>
#include <tao/json/value.hpp>

namespace couchbase::core
{
auto
orphan_attributes::operator<(const orphan_attributes& other) const -> bool
{
  return total_duration < other.total_duration;
}

auto
orphan_attributes::operator>(const orphan_attributes& other) const -> bool
{
  return total_duration > other.total_duration;
}

auto
orphan_attributes::to_json() const -> tao::json::value
{
  return tao::json::value{
    { "total_duration_us", total_duration.count() },
    { "last_server_duration_us", last_server_duration.count() },
    { "total_server_duration_us", total_server_duration.count() },
    { "operation_name", operation_name },
    { "last_local_id", connection_id },
    { "operation_id", operation_id },
    { "last_local_socket", last_local_socket },
    { "last_remote_socket", last_remote_socket },
  };
}

class orphan_reporter_impl : public std::enable_shared_from_this<orphan_reporter_impl>
{
public:
  orphan_reporter_impl(asio::io_context& ctx, const orphan_reporter_options& options)
    : options_{ options }
    , orphan_queue_{ options.sample_size }
    , emit_timer_{ ctx }
  {
  }

  void add_orphan(orphan_attributes&& orphan)
  {
    orphan_queue_.emplace(std::move(orphan));
  }

  void start()
  {
    rearm();
  }

  void stop()
  {
    emit_timer_.cancel();
  }

  auto flush_and_create_output() -> std::optional<std::string>
  {
    if (orphan_queue_.empty()) {
      return std::nullopt;
    }

    auto [queue, dropped_count] = orphan_queue_.steal_data();

    auto total_count = queue.size() + dropped_count;

    // We only do orphan reporting for KV at the moment. If we extend this to HTTP services, we must
    // update this to handle other types of services as well.
    tao::json::value report{
#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
      { "emit_interval_ms", options_.emit_interval.count() },
      { "sample_size", options_.sample_size },
#endif
      { "kv", tao::json::value{ { "total_count", total_count } } },
    };

    tao::json::value entries = tao::json::empty_array;
    while (!queue.empty()) {
      entries.emplace_back(queue.top().to_json());
      queue.pop();
    }
    report["kv"]["top_requests"] = entries;

    return utils::json::generate(report);
  }

private:
  void rearm()
  {
    emit_timer_.expires_after(options_.emit_interval);
    emit_timer_.async_wait([self = shared_from_this()](std::error_code ec) -> void {
      if (ec == asio::error::operation_aborted) {
        return;
      }

      if (auto report = self->flush_and_create_output(); report.has_value()) {
        CB_LOG_WARNING("Orphan responses observed: {}", report.value());
      }

      self->rearm();
    });
  }

  orphan_reporter_options options_;
  utils::concurrent_fixed_priority_queue<orphan_attributes> orphan_queue_;
  asio::steady_timer emit_timer_;
};

orphan_reporter::orphan_reporter(asio::io_context& ctx, const orphan_reporter_options& options)
  : impl_{ std::make_shared<orphan_reporter_impl>(ctx, options) }
{
}

void
orphan_reporter::add_orphan(orphan_attributes&& orphan)
{
  impl_->add_orphan(std::move(orphan));
}

void
orphan_reporter::start()
{
  impl_->start();
}

void
orphan_reporter::stop()
{
  impl_->stop();
}

auto
orphan_reporter::flush_and_create_output() -> std::optional<std::string>
{
  return impl_->flush_and_create_output();
}

} // namespace couchbase::core
