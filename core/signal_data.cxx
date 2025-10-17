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

#include "signal_data.hxx"

#include <array>
#include <chrono>

namespace couchbase::core
{
auto
to_iso8601_utc(std::time_t time_in_seconds, std::int64_t microseconds) -> std::string
{
  std::tm tm{};
#if defined(_MSC_VER)
  gmtime_s(&tm, &time_in_seconds);
#else
  gmtime_r(&time_in_seconds, &tm);
#endif

  std::array<char, 100> buffer{};
  std::snprintf(buffer.data(),
                buffer.size(),
                "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ",
                tm.tm_year + 1900,
                tm.tm_mon + 1,
                tm.tm_mday,
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec,
                static_cast<long long>(microseconds));
  return { buffer.data(), buffer.size() };
}

auto
to_iso8601_utc(const std::chrono::system_clock::time_point& time_point) -> std::string
{
  const auto duration{ time_point.time_since_epoch() };
  const auto seconds{ std::chrono::duration_cast<std::chrono::seconds>(duration) };
  const auto micros{ std::chrono::duration_cast<std::chrono::microseconds>(duration - seconds) };

  return to_iso8601_utc(std::chrono::system_clock::to_time_t(time_point), micros.count());
}

signal_data::signal_data(trace_record record)
  : record_{ std::move(record) }
{
}

signal_data::signal_data(metric_record record)
  : record_{ std::move(record) }
{
}

signal_data::signal_data(log_record record)
  : record_{ std::move(record) }
{
}
} // namespace couchbase::core
