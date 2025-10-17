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

#pragma once

#include <chrono>
#include <string>
#include <variant>
#include <vector>

namespace couchbase::core
{
auto
to_iso8601_utc(std::time_t time_in_seconds, std::int64_t microseconds = 0) -> std::string;

auto
to_iso8601_utc(const std::chrono::system_clock::time_point& time_point) -> std::string;

struct signal_attribute {
  std::string name;
  std::string value;
};

struct trace_event {
  std::string name;
  std::string timestamp{};

  std::vector<signal_attribute> attributes{};
};

struct trace_record {
  std::string name;

  struct {
    std::string trace_id{};
    std::string span_id{};
  } context{};

  std::string parent_id{};

  std::string start_time{};
  std::string end_time{};

  std::vector<signal_attribute> attributes{};
  std::vector<trace_event> events{};
};

struct metric_record {
  std::string name;
  std::variant<double, std::int64_t> value;
};

struct log_record {
  std::string timestamp;
  std::string severity;
  std::string message;

  struct {
    std::string trace_id{};
    std::string span_id{};
  } context{};

  std::vector<signal_attribute> attributes{};
};

class signal_data
{
public:
  explicit signal_data(trace_record record);
  explicit signal_data(metric_record record);
  explicit signal_data(log_record record);

  signal_data(const signal_data&) = default;
  signal_data(signal_data&&) = default;
  auto operator=(const signal_data&) -> signal_data& = default;
  auto operator=(signal_data&&) -> signal_data& = default;
  ~signal_data() = default;

private:
  std::variant<trace_record, metric_record, log_record> record_;
};
} // namespace couchbase::core
