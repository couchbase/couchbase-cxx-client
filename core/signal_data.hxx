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

#include "log_entry.hxx"
#include "metric_measurement.hxx"
#include "trace_span.hxx"

#include <optional>
#include <variant>

namespace couchbase::core
{
class signal_data
{
public:
  explicit signal_data(trace_span record);
  explicit signal_data(metric_measurement record);
  explicit signal_data(log_entry record);

  signal_data(const signal_data&) = default;
  signal_data(signal_data&& other) noexcept;
  auto operator=(const signal_data&) -> signal_data& = default;
  auto operator=(signal_data&& other) noexcept -> signal_data&;
  ~signal_data() = default;

  [[nodiscard]] auto is_null() const -> bool;
  [[nodiscard]] explicit operator bool() const
  {
    return !is_null();
  }

  [[nodiscard]] auto is_trace_span() const noexcept -> bool;
  [[nodiscard]] auto as_trace_span() const& -> const trace_span&;
  [[nodiscard]] auto as_trace_span() & -> trace_span;
  [[nodiscard]] auto as_trace_span() && -> trace_span;
  [[nodiscard]] auto try_as_trace_span() && -> std::optional<trace_span>;
  explicit operator trace_span() const&
  {
    return as_trace_span();
  }
  explicit operator trace_span() &&
  {
    return as_trace_span();
  }

  [[nodiscard]] auto is_metric_measurement() const noexcept -> bool;
  [[nodiscard]] auto as_metric_measurement() const& -> const metric_measurement&;
  [[nodiscard]] auto as_metric_measurement() & -> metric_measurement;
  [[nodiscard]] auto as_metric_measurement() && -> metric_measurement;
  [[nodiscard]] auto try_as_metric_measurement() && -> std::optional<metric_measurement>;
  explicit operator metric_measurement() const&
  {
    return as_metric_measurement();
  }
  explicit operator metric_measurement() &&
  {
    return as_metric_measurement();
  }

  [[nodiscard]] auto is_log_entry() const noexcept -> bool;
  [[nodiscard]] auto as_log_entry() const& -> const log_entry&;
  [[nodiscard]] auto as_log_entry() & -> log_entry;
  [[nodiscard]] auto as_log_entry() && -> log_entry;
  [[nodiscard]] auto try_as_log_entry() && -> std::optional<log_entry>;
  explicit operator log_entry() const&
  {
    return as_log_entry();
  }
  explicit operator log_entry() &&
  {
    return as_log_entry();
  }

  friend auto operator==(const signal_data& lhs, const signal_data& rhs) -> bool;
  friend auto to_string(const signal_data& data) -> std::string;

private:
  std::variant<std::monostate, trace_span, metric_measurement, log_entry> record_{};
};

auto
to_string(const signal_data& data) -> std::string;

inline auto
operator==(const signal_data& lhs, const signal_data& rhs) -> bool
{
  return lhs.record_ == rhs.record_;
}

inline auto
operator!=(const signal_data& lhs, const signal_data& rhs) -> bool
{
  return !(lhs == rhs);
}

} // namespace couchbase::core
