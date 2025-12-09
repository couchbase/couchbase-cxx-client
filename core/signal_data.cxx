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

namespace couchbase::core
{
signal_data::signal_data(trace_span record)
  : record_{ std::move(record) }
{
}

signal_data::signal_data(metric_measurement record)
  : record_{ std::move(record) }
{
}

signal_data::signal_data(log_entry record)
  : record_{ std::move(record) }
{
}

signal_data::signal_data(signal_data&& other) noexcept
  : record_(std::exchange(other.record_, std::monostate{}))
{
}

auto
signal_data::operator=(signal_data&& other) noexcept -> signal_data&
{
  if (this != &other) {
    record_ = std::exchange(other.record_, std::monostate{});
  }
  return *this;
}

[[nodiscard]] auto
signal_data::is_null() const -> bool
{
  return std::holds_alternative<std::monostate>(record_);
}

[[nodiscard]] auto
signal_data::is_trace_span() const noexcept -> bool
{
  return std::holds_alternative<trace_span>(record_);
}

[[nodiscard]] auto
signal_data::as_trace_span() const& -> const trace_span&
{
  return std::get<trace_span>(record_);
}

[[nodiscard]] auto
signal_data::as_trace_span() & -> trace_span
{
  return std::get<trace_span>(record_);
}

[[nodiscard]] auto
signal_data::as_trace_span() && -> trace_span
{
  return std::get<trace_span>(std::exchange(record_, std::monostate{}));
}

[[nodiscard]] auto
signal_data::try_as_trace_span() && -> std::optional<trace_span>
{
  if (auto* ptr = std::get_if<trace_span>(&record_)) {
    record_ = std::monostate{};
    return { std::move(*ptr) };
  }
  return std::nullopt;
}

[[nodiscard]] auto
signal_data::is_metric_measurement() const noexcept -> bool
{
  return std::holds_alternative<metric_measurement>(record_);
}

[[nodiscard]] auto
signal_data::as_metric_measurement() const& -> const metric_measurement&
{
  return std::get<metric_measurement>(record_);
}

[[nodiscard]] auto
signal_data::as_metric_measurement() & -> metric_measurement
{
  return std::get<metric_measurement>(record_);
}

[[nodiscard]] auto
signal_data::as_metric_measurement() && -> metric_measurement
{
  return std::get<metric_measurement>(std::exchange(record_, std::monostate{}));
}

[[nodiscard]] auto
signal_data::try_as_metric_measurement() && -> std::optional<metric_measurement>
{
  if (auto* ptr = std::get_if<metric_measurement>(&record_)) {
    record_ = std::monostate{};
    return { std::move(*ptr) };
  }
  return std::nullopt;
}

[[nodiscard]] auto
signal_data::is_log_entry() const noexcept -> bool
{
  return std::holds_alternative<log_entry>(record_);
}

[[nodiscard]] auto
signal_data::as_log_entry() const& -> const log_entry&
{
  return std::get<log_entry>(record_);
}

[[nodiscard]] auto
signal_data::as_log_entry() & -> log_entry
{
  return std::get<log_entry>(record_);
}

[[nodiscard]] auto
signal_data::as_log_entry() && -> log_entry
{
  return std::get<log_entry>(std::exchange(record_, std::monostate{}));
}

[[nodiscard]] auto
signal_data::try_as_log_entry() && -> std::optional<log_entry>
{
  if (is_log_entry()) {
    return { std::move(*this).as_log_entry() };
  }
  return std::nullopt;
}

auto
to_string(const signal_data& data) -> std::string
{
  return std::visit(
    [](const auto& value) -> std::string {
      if constexpr (std::is_same_v<std::decay_t<decltype(value)>, std::monostate>) {
        return "{}";
      } else {
        return to_string(value);
      }
    },
    data.record_);
};
} // namespace couchbase::core
