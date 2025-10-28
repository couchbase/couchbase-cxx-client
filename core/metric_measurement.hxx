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

#include "signal_attribute.hxx"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace couchbase::core
{
class metric_measurement
{
public:
  template<typename Float, std::enable_if_t<std::is_floating_point_v<Float>, int> = 0>
  metric_measurement(std::string name, Float value, std::vector<signal_attribute> attributes = {})
    : name_{ std::move(name) }
    , value_{ value }
    , attributes_{ std::move(attributes) }
  {
  }

  template<
    typename Integer,
    std::enable_if_t<std::is_integral_v<Integer> && std::is_signed_v<Integer> &&
                       !std::is_same_v<Integer, bool> && sizeof(Integer) <= sizeof(std::int64_t),
                     int> = 0>
  metric_measurement(std::string name, Integer value, std::vector<signal_attribute> attributes = {})
    : name_{ std::move(name) }
    , value_{ value }
    , attributes_{ std::move(attributes) }
  {
  }

  metric_measurement(const metric_measurement&) = default;
  metric_measurement(metric_measurement&&) = default;
  ~metric_measurement() = default;
  auto operator=(const metric_measurement&) -> metric_measurement& = default;
  auto operator=(metric_measurement&&) -> metric_measurement& = default;

  [[nodiscard]] auto name() const noexcept -> const std::string&;
  [[nodiscard]] auto attributes() const noexcept -> const std::vector<signal_attribute>&;

  [[nodiscard]] auto is_double() const noexcept -> bool;
  [[nodiscard]] auto as_double() const -> double;
  [[nodiscard]] auto try_as_double() && -> std::optional<double>;
  explicit operator double() const
  {
    return as_double();
  }

  [[nodiscard]] auto is_int64() const noexcept -> bool;
  [[nodiscard]] auto as_int64() const -> std::int64_t;
  [[nodiscard]] auto try_as_int64() && -> std::optional<std::int64_t>;
  explicit operator std::int64_t() const
  {
    return as_int64();
  }

  friend auto operator==(const metric_measurement& lhs, const metric_measurement& rhs) -> bool;

private:
  std::string name_;
  std::variant<double, std::int64_t> value_;
  std::vector<signal_attribute> attributes_;
};

auto
to_string(const metric_measurement& data) -> std::string;

inline auto
operator==(const metric_measurement& lhs, const metric_measurement& rhs) -> bool
{
  return lhs.name_ == rhs.name_ &&   //
         lhs.value_ == rhs.value_ && //
         lhs.attributes_ == rhs.attributes_;
}

inline auto
operator!=(const metric_measurement& lhs, const metric_measurement& rhs) -> bool
{
  return !(lhs == rhs);
}
} // namespace couchbase::core
