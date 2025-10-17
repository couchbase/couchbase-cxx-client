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

#include "metric_measurement.hxx"

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

namespace tao::json
{
template<>
struct traits<couchbase::core::signal_attribute> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::signal_attribute& attr)
  {
    v = {
      { "name", attr.name },
      { "value", attr.value },
    };
  }
};

template<>
struct traits<couchbase::core::metric_measurement> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::metric_measurement& measurement)
  {
    v = basic_value<Traits>::object({});
    v["name"] = measurement.name();

    if (measurement.is_int64()) {
      v["value"] = measurement.as_int64();
    } else if (measurement.is_double()) {
      v["value"] = measurement.as_double();
    }

    v["attributes"] = measurement.attributes();
  }
};
} // namespace tao::json

namespace couchbase::core
{
[[nodiscard]] auto
metric_measurement::name() const noexcept -> const std::string&
{
  return name_;
}

[[nodiscard]] auto
metric_measurement::attributes() const noexcept -> const std::vector<signal_attribute>&
{
  return attributes_;
}

[[nodiscard]] auto
metric_measurement::is_double() const noexcept -> bool
{
  return std::holds_alternative<double>(value_);
}

[[nodiscard]] auto
metric_measurement::as_double() const -> double
{
  return std::get<double>(value_);
}

[[nodiscard]] auto
metric_measurement::try_as_double() && -> std::optional<double>
{
  if (auto* ptr = std::get_if<double>(&value_)) {
    return *ptr;
  }
  return std::nullopt;
}

[[nodiscard]] auto
metric_measurement::is_int64() const noexcept -> bool
{
  return std::holds_alternative<std::int64_t>(value_);
}

[[nodiscard]] auto
metric_measurement::as_int64() const -> std::int64_t
{
  return std::get<std::int64_t>(value_);
}

[[nodiscard]] auto
metric_measurement::try_as_int64() && -> std::optional<std::int64_t>
{
  if (auto* ptr = std::get_if<std::int64_t>(&value_)) {
    return *ptr;
  }
  return std::nullopt;
}

auto
to_string(const metric_measurement& data) -> std::string
{
  tao::json::value json = data;
  return tao::json::to_string(json);
}
} // namespace couchbase::core
