/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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
#include <cstddef>
#include <string>

namespace couchbase
{
class application_telemetry_options
{
public:
  static constexpr std::chrono::milliseconds default_ping_interval{ std::chrono::seconds{ 30 } };
  static constexpr std::chrono::milliseconds default_ping_deadline{ std::chrono::seconds{ 2 } };

  auto enable(bool enable) -> application_telemetry_options&
  {
    enabled_ = enable;
    return *this;
  }

  auto ping_interval(std::chrono::milliseconds interval) -> application_telemetry_options&
  {
    ping_interval_ = interval;
    return *this;
  }

  auto override_endpoint(std::string endpoint) -> application_telemetry_options&
  {
    endpoint_ = std::move(endpoint);
    return *this;
  }

  struct built {
    bool enabled;
    std::chrono::milliseconds ping_interval;
    std::chrono::milliseconds ping_deadline;
    std::string endpoint;
  };

  [[nodiscard]] auto build() const -> built
  {
    return {
      enabled_,
      ping_interval_,
      ping_deadline_,
      endpoint_,
    };
  }

private:
  bool enabled_{ true };
  std::chrono::milliseconds ping_interval_{ default_ping_interval };
  std::chrono::milliseconds ping_deadline_{ default_ping_deadline };
  std::string endpoint_{};
};
} // namespace couchbase
