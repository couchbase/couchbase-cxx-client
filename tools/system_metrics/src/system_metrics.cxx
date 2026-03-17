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

#include "system_metrics_reporter.hxx"

#include "os/linux_system_metrics.hxx"
#include "os/macos_system_metrics.hxx"
#include "os/windows_system_metrics.hxx"

#include <memory>
#include <stdexcept>

namespace system_metrics
{

auto
system_metrics_reporter::create(OperatingSystem os) -> std::unique_ptr<system_metrics>
{
  switch (os) {
    case OperatingSystem::Native:
#ifdef __APPLE__
      return std::make_unique<macos_system_metrics>();
#elif defined(__linux__)
      return std::make_unique<linux_system_metrics>();
#elif defined(_WIN32)
      return std::make_unique<windows_system_metrics>();
#else
      throw std::invalid_argument(
        "system_metrics_reporter::create: unsupported native operating system.");
#endif
    case OperatingSystem::Apple:
      return std::make_unique<macos_system_metrics>();

    case OperatingSystem::Linux:
      return std::make_unique<linux_system_metrics>();

    case OperatingSystem::Windows:
      return std::make_unique<windows_system_metrics>();
  }
  throw std::invalid_argument("system_metrics_reporter::create: unsupported operating system.");
}

} // namespace system_metrics
