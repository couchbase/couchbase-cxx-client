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
SystemMetricsReporter::New(OperatingSystem os) -> std::unique_ptr<SystemMetrics>
{
  switch (os) {
    case OperatingSystem::Native:
#ifdef __APPLE__
      return std::make_unique<MacosSystemMetrics>();
#elif defined(__linux__)
      return std::make_unique<LinuxSystemMetrics>();
#elif defined(_WIN32)
      return std::make_unique<WindowsSystemMetrics>();
#else
      throw std::invalid_argument(
        "SystemMetricsReporter::New: unsupported native operating system.");
#endif
    case OperatingSystem::Apple:
      return std::make_unique<MacosSystemMetrics>();

    case OperatingSystem::Linux:
      return std::make_unique<LinuxSystemMetrics>();

    case OperatingSystem::Windows:
      return std::make_unique<WindowsSystemMetrics>();
  }
  throw std::invalid_argument("SystemMetricsReporter::New: unsupported operating system.");
}

} // namespace system_metrics
