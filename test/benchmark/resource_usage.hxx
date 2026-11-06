/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include <cstdint>
#include <optional>

namespace couchbase::test::bench
{
// Process-wide counters: every thread of the process contributes, including the SDK's I/O threads,
// the runner's threads and an in-process fake server. A counter the platform does not provide is
// empty.
struct resource_usage {
  // User plus system CPU time. getrusage(RUSAGE_SELF) on POSIX, GetProcessTimes on Windows.
  std::optional<std::uint64_t> cpu_ns{};
  // Voluntary plus involuntary context switches. POSIX only.
  std::optional<std::uint64_t> context_switches{};
  // Peak resident set size since the process started, so it never decreases between snapshots.
  // ru_maxrss on POSIX, PeakWorkingSetSize on Windows.
  std::optional<std::uint64_t> max_rss_bytes{};
};

auto
current_resource_usage() -> resource_usage;
} // namespace couchbase::test::bench
