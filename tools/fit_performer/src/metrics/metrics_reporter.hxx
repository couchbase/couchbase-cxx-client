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

#include <atomic>
#include <chrono>
#include <mutex>

#include <asio/steady_timer.hpp>
#include <spdlog/spdlog.h>

#include "../batcher.hxx"
#include "metrics.top_level.pb.h"
#include "run.top_level.pb.h"

#include <grpcpp/support/sync_stream.h>

#include <system_metrics_reporter.hxx>

namespace fit_cxx
{

/// Sends a protocol::metrics::Result about once a second while a run is in progress, stamped with
/// the time it was sampled. Field names follow metrics.top_level.proto where it standardises one.
///
///   processCpu     CPU time of this process over the interval, as a percentage of the CPU time
///                  of the whole machine over the same interval: 0 to 100.
///   systemCpu      non-idle CPU time of the whole machine over the interval, 0 to 100 percent.
///   memHeapUsedMB  heap_used_bytes() in metrics_reporter.cxx; absent without glibc 2.33.
///   memRssUsedMB   resident set size.
///   memVmsMB       virtual memory size.
///   threadCount    threads of this process.
///   freeSwapSizeMB unused swap space of the whole machine; Linux and macOS only.
///
/// The CPU fields are deltas, so the first tick sends nothing and records their baseline. A sample
/// in which the machine's CPU time did not advance leaves them out. Megabytes are 2^20 bytes.
class metrics_reporter : public std::enable_shared_from_this<metrics_reporter>
{
public:
  metrics_reporter(asio::io_context& ctx,
                   system_metrics::sm_pid_t pid,
                   std::weak_ptr<fit_cxx::Batcher> batcher,
                   const std::string& run_id);

  void start();
  void stop();

private:
  void rearm_reporter(std::error_code ec);
  void log_metrics();
  void collect_and_send();

  asio::steady_timer metrics_timer_;
  system_metrics::sm_pid_t pid_;
  std::unique_ptr<system_metrics::system_metrics> sm_reporter_;
  std::mutex metrics_mutex_{};
  bool initialized_{ false };
  std::atomic<bool> stopped_{ false };
  bool warned_{ false };
  system_metrics::proc_cpu_t last_proc_cpu_{};
  system_metrics::system_cpu_t last_system_cpu_{};
  // FIT related 'things'
  std::weak_ptr<fit_cxx::Batcher> batcher_;
  std::string run_id_;
};

} // namespace fit_cxx