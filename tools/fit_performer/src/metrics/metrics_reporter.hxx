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
#include <map>
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

struct thread_info {
  system_metrics::sm_tid_t tid;
  std::uint64_t utime;
  std::uint64_t stime;
};

class metrics_reporter : public std::enable_shared_from_this<metrics_reporter>
{
public:
  metrics_reporter(asio::io_context& ctx,
                   system_metrics::sm_pid_t pid,
                   std::weak_ptr<fit_cxx::Batcher> batcher,
                   const std::string& run_id);

  ~metrics_reporter();

  void start();
  void stop();

private:
  void rearm_reporter(std::error_code ec);
  void log_metrics();
  double get_proc_cpu_percent(system_metrics::proc_cpu_t t1,
                              system_metrics::proc_cpu_t t2,
                              std::chrono::milliseconds time_delta);
  std::map<std::string, double> get_proc_threads_cpu_percent(
    std::map<system_metrics::sm_pid_t, thread_info> t1,
    std::map<system_metrics::sm_pid_t, thread_info> t2,
    std::chrono::milliseconds time_delta);
  std::map<std::string, double> get_proc_memory();

  asio::steady_timer metrics_timer_;
  system_metrics::sm_pid_t pid_;
  std::unique_ptr<system_metrics::system_metrics> sm_reporter_;
  std::mutex metrics_mutex_{};
  bool initialized_{ false };
  bool stopped_{ false };
  std::chrono::time_point<std::chrono::steady_clock> last_report_{};
  system_metrics::proc_cpu_t last_proc_cpu_{};
  std::map<system_metrics::sm_pid_t, thread_info> last_threads_{};
  // FIT related 'things'
  std::weak_ptr<fit_cxx::Batcher> batcher_;
  std::string run_id_;

  // potentially useful in the future (can be used for calculating CPU utilization)
  // uint64_t boot_time_{};
  // uint32_t logical_cpu_count_{};
};

} // namespace fit_cxx