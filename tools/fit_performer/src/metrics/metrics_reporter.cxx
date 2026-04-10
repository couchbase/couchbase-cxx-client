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

#include "metrics_reporter.hxx"

#include <tao/json.hpp>

namespace fit_cxx
{

metrics_reporter::metrics_reporter(asio::io_context& ctx,
                                   system_metrics::sm_pid_t pid,
                                   std::weak_ptr<fit_cxx::Batcher> batcher,
                                   const std::string& run_id)
  : metrics_timer_(ctx)
  , pid_(pid)
  , sm_reporter_(system_metrics::system_metrics_reporter::create())
  , batcher_(std::move(batcher))
  , run_id_(run_id)
{
  // TODO: Possible future use
  // logical_cpu_count_ = sm_reporter_->get_logical_cpu_count();
  // spdlog::info("logical_cpu_count: {}", logical_cpu_count_);
  // auto boot_time = sm_reporter_->get_system_boot_time();
  // spdlog::info("boot_time: {}", boot_time);
}

metrics_reporter::~metrics_reporter()
{
  stop();
}

void
metrics_reporter::rearm_reporter(std::error_code ec)
{
  if (ec == asio::error::operation_aborted) {
    return;
  }

  if (metrics_timer_.expiry() > std::chrono::steady_clock::now()) {
    return;
  }

  metrics_timer_.expires_after(std::chrono::seconds(1));
  metrics_timer_.async_wait([self = shared_from_this()](std::error_code e) {
    if (e == asio::error::operation_aborted) {
      return;
    }
    self->log_metrics();
    self->rearm_reporter(e);
  });
}

void
metrics_reporter::start()
{
  spdlog::info("Starting the metrics reporter (run: {}, pid: {}).", run_id_, pid_);
  rearm_reporter({});
}

void
metrics_reporter::stop()
{
  if (stopped_) {
    return;
  }
  spdlog::info("Stopping the metrics reporter (run: {}, pid: {}).", run_id_, pid_);
  if (metrics_timer_.cancel() > 0) {
    stopped_ = true;
  };
}

double
metrics_reporter::get_proc_cpu_percent(system_metrics::proc_cpu_t t1,
                                       system_metrics::proc_cpu_t t2,
                                       std::chrono::milliseconds time_delta)
{
  auto delta_cpu = (t2.user - t1.user) + (t2.sys - t1.sys);

  if (time_delta == std::chrono::steady_clock::duration::zero()) {
    return 0;
  }

  auto cpu_percent = static_cast<double>(delta_cpu) / static_cast<double>(time_delta.count());
  cpu_percent *= 100;
  return cpu_percent;
}

std::map<std::string, double>
metrics_reporter::get_proc_threads_cpu_percent(std::map<system_metrics::sm_pid_t, thread_info> t1,
                                               std::map<system_metrics::sm_pid_t, thread_info> t2,
                                               std::chrono::milliseconds time_delta)
{
  std::map<system_metrics::sm_pid_t, std::chrono::milliseconds> delta_threads;
  for (const auto& t : t2) {
    auto it = t1.find(t.first);
    if (it != t1.end()) {
      auto delta = (t.second.utime - it->second.utime) + (t.second.stime - it->second.stime);
      delta_threads[t.first] =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::microseconds(delta));
    }
  }

  std::map<std::string, double> threads_cpu_percent;
  double total_thread_percent = 0;
  for (const auto& t : delta_threads) {
    if (time_delta == std::chrono::steady_clock::duration::zero()) {
      threads_cpu_percent[std::to_string(t.first)] = 0;
      continue;
    }
    auto thread_percent =
      static_cast<double>(t.second.count()) / static_cast<double>(time_delta.count());
    thread_percent *= 100;
    threads_cpu_percent[std::to_string(t.first)] = thread_percent;
    total_thread_percent += thread_percent;
  }

  threads_cpu_percent["total_thread_cpu"] = total_thread_percent;
  return threads_cpu_percent;
}

std::map<std::string, double>
metrics_reporter::get_proc_memory()
{
  std::map<std::string, double> memory;
  auto mem = sm_reporter_->get_proc_memory(pid_);
  // convert to MB
  memory["rss"] = std::round((static_cast<double>(mem.resident) / (1024 * 1024)) * 100) / 100;
  memory["vms"] = std::round((static_cast<double>(mem.size) / (1024 * 1024)) * 100) / 100;

  return memory;
}

void
metrics_reporter::log_metrics()
{
  if (stopped_) {
    return;
  }
  const std::scoped_lock lock(metrics_mutex_);
  auto now = std::chrono::steady_clock::now();
  auto proc_cpu = sm_reporter_->get_proc_cpu(pid_);
  std::map<system_metrics::sm_pid_t, thread_info> threads;
  sm_reporter_->iterate_proc_threads(pid_, [&threads](auto tid, auto utime, auto stime) {
    threads[tid] = thread_info{ tid, utime, stime };
  });

  if (!initialized_) {
    last_report_ = now;
    last_proc_cpu_ = proc_cpu;
    last_threads_ = threads;
    initialized_ = true;
    return;
  }

  auto delta_time = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_report_);
  [[maybe_unused]] auto cpu_proc_percent =
    get_proc_cpu_percent(last_proc_cpu_, proc_cpu, delta_time);
  auto threads_cpu_percent = get_proc_threads_cpu_percent(last_threads_, threads, delta_time);
  auto memory = get_proc_memory();

  last_report_ = now;
  last_proc_cpu_ = proc_cpu;
  last_threads_ = threads;

  tao::json::value json_report = {
    { "processCpu", std::round(threads_cpu_percent["total_thread_cpu"] * 100) / 100 },
    { "memRssUsedMB", memory["rss"] },
    { "memVmsMB", memory["vms"] },
    { "threadCount", threads_cpu_percent.size() - 1 } // subtract 1 for total_thread_cpu
  };

  // Useful for debugging
  // spdlog::info("proc CPU: {:.2f}%", cpu_proc_percent);
  // for (const auto& pair : threads_cpu_percent) {
  //   spdlog::info("{}: {:.2f}%", pair.first, pair.second);
  // }

  auto metrics_report = tao::json::to_string(json_report);
  spdlog::info("Metrics (run: {}, pid: {}): {}", run_id_, pid_, metrics_report);

  protocol::run::Result result;
  result.mutable_metrics()->set_metrics(metrics_report);
  if (std::shared_ptr<fit_cxx::Batcher> sp_batcher = batcher_.lock()) {
    sp_batcher->add_metrics_result(result);
  }
}

} // namespace fit_cxx