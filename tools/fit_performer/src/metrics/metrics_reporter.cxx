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
#include "run.top_level.pb.h"

#include <google/protobuf/util/time_util.h>
#ifdef _WIN32
#undef GetCurrentTime
#endif
#include <spdlog/spdlog.h>
#include <tao/json.hpp>

#include <asio/post.hpp>
#include <asio/strand.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
#include <malloc.h>
#define FIT_PERFORMER_HAVE_MALLINFO2 1
#endif

#if defined(__linux__)
#include <sys/sysinfo.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#endif

namespace fit_cxx
{
namespace
{
/// mallinfo2() in-use bytes: uordblks (which includes chunks held in thread caches) plus hblkhd
/// (mmap-backed chunks). Absent without glibc 2.33.
auto
heap_used_bytes() -> std::optional<std::uint64_t>
{
#ifdef FIT_PERFORMER_HAVE_MALLINFO2
  const auto info = mallinfo2();
  return info.uordblks + info.hblkhd;
#else
  return std::nullopt;
#endif
}

/// Unused swap space of the whole machine, Linux and macOS only. Windows reports a commit limit
/// rather than swap.
auto
free_swap_bytes() -> std::optional<std::uint64_t>
{
#if defined(__linux__)
  struct sysinfo info{};
  if (sysinfo(&info) != 0) {
    return std::nullopt;
  }
  return static_cast<std::uint64_t>(info.freeswap) * info.mem_unit;
#elif defined(__APPLE__)
  xsw_usage usage{};
  auto size = sizeof(usage);
  if (sysctlbyname("vm.swapusage", &usage, &size, nullptr, 0) != 0) {
    return std::nullopt;
  }
  return usage.xsu_avail;
#else
  return std::nullopt;
#endif
}
} // namespace

metrics_reporter::metrics_reporter(asio::io_context& ctx,
                                   system_metrics::sm_pid_t pid,
                                   std::weak_ptr<fit_cxx::Batcher> batcher,
                                   const std::string& run_id)
  : metrics_timer_(asio::make_strand(ctx))
  , pid_(pid)
  , sm_reporter_(system_metrics::system_metrics_reporter::create())
  , batcher_(std::move(batcher))
  , run_id_(run_id)
{
}

void
metrics_reporter::rearm_reporter(std::error_code ec)
{
  if (ec == asio::error::operation_aborted || stopped_) {
    return;
  }

  if (metrics_timer_.expiry() > std::chrono::steady_clock::now()) {
    return;
  }

  metrics_timer_.expires_after(std::chrono::seconds(1));
  metrics_timer_.async_wait([self = shared_from_this()](std::error_code e) {
    if (e == asio::error::operation_aborted || self->stopped_) {
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
  if (stopped_.exchange(true)) {
    return;
  }
  spdlog::info("Stopping the metrics reporter (run: {}, pid: {}).", run_id_, pid_);
  // Wait for a sample in progress, so that nothing reaches the batcher after stop() returns, even
  // if the post below throws. post() never runs the handler inline, so holding the lock is safe.
  const std::scoped_lock lock(metrics_mutex_);
  // The timer runs on a strand, and the cancel is posted to it, so the cancel never runs alongside
  // the handler that re-arms the timer. A wait that has already completed when the cancel runs
  // still calls its handler, which returns on stopped_.
  asio::post(metrics_timer_.get_executor(), [self = shared_from_this()]() {
    self->metrics_timer_.cancel();
  });
}

void
metrics_reporter::log_metrics()
{
  if (stopped_) {
    return;
  }
  // An exception escaping this handler would leave the connection's io_context::run() and
  // terminate the performer, so a sample that cannot be read is skipped, with one warning per run.
  try {
    collect_and_send();
  } catch (const std::exception& e) {
    if (!warned_) {
      warned_ = true;
      spdlog::warn("Metrics (run: {}, pid: {}) not collected: {}", run_id_, pid_, e.what());
    }
  }
}

void
metrics_reporter::collect_and_send()
{
  const std::scoped_lock lock(metrics_mutex_);
  if (stopped_) {
    return;
  }

  const auto initiated = google::protobuf::util::TimeUtil::GetCurrentTime();
  metrics_sample_input sample{};
  sample.sampled_at = std::chrono::steady_clock::now();
  try {
    sample.logical_cpu_count = sm_reporter_->get_logical_cpu_count();
  } catch (const std::exception&) {
    sample.logical_cpu_count = 0;
  }
  sample.proc_cpu = sm_reporter_->get_proc_cpu(pid_);
  sample.system_cpu = sm_reporter_->get_system_cpu();
  sm_reporter_->iterate_proc_threads(
    pid_, [&threads = sample.thread_cpu_usec](auto tid, auto utime, auto stime) {
      threads[tid] = utime + stime;
    });
  sample.memory = sm_reporter_->get_proc_memory(pid_);
  sample.heap_used_bytes = heap_used_bytes();
  sample.free_swap_bytes = free_swap_bytes();

  const auto report = accumulator_.record(std::move(sample));
  if (!report.has_value()) {
    return;
  }

  auto metrics_report = tao::json::to_string(*report);
  spdlog::info("Metrics (run: {}, pid: {}): {}", run_id_, pid_, metrics_report);

  protocol::run::Result result;
  result.mutable_metrics()->set_metrics(std::move(metrics_report));
  *result.mutable_metrics()->mutable_initiated() = initiated;
  if (std::shared_ptr<fit_cxx::Batcher> sp_batcher = batcher_.lock()) {
    sp_batcher->add_metrics_result(result);
  }
}

} // namespace fit_cxx
