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
#include <memory>
#include <mutex>
#include <string>

#include <asio/steady_timer.hpp>

#include "../batcher.hxx"
#include "metrics_accumulator.hxx"

#include <system_metrics.hxx>
#include <system_metrics_reporter.hxx>

namespace fit_cxx
{

/// Sends a `protocol::metrics::Result` once a second while a run is active.
/// The first tick records baseline counters and emits nothing. Field definitions live on
/// `metrics_accumulator` (`metrics.top_level.proto`).
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
  std::atomic<bool> stopped_{ false };
  bool warned_{ false };
  metrics_accumulator accumulator_{};
  std::weak_ptr<fit_cxx::Batcher> batcher_;
  std::string run_id_;
};

} // namespace fit_cxx
