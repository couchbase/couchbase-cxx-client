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

#include "run.top_level.pb.h"

#include <grpcpp/support/sync_stream.h>
#include <spdlog/spdlog.h>

#include <future>
#include <optional>
#include <queue>

namespace fit_cxx
{

template<typename T>
class concurrent_queue
{
public:
  using size_type = typename std::queue<T>::size_type;

  concurrent_queue() = default;
  ~concurrent_queue() = default;

  void pop()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    data_.pop();
  }

  auto get_if_not_empty() -> std::optional<T>
  {
    const std::unique_lock<std::mutex> lock(mutex_);
    if (data_.empty()) {
      return std::nullopt;
    }
    auto item = data_.front();
    data_.pop();
    return item;
  }

  auto empty() -> bool
  {
    const std::unique_lock<std::mutex> lock(mutex_);
    return data_.empty();
  }

  void push(const T&& item)
  {
    const std::unique_lock<std::mutex> lock(mutex_);
    data_.push(std::forward<const T>(item));
  }

  auto size() -> size_type
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return data_.size();
  }

private:
  std::mutex mutex_;
  std::queue<T> data_;
};

class Batcher : public std::enable_shared_from_this<Batcher>
{
public:
  Batcher(grpc::ServerWriter<protocol::run::Result>* writer,
          concurrent_queue<protocol::run::Result>* result_queue,
          int max_batch_size,
          bool metrics_enabled = false)
    : writer_(writer)
    , result_queue_(result_queue)
    , max_batch_size_(max_batch_size)
    , metrics_enabled_(metrics_enabled)
    , metrics_queue_(concurrent_queue<protocol::run::Result>())
  {
  }

  static std::shared_ptr<Batcher> build_batcher(
    grpc::ServerWriter<protocol::run::Result>* writer,
    concurrent_queue<protocol::run::Result>* result_queue,
    const protocol::run::Request* run_request,
    bool metrics_enabled = false)
  {
    int max_batch_size = 1;
    if (run_request->has_config()) {
      const auto& config = run_request->config();
      if (config.has_streaming_config()) {
        const auto& streaming_config = config.streaming_config();
        if (streaming_config.has_batch_size()) {
          max_batch_size = streaming_config.batch_size();
        }
      }
    }
    return std::make_shared<Batcher>(writer, result_queue, max_batch_size, metrics_enabled);
  }

  void start()
  {
    future_ = std::async(std::launch::async, [this] {
      spdlog::info("Starting batcher");
      this->run();
    });
  }

  void set_workloads_complete()
  {
    workloads_complete_ = true;
  }

  void wait()
  {
    future_.get();
    flush_metrics();
    spdlog::info("Exiting from batcher");
  }

  void send_result(protocol::run::Result& result)
  {
    result_queue_->push(std::move(result));
  }

  void add_metrics_result(protocol::run::Result& result)
  {
    if (metrics_enabled_) {
      metrics_queue_.push(std::move(result));
    }
  }

private:
  void run()
  {
    while (!all_results_sent_) {
      write_next_batch();
    }
    spdlog::info("Batcher has finished running");
  }

  void write_next_batch()
  {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    auto batched_result = protocol::run::BatchedResult::default_instance().New();
    while (true) {
      if ((batched_result->result_size() == max_batch_size_) ||
          (std::chrono::steady_clock::now() > deadline)) {
        break;
      }
      if (auto next_res = result_queue_->get_if_not_empty(); next_res.has_value()) {
        auto res = batched_result->add_result();
        *res = next_res.value();
      } else if (workloads_complete_) {
        break;
      }
    }

    if (batched_result->result_size() > 1) {
      protocol::run::Result result;
      result.set_allocated_batched(batched_result);
      writer_->Write(result);
    } else if (batched_result->result_size() > 0) {
      writer_->Write(batched_result->result(0));
    }

    if (metrics_enabled_) {
      if (auto next_res = metrics_queue_.get_if_not_empty(); next_res.has_value()) {
        writer_->Write(next_res.value());
      }
    }

    if (workloads_complete_ && result_queue_->empty()) {
      spdlog::info("All results have been sent");
      all_results_sent_ = true;
    }
  }

  void flush_metrics()
  {
    if (!metrics_enabled_) {
      return;
    }
    spdlog::info("Flushing remaining metrics (size: {})", metrics_queue_.size());
    while (true) {
      if (auto next_res = metrics_queue_.get_if_not_empty(); next_res.has_value()) {
        writer_->Write(next_res.value());
      } else {
        break;
      }
    }
    spdlog::info("Metrics flushed.");
  }

  grpc::ServerWriter<protocol::run::Result>* writer_;
  concurrent_queue<protocol::run::Result>* result_queue_;
  std::future<void> future_;
  int max_batch_size_;
  bool workloads_complete_ = false;
  bool all_results_sent_ = false;
  bool metrics_enabled_ = false;
  concurrent_queue<protocol::run::Result> metrics_queue_;
};

} // namespace fit_cxx
