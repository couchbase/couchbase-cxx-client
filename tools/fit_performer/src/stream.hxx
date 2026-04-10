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

#include "batcher.hxx"
#include "performer.grpc.pb.h"
#include "performer.pb.h"

#include <grpc++/grpc++.h>

#include <condition_variable>
#include <limits>
#include <list>
#include <mutex>
#include <optional>

namespace fit_cxx
{
using next_function = std::function<std::optional<protocol::run::Result>()>;
using cleanup_function = std::function<void(std::string)>;

class run_result_stream
{

public:
  run_result_stream(std::shared_ptr<fit_cxx::Batcher> batcher)
    : batcher_(std::move(batcher))
  {
  }
  std::future<void> begin(std::string id, next_function&& next_fn, cleanup_function&& cleanup)
  {
    return std::async(std::launch::async,
                      [this, next_fn = std::move(next_fn), cleanup = std::move(cleanup), id]() {
                        // send created first...
                        protocol::run::Result run_result;
                        run_result.mutable_stream()->mutable_created()->set_stream_id(id);
                        batcher_->send_result(run_result);
                        // take lock here.   Then, one could change the num_to_write_ only during
                        // the wait
                        std::unique_lock<std::mutex> lock(mut_);
                        while (true) {
                          cv_.wait(lock, [&]() {
                            return num_to_write_ > 0 || canceled_ || completed_;
                          });
                          if (canceled_) {
                            protocol::run::Result res;
                            res.mutable_stream()->mutable_cancelled()->set_stream_id(id);
                            batcher_->send_result(res);
                            break;
                          }
                          if (completed_) {
                            protocol::run::Result finished;
                            finished.mutable_stream()->mutable_complete()->set_stream_id(id);
                            batcher_->send_result(finished);
                            break;
                          }
                          if (num_to_write_ == 0) {
                            // should never be in a situation where num_to_write is 0, and canceled
                            // is false and we woke up, but just in case lets keep waiting...
                            continue;
                          }
                          while (num_to_write_ != 0) {
                            auto ret = next_fn();
                            num_to_write_--;
                            if (ret.has_value()) {
                              batcher_->send_result(ret.value());
                            } else {
                              // we ran out before we expected, or just reached the end of an
                              // automatic stream
                              completed_ = true;
                              break;
                            }
                          }
                        }
                        cleanup(id);
                      });
  }

  void set_num_to_write(size_t num)
  {
    std::unique_lock<std::mutex> lock(mut_);
    num_to_write_ = num;
    lock.unlock();
    cv_.notify_one();
  }

  void cancel()
  {
    std::unique_lock<std::mutex> lock(mut_);
    // ok, now set the canceled flag and wake up the writer(s)
    canceled_ = true;
    cv_.notify_all();
  }

private:
  std::shared_ptr<fit_cxx::Batcher> batcher_;
  std::mutex mut_;
  std::condition_variable cv_;
  size_t num_to_write_{ 0 };
  bool canceled_{ false };
  bool completed_{ false };
};

class run_result_streams
{
public:
  run_result_streams() = default;
  run_result_stream& find(const std::string& id)
  {
    std::lock_guard<std::mutex> lock(mut_);
    if (auto it = streams_.find(id); it != streams_.end()) {
      return it->second;
    }
    throw std::runtime_error("stream id not found");
  }

  run_result_stream& insert(std::string id,
                            std::shared_ptr<fit_cxx::Batcher> batcher,
                            bool is_automatic)
  {
    // insert one, and return a reference to it.
    std::lock_guard<std::mutex> lock(mut_);
    auto& retval = streams_.emplace(std::move(id), batcher).first->second;
    if (is_automatic) {
      retval.set_num_to_write(std::numeric_limits<std::size_t>::max());
    }
    spdlog::trace("inserted new stream {} ({} total streams)", id, streams_.size());
    return retval;
  }

  void remove(std::string id)
  {
    std::lock_guard<std::mutex> lock(mut_);
    if (auto it = streams_.find(id); it != streams_.end()) {
      streams_.erase(it);
      spdlog::trace("removed stream {} ({} total streams)", id, streams_.size());
    }
    spdlog::trace("couldn't find stream {} to remove! ({} total streams)", id, streams_.size());
  }

private:
  std::mutex mut_;
  std::map<std::string, run_result_stream> streams_;
};
} // namespace fit_cxx