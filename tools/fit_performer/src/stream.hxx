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
#include <spdlog/spdlog.h>

#include <condition_variable>
#include <functional>
#include <future>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace fit_cxx
{
using next_function = std::function<std::optional<protocol::run::Result>()>;
using cleanup_function = std::function<void(std::string)>;

// RAII helper that invokes the cleanup callback when it goes out of scope, so a stream is always
// unregistered even if the writer task exits early via an exception thrown by a producer or the
// batcher. Without this, a throwing producer would leave the stream registered and leak it.
class cleanup_on_exit
{
public:
  cleanup_on_exit(const cleanup_function& cleanup, std::string id)
    : cleanup_(cleanup)
    , id_(std::move(id))
  {
  }
  cleanup_on_exit(const cleanup_on_exit&) = delete;
  cleanup_on_exit& operator=(const cleanup_on_exit&) = delete;
  cleanup_on_exit(cleanup_on_exit&&) = delete;
  cleanup_on_exit& operator=(cleanup_on_exit&&) = delete;
  ~cleanup_on_exit()
  {
    cleanup_(id_);
  }

private:
  const cleanup_function& cleanup_;
  std::string id_;
};

class run_result_stream : public std::enable_shared_from_this<run_result_stream>
{

public:
  explicit run_result_stream(std::shared_ptr<fit_cxx::Batcher> batcher)
    : batcher_(std::move(batcher))
  {
  }
  std::future<void> begin(std::string id, next_function&& next_fn, cleanup_function&& cleanup)
  {
    // Capture a shared_ptr to self so the stream cannot be destroyed (e.g. by cleanup() ->
    // run_result_streams::remove() dropping the last reference) while this writer task is still
    // running and dereferencing `this`.
    return std::async(std::launch::async,
                      [this,
                       self = shared_from_this(),
                       next_fn = std::move(next_fn),
                       cleanup = std::move(cleanup),
                       id]() {
                        // Unregister the stream on any exit path (normal completion, cancellation,
                        // or an exception from next_fn()/the batcher), so it never leaks in the
                        // registry.
                        const cleanup_on_exit guard{ cleanup, id };
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
                            // Honour a cancellation that arrived while we were producing results.
                            // This matters most for automatic streams, where num_to_write_ is
                            // SIZE_MAX and the loop would otherwise run until the producer is
                            // exhausted. The outer loop emits the cancelled message.
                            if (canceled_) {
                              break;
                            }
                            // Reserve this write by decrementing under the lock before releasing
                            // it. Decrementing after the unlock would race a concurrent
                            // set_num_to_write() and could underflow num_to_write_ (a size_t).
                            num_to_write_--;
                            // Release the lock while running the (potentially slow) producer and
                            // delivering the result, so set_num_to_write()/cancel() are not blocked
                            // waiting on mut_.
                            lock.unlock();
                            auto ret = next_fn();
                            if (ret.has_value()) {
                              batcher_->send_result(ret.value());
                            }
                            lock.lock();
                            if (!ret.has_value()) {
                              // we ran out before we expected, or just reached the end of an
                              // automatic stream
                              completed_ = true;
                              break;
                            }
                          }
                        }
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

  // Returns a shared_ptr (not a reference) so the caller keeps the stream alive even if another
  // thread erases it from the map via remove() concurrently. Returning a bare reference here was a
  // use-after-free: the map mutex is released before the caller dereferences the result.
  std::shared_ptr<run_result_stream> find(const std::string& id)
  {
    std::scoped_lock<std::mutex> lock(mut_);
    if (auto it = streams_.find(id); it != streams_.end()) {
      return it->second;
    }
    throw std::runtime_error("stream id not found: " + id);
  }

  std::shared_ptr<run_result_stream> insert(const std::string& id,
                                            std::shared_ptr<fit_cxx::Batcher> batcher,
                                            bool is_automatic)
  {
    std::scoped_lock<std::mutex> lock(mut_);
    auto stream = std::make_shared<run_result_stream>(std::move(batcher));
    // emplace() does not overwrite an existing entry, so always return the stream that is actually
    // stored in the map (it->second). Otherwise a duplicate id would hand back a stream that
    // find()/cancel()/remove() can never see, causing inconsistent state and leaks.
    auto [it, inserted] = streams_.emplace(id, stream);
    if (!inserted) {
      spdlog::warn("stream {} already registered; reusing the existing stream", id);
    }
    if (is_automatic) {
      it->second->set_num_to_write(std::numeric_limits<std::size_t>::max());
    }
    spdlog::trace("inserted new stream {} ({} total streams)", id, streams_.size());
    return it->second;
  }

  void remove(const std::string& id)
  {
    std::scoped_lock<std::mutex> lock(mut_);
    if (auto it = streams_.find(id); it != streams_.end()) {
      streams_.erase(it);
      spdlog::trace("removed stream {} ({} total streams)", id, streams_.size());
      return;
    }
    spdlog::trace("couldn't find stream {} to remove! ({} total streams)", id, streams_.size());
  }

private:
  std::mutex mut_;
  std::map<std::string, std::shared_ptr<run_result_stream>> streams_;
};
} // namespace fit_cxx