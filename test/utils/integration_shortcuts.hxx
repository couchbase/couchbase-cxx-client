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

#include "core/cluster.hxx"

#include <condition_variable>
#include <mutex>
#include <optional>

namespace test::utils
{

/**
 * TSan-safe synchronization barrier replacing std::promise/std::future.
 *
 * libstdc++'s std::promise/std::future uses pthread_once internally, whose
 * happens-before semantics are not fully tracked by TSan, producing false
 * data-race reports. This barrier uses std::mutex + std::condition_variable
 * which TSan fully understands.
 */
template<typename T>
class barrier
{
public:
  void set_value(T v)
  {
    std::scoped_lock lock(mutex_);
    value_ = std::move(v);
    cv_.notify_one();
  }

  T get()
  {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] {
      return value_.has_value();
    });
    return std::move(*value_);
  }

private:
  std::mutex mutex_{};
  std::condition_variable cv_{};
  std::optional<T> value_{};
};

template<>
class barrier<void>
{
public:
  void set_value()
  {
    std::scoped_lock lock(mutex_);
    ready_ = true;
    cv_.notify_one();
  }

  void get()
  {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] {
      return ready_;
    });
  }

private:
  std::mutex mutex_{};
  std::condition_variable cv_{};
  bool ready_{ false };
};

template<class Request>
auto
execute(const couchbase::core::cluster& cluster, Request request)
{
  using response_type = typename Request::response_type;
  auto b = std::make_shared<barrier<response_type>>();
  cluster.execute(request, [b](response_type resp) {
    b->set_value(std::move(resp));
  });
  return b->get();
}

void
open_cluster(const couchbase::core::cluster& cluster, const couchbase::core::origin& origin);

void
close_cluster(const couchbase::core::cluster& cluster);

void
open_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name);

void
close_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name);
} // namespace test::utils
