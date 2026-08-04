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

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <thread>

namespace couchbase::cng::test
{

// RAII owner of the io_context worker thread the live tests run against. It keeps a work guard so
// io_context::run() stays alive on a dedicated thread, and on scope exit -- normal OR via an
// assertion/skip exception -- it releases the guard and joins the thread. Declare it after the
// io_context and before the cluster so the cluster (which drains its gRPC calls in its destructor)
// tears down while the io thread is still running, and the thread is joined afterwards. Without
// this an early throw would unwind past a still-joinable std::thread and call std::terminate.
class io_thread_guard
{
public:
  explicit io_thread_guard(asio::io_context& io)
    : work_{ asio::make_work_guard(io) }
    , thread_{ [&io]() {
      io.run();
    } }
  {
  }

  io_thread_guard(const io_thread_guard&) = delete;
  io_thread_guard(io_thread_guard&&) = delete;
  auto operator=(const io_thread_guard&) -> io_thread_guard& = delete;
  auto operator=(io_thread_guard&&) -> io_thread_guard& = delete;

  ~io_thread_guard()
  {
    work_.reset();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

private:
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  std::thread thread_;
};

} // namespace couchbase::cng::test
