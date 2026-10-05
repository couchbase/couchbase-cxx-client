/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "integration_shortcuts.hxx"

#include <couchbase/error_codes.hxx>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

namespace test::utils
{
/**
 * Executes a management request that removes a resource -- a drop, an undeploy, a link
 * disconnect -- when the scope exits, including when an assertion unwinds it, so a failing test
 * does not leave the resource on the cluster. Analytics datasets, GSI and FTS indexes and
 * deployed eventing functions each hold a DCP stream, and a leftover one slows every later test on
 * the bucket.
 *
 * A resource that is already gone counts as dropped, so a test that drops it itself can still hold
 * a guard. HTTP 503, and an eventing function that is still deployed or not yet bootstrapped, are
 * retried until `budget` runs out. Any other failure, including an exception, is printed and not
 * thrown: the destructor is noexcept, so a throw would terminate the process.
 *
 * Guards run in reverse declaration order, so declare an eventing undeploy guard after the drop
 * guard for the same function.
 */
template<typename Request>
class drop_guard
{
public:
  drop_guard(const couchbase::core::cluster& cluster,
             std::string what,
             Request request,
             std::chrono::seconds budget = std::chrono::seconds{ 30 })
    : cluster_{ cluster }
    , what_{ std::move(what) }
    , request_{ std::move(request) }
    , budget_{ budget }
  {
  }

  drop_guard(const drop_guard&) = delete;
  drop_guard(drop_guard&&) = delete;
  auto operator=(const drop_guard&) -> drop_guard& = delete;
  auto operator=(drop_guard&&) -> drop_guard& = delete;

  // For a request the test has since made itself and asserted: the guard no longer repeats it.
  void dismiss()
  {
    dismissed_ = true;
  }

  ~drop_guard()
  {
    if (dismissed_) {
      return;
    }
    try {
      // Each attempt gets only what is left of the budget, so a hung request cannot extend it.
      const auto deadline = std::chrono::steady_clock::now() + budget_;
      std::error_code ec{};
      // The last error the server gave, reported if the final attempt only timed out.
      std::error_code cause{};
      while (true) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now());
        request_.timeout = std::max(left, std::chrono::milliseconds{ 1 });
        auto resp = execute(cluster_, request_);
        ec = resp.ctx.ec;
        // A decoder can leave a 503 with an empty body as success; the drop has not happened.
        if (!ec && resp.ctx.http_status == service_unavailable) {
          ec = couchbase::errc::common::service_not_available;
        }
        if (ec && !is_timeout(ec)) {
          cause = ec;
        }
        // A retry needs at least retry_delay of its own after the sleep; a shorter one could only
        // time out.
        if (!ec || is_gone(ec) || !is_transient(ec, resp.ctx.http_status) ||
            std::chrono::steady_clock::now() + 2 * retry_delay >= deadline) {
          break;
        }
        std::this_thread::sleep_for(retry_delay);
      }
      if (ec && !is_gone(ec)) {
        const auto& reported = is_timeout(ec) && cause ? cause : ec;
        std::fprintf(stderr, "unable to drop %s: %s\n", what_.c_str(), reported.message().c_str());
      }
    } catch (const std::exception& e) {
      std::fprintf(stderr, "unable to drop %s: %s\n", what_.c_str(), e.what());
    } catch (...) {
      std::fprintf(stderr, "unable to drop %s: unknown exception\n", what_.c_str());
    }
  }

private:
  static constexpr std::chrono::milliseconds retry_delay{ 500 };
  static constexpr std::uint32_t service_unavailable{ 503 };

  // The resource is already in the state the request leaves it in. An undeploy of a function that
  // was never deployed is the same case.
  static auto is_gone(std::error_code ec) -> bool
  {
    using namespace couchbase::errc;
    return ec == common::bucket_not_found || ec == common::scope_not_found ||
           ec == common::collection_not_found || ec == common::index_not_found ||
           ec == analytics::dataset_not_found || ec == analytics::dataverse_not_found ||
           ec == analytics::link_not_found || ec == management::user_not_found ||
           ec == management::group_not_found || ec == management::eventing_function_not_found ||
           ec == management::eventing_function_not_deployed;
  }

  static auto is_timeout(std::error_code ec) -> bool
  {
    return ec == couchbase::errc::common::unambiguous_timeout ||
           ec == couchbase::errc::common::ambiguous_timeout;
  }

  static auto is_transient(std::error_code ec, std::uint32_t http_status) -> bool
  {
    return http_status == service_unavailable ||
           ec == couchbase::errc::management::eventing_function_deployed ||
           ec == couchbase::errc::management::eventing_function_not_bootstrapped;
  }

  const couchbase::core::cluster& cluster_;
  std::string what_;
  Request request_;
  std::chrono::seconds budget_;
  bool dismissed_{ false };
};
} // namespace test::utils
