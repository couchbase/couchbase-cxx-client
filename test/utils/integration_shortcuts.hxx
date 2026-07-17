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
#include "core/service_type.hxx"

#include <couchbase/cluster_state.hxx>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace test::utils
{
template<class Request>
auto
execute(const couchbase::core::cluster& cluster, Request request)
{
  using response_type = typename Request::response_type;
  auto barrier = std::make_shared<std::promise<response_type>>();
  auto f = barrier->get_future();
  cluster.execute(request, [barrier](response_type resp) {
    barrier->set_value(std::move(resp));
  });
  return f.get();
}

void
open_cluster(const couchbase::core::cluster& cluster, const couchbase::core::origin& origin);

void
close_cluster(const couchbase::core::cluster& cluster);

void
open_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name);

void
close_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name);

// Block until the cluster (or, when bucket_name is set, the bucket) reaches desired_state or
// timeout elapses, returning the resulting error code. Wraps the core's async wait_until_ready with
// a promise, mirroring open_cluster/open_bucket above. The bucket variant opens the bucket itself.
auto
wait_until_ready(const couchbase::core::cluster& cluster,
                 std::optional<std::string> bucket_name,
                 std::chrono::milliseconds timeout,
                 couchbase::cluster_state desired_state,
                 std::set<couchbase::core::service_type> services) -> std::error_code;
} // namespace test::utils
