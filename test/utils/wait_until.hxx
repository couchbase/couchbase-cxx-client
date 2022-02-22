/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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
#include "server_version.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/operations/management/bucket.hxx>
#include <couchbase/operations/management/collections.hxx>

namespace test::utils
{
template<class ConditionChecker>
bool
wait_until(ConditionChecker&& condition_checker, std::chrono::milliseconds timeout, std::chrono::milliseconds delay)
{
    const auto start = std::chrono::high_resolution_clock::now();
    while (!condition_checker()) {
        if (std::chrono::high_resolution_clock::now() > start + timeout) {
            return false;
        }
        std::this_thread::sleep_for(delay);
    }
    return true;
}

template<class ConditionChecker>
bool
wait_until(ConditionChecker&& condition_checker, std::chrono::milliseconds timeout)
{
    return wait_until(condition_checker, timeout, std::chrono::milliseconds(100));
}

template<class ConditionChecker>
bool
wait_until(ConditionChecker&& condition_checker)
{
    return wait_until(condition_checker, std::chrono::minutes(1));
}

bool
wait_until_bucket_healthy(std::shared_ptr<couchbase::cluster> cluster, const std::string& bucket_name);

bool
wait_until_collection_manifest_propagated(std::shared_ptr<couchbase::cluster> cluster,
                                          const std::string& bucket_name,
                                          const std::uint64_t current_manifest_uid);

couchbase::operations::management::bucket_get_response
wait_for_bucket_created(std::shared_ptr<couchbase::cluster> cluster, const std::string& bucket_name);

template<typename Request>
auto
retry_on_error(std::shared_ptr<couchbase::cluster> cluster, Request req, std::error_code error)
{
    using response_type = typename Request::response_type;
    response_type resp;
    test::utils::wait_until([&]() {
        resp = test::utils::execute(cluster, req);
        return resp.ctx.ec != error;
    });
    return resp;
};
} // namespace test::utils
