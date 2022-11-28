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

#include "integration_shortcuts.hxx"

#include "core/utils/join_strings.hxx"

namespace test::utils
{
void
open_cluster(std::shared_ptr<couchbase::core::cluster> cluster, const couchbase::core::origin& origin)
{
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster->open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    if (rc) {
        CB_LOG_CRITICAL(
          "unable to connect to the cluster: {}, nodes={}", rc.message(), couchbase::core::utils::join_strings(origin.get_nodes(), ", "));
        throw std::system_error(rc);
    }
}
void
close_cluster(std::shared_ptr<couchbase::core::cluster> cluster)
{
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    cluster->close([barrier]() { barrier->set_value(); });
    f.get();
}

void
open_bucket(std::shared_ptr<couchbase::core::cluster> cluster, const std::string& bucket_name)
{
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster->open_bucket(bucket_name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    if (rc) {
        CB_LOG_CRITICAL("unable to open bucket: {}, name={}", rc.message(), bucket_name);
        throw std::system_error(rc);
    }
}

void
close_bucket(std::shared_ptr<couchbase::core::cluster> cluster, const std::string& bucket_name)
{
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster->close_bucket(bucket_name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    if (rc) {
        CB_LOG_CRITICAL("unable to close bucket: {}, name={}", rc.message(), bucket_name);
        throw std::system_error(rc);
    }
}
} // namespace test::utils
