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
#include "test_context.hxx"

#include <couchbase/management/bucket_settings.hxx>
#include <couchbase/operations/management/bucket_describe.hxx>
#include <couchbase/operations/management/cluster_describe.hxx>

#include <optional>

namespace test::utils
{
class integration_test_guard
{
  public:
    integration_test_guard();
    ~integration_test_guard();

    inline const couchbase::operations::management::bucket_describe_response::bucket_info& load_bucket_info(bool refresh = false)
    {
        return load_bucket_info(ctx.bucket, refresh);
    }

    const couchbase::operations::management::bucket_describe_response::bucket_info& load_bucket_info(const std::string& bucket_name,
                                                                                                     bool refresh = false);

    inline std::size_t number_of_nodes()
    {
        return load_bucket_info(ctx.bucket).number_of_nodes;
    }

    std::size_t number_of_nodes(const std::string& bucket_name)
    {
        return load_bucket_info(bucket_name).number_of_nodes;
    }

    inline std::size_t number_of_replicas()
    {
        return load_bucket_info(ctx.bucket).number_of_replicas;
    }

    std::size_t number_of_replicas(const std::string& bucket_name)
    {
        return load_bucket_info(bucket_name).number_of_replicas;
    }

    inline couchbase::management::cluster::bucket_storage_backend storage_backend()
    {
        return load_bucket_info(ctx.bucket).storage_backend;
    }

    const couchbase::operations::management::cluster_describe_response::cluster_info& load_cluster_info(bool refresh = false);

    inline bool has_service(couchbase::service_type service)
    {
        return load_cluster_info().services.count(service) > 0;
    }

    inline bool has_eventing_service()
    {
        return has_service(couchbase::service_type::eventing);
    }

    inline bool has_analytics_service()
    {
        return has_service(couchbase::service_type::analytics);
    }

    auto number_of_query_nodes()
    {
        const auto& ci = load_cluster_info();
        return std::count_if(ci.nodes.begin(), ci.nodes.end(), [](const auto& node) {
            return std::find(node.services.begin(), node.services.end(), "n1ql") != node.services.end();
        });
    }

    server_version cluster_version();

    std::thread io_thread{};
    asio::io_context io{};
    std::shared_ptr<couchbase::cluster> cluster;
    test_context ctx;
    couchbase::origin origin;

    std::map<std::string, couchbase::operations::management::bucket_describe_response::bucket_info, std::less<>> info{};
    std::optional<couchbase::operations::management::cluster_describe_response::cluster_info> cluster_info{};
};
} // namespace test::utils
