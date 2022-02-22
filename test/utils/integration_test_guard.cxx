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

#include "integration_test_guard.hxx"
#include "logger.hxx"

namespace test::utils
{

integration_test_guard::integration_test_guard(couchbase::origin origin_, test_context ctx_, bool connect)
  : cluster(couchbase::cluster::create(io))
  , ctx(ctx_)
  , origin(origin_)
{
    init_logger();
    io_thread = std::thread([this]() { io.run(); });
    if (connect) {
        open_cluster(cluster, origin_);
    }
}

couchbase::origin
origin_from_test_context(test_context ctx_)
{
    auto connstr = couchbase::utils::parse_connection_string(ctx_.connection_string);
    auto auth = ctx_.build_auth();
    return couchbase::origin(auth, connstr);
}

integration_test_guard::integration_test_guard(test_context ctx_, bool connect)
  : integration_test_guard(origin_from_test_context(ctx_), ctx_, connect)
{
}

integration_test_guard::integration_test_guard(bool connect)
  : integration_test_guard(test_context::load_from_environment(), connect)
{
}

integration_test_guard::~integration_test_guard()
{
    close_cluster(cluster);
    io_thread.join();
}

const couchbase::operations::management::bucket_info&
integration_test_guard::load_bucket_info(const std::string& bucket_name, bool refresh)
{
    if (info.count(bucket_name) > 0 && !refresh) {
        return info[bucket_name];
    }

    auto resp = execute(cluster, couchbase::operations::management::bucket_describe_request{ bucket_name });
    if (resp.ctx.ec == couchbase::error::common_errc::service_not_available) {
        open_bucket(cluster, ctx.bucket);
        resp = execute(cluster, couchbase::operations::management::bucket_describe_request{ bucket_name });
    }
    if (resp.ctx.ec) {
        LOG_CRITICAL("unable to load info for bucket \"{}\": {}", bucket_name, resp.ctx.ec.message());
        throw std::system_error(resp.ctx.ec);
    }

    info[bucket_name] = resp.info;
    return info[bucket_name];
}

const couchbase::operations::management::cluster_info&
integration_test_guard::load_cluster_info(bool refresh)
{
    if (cluster_info && !refresh) {
        return cluster_info.value();
    }

    auto resp = execute(cluster, couchbase::operations::management::cluster_describe_request{});
    if (resp.ctx.ec == couchbase::error::common_errc::service_not_available) {
        open_bucket(cluster, ctx.bucket);
        resp = execute(cluster, couchbase::operations::management::cluster_describe_request{});
    }
    if (resp.ctx.ec) {
        LOG_CRITICAL("unable to load info for cluster: {}", resp.ctx.ec.message());
        throw std::system_error(resp.ctx.ec);
    }

    cluster_info.emplace(std::move(resp.info));

    return cluster_info.value();
}

server_version
integration_test_guard::cluster_version()
{
    load_cluster_info();
    std::string runtime_version{};
    for (const auto& node : cluster_info->nodes) {
        if (runtime_version.empty()) {
            runtime_version = node.version;
        } else if (runtime_version != node.version) {
            /* mixed version cluster, ignore it and fall back to version from test context */
            runtime_version.clear();
            break;
        }
    }
    if (runtime_version.empty()) {
        return ctx.version;
    }
    auto parsed_version = server_version::parse(runtime_version);
    if (parsed_version.major == 0) {
        /* the build does not specify version properly */
        return ctx.version;
    }
    return parsed_version;
}

} // namespace test::utils
