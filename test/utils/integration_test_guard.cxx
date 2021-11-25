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
integration_test_guard::integration_test_guard()
  : cluster(couchbase::cluster(io))
  , ctx(test_context::load_from_environment())
{
    init_logger();
    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;
    io_thread = std::thread([this]() { io.run(); });
    origin = couchbase::origin(auth, connstr);
    open_cluster(cluster, origin);
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

} // namespace test::utils
