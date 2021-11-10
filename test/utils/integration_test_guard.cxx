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
    open_cluster(cluster, couchbase::origin(auth, connstr));
}

integration_test_guard::~integration_test_guard()
{
    close_cluster(cluster);
    io_thread.join();
}
} // namespace test::utils
