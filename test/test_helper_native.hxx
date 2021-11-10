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

#include "test_helper.hxx"

#include <asio/version.hpp>

#include <spdlog/cfg/env.h>

#include <couchbase/logger/configuration.hxx>

#include <couchbase/cluster.hxx>

inline void
native_init_logger()
{
    static bool initialized = false;

    if (!initialized) {
        couchbase::logger::create_console_logger();
        if (auto env_val = spdlog::details::os::getenv("COUCHBASE_CXX_CLIENT_LOG_LEVEL"); !env_val.empty()) {
            couchbase::logger::set_log_levels(spdlog::level::from_str(env_val));
        }
        initialized = true;
    }
}

template<class Request>
auto
execute(couchbase::cluster& cluster, Request request)
{
    using response_type = typename Request::response_type;
    auto barrier = std::make_shared<std::promise<response_type>>();
    auto f = barrier->get_future();
    cluster.execute(request, [barrier](response_type resp) mutable { barrier->set_value(std::move(resp)); });
    auto resp = f.get();
    return resp;
}

inline std::error_code
open_cluster(couchbase::cluster& cluster, const couchbase::origin& origin)
{
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    INFO(rc.message());
    REQUIRE_FALSE(rc);
    return rc;
}

inline void
close_cluster(couchbase::cluster& cluster)
{
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    cluster.close([barrier]() { barrier->set_value(); });
    f.get();
}

inline std::error_code
open_bucket(couchbase::cluster& cluster, const std::string& bucket_name)
{
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.open_bucket(bucket_name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    INFO(rc.message());
    REQUIRE_FALSE(rc);
    return rc;
}

inline std::string
uniq_id(const std::string& prefix)
{
    return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

class IntegrationTest
{
  public:
    std::thread io_thread;
    asio::io_context io;
    couchbase::cluster cluster;
    test_context ctx;
    IntegrationTest();
    ~IntegrationTest();
};