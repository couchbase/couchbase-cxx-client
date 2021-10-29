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
#include <spdlog/spdlog.h>

#include <couchbase/cluster.hxx>

inline void
native_init_logger()
{
    static bool initialized = false;

    if (!initialized) {
        spdlog::set_pattern("[%Y-%m-%d %T.%e] [%P,%t] [%^%l%$] %oms, %v");

        if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_LEVEL"); env_val.empty()) {
            spdlog::set_level(spdlog::level::warn);
        } else {
            spdlog::cfg::helpers::load_levels(env_val);
        }

        initialized = true;
    }
}

template<class Request>
typename Request::response_type
execute_http(couchbase::cluster& cluster, Request request)
{
    using response_type = typename Request::response_type;
    auto barrier = std::make_shared<std::promise<response_type>>();
    auto f = barrier->get_future();
    cluster.execute_http(request, [barrier](response_type resp) mutable { barrier->set_value(std::move(resp)); });
    auto resp = f.get();
    return resp;
}
