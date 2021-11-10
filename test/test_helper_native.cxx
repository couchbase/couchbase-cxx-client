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

#include "test_helper_native.hxx"

#include <spdlog/cfg/env.h>

void
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