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

#include "core/logger/logger.hxx"

#include <spdlog/details/os.h>
#include <spdlog/spdlog.h>

namespace test::utils
{
void
init_logger()
{
    static bool initialized = false;

    if (!initialized) {
        couchbase::core::logger::create_console_logger();
        if (auto env_val = spdlog::details::os::getenv("TEST_LOG_LEVEL"); !env_val.empty()) {
            couchbase::core::logger::set_log_levels(couchbase::core::logger::level_from_str(env_val));
        }
        if (auto env_val = spdlog::details::os::getenv("TEST_LOG_INCLUDE_LOCATION"); !env_val.empty()) {
            spdlog::set_pattern("[%Y-%m-%d %T.%e] [%P,%t] [%^%l%$] %oms, %v at %@ %!");
        }
        initialized = true;
    }
}
} // namespace test::utils
