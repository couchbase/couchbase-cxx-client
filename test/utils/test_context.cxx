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

#include "test_context.hxx"

#include <spdlog/details/os.h>

#include <cstring>

namespace test::utils
{
test_context
test_context::load_from_environment()
{
    test_context ctx{};

    if (auto var = spdlog::details::os::getenv("TEST_CONNECTION_STRING"); !var.empty()) {
        ctx.connection_string = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_USERNAME"); !var.empty()) {
        ctx.username = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_PASSWORD"); !var.empty()) {
        ctx.password = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_CERTIFICATE_PATH"); !var.empty()) {
        ctx.certificate_path = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_KEY_PATH"); !var.empty()) {
        ctx.key_path = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_BUCKET"); !var.empty()) {
        ctx.bucket = var;
    }

    if (auto var = spdlog::details::os::getenv("OTHER_TEST_BUCKET"); !var.empty()) {
        ctx.other_bucket = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_DNS_NAMESERVER"); !var.empty()) {
        ctx.dns_nameserver = var;
    }

    if (auto var = spdlog::details::os::getenv("TEST_DNS_PORT"); !var.empty()) {
        ctx.dns_port = std::stol(var);
    }

    if (auto var = spdlog::details::os::getenv("TEST_DEPLOYMENT_TYPE"); !var.empty()) {
        if (var == "on_prem") {
            ctx.deployment = deployment_type::on_prem;
        } else if (var == "capella") {
            ctx.deployment = deployment_type::capella;
        } else if (var == "elixir") {
            ctx.deployment = deployment_type::elixir;
        }
    }

    // TODO: I believe this + TEST_DEVELOPER_PREVIEW will conflict

    if (auto var = spdlog::details::os::getenv("TEST_SERVER_VERSION"); !var.empty()) {
        ctx.version = server_version::parse(var, ctx.deployment);
    }

    if (auto var = spdlog::details::os::getenv("TEST_DEVELOPER_PREVIEW"); !var.empty()) {
        if (var == "true" || var == "yes" || var == "1") {
            ctx.version.developer_preview = true;
        } else if (var == "false" || var == "no" || var == "0") {
            ctx.version.developer_preview = false;
        }
    }

    if (auto var = spdlog::details::os::getenv("TEST_NUMBER_OF_IO_THREADS"); !var.empty()) {
        ctx.number_of_io_threads = std::stoul(var);
    }

    if (auto var = spdlog::details::os::getenv("TEST_USE_GOCAVES"); !var.empty()) {
        if (var == "true" || var == "yes" || var == "1") {
            ctx.version.use_gocaves = true;
        } else if (var == "false" || var == "no" || var == "0") {
            ctx.version.use_gocaves = false;
        }
    }

    return ctx;
}
} // namespace test::utils
