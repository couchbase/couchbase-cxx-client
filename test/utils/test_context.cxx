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

#include <cstring>
namespace test::utils
{
test_context
test_context::load_from_environment()
{
    test_context ctx{};

    const char* var = nullptr;

    var = getenv("TEST_CONNECTION_STRING");
    if (var != nullptr) {
        ctx.connection_string = var;
    }
    var = getenv("TEST_USERNAME");
    if (var != nullptr) {
        ctx.username = var;
    }
    var = getenv("TEST_PASSWORD");
    if (var != nullptr) {
        ctx.password = var;
    }
    var = getenv("TEST_CERTIFICATE_PATH");
    if (var != nullptr) {
        ctx.certificate_path = var;
    }
    var = getenv("TEST_KEY_PATH");
    if (var != nullptr) {
        ctx.key_path = var;
    }
    var = getenv("TEST_BUCKET");
    if (var != nullptr) {
        ctx.bucket = var;
    }

    // TODO: I believe this + TEST_DEVELOPER_PREVIEW will conflict
    var = getenv("TEST_SERVER_VERSION");
    if (var != nullptr) {
        ctx.version = server_version::parse(var);
    }
    var = getenv("TEST_DEVELOPER_PREVIEW");
    if (var != nullptr) {
        if (strcmp(var, "true") == 0 || strcmp(var, "yes") == 0 || strcmp(var, "1") == 0) {
            ctx.version.developer_preview = true;
        } else if (strcmp(var, "false") == 0 || strcmp(var, "no") == 0 || strcmp(var, "0") == 0) {
            ctx.version.developer_preview = false;
        }
    }

    return ctx;
}
} // namespace test::utils
