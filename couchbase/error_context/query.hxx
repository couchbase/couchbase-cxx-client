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

#include <couchbase/io/retry_reason.hxx>

#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace couchbase::error_context
{

struct query {
    std::error_code ec{};
    std::uint64_t first_error_code{};
    std::string first_error_message{};
    std::string client_context_id{};
    std::string statement{};
    std::optional<std::string> parameters{};

    std::string method{};
    std::string path{};
    std::uint32_t http_status{};
    std::string http_body{};
    std::string hostname{};
    std::uint16_t port{};

    std::optional<std::string> last_dispatched_to{};
    std::optional<std::string> last_dispatched_from{};
    int retry_attempts{ 0 };
    std::set<io::retry_reason> retry_reasons{};
};

} // namespace couchbase::error_context
