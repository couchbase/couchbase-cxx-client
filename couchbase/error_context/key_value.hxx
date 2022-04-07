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

#include <couchbase/cas.hxx>
#include <couchbase/document_id.hxx>
#include <couchbase/io/retry_reason.hxx>
#include <couchbase/protocol/enhanced_error_info.hxx>
#include <couchbase/protocol/status.hxx>
#include <couchbase/topology/error_map.hxx>

#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace couchbase::error_context
{

struct key_value {
    document_id id;
    std::error_code ec{};
    std::uint32_t opaque{};
    couchbase::cas cas{};
    std::optional<protocol::status> status_code{};
    std::optional<error_map::error_info> error_map_info{};
    std::optional<protocol::enhanced_error_info> enhanced_error_info{};

    std::optional<std::string> last_dispatched_to{};
    std::optional<std::string> last_dispatched_from{};
    int retry_attempts{ 0 };
    std::set<io::retry_reason> retry_reasons{};
};

} // namespace couchbase::error_context
