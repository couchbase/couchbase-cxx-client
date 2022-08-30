/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <optional>
#include <vector>

namespace couchbase::subdoc
{
/**
 * @since 1.0.0
 * @volatile
 */
enum class lookup_in_macro {
    document,
    expiry_time,
    cas,
    sequence_number,
    vbucket_uuid,
    last_modified,
    is_deleted,
    value_size_bytes,
    revision_id,
    flags,
};

/**
 * Parses string as lookup_in macro.
 *
 * @param input string
 * @return empty `optional` if the string does not contain macro, corresponding enum value otherwise.
 *
 * @since 1.0.0
 * @volatile
 */
auto
to_lookup_in_macro(std::string_view input) -> std::optional<lookup_in_macro>;

/**
 * Converts macro into binary array suitable for sending to the server.
 *
 * @param value macro
 * @return binary string
 *
 * @since 1.0.0
 * @volatile
 */
auto
to_string(lookup_in_macro value) -> const std::string&;
} // namespace couchbase::subdoc
