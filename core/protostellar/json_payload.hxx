/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "core/json_string.hxx"

#include <string>

namespace couchbase::core::protostellar
{
// The JSON a json_string carries, whichever variant arm holds it.
//
// json_string is a variant over nullptr/std::string/std::vector<std::byte>, and str() returns a
// reference to a static empty string unless the std::string arm is active. Statement parameters
// never use that arm: the public options types declare them as std::vector<codec::binary> (see
// couchbase/query_options.hxx) and core/impl/query.cxx moves each one in, which selects the binary
// arm -- so reading them with str() alone sends every parameter as "".
//
// Returning the bytes as they came in also avoids a parse-and-regenerate round trip, since the
// proto fields these feed are `bytes` rather than structured messages.
//
// Lives here rather than in one converter because every service whose request carries
// caller-supplied JSON needs it, and a per-converter copy is how the query defect reached the
// analytics converter in the first place.
[[nodiscard]] inline auto
json_payload(const core::json_string& value) -> std::string
{
  if (value.is_binary()) {
    const auto& bytes = value.bytes();
    return { reinterpret_cast<const char*>(bytes.data()), bytes.size() };
  }
  return value.str();
}
} // namespace couchbase::core::protostellar
