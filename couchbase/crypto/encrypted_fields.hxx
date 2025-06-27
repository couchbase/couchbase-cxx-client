/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025. Couchbase, Inc.
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
#include <string>
#include <vector>

namespace couchbase::crypto
{
struct encrypted_field {
  std::vector<std::string> field_path;
  std::optional<std::string> encrypter_alias{};
};

template<typename Document, typename = void>
struct has_encrypted_fields : std::false_type {
};

template<typename Document>
struct has_encrypted_fields<Document, std::void_t<decltype(Document::encrypted_fields)>>
  : std::is_same<decltype(Document::encrypted_fields),
                 const std::vector<couchbase::crypto::encrypted_field>> {
};

template<typename Document>
constexpr bool has_encrypted_fields_v = has_encrypted_fields<Document>::value;

} // namespace couchbase::crypto
