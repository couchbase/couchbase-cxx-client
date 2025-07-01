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

#include <couchbase/crypto/document.hxx>

namespace couchbase::crypto
{
document::document(codec::binary raw, std::vector<encrypted_field> encrypted_fields)
  : raw_{ std::move(raw) }
  , encrypted_fields_{ std::move(encrypted_fields) }
{
}

void
document::add_encrypted_field(std::vector<std::string> path,
                              std::optional<std::string> encrypter_alias)
{
  encrypted_fields_.emplace_back(encrypted_field{ std::move(path), std::move(encrypter_alias) });
}

auto
document::encrypted_fields() const -> const std::vector<encrypted_field>&
{
  return encrypted_fields_;
}

auto
document::raw() const -> const codec::binary&
{
  return raw_;
}
} // namespace couchbase::crypto
