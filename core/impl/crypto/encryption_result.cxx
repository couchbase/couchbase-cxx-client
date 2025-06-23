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

#include <couchbase/crypto/encryption_result.hxx>

#include "core/platform/base64.h"

#include <stdexcept>

namespace couchbase::crypto
{
encryption_result::encryption_result(std::string algorithm)
  : internal_{ { "alg", std::move(algorithm) } }
{
}

encryption_result::encryption_result(std::map<std::string, std::string> encrypted_node)
  : internal_{ std::move(encrypted_node) }
{
}

auto
encryption_result::algorithm() const -> std::string
{
  return internal_.at("alg");
}

auto
encryption_result::get(const std::string& field_name) const -> std::optional<std::string>
{
  if (internal_.count(field_name) == 0) {
    return std::nullopt;
  }
  return internal_.at(field_name);
}

auto
encryption_result::get_bytes(const std::string& field_name) const
  -> std::optional<std::vector<std::byte>>
{
  if (internal_.count(field_name) == 0) {
    return std::nullopt;
  }
  return core::base64::decode(internal_.at(field_name));
}

auto
encryption_result::as_map() const -> std::map<std::string, std::string>
{
  return internal_;
}

void
encryption_result::put(std::string field_name, std::string value)
{
  if (field_name == "alg") {
    throw std::invalid_argument("`alg` is a reserved field");
  }
  internal_.emplace(std::move(field_name), std::move(value));
}

void
encryption_result::put(std::string field_name, std::vector<std::byte> value)
{
  if (field_name == "alg") {
    throw std::invalid_argument("`alg` is a reserved field");
  }
  internal_.emplace(std::move(field_name), core::base64::encode(value));
}
} // namespace couchbase::crypto
