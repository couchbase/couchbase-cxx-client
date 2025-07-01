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

#include <couchbase/crypto/key.hxx>
#include <couchbase/error.hxx>

namespace couchbase::crypto
{
class keyring
{
public:
  keyring() = default;
  keyring(const keyring& other) = default;
  keyring(keyring&& other) = default;
  auto operator=(const keyring& other) -> keyring& = default;
  auto operator=(keyring&& other) -> keyring& = default;
  virtual ~keyring() = default;

  [[nodiscard]] virtual auto get(const std::string& key_id) const -> std::pair<error, key> = 0;
};
} // namespace couchbase::crypto
