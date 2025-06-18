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
#include <couchbase/crypto/keyring.hxx>

#include <map>
#include <string>

namespace couchbase::crypto
{
class insecure_keyring : public keyring
{
public:
  insecure_keyring() = default;
  explicit insecure_keyring(const std::vector<key>& keys);

  void add_key(key k);

  [[nodiscard]] auto get(const std::string& key_id) const -> std::pair<error, key> override;

private:
  std::map<std::string, key> keys_;
};
} // namespace couchbase::crypto
