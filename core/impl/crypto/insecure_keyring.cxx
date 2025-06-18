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

#include <couchbase/crypto/insecure_keyring.hxx>
#include <couchbase/error_codes.hxx>

namespace couchbase::crypto
{
insecure_keyring::insecure_keyring(const std::vector<key>& keys)
{
  for (const auto& k : keys) {
    keys_[k.id()] = k;
  }
}

auto
insecure_keyring::get(const std::string& key_id) const -> std::pair<error, key>
{
  if (const auto it = keys_.find(key_id); it == keys_.end()) {
    return {
      error{ errc::field_level_encryption::crypto_key_not_found, "Key not found: " + key_id }, {}
    };
  }
  return { {}, keys_.at(key_id) };
}

void
insecure_keyring::add_key(key k)
{
  keys_[k.id()] = std::move(k);
}
} // namespace couchbase::crypto
