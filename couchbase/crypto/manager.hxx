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

#include <couchbase/crypto/decrypter.hxx>
#include <couchbase/crypto/encrypter.hxx>
#include <couchbase/crypto/encryption_result.hxx>
#include <couchbase/crypto/keyring.hxx>
#include <couchbase/error.hxx>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace couchbase::crypto
{
class manager
{
public:
  manager() = default;
  manager(const manager& other) = default;
  manager(manager&& other) = default;
  auto operator=(const manager& other) -> manager& = default;
  auto operator=(manager&& other) -> manager& = default;
  virtual ~manager() = default;

  virtual auto encrypt(std::vector<std::byte> plaintext,
                       const std::optional<std::string>& encrypter_alias)
    -> std::pair<error, std::map<std::string, std::string>> = 0;
  virtual auto decrypt(std::map<std::string, std::string> encrypted_node)
    -> std::pair<error, std::vector<std::byte>> = 0;
  virtual auto mangle(std::string) -> std::string = 0;
  virtual auto demangle(std::string) -> std::string = 0;
  virtual auto is_mangled(const std::string&) -> bool = 0;
};
} // namespace couchbase::crypto
