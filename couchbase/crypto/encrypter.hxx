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

#include <couchbase/crypto/encryption_result.hxx>
#include <couchbase/error.hxx>

#include <utility>
#include <vector>

namespace couchbase::crypto
{
class encrypter
{
public:
  encrypter() = default;
  encrypter(const encrypter& other) = default;
  encrypter(encrypter&& other) = default;
  auto operator=(const encrypter& other) -> encrypter& = default;
  auto operator=(encrypter&& other) -> encrypter& = default;
  virtual ~encrypter() = default;

  virtual auto encrypt(std::vector<std::byte> plaintext) -> std::pair<error, encryption_result> = 0;
};
} // namespace couchbase::crypto
