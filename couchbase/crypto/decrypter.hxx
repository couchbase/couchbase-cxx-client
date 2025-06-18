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

#include <string>
#include <utility>
#include <vector>

namespace couchbase::crypto
{
class decrypter
{
public:
  decrypter() = default;
  decrypter(const decrypter& other) = default;
  decrypter(decrypter&& other) = default;
  auto operator=(const decrypter& other) -> decrypter& = default;
  auto operator=(decrypter&& other) -> decrypter& = default;
  virtual ~decrypter() = default;

  [[nodiscard]] virtual auto decrypt(encryption_result encrypted)
    -> std::pair<error, std::vector<std::byte>> = 0;
  [[nodiscard]] virtual auto algorithm() const -> const std::string& = 0;
};
} // namespace couchbase::crypto
