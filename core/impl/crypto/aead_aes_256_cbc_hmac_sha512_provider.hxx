/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2025. Couchbase, Inc.
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

#include <couchbase/error.hxx>

#include <vector>

namespace couchbase::core::impl::crypto
{
inline const std::string aead_aes_256_cbc_hmac_sha512_algorithm_name =
  "AEAD_AES_256_CBC_HMAC_SHA512";

auto
aead_aes_256_cbc_hmac_sha512_encrypt(std::vector<std::byte> key,
                                     std::vector<std::byte> iv,
                                     std::vector<std::byte> plaintext,
                                     std::vector<std::byte> associated_data)
  -> std::pair<error, std::vector<std::byte>>;

auto
aead_aes_256_cbc_hmac_sha512_decrypt(std::vector<std::byte> key,
                                     std::vector<std::byte> ciphertext,
                                     std::vector<std::byte> associated_data)
  -> std::pair<error, std::vector<std::byte>>;
} // namespace couchbase::core::impl::crypto
