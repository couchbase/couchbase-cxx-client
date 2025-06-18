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

#include <memory>
#include <string>
#include <utility>

namespace couchbase::crypto
{
class aead_aes_256_cbc_hmac_sha512_provider
{
public:
  explicit aead_aes_256_cbc_hmac_sha512_provider(std::shared_ptr<keyring> keyring);

  [[nodiscard]] auto encrypter_for_key(const std::string& key_id) const
    -> std::shared_ptr<encrypter>;
  [[nodiscard]] auto decrypter() const -> std::shared_ptr<decrypter>;

private:
  std::shared_ptr<keyring> keyring_;
};

class aead_aes_256_cbc_hmac_sha512_encrypter : public encrypter
{
public:
  explicit aead_aes_256_cbc_hmac_sha512_encrypter(std::string key_id,
                                                  std::shared_ptr<keyring> keyring);

  auto encrypt(std::vector<std::byte> plaintext) -> std::pair<error, encryption_result> override;

private:
  std::shared_ptr<keyring> keyring_;
  std::string key_id_;
};

class aead_aes_256_cbc_hmac_sha512_decrypter : public decrypter
{
public:
  explicit aead_aes_256_cbc_hmac_sha512_decrypter(std::shared_ptr<keyring> keyring);

  auto decrypt(encryption_result encrypted) -> std::pair<error, std::vector<std::byte>> override;
  [[nodiscard]] auto algorithm() const -> const std::string& override;

private:
  std::shared_ptr<keyring> keyring_;
};
} // namespace couchbase::crypto
