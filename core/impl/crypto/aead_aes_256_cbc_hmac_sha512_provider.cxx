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

#include "core/impl/crypto/aead_aes_256_cbc_hmac_sha512_provider.hxx"

#include "core/mcbp/big_endian.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/crypto/aead_aes_256_cbc_hmac_sha512_provider.hxx>
#include <couchbase/error_codes.hxx>

#include <openssl/rand.h>
#include <spdlog/fmt/bundled/format.h>

#include "core/crypto/cbcrypto.h"
#include "core/platform/base64.h"

namespace couchbase::core::impl::crypto
{
auto
aead_aes_256_cbc_hmac_sha512_encrypt(std::vector<std::byte> key,
                                     std::vector<std::byte> iv,
                                     std::vector<std::byte> plaintext,
                                     std::vector<std::byte> associated_data)
  -> std::pair<error, std::vector<std::byte>>
{
  if (key.size() != 64) {
    return {
      error{ errc::field_level_encryption::invalid_crypto_key, "Key must be 64 bytes long." }, {}
    };
  }
  auto hmac_key = std::vector<std::byte>{ key.begin(), key.begin() + 32 };
  auto aes_key = std::vector<std::byte>{ key.begin() + 32, key.end() };

  std::vector<std::byte> ciphertext;
  try {
    ciphertext = core::utils::to_binary(
      core::crypto::encrypt(core::crypto::Cipher::AES_256_cbc,
                            { reinterpret_cast<char*>(aes_key.data()), aes_key.size() },
                            { reinterpret_cast<char*>(iv.data()), iv.size() },
                            { reinterpret_cast<char*>(plaintext.data()), plaintext.size() }));
  } catch (const std::exception& e) {
    return { error{ errc::field_level_encryption::encryption_failure,
                    fmt::format("Encryption failed: {}", e.what()) },
             {} };
  }

  ciphertext.insert(ciphertext.begin(), iv.begin(), iv.end());

  std::vector<std::byte> associated_data_length{ sizeof(std::uint64_t) };
  mcbp::big_endian::put_uint64(associated_data_length, associated_data.size() * 8); // In bits

  std::vector<std::byte> digest_data;
  digest_data.reserve(associated_data.size() + ciphertext.size() + sizeof(std::uint64_t));
  digest_data.insert(digest_data.end(), associated_data.begin(), associated_data.end());
  digest_data.insert(digest_data.end(), ciphertext.begin(), ciphertext.end());
  digest_data.insert(
    digest_data.end(), associated_data_length.begin(), associated_data_length.end());

  std::vector<std::byte> auth_tag;
  try {
    auth_tag = core::utils::to_binary(
      core::crypto::CBC_HMAC(core::crypto::Algorithm::ALG_SHA512,
                             { reinterpret_cast<char*>(hmac_key.data()), hmac_key.size() },
                             { reinterpret_cast<char*>(digest_data.data()), digest_data.size() }));
  } catch (const std::exception& e) {
    return { error{ errc::field_level_encryption::encryption_failure,
                    fmt::format("Generating the HMAC SHA-512 auth tag failed: {}", e.what()) },
             {} };
  }

  if (auth_tag.size() != 64) {
    return { { errc::field_level_encryption::encryption_failure,
               fmt::format("Unexpected HMAC-SHA512 auth tag size: expected 64 bytes, got {}.",
                           auth_tag.size()) },
             {} };
  }

  // We append the first 32 bytes of the auth tag to the ciphertext to get the authenticated
  // ciphertext
  ciphertext.insert(ciphertext.end(), auth_tag.begin(), auth_tag.begin() + 32);

  return { {}, ciphertext };
}

auto
aead_aes_256_cbc_hmac_sha512_decrypt(std::vector<std::byte> key,
                                     std::vector<std::byte> ciphertext,
                                     std::vector<std::byte> associated_data)
  -> std::pair<error, std::vector<std::byte>>
{
  if (ciphertext.size() < 48) {
    return { error{ errc::field_level_encryption::invalid_ciphertext,
                    "ciphertext is not long enough to include auth tag and IV." },
             {} };
  }
  if (key.size() != 64) {
    return {
      error{ errc::field_level_encryption::invalid_crypto_key, "key must be 64 bytes long." }, {}
    };
  }
  auto hmac_key = std::vector<std::byte>{ key.begin(), key.begin() + 32 };
  auto aes_key = std::vector<std::byte>{ key.begin() + 32, key.end() };

  std::vector<std::byte> associated_data_length{ sizeof(std::uint64_t) };
  mcbp::big_endian::put_uint64(associated_data_length, associated_data.size() * 8); // In bits

  const auto expected_auth_tag = std::vector<std::byte>{ ciphertext.end() - 32, ciphertext.end() };
  ciphertext.erase(ciphertext.end() - 32, ciphertext.end());

  std::vector<std::byte> digest_data;
  digest_data.reserve(associated_data.size() + ciphertext.size() + sizeof(std::uint64_t));
  digest_data.insert(digest_data.end(), associated_data.begin(), associated_data.end());
  digest_data.insert(digest_data.end(), ciphertext.begin(), ciphertext.end());
  digest_data.insert(
    digest_data.end(), associated_data_length.begin(), associated_data_length.end());

  std::vector<std::byte> auth_tag;
  try {
    auth_tag = core::utils::to_binary(
      core::crypto::CBC_HMAC(core::crypto::Algorithm::ALG_SHA512,
                             { reinterpret_cast<char*>(hmac_key.data()), hmac_key.size() },
                             { reinterpret_cast<char*>(digest_data.data()), digest_data.size() }));
  } catch (const std::exception& e) {
    return { error{ errc::field_level_encryption::decryption_failure,
                    fmt::format("Generating the HMAC SHA-512 auth tag failed: {}.", e.what()) },
             {} };
  }

  if (auth_tag.size() != 64) {
    return { { errc::field_level_encryption::decryption_failure,
               fmt::format("Unexpected HMAC-SHA512 auth tag size: expected 64 bytes, got {}.",
                           auth_tag.size()) },
             {} };
  }

  // Time-constant comparison of auth_tag and expected_auth_tag
  bool auth_tag_matches = true;
  for (std::size_t i = 0; i < expected_auth_tag.size(); ++i) {
    auth_tag_matches &= auth_tag.at(i) == expected_auth_tag.at(i);
  }
  if (!auth_tag_matches) {
    return { error{ errc::field_level_encryption::invalid_ciphertext,
                    "Invalid HMAC SHA-512 auth tag." },
             {} };
  }

  auto iv = std::vector<std::byte>{ ciphertext.begin(), ciphertext.begin() + 16 };
  ciphertext.erase(ciphertext.begin(), ciphertext.begin() + 16);

  std::vector<std::byte> plaintext;
  try {
    plaintext = core::utils::to_binary(
      core::crypto::decrypt(core::crypto::Cipher::AES_256_cbc,
                            { reinterpret_cast<char*>(aes_key.data()), aes_key.size() },
                            { reinterpret_cast<char*>(iv.data()), iv.size() },
                            { reinterpret_cast<char*>(ciphertext.data()), ciphertext.size() }));
  } catch (const std::exception& e) {
    return { error{ errc::field_level_encryption::decryption_failure,
                    fmt::format("Decryption failed: {}", e.what()) },
             {} };
  }
  return { {}, plaintext };
}
} // namespace couchbase::core::impl::crypto

namespace couchbase::crypto
{
namespace
{
auto
generate_initialization_vector() -> std::pair<error, std::vector<std::byte>>
{
  std::vector<std::byte> iv{ 16 };
#ifdef COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL
  auto iv_size = iv.size();
#else
  auto iv_size = static_cast<int>(iv.size());
#endif
  if (RAND_bytes(reinterpret_cast<unsigned char*>(iv.data()), iv_size) != 1) {
    return { error{ errc::field_level_encryption::encryption_failure,
                    "Failed to generate random initialization vector" },
             {} };
  }
  return { {}, iv };
}
} // namespace

aead_aes_256_cbc_hmac_sha512_provider::aead_aes_256_cbc_hmac_sha512_provider(
  std::shared_ptr<keyring> keyring)
  : keyring_(std::move(keyring))
{
}

auto
aead_aes_256_cbc_hmac_sha512_provider::encrypter_for_key(const std::string& key_id) const
  -> std::shared_ptr<encrypter>
{
  return std::make_shared<aead_aes_256_cbc_hmac_sha512_encrypter>(key_id, keyring_);
}

auto
aead_aes_256_cbc_hmac_sha512_provider::decrypter() const -> std::shared_ptr<crypto::decrypter>
{
  return std::make_shared<aead_aes_256_cbc_hmac_sha512_decrypter>(keyring_);
}

aead_aes_256_cbc_hmac_sha512_encrypter::aead_aes_256_cbc_hmac_sha512_encrypter(
  std::string key_id,
  std::shared_ptr<keyring> keyring)
  : keyring_{ std::move(keyring) }
  , key_id_{ std::move(key_id) }
{
}

auto
aead_aes_256_cbc_hmac_sha512_encrypter::encrypt(std::vector<std::byte> plaintext)
  -> std::pair<error, encryption_result>
{
  auto [key_err, key] = keyring_->get(key_id_);
  if (key_err) {
    return { key_err, {} };
  }

  auto [iv_err, iv] = generate_initialization_vector();
  if (iv_err) {
    return { iv_err, {} };
  }

  auto [enc_err, ciphertext] =
    core::impl::crypto::aead_aes_256_cbc_hmac_sha512_encrypt(key.bytes(), iv, plaintext, {});
  if (enc_err) {
    return { enc_err, {} };
  }

  auto res = encryption_result(core::impl::crypto::aead_aes_256_cbc_hmac_sha512_algorithm_name);
  res.put("kid", key_id_);
  res.put("ciphertext", core::base64::encode(ciphertext));

  return { {}, res };
}

aead_aes_256_cbc_hmac_sha512_decrypter::aead_aes_256_cbc_hmac_sha512_decrypter(
  std::shared_ptr<keyring> keyring)
  : keyring_{ std::move(keyring) }
{
}

auto
aead_aes_256_cbc_hmac_sha512_decrypter::decrypt(encryption_result encrypted)
  -> std::pair<error, std::vector<std::byte>>
{
  const auto key_id = encrypted.get("kid");
  if (!key_id.has_value()) {
    return { error{ errc::field_level_encryption::decryption_failure,
                    "failed to get key ID from document" },
             {} };
  }
  std::optional<std::vector<std::byte>> ciphertext{};
  try {
    ciphertext = encrypted.get_bytes("ciphertext");
  } catch (const std::invalid_argument& e) {
    return { error{ errc::field_level_encryption::invalid_ciphertext,
                    fmt::format("ciphertext could not be decoded: {}", e.what()) },
             {} };
  }
  if (!ciphertext.has_value()) {
    return { error{ errc::field_level_encryption::decryption_failure,
                    "failed to get ciphertext from document" },
             {} };
  }
  const auto [key_err, key] = keyring_->get(key_id.value());
  if (key_err) {
    return { key_err, {} };
  }

  return core::impl::crypto::aead_aes_256_cbc_hmac_sha512_decrypt(
    key.bytes(), ciphertext.value(), {});
}

auto
aead_aes_256_cbc_hmac_sha512_decrypter::algorithm() const -> const std::string&
{
  return core::impl::crypto::aead_aes_256_cbc_hmac_sha512_algorithm_name;
}
} // namespace couchbase::crypto
