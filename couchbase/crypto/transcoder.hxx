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

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/crypto/document.hxx>
#include <couchbase/crypto/manager.hxx>
#include <couchbase/error_codes.hxx>

namespace couchbase::crypto
{
namespace internal
{
auto
encrypt(const codec::binary& raw,
        const std::vector<encrypted_field>& encrypted_fields,
        const std::shared_ptr<manager>& crypto_manager) -> std::pair<error, codec::binary>;

auto
decrypt(const codec::binary& encrypted, const std::shared_ptr<manager>& crypto_manager)
  -> std::pair<error, codec::binary>;
} // namespace internal

template<typename Serializer>
class transcoder
{
public:
  static auto encode(const document& doc, const std::shared_ptr<manager>& crypto_manager)
    -> codec::encoded_value
  {
    if (crypto_manager == nullptr) {
      throw std::system_error(errc::field_level_encryption::generic_cryptography_failure,
                              "crypto manager is not set, cannot use transcoder with FLE");
    }
    auto [err, encrypted_data] =
      internal::encrypt(doc.raw(), doc.encrypted_fields(), crypto_manager);
    if (err) {
      throw std::system_error(err.ec(), "Failed to encrypt document: " + err.message());
    }
    return { std::move(encrypted_data), codec::codec_flags::json_common_flags };
  }

  template<typename Document>
  static auto encode(Document document, const std::shared_ptr<manager>& crypto_manager)
    -> codec::encoded_value
  {
    if (crypto_manager == nullptr) {
      throw std::system_error(errc::field_level_encryption::generic_cryptography_failure,
                              "crypto manager is not set, cannot use transcoder with FLE");
    }
    auto data = Serializer::serialize(document);
    auto [err, encrypted_data] =
      internal::encrypt(data, encrypted_fields<Document>, crypto_manager);
    if (err) {
      throw std::system_error(err.ec(), "Failed to encrypt document: " + err.message());
    }
    return { std::move(encrypted_data), codec::codec_flags::json_common_flags };
  }

  template<typename Document>
  static auto decode(const codec::encoded_value& encoded,
                     const std::shared_ptr<manager>& crypto_manager) -> Document
  {
    if (crypto_manager == nullptr) {
      throw std::system_error(errc::field_level_encryption::generic_cryptography_failure,
                              "crypto manager is not set, cannot use transcoder with FLE");
    }
    if (!codec::codec_flags::has_common_flags(encoded.flags,
                                              codec::codec_flags::json_common_flags)) {
      throw std::system_error(
        errc::common::decoding_failure,
        "crypto::transcoder expects document to have JSON common flags, flags=" +
          std::to_string(encoded.flags));
    }

    auto [err, decrypted_data] = internal::decrypt(encoded.data, crypto_manager);
    if (err) {
      throw std::system_error(err.ec(), "Failed to decrypt document: " + err.message());
    }
    return Serializer::template deserialize<Document>(decrypted_data);
  }
};
} // namespace couchbase::crypto
