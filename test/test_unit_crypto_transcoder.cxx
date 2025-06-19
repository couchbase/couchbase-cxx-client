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

#include "test_helper.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/crypto/aead_aes_256_cbc_hmac_sha512_provider.hxx>
#include <couchbase/crypto/default_manager.hxx>
#include <couchbase/crypto/default_transcoder.hxx>
#include <couchbase/crypto/encrypted_fields.hxx>
#include <couchbase/crypto/insecure_keyring.hxx>

#include <tao/json/to_string.hpp>
#include <tao/json/value.hpp>

auto
make_bytes(std::vector<unsigned char> v) -> std::vector<std::byte>
{
  std::vector<std::byte> out{ v.size() };
  std::transform(v.begin(), v.end(), out.begin(), [](int c) {
    return static_cast<std::byte>(c);
  });
  return out;
}

struct doc {
  std::string maxim;

  auto operator==(const doc& other) const -> bool
  {
    return maxim == other.maxim;
  }

  inline static const std::vector<couchbase::crypto::encrypted_field> encrypted_fields{
    {
      /* .field_path = */ { "maxim" },
      /* .encrypter_alias = */ {},
    },
  };
};

template<>
struct tao::json::traits<doc> {
  template<template<typename...> class Traits>
  static void assign(tao::json::basic_value<Traits>& v, const doc& d)
  {
    v = { { "maxim", d.maxim } };
  }

  template<template<typename...> class Traits>
  static auto as(const tao::json::basic_value<Traits>& v) -> doc
  {
    doc d;
    d.maxim = v.at("maxim").get_string();
    return d;
  }
};

const auto KEY =
  make_bytes({ 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
               0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
               0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26,
               0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33,
               0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f });

auto
make_crypto_manager() -> std::shared_ptr<couchbase::crypto::default_manager>
{
  auto keyring = std::make_shared<couchbase::crypto::insecure_keyring>();
  keyring->add_key(couchbase::crypto::key("test-key", KEY));

  auto provider = couchbase::crypto::aead_aes_256_cbc_hmac_sha512_provider(keyring);

  auto manager = std::make_shared<couchbase::crypto::default_manager>();
  manager->register_default_encrypter(provider.encrypter_for_key("test-key"));
  manager->register_decrypter(provider.decrypter());

  return manager;
}

TEST_CASE("unit: crypto transcoder", "[unit]")
{
  const auto crypto_manager = make_crypto_manager();

  const doc d{ "The enemy knows the system." };

  static_assert(couchbase::crypto::has_encrypted_fields_v<doc>,
                "profile should have encrypted_fields");

  SECTION("encoding")
  {
    const couchbase::codec::encoded_value encoded =
      couchbase::crypto::default_transcoder::encode(d, crypto_manager);
    REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);

    auto encrypted_document = couchbase::core::utils::json::parse_binary(encoded.data);
    REQUIRE(encrypted_document.is_object());
    REQUIRE(encrypted_document.find("maxim") == nullptr);
    REQUIRE(encrypted_document.find("encrypted$maxim") != nullptr);
    REQUIRE(encrypted_document["encrypted$maxim"].is_object());
  }

  SECTION("decoding")
  {
    const auto data = couchbase::core::utils::json::generate_binary(tao::json::value{
      { "encrypted$maxim",
        tao::json::value{
          { "alg", "AEAD_AES_256_CBC_HMAC_SHA512" },
          { "kid", "test-key" },
          { "ciphertext",
            "GvOMLcK5b/"
            "3YZpQJI0G8BLm98oj20ZLdqKDV3MfTuGlWL4R5p5Deykuv2XLW4LcDvnOkmhuUSRbQ8QVEmbjq43XHdOm3ColJ"
            "6LzoaAtJihk=" },
        } },
    });
    const auto encoded =
      couchbase::codec::encoded_value{ data, couchbase::codec::codec_flags::json_common_flags };
    const auto decoded_doc =
      couchbase::crypto::default_transcoder::decode<doc>(encoded, crypto_manager);
    REQUIRE(d == decoded_doc);

    const auto decoded_doc_as_tao_json =
      couchbase::crypto::default_transcoder::decode<tao::json::value>(encoded, crypto_manager);
    REQUIRE(decoded_doc_as_tao_json.get_object().size() == 1);
    REQUIRE(decoded_doc_as_tao_json.find("maxim") != nullptr);
    REQUIRE(decoded_doc_as_tao_json.find("encrypted$maxim") == nullptr);
  }

  SECTION("encoding and decoding")
  {
    const couchbase::codec::encoded_value encoded =
      couchbase::crypto::default_transcoder::encode(d, crypto_manager);
    REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);

    auto encrypted_document = couchbase::core::utils::json::parse_binary(encoded.data);
    REQUIRE(encrypted_document.is_object());
    REQUIRE(encrypted_document.get_object().size() == 1);
    REQUIRE(encrypted_document.find("maxim") == nullptr);
    REQUIRE(encrypted_document.find("encrypted$maxim") != nullptr);
    REQUIRE(encrypted_document["encrypted$maxim"].is_object());

    auto encrypted_node = encrypted_document["encrypted$maxim"];
    REQUIRE(encrypted_node["ciphertext"].is_string());
    REQUIRE(encrypted_node["kid"].get_string() == "test-key");
    REQUIRE(encrypted_node["alg"].get_string() == "AEAD_AES_256_CBC_HMAC_SHA512");

    const auto decoded_doc =
      couchbase::crypto::default_transcoder::decode<doc>(encoded, crypto_manager);
    REQUIRE(d == decoded_doc);

    const auto decoded_doc_as_tao_json =
      couchbase::crypto::default_transcoder::decode<tao::json::value>(encoded, crypto_manager);
    REQUIRE(decoded_doc_as_tao_json.get_object().size() == 1);
    REQUIRE(decoded_doc_as_tao_json.find("maxim") != nullptr);
    REQUIRE(decoded_doc_as_tao_json.find("encrypted$maxim") == nullptr);
  }
}
