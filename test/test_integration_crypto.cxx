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

#include "profile.hxx"
#include "test_helper_integration.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/crypto/aead_aes_256_cbc_hmac_sha512_provider.hxx>
#include <couchbase/crypto/default_manager.hxx>
#include <couchbase/crypto/default_transcoder.hxx>
#include <couchbase/crypto/encrypted_fields.hxx>
#include <couchbase/crypto/insecure_keyring.hxx>

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

TEST_CASE("integration: upsert and get with encryption", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto cluster_options = integration.ctx.build_options();
  cluster_options.crypto_manager(make_crypto_manager());

  auto [err, cluster] =
    couchbase::cluster::connect(integration.ctx.connection_string, cluster_options).get();
  REQUIRE_NO_ERROR(err);

  profile albert{ "this_guy_again", "Albert Einstein", 1879 };

  static_assert(couchbase::crypto::has_encrypted_fields_v<profile>,
                "profile should have encrypted_fields");

  const auto collection = cluster.bucket(integration.ctx.bucket).default_collection();
  {
    const auto [err, res] =
      collection.upsert<couchbase::crypto::default_transcoder>("albert", albert).get();
    REQUIRE_NO_ERROR(err);
    REQUIRE_FALSE(res.cas().empty());
  }

  {
    const auto [err, res] = collection.get("albert").get();
    REQUIRE_NO_ERROR(err);
    REQUIRE_FALSE(res.cas().empty());

    const auto encrypted_content = res.content_as<tao::json::value>();
    REQUIRE(encrypted_content.get_object().size() == 3);
    REQUIRE(encrypted_content.find("full_name") == nullptr);
    REQUIRE(encrypted_content.find("encrypted$full_name") != nullptr);
    REQUIRE(encrypted_content.at("encrypted$full_name").get_object().size() == 3);
    REQUIRE(encrypted_content.at("encrypted$full_name").at("alg").get_string() ==
            "AEAD_AES_256_CBC_HMAC_SHA512");
    REQUIRE(encrypted_content.at("encrypted$full_name").at("kid").get_string() == "test-key");
    REQUIRE_FALSE(
      encrypted_content.at("encrypted$full_name").at("ciphertext").get_string().empty());

    // Encrypted document cannot be deserialized as profile, as the full_name field will not be
    // found.
    try {
      const auto _ = res.content_as<profile>();
      FAIL("Expected an exception when trying to deserialize encrypted content with a non-crypto "
           "transcoder");
    } catch (const std::system_error& e) {
      REQUIRE(e.code() == couchbase::errc::common::decoding_failure);
    } catch (...) {
      FAIL("Expected std::system_error, but got a different exception type");
    }

    const auto decrypted_albert = res.content_as<profile, couchbase::crypto::default_transcoder>();
    REQUIRE(decrypted_albert == albert);
  }
}
