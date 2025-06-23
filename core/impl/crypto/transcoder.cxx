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

#include <couchbase/crypto/transcoder.hxx>

#include "core/utils/json.hxx"

#include <spdlog/fmt/bundled/format.h>
#include <spdlog/fmt/bundled/ranges.h>
#include <tao/json/value.hpp>

#include "core/logger/logger.hxx"

namespace couchbase::crypto::internal
{
namespace
{
auto
decrypt_top_level_object_fields(tao::json::value& object,
                                const std::shared_ptr<manager>& crypto_manager) -> error
{
  std::vector<std::string> encrypted_keys{};

  for (const auto& [k, v] : object.get_object()) {
    if (crypto_manager->is_mangled(k)) {
      if (!v.is_object()) {
        return error{ errc::field_level_encryption::invalid_ciphertext,
                      "Expected an object for encrypted field" };
      }

      std::map<std::string, std::string> encrypted_node;
      for (const auto& [node_k, node_v] : v.get_object()) {
        encrypted_node[node_k] = node_v.get_string();
      }

      auto [err, decrypted] = crypto_manager->decrypt(encrypted_node);
      if (err) {
        return std::move(err);
      }

      object[crypto_manager->demangle(k)] = core::utils::json::parse_binary(decrypted);
      encrypted_keys.push_back(k);
    }
  }

  for (const auto& k : encrypted_keys) {
    object.get_object().erase(k);
  }
  return {};
}

auto
decrypt_json_value(tao::json::value& value, const std::shared_ptr<manager>& crypto_manager) -> error
{
  if (value.is_object()) {
    if (auto err = decrypt_top_level_object_fields(value, crypto_manager)) {
      return err;
    }
    for (auto& [k, v] : value.get_object()) {
      if (auto err = decrypt_json_value(v, crypto_manager)) {
        return err;
      }
    }
  } else if (value.is_array()) {
    for (auto& item : value.get_array()) {
      if (auto err = decrypt_json_value(item, crypto_manager)) {
        return err;
      }
    }
  }
  return {};
}
} // namespace

auto
encrypt(const codec::binary& raw,
        const std::vector<encrypted_field>& encrypted_fields,
        const std::shared_ptr<manager>& crypto_manager) -> std::pair<error, codec::binary>
{
  auto ordered_encrypted_fields = encrypted_fields;
  std::sort(ordered_encrypted_fields.begin(),
            ordered_encrypted_fields.end(),
            [](const encrypted_field& a, const encrypted_field& b) {
              return a.field_path.size() > b.field_path.size();
            });

  auto document = core::utils::json::parse_binary(raw);
  if (!document.is_object()) {
    return {
      error{ errc::field_level_encryption::encryption_failure,
             "Failed to parse document for encryption: not a JSON object" },
      {},
    };
  }

  for (const auto& [path, alias] : ordered_encrypted_fields) {
    if (path.empty()) {
      return { error{
                 errc::field_level_encryption::encryption_failure,
                 fmt::format("Empty path is not allowed for encryption"),
               },
               {} };
    }

    tao::json::value* blob = &document;

    for (std::size_t i = 0; i < path.size() - 1; ++i) {
      blob = blob->find(path.at(i));
      if (blob == nullptr) {
        return { error{
                   errc::field_level_encryption::encryption_failure,
                   fmt::format("Failed to find path '{}' in document for encryption",
                               fmt::join(path, ".")),
                 },
                 {} };
      }
      if (!blob->is_object()) {
        return { error{
                   errc::field_level_encryption::encryption_failure,
                   fmt::format(
                     "Path '{}' in document for encryption points to {} instead of an object",
                     fmt::join(path, "."),
                     tao::json::to_string(blob->type())),
                 },
                 {} };
      }
    }
    auto current_field_key = path.at(path.size() - 1);
    if (blob->find(current_field_key) == nullptr) {
      return { error{
                 errc::field_level_encryption::encryption_failure,
                 fmt::format("Failed to find path '{}' in document for encryption",
                             fmt::join(path, ".")),
               },
               {} };
    }
    const auto& current_field_value = blob->at(current_field_key);
    auto [err, encrypted] =
      crypto_manager->encrypt(core::utils::json::generate_binary(current_field_value), alias);
    if (err) {
      return { err, {} };
    }
    blob->erase(current_field_key);

    (*blob)[crypto_manager->mangle(current_field_key)] = tao::json::empty_object;
    for (const auto& [k, v] : encrypted) {
      (*blob)[crypto_manager->mangle(current_field_key)][k] = v;
    }
  }
  return { {}, core::utils::json::generate_binary(document) };
}

auto
decrypt(const codec::binary& encrypted, const std::shared_ptr<manager>& crypto_manager)
  -> std::pair<error, codec::binary>
{
  auto document = core::utils::json::parse_binary(encrypted);
  if (auto err = decrypt_json_value(document, crypto_manager)) {
    return { err, {} };
  }
  return { {}, core::utils::json::generate_binary(document) };
}
} // namespace couchbase::crypto::internal
