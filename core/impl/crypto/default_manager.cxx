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

#include <couchbase/crypto/default_manager.hxx>
#include <couchbase/error_codes.hxx>

#include <spdlog/fmt/bundled/format.h>

namespace couchbase::crypto
{
default_manager::default_manager(std::string encrypted_field_name_prefix)
  : encrypted_field_name_prefix_{ std::move(encrypted_field_name_prefix) }
{
}

auto
default_manager::register_encrypter(std::string alias, std::shared_ptr<encrypter> encrypter)
  -> error
{
  alias_to_encrypter_[std::move(alias)] = std::move(encrypter);
  return {};
}

auto
default_manager::register_decrypter(std::shared_ptr<decrypter> decrypter) -> error
{
  algorithm_to_decrypter_[decrypter->algorithm()] = std::move(decrypter);
  return {};
}

auto
default_manager::register_default_encrypter(std::shared_ptr<encrypter> encrypter) -> error
{
  return register_encrypter(default_encrypter_alias, std::move(encrypter));
}

auto
default_manager::encrypt(std::vector<std::byte> plaintext,
                         const std::optional<std::string>& encrypter_alias)
  -> std::pair<error, std::map<std::string, std::string>>
{
  const auto alias = encrypter_alias.value_or(default_encrypter_alias);
  if (alias_to_encrypter_.find(alias) == alias_to_encrypter_.end()) {
    return { error{ errc::field_level_encryption::encrypter_not_found,
                    fmt::format("Could not find encrypter with alias `{}`.", alias) },
             {} };
  }

  auto [err, res] = alias_to_encrypter_.at(alias)->encrypt(std::move(plaintext));
  if (err) {
    return { err, {} };
  }
  return { {}, res.as_map() };
}

auto
default_manager::decrypt(std::map<std::string, std::string> encrypted_node)
  -> std::pair<error, std::vector<std::byte>>
{
  auto enc_result = encryption_result{ std::move(encrypted_node) };
  if (algorithm_to_decrypter_.find(enc_result.algorithm()) == algorithm_to_decrypter_.end()) {
    return { error{ errc::field_level_encryption::decrypter_not_found,
                    fmt::format("Could not find decrypter for algorithm `{}`.",
                                enc_result.algorithm()) },
             {} };
  }
  const auto decrypter = algorithm_to_decrypter_.at(enc_result.algorithm());
  return decrypter->decrypt(std::move(enc_result));
}

auto
default_manager::mangle(std::string field_name) -> std::string
{
  return encrypted_field_name_prefix_ + std::move(field_name);
}

auto
default_manager::demangle(std::string field_name) -> std::string
{
  if (!is_mangled(field_name)) {
    // TODO(DC): Error?
  }
  return field_name.substr(encrypted_field_name_prefix_.size());
}

auto
default_manager::is_mangled(const std::string& field_name) -> bool
{
  return field_name.compare(0, encrypted_field_name_prefix_.size(), encrypted_field_name_prefix_) ==
         0;
}
} // namespace couchbase::crypto
