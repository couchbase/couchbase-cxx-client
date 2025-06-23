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

#include <couchbase/crypto/manager.hxx>
#include <couchbase/error.hxx>

namespace couchbase::crypto
{
class default_manager : public manager
{
public:
  static const inline std::string default_encrypter_alias{ "__DEFAULT__" };
  static const inline std::string default_encrypted_field_name_prefix{ "encrypted$" };

  explicit default_manager(
    std::string encrypted_field_name_prefix = default_encrypted_field_name_prefix);

  auto register_encrypter(std::string alias, std::shared_ptr<encrypter> encrypter) -> error;
  auto register_decrypter(std::shared_ptr<decrypter> decrypter) -> error;
  auto register_default_encrypter(std::shared_ptr<encrypter> encrypter) -> error;

  auto encrypt(std::vector<std::byte> plaintext, const std::optional<std::string>& encrypter_alias)
    -> std::pair<error, std::map<std::string, std::string>> override;
  auto decrypt(std::map<std::string, std::string> encrypted_node)
    -> std::pair<error, std::vector<std::byte>> override;
  auto mangle(std::string field_name) -> std::string override;
  auto demangle(std::string field_name) -> std::string override;
  auto is_mangled(const std::string& field_name) -> bool override;

private:
  std::string encrypted_field_name_prefix_;
  std::map<std::string, std::shared_ptr<encrypter>> alias_to_encrypter_{};
  std::map<std::string, std::shared_ptr<decrypter>> algorithm_to_decrypter_{};
};
} // namespace couchbase::crypto
