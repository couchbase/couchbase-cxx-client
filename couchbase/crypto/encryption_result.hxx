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

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::crypto
{
class encryption_result
{
public:
  encryption_result() = default;
  explicit encryption_result(std::string algorithm);
  explicit encryption_result(std::map<std::string, std::string> encrypted_node);

  [[nodiscard]] auto algorithm() const -> std::string;
  [[nodiscard]] auto get(const std::string& field_name) const -> std::optional<std::string>;
  [[nodiscard]] auto get_bytes(const std::string& field_name) const
    -> std::optional<std::vector<std::byte>>;
  [[nodiscard]] auto as_map() const -> std::map<std::string, std::string>;

  void put(std::string field_name, std::string value);
  void put(std::string field_name, std::vector<std::byte> value);

private:
  std::map<std::string, std::string> internal_;
};
} // namespace couchbase::crypto
