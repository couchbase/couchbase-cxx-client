/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include <cstdint>
#include <string>
#include <vector>

namespace cbc
{

static constexpr std::uint16_t default_number_of_vbuckets{ 1024 };

struct key_generator_options {
  std::string prefix{};
  bool randomize{ false };
  std::uint16_t number_of_vbuckets{ default_number_of_vbuckets };
  std::size_t fixed_length{ 16 };
};

class key_generator
{
public:
  explicit key_generator(const key_generator_options& options = {});

  /**
   * Generates a key
   *
   * @return generated key
   */
  auto next_key() -> std::string;

  /**
   * Generates a given number of keys
   *
   * @param count number of keys to generate
   * @return generated keys
   */
  auto next_keys(std::size_t count) -> std::vector<std::string>;

  /**
   * Generates a key for given vBucket
   *
   * @return generated key
   */
  auto next_key_for_vbucket(std::uint16_t vbucket) -> std::string;

  /**
   * Generates a number of keys for given vBucket
   *
   * @param count number of keys to generate
   * @param vbucket ID of vBucket
   * @return generated keys
   */
  auto next_keys_for_vbucket(std::size_t count, std::uint16_t vbucket) -> std::vector<std::string>;

  /**
   * Generates a key that is mapped to the same vBucket as given key.
   *
   * @param parent_key parent key
   * @return generated key
   */
  auto next_key_for_parent(const std::string& parent_key) -> std::string;

  /**
   * Generates a number of keys that are mapped to the same vBucket as given key.
   *
   * @param count number of keys to generate
   * @param parent_key parent key
   * @return generated keys
   */
  auto next_keys_for_parent(std::size_t count, const std::string& parent_key)
    -> std::vector<std::string>;

private:
  std::string prefix_;
  std::size_t number_of_vbuckets_;
  std::vector<char> alphabet_;
  std::size_t fixed_length_;
  std::string state_{};
};

} // namespace cbc
