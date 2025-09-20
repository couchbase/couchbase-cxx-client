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

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace couchbase::core::topology
{
struct configuration;
} // namespace couchbase::core::topology

namespace cbc
{

static constexpr std::uint16_t default_number_of_vbuckets{ 1024 };

class key_value_node
{
public:
  key_value_node(std::size_t index, std::string endpoint)
    : index_{ index }
    , endpoint_{ std::move(endpoint) }
  {
  }

  [[nodiscard]] auto vbuckets(const std::string& type) const -> const std::set<std::uint16_t>&
  {
    if (type == "active") {
      return active_;
    }
    if (type == "replica_1") {
      return replica_1_;
    }
    if (type == "replica_2") {
      return replica_2_;
    }
    if (type == "replica_3") {
      return replica_3_;
    }

    static std::set<std::uint16_t> empty_set{};
    return empty_set;
  }

  [[nodiscard]] auto has_vbuckets(const std::string& type) const -> bool
  {
    return vbuckets(type).empty();
  }

  void add_active(std::uint16_t vbucket)
  {
    active_.insert(vbucket);
  }

  void add_replica_1(std::uint16_t vbucket)
  {
    replica_1_.insert(vbucket);
  }

  void add_replica_2(std::uint16_t vbucket)
  {
    replica_2_.insert(vbucket);
  }

  void add_replica_3(std::uint16_t vbucket)
  {
    replica_3_.insert(vbucket);
  }

  [[nodiscard]] auto index() const -> std::size_t
  {
    return index_;
  }

private:
  std::size_t index_;
  std::string endpoint_;
  std::set<std::uint16_t> active_{};
  std::set<std::uint16_t> replica_1_{};
  std::set<std::uint16_t> replica_2_{};
  std::set<std::uint16_t> replica_3_{};
};

auto
extract_vbucket_map(const couchbase::core::topology::configuration& config)
  -> std::map<std::string, key_value_node>;

struct key_generator_options {
  std::string prefix{};
  bool randomize{ false };
  std::uint16_t number_of_vbuckets{ default_number_of_vbuckets };
  std::map<std::string, key_value_node> vbuckets_by_node{};
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
   * @param skip_duplicates do not include duplicates
   * @return generated keys
   */
  auto next_keys(std::size_t count, bool skip_duplicates = false) -> std::vector<std::string>;

  /**
   * Generates a key for given vBucket
   *
   * @param vbucket the vBucket ID
   * @return generated key
   */
  auto next_key_for_vbucket(std::uint16_t vbucket) -> std::string;

  /**
   * Generates a number of keys for given vBucket
   *
   * @param count number of keys to generate
   * @param vbucket ID of vBucket
   * @param skip_duplicates do not include duplicates
   * @return generated keys
   */
  auto next_keys_for_vbucket(std::size_t count, std::uint16_t vbucket, bool skip_duplicates = false)
    -> std::vector<std::string>;

  /**
   * Generates a key that maps to one of the vbuckets in the given set.
   *
   * @param vbuckets the set of the vBucket IDs
   * @return generated key
   */
  auto next_key_for_vbucket_set(const std::set<std::uint16_t>& vbuckets) -> std::string;

  /**
   * Generates a number of keys that maps to one of the vbuckets in the given set.
   *
   * @param count number of keys to generate
   * @param vbuckets the set of the vBucket IDs
   * @param skip_duplicates do not include duplicates
   * @return generated keys
   */
  auto next_keys_for_vbucket_set(std::size_t count,
                                 const std::set<std::uint16_t>& vbuckets,
                                 bool skip_duplicates = false) -> std::vector<std::string>;

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
  auto next_keys_for_parent(std::size_t count,
                            const std::string& parent_key,
                            bool skip_duplicates = false) -> std::vector<std::string>;

  /**
   * Generates a key that are mapped to any of vBUckets that mapped to the given node
   *
   * @param node the node with list of the vbuckets
   * @param type level of the vbucket: active or one of the replicas
   * @return generated key
   */
  auto next_key_for_node(const key_value_node& node, const std::string& type) -> std::string;

  /**
   * Generates a number of keys that are mapped to any of vBUckets that mapped to the given node
   *
   * @param count number of keys to generate
   * @param node the node with list of the vbuckets
   * @param type level of the vbucket: active or one of the replicas
   * @param skip_duplicates do not include duplicates
   * @return generated keys
   */
  auto next_keys_for_node(std::size_t count,
                          const key_value_node& node,
                          const std::string& type,
                          bool skip_duplicates = false) -> std::vector<std::string>;

private:
  std::string prefix_;
  std::size_t number_of_vbuckets_;
  std::vector<char> alphabet_;
  std::size_t fixed_length_;
  std::string state_{};
};

} // namespace cbc
