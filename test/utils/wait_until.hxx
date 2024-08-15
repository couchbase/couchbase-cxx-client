/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "integration_shortcuts.hxx"
#include "server_version.hxx"

#include "core/operations/management/bucket.hxx"
#include "core/operations/management/collections.hxx"
#include "core/operations/management/user.hxx"

#include <thread>

namespace couchbase::core
{
class cluster;
} // namespace couchbase::core

namespace test::utils
{
template<class ConditionChecker>
auto
wait_until(ConditionChecker&& condition_checker,
           std::chrono::milliseconds timeout,
           std::chrono::milliseconds delay) -> bool
{
  const auto start = std::chrono::high_resolution_clock::now();
  while (!condition_checker()) {
    if (std::chrono::high_resolution_clock::now() > start + timeout) {
      return false;
    }
    std::this_thread::sleep_for(delay);
  }
  return true;
}

template<class ConditionChecker>
auto
wait_until(ConditionChecker&& condition_checker, std::chrono::milliseconds timeout) -> bool
{
  return wait_until(condition_checker, timeout, std::chrono::milliseconds(100));
}

template<class ConditionChecker>
auto
wait_until(ConditionChecker&& condition_checker) -> bool
{
  return wait_until(condition_checker, std::chrono::minutes(1));
}

auto
wait_until_bucket_healthy(const couchbase::core::cluster& cluster,
                          const std::string& bucket_name) -> bool;

auto
wait_until_collection_manifest_propagated(const couchbase::core::cluster& cluster,
                                          const std::string& bucket_name,
                                          std::uint64_t current_manifest_uid,
                                          std::size_t successful_rounds = 7,
                                          std::chrono::seconds total_timeout = std::chrono::minutes{
                                            5 }) -> bool;
auto
wait_until_user_present(const couchbase::core::cluster& cluster,
                        const std::string& username) -> bool;

auto
wait_until_cluster_connected(const std::string& username,
                             const std::string& password,
                             const std::string& connection_string) -> bool;

auto
wait_for_search_pindexes_ready(const couchbase::core::cluster& cluster,
                               const std::string& bucket_name,
                               const std::string& index_name) -> bool;

auto
wait_for_function_created(const couchbase::core::cluster& cluster,
                          const std::string& function_name,
                          const std::optional<std::string>& bucket_name = {},
                          const std::optional<std::string>& scope_name = {},
                          std::size_t successful_rounds = 4,
                          std::chrono::seconds total_timeout = std::chrono::seconds{ 120 }) -> bool;

auto
wait_until_indexed(const couchbase::core::cluster& cluster,
                   const std::string& index_name,
                   std::uint64_t expected_count) -> bool;

auto
create_primary_index(const couchbase::core::cluster& cluster,
                     const std::string& bucket_name) -> bool;

class integration_test_guard;
/**
 *
 * @param integration cluster object
 * @param bucket_name name of the bucket
 * @param index_name name of the search index
 * @param index_params_file_name the filename with index parameters in JSON format
 * @param expected_number_of_documents_indexed consider job done when this number of the document
 * has been indexed
 * @return pair of boolean value (success if true), and name of the index created (service might
 * rename index)
 */
auto
create_search_index(integration_test_guard& integration,
                    const std::string& bucket_name,
                    const std::string& index_name,
                    const std::string& index_params_file_name,
                    std::size_t expected_number_of_documents_indexed = 800)
  -> std::pair<bool, std::string>;

auto
drop_search_index(integration_test_guard& integration, const std::string& index_name) -> bool;

class collection_guard
{
public:
  explicit collection_guard(test::utils::integration_test_guard& integration);
  ~collection_guard();

  collection_guard(const collection_guard&) = delete;
  collection_guard(collection_guard&&) noexcept = delete;
  auto operator=(const collection_guard&) -> collection_guard& = delete;
  auto operator=(collection_guard&&) -> collection_guard& = delete;

  [[nodiscard]] auto scope_name() const -> const std::string&;
  [[nodiscard]] auto collection_name() const -> const std::string&;

private:
  test::utils::integration_test_guard& integration_;
  std::string scope_name_;
  std::string collection_name_;
};

} // namespace test::utils
