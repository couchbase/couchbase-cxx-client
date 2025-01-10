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

#include "wait_until.hxx"

#include "integration_test_guard.hxx"
#include "test_data.hxx"

#include "core/logger/logger.hxx"
#include "core/management/eventing_function.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "core/operations/management/collections_manifest_get.hxx"
#include "core/operations/management/eventing_get_function.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/search_get_stats.hxx"
#include "core/operations/management/search_index_drop.hxx"
#include "core/operations/management/search_index_get_documents_count.hxx"
#include "core/operations/management/search_index_upsert.hxx"
#include "core/topology/collections_manifest_fmt.hxx"
#include "core/utils/json.hxx"

#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json/value.hpp>

namespace test::utils
{
auto
wait_until_bucket_healthy(const couchbase::core::cluster& cluster,
                          const std::string& bucket_name) -> bool
{
  return wait_until([cluster, bucket_name]() {
    couchbase::core::operations::management::bucket_get_request req{ bucket_name };
    auto resp = execute(cluster, req);
    if (resp.ctx.ec) {
      return false;
    }
    if (resp.bucket.nodes.empty()) {
      return false;
    }
    for (const auto& node : resp.bucket.nodes) {
      if (node.status != "healthy") {
        return false;
      }
    }
    return true;
  });
}

auto
wait_until_collection_manifest_propagated(const couchbase::core::cluster& cluster,
                                          const std::string& bucket_name,
                                          std::uint64_t current_manifest_uid,
                                          std::size_t successful_rounds,
                                          std::chrono::seconds total_timeout) -> bool
{
  std::size_t round = 0;
  auto deadline = std::chrono::system_clock::now() + total_timeout;
  while (std::chrono::system_clock::now() < deadline) {
    auto propagated =
      wait_until([&cluster, bucket_name, current_manifest_uid, round, successful_rounds]() {
        couchbase::core::operations::management::collections_manifest_get_request req{
          { bucket_name, "_default", "_default", "" }
        };
        auto resp = execute(cluster, req);
        CB_LOG_INFO("wait_until_collection_manifest_propagated \"{}\", expected: {}, actual: {}, "
                    "round: {} ({}), manifest: {}",
                    bucket_name,
                    current_manifest_uid,
                    resp.manifest.uid,
                    round,
                    successful_rounds,
                    resp.manifest);
        return resp.manifest.uid >= current_manifest_uid;
      });
    if (propagated) {
      round += 1;
      if (round >= successful_rounds) {
        std::this_thread::sleep_for(std::chrono::seconds{ 1 });
        return propagated;
      }
    } else {
      round = 0;
    }
  }
  return false;
}

auto
wait_until_user_present(const couchbase::core::cluster& cluster,
                        const std::string& username) -> bool
{
  auto present = wait_until([cluster, username]() {
    couchbase::core::operations::management::user_get_request req{};
    req.username = username;
    auto resp = execute(cluster, req);
    return resp.ctx.ec == couchbase::errc::management::user_exists ||
           (!resp.ctx.ec && resp.user.username == username);
  });
  if (present) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return present;
}

auto
wait_until_cluster_connected(const std::string& username,
                             const std::string& password,
                             const std::string& connection_string) -> bool
{
  auto cluster_options = couchbase::cluster_options(username, password);

  auto connected = wait_until([cluster_options, connection_string]() {
    auto [err, _] = couchbase::cluster::connect(connection_string, cluster_options).get();
    return err.ec().value() == 0;
  });
  if (connected) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return connected;
}

static auto
to_string(std::optional<std::uint64_t> value) -> std::string
{
  if (value) {
    return std::to_string(*value);
  }
  return "(empty)";
}

static auto
refresh_config_on_search_service(const couchbase::core::cluster& cluster) -> bool
{
  const couchbase::core::operations::management::freeform_request req{
    couchbase::core::service_type::search,
    "POST",
    "/api/cfgRefresh",
    {
      { "content-type", "application/json" },
    },
  };
  auto resp = execute(cluster, req);
  return !resp.ctx.ec;
}

/**
 * Forces the node to replan resource assignments (by running the planner, if enabled)
 * and to update its runtime state to reflect the latest plan (by running the janitor,
 * if enabled).
 */
static auto
kick_manager_manager_on_search_service(const couchbase::core::cluster& cluster) -> bool
{
  const couchbase::core::operations::management::freeform_request req{
    couchbase::core::service_type::search,
    "POST",
    "/api/managerKick",
    {
      { "content-type", "application/json" },
    },
  };
  auto resp = execute(cluster, req);
  return !resp.ctx.ec;
}

static auto
starts_with(const std::string& str, const std::string& prefix) -> bool
{
  if (str.length() < prefix.length()) {
    return false;
  }
  return str.compare(0, prefix.length(), prefix) == 0;
}

static auto
ends_with(const std::string& str, const std::string& suffix) -> bool
{
  if (str.length() < suffix.length()) {
    return false;
  }
  return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
}

auto
wait_for_search_pindexes_ready(const couchbase::core::cluster& cluster,
                               const std::string& bucket_name,
                               const std::string& index_name) -> bool
{
  return wait_until(
    [&]() {
      if (!refresh_config_on_search_service(cluster)) {
        return false;
      }

      couchbase::core::operations::management::search_get_stats_request req{};
      auto resp = execute(cluster, req);
      if (resp.ctx.ec || resp.stats.empty()) {
        return false;
      }
      auto stats = couchbase::core::utils::json::parse(resp.stats);

      std::optional<std::uint64_t> num_pindexes_target{};
      std::optional<std::uint64_t> num_pindexes_actual{};

      auto target_suffix = fmt::format("{}:num_pindexes_target", index_name);
      auto actual_suffix = fmt::format("{}:num_pindexes_actual", index_name);
      for (const auto& [key, value] : stats.get_object()) {
        if (starts_with(key, bucket_name) && ends_with(key, target_suffix)) {
          num_pindexes_target = value.as<std::uint64_t>();
        }
        if (starts_with(key, bucket_name) && ends_with(key, actual_suffix)) {
          num_pindexes_actual = value.as<std::uint64_t>();
        }
      }

      CB_LOG_INFO("wait_for_search_pindexes_ready for \"{}\", target: {}, actual: {}",
                  index_name,
                  to_string(num_pindexes_target),
                  to_string(num_pindexes_actual));
      if (!num_pindexes_actual.has_value() || !num_pindexes_target.has_value()) {
        kick_manager_manager_on_search_service(cluster);
        return false;
      }

      if (num_pindexes_target.value() == 0) {
        kick_manager_manager_on_search_service(cluster);
        return false;
      }
      return num_pindexes_actual.value() == num_pindexes_target.value();
    },
    std::chrono::minutes(5),
    std::chrono::seconds{ 1 });
}

auto
wait_until_indexed(const couchbase::core::cluster& cluster,
                   const std::string& index_name,
                   std::uint64_t expected_count) -> bool
{
  return wait_until(
    [cluster, &index_name, &expected_count]() {
      if (!refresh_config_on_search_service(cluster)) {
        return false;
      }

      couchbase::core::operations::management::search_index_get_documents_count_request req{};
      req.index_name = index_name;
      req.timeout = std::chrono::seconds{ 1 };
      auto resp = execute(cluster, req);
      CB_LOG_INFO("wait_until_indexed for \"{}\", expected: {}, actual: {}",
                  index_name,
                  expected_count,
                  resp.count);
      return resp.count >= expected_count;
    },
    std::chrono::minutes(10),
    std::chrono::seconds{ 5 });
}

auto
create_primary_index(const couchbase::core::cluster& cluster,
                     const std::string& bucket_name) -> bool
{
  couchbase::core::operations::management::query_index_create_response resp;
  bool operation_completed = wait_until([&cluster, &bucket_name, &resp]() {
    couchbase::core::operations::management::query_index_create_request req{};
    req.bucket_name = bucket_name;
    req.ignore_if_exists = true;
    req.is_primary = true;
    resp = execute(cluster, req);
    if (resp.ctx.ec) {
      CB_LOG_INFO("create_primary_index for \"{}\", rc: {}, body:\n{}",
                  bucket_name,
                  resp.ctx.ec.message(),
                  resp.ctx.http_body);
    }
    return resp.ctx.ec != couchbase::errc::common::bucket_not_found &&
           resp.ctx.ec != couchbase::errc::common::scope_not_found;
  });
  if (resp.ctx.ec) {
    CB_LOG_ERROR("failed to create primary index for \"{}\", rc: {}, body:\n{}",
                 bucket_name,
                 resp.ctx.ec.message(),
                 resp.ctx.http_body);
    return false;
  }
  return operation_completed;
}

auto
create_search_index(integration_test_guard& integration,
                    const std::string& bucket_name,
                    const std::string& index_name,
                    const std::string& index_params_file_name,
                    std::size_t expected_number_of_documents_indexed)
  -> std::pair<bool, std::string>
{
  auto params = read_test_data(index_params_file_name);

  couchbase::core::operations::management::search_index_upsert_response resp{};

  bool operation_completed = wait_until([&integration, bucket_name, index_name, params, &resp]() {
    couchbase::core::management::search::index index{};
    index.name = index_name;
    index.params_json = params;
    index.type = "fulltext-index";
    index.source_name = bucket_name;
    index.source_type = "couchbase";
    if (integration.cluster_version().requires_search_replicas()) {
      index.plan_params_json = couchbase::core::utils::json::generate({
        { "indexPartitions", 1 },
        { "numReplicas", 1 },
      });
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;
    resp = execute(integration.cluster, req);

    if (resp.ctx.ec) {
      CB_LOG_INFO("create_search_index bucket: \"{}\", index_name: \"{}\", rc: {}, body:\n{}",
                  bucket_name,
                  index_name,
                  resp.ctx.ec.message(),
                  resp.ctx.http_body);
    } else {
      if (index_name != resp.name) {
        CB_LOG_INFO("update index name \"{}\" -> \"{}\"", index_name, resp.name);
      }
    }
    return !resp.ctx.ec || resp.ctx.ec == couchbase::errc::common::index_exists;
  });

  CB_LOG_INFO("completed: {}, index_name \"{}\" -> \"{}\", ec: {}",
              operation_completed,
              index_name,
              resp.name,
              resp.ctx.ec.message());
  if (!operation_completed) {
    return { false, "" };
  }

  std::string actual_index_name{ index_name };
  if (!resp.ctx.ec) {
    actual_index_name = resp.name;
  }

  operation_completed =
    wait_until_indexed(integration.cluster, index_name, expected_number_of_documents_indexed);

  return { operation_completed, actual_index_name };
}

auto
wait_for_function_created(const couchbase::core::cluster& cluster,
                          const std::string& function_name,
                          const std::optional<std::string>& bucket_name,
                          const std::optional<std::string>& scope_name,
                          std::size_t successful_rounds,
                          std::chrono::seconds total_timeout) -> bool
{
  std::size_t round = 0;
  auto deadline = std::chrono::system_clock::now() + total_timeout;

  couchbase::core::operations::management::eventing_get_function_response resp{};
  while (std::chrono::system_clock::now() < deadline) {
    auto exists = wait_until([&cluster, &resp, function_name, bucket_name, scope_name]() {
      couchbase::core::operations::management::eventing_get_function_request req{ function_name,
                                                                                  bucket_name,
                                                                                  scope_name };
      resp = execute(cluster, req);
      if (resp.ctx.ec) {
        return false;
      }

      // The function scope sometimes takes longer to be set correctly (especially for the admin
      // scope).
      if (bucket_name.has_value() && scope_name.has_value()) {
        return resp.function.internal.bucket_name.has_value() &&
               resp.function.internal.scope_name.has_value() &&
               resp.function.internal.bucket_name.value() == bucket_name.value() &&
               resp.function.internal.scope_name.value() == scope_name.value();
      }
      return (!resp.function.internal.bucket_name.has_value() &&
              !resp.function.internal.scope_name.has_value()) ||
             (resp.function.internal.bucket_name.has_value() &&
              resp.function.internal.scope_name.has_value() &&
              resp.function.internal.bucket_name.value() == "*" &&
              resp.function.internal.scope_name.value() == "*");
    });
    if (exists) {
      round += 1;
      if (round >= successful_rounds) {
        std::this_thread::sleep_for(std::chrono::seconds{ 1 });
        return exists;
      }
    } else {
      round = 0;
    }
  }
  return false;
}

auto
drop_search_index(integration_test_guard& integration, const std::string& index_name) -> bool
{
  couchbase::core::operations::management::search_index_drop_request req{};
  req.index_name = index_name;
  auto resp = execute(integration.cluster, req);
  return !resp.ctx.ec;
}

collection_guard::collection_guard(integration_test_guard& integration)
  : integration_{ integration }
  , scope_name_{ couchbase::scope::default_name }
  , collection_name_{ uniq_id("collection") }
{
  auto resp = execute(integration_.cluster,
                      couchbase::core::operations::management::collection_create_request{
                        integration_.ctx.bucket,
                        scope_name_,
                        collection_name_,
                      });
  if (resp.ctx.ec) {
    throw std::runtime_error(
      fmt::format("unable to create collection {}: {}", collection_name_, resp.ctx.ec.message()));
  }
  auto propagated = wait_until_collection_manifest_propagated(
    integration_.cluster, integration_.ctx.bucket, resp.uid);
  if (!propagated) {
    throw std::runtime_error(
      fmt::format("unable to wait for collection manifest to reach UID {}", resp.uid));
  }
}

collection_guard::~collection_guard()
{
  auto resp = execute(integration_.cluster,
                      couchbase::core::operations::management::collection_drop_request{
                        integration_.ctx.bucket,
                        couchbase::scope::default_name,
                        collection_name_,
                      });
  if (resp.ctx.ec) {
    fmt::println(
      stderr, "unable to drop collection {}: {}", collection_name_, resp.ctx.ec.message());
    return;
  }
  auto propagated = wait_until_collection_manifest_propagated(
    integration_.cluster, integration_.ctx.bucket, resp.uid);
  if (!propagated) {
    fmt::println(stderr, "unable to wait for collection manifest to reach UID {}", resp.uid);
  }
}

auto
collection_guard::scope_name() const -> const std::string&
{
  return scope_name_;
}

auto
collection_guard::collection_name() const -> const std::string&
{
  return collection_name_;
}
} // namespace test::utils
