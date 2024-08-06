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
#include "test_context.hxx"

#include "core/management/bucket_settings.hxx"
#include "core/operations/management/bucket_describe.hxx"
#include "core/operations/management/cluster_describe.hxx"

#include <couchbase/cluster.hxx>

#include <optional>
#include <string>
#include <vector>

namespace test::utils
{

struct pools_response {
  bool is_developer_preview{ false };
  server_config_profile config_profile{ server_config_profile::unknown };
};

class integration_test_guard
{
public:
  integration_test_guard();
  integration_test_guard(const couchbase::core::cluster_options& opts);
  ~integration_test_guard();

  inline auto load_bucket_info(bool refresh = false)
    -> const couchbase::core::operations::management::bucket_describe_response::bucket_info&
  {
    return load_bucket_info(ctx.bucket, refresh);
  }

  auto load_bucket_info(const std::string& bucket_name, bool refresh = false)
    -> const couchbase::core::operations::management::bucket_describe_response::bucket_info&;

  inline auto number_of_nodes() -> std::size_t
  {
    return load_bucket_info(ctx.bucket).number_of_nodes;
  }

  auto server_groups() -> std::vector<std::string>;
  auto generate_key_not_in_server_group(const std::string& group_name) -> std::string;

  auto number_of_nodes(const std::string& bucket_name) -> std::size_t
  {
    return load_bucket_info(bucket_name).number_of_nodes;
  }

  inline auto number_of_replicas() -> std::size_t
  {
    return load_bucket_info(ctx.bucket).number_of_replicas;
  }

  auto number_of_replicas(const std::string& bucket_name) -> std::size_t
  {
    return load_bucket_info(bucket_name).number_of_replicas;
  }

  inline auto storage_backend() -> couchbase::core::management::cluster::bucket_storage_backend
  {
    return load_bucket_info(ctx.bucket).storage_backend;
  }

  inline auto has_bucket_capability(const std::string& bucket_name,
                                    const std::string& capability) -> bool
  {
    return load_bucket_info(bucket_name).has_capability(capability);
  }

  inline auto has_bucket_capability(const std::string& capability) -> bool
  {
    return has_bucket_capability(ctx.bucket, capability);
  }

  auto load_cluster_info(bool refresh = false)
    -> const couchbase::core::operations::management::cluster_describe_response::cluster_info&;

  auto load_pools_info(bool refresh = false) -> pools_response;

  inline auto has_service(couchbase::core::service_type service) -> bool
  {
    return load_cluster_info().services.count(service) > 0;
  }

  inline auto has_eventing_service() -> bool
  {
    return has_service(couchbase::core::service_type::eventing);
  }

  inline auto has_analytics_service() -> bool
  {
    return has_service(couchbase::core::service_type::analytics);
  }

  auto number_of_nodes_with_service(std::string type) -> std::size_t;

  auto number_of_query_nodes() -> std::size_t
  {
    return number_of_nodes_with_service("n1ql");
  }

  auto number_of_analytics_nodes() -> std::size_t
  {
    return number_of_nodes_with_service("cbas");
  }

  [[nodiscard]] auto transactions() const
    -> std::shared_ptr<couchbase::core::transactions::transactions>;

  [[nodiscard]] auto public_cluster() const -> couchbase::cluster;

  auto cluster_version() -> server_version;

  test_context ctx;
  asio::io_context io;
  std::vector<std::thread> io_threads;
  couchbase::core::cluster cluster;
  couchbase::core::origin origin;

  std::map<std::string,
           couchbase::core::operations::management::bucket_describe_response::bucket_info,
           std::less<>>
    info{};
  std::optional<couchbase::core::operations::management::cluster_describe_response::cluster_info>
    cluster_info{};
  std::optional<pools_response> pools_info{};
};
} // namespace test::utils
