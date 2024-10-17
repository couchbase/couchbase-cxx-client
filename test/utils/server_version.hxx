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

#include <cstdlib>
#include <string>

namespace test::utils
{

enum class server_edition {
  unknown,
  enterprise,
  community,
  columnar
};

enum class deployment_type {
  on_prem,
  capella,
  elixir
};

enum class server_config_profile {
  unknown,
  serverless
};

struct server_version {
  unsigned long major{ 0 };
  unsigned long minor{ 0 };
  unsigned long micro{ 0 };
  unsigned long build{ 0 };
  bool developer_preview{ false };
  server_edition edition{ server_edition::unknown };
  deployment_type deployment{ deployment_type::on_prem };
  server_config_profile profile{ server_config_profile::unknown };
  bool use_gocaves{ false };

  static auto parse(const std::string& str, deployment_type deployment) -> server_version;

  [[nodiscard]] auto is_mock() const -> bool
  {
    return use_gocaves;
  }

  [[nodiscard]] auto is_alice() const -> bool
  {
    // [6.0.0, 6.5.0)
    return major == 6 && minor < 5;
  }

  [[nodiscard]] auto is_mad_hatter() const -> bool
  {
    // [6.5.0, 7.0.0)
    return major == 6 && minor >= 5;
  }

  [[nodiscard]] auto is_cheshire_cat() const -> bool
  {
    // [7.0.0, 7.1.0)
    return major == 7 && minor < 1;
  }

  [[nodiscard]] auto is_neo() const -> bool
  {
    // [7.1.0, inf)
    return (major == 7 && minor >= 1) || major > 7;
  }

  [[nodiscard]] auto supports_gcccp() const -> bool
  {
    return is_mad_hatter() || is_cheshire_cat() || is_neo();
  }

  [[nodiscard]] auto supports_sync_replication() const -> bool
  {
    return is_mad_hatter() || is_cheshire_cat() || is_neo();
  }

  [[nodiscard]] auto supports_enhanced_durability() const -> bool
  {
    return is_mad_hatter() || is_cheshire_cat() || is_neo();
  }

  [[nodiscard]] auto supports_scoped_queries() const -> bool
  {
    return is_cheshire_cat() || is_neo();
  }

  [[nodiscard]] auto supports_queries_in_transactions() const -> bool
  {
    return is_neo();
  }

  [[nodiscard]] auto supports_collections() const -> bool
  {
    return (is_mad_hatter() && developer_preview) || is_cheshire_cat() || is_neo();
  }

  [[nodiscard]] auto supports_storage_backend() const -> bool
  {
    return is_neo() && is_enterprise();
  }

  [[nodiscard]] auto supports_preserve_expiry() const -> bool
  {
    return !use_gocaves && (is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_preserve_expiry_for_query() const -> bool
  {
    return is_neo();
  }

  [[nodiscard]] auto supports_user_groups() const -> bool
  {
    return supports_user_management() && (is_mad_hatter() || is_cheshire_cat() || is_neo()) &&
           is_enterprise();
  }

  [[nodiscard]] auto supports_query_index_management() const -> bool
  {
    return !use_gocaves && (is_mad_hatter() || is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_analytics() const -> bool
  {
    return !use_gocaves && is_enterprise() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_query() const -> bool
  {
    return !use_gocaves;
  }

  [[nodiscard]] auto has_fixed_consistency_check_in_search_engine_MB_55920() const -> bool
  {
    return supports_search() &&
           ((major == 7 && minor == 2 && micro >= 1) || (major == 7 && minor > 2) || major > 7);
  }

  [[nodiscard]] auto supports_search() const -> bool
  {
    return !use_gocaves;
  }

  [[nodiscard]] auto supports_analytics_pending_mutations() const -> bool
  {
    return supports_analytics() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_analytics_link_azure_blob() const -> bool
  {
    return supports_analytics() && is_cheshire_cat() && developer_preview;
  }

  [[nodiscard]] auto supports_analytics_links() const -> bool
  {
    return supports_analytics() && ((major == 6 && minor >= 6) || major > 6);
  }

  [[nodiscard]] auto supports_minimum_durability_level() const -> bool
  {
    return (major == 6 && minor >= 6) || major > 6;
  }

  [[nodiscard]] auto supports_bucket_history() const -> bool
  {
    return (major == 7 && minor >= 2) || major > 7;
  }

  [[nodiscard]] auto supports_search_analyze() const -> bool
  {
    return supports_search() && (is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_analytics_links_cert_auth() const -> bool
  {
    return supports_analytics() && is_neo();
  }

  [[nodiscard]] auto supports_eventing_functions() const -> bool
  {
    return !use_gocaves && is_enterprise() && (is_cheshire_cat() || is_neo()) &&
           deployment == deployment_type::on_prem;
  }

  [[nodiscard]] auto supports_scoped_eventing_functions() const -> bool
  {
    return !use_gocaves && is_enterprise() && is_neo() && deployment == deployment_type::on_prem;
  }

  [[nodiscard]] auto supports_scope_search() const -> bool
  {
    return (major == 7 && minor >= 6) || major > 7;
  }

  [[nodiscard]] auto supports_vector_search() const -> bool
  {
    return (major == 7 && minor >= 6) || major > 7;
  }

  [[nodiscard]] auto supports_scope_search_analyze() const -> bool
  {
    // Scoped endpoint for analyze_document added in 7.6.2 (MB-60643)
    return (major == 7 && minor == 6 && micro >= 2) || (major == 7 && minor > 6) || major > 7;
  }

  [[nodiscard]] auto is_enterprise() const -> bool
  {
    return edition == server_edition::enterprise;
  }

  [[nodiscard]] auto is_community() const -> bool
  {
    return edition == server_edition::community;
  }

  [[nodiscard]] auto supports_bucket_management() const -> bool
  {
    return !use_gocaves && deployment == deployment_type::on_prem;
  }

  [[nodiscard]] auto supports_user_management() const -> bool
  {
    return !use_gocaves && deployment == deployment_type::on_prem;
  }

  [[nodiscard]] auto requires_search_replicas() const -> bool
  {
    return deployment == deployment_type::elixir || deployment == deployment_type::capella ||
           is_serverless_config_profile();
  }

  [[nodiscard]] auto supports_views() const -> bool
  {
    return !use_gocaves && deployment == deployment_type::on_prem &&
           (major < 7 || (major == 7 && minor < 2));
  }

  [[nodiscard]] auto supports_memcached_buckets() const -> bool
  {
    return !is_serverless_config_profile();
  }

  [[nodiscard]] auto is_serverless_config_profile() const -> bool
  {
    return profile == server_config_profile::serverless;
  }

  [[nodiscard]] auto supports_search_disable_scoring() const -> bool
  {
    return supports_search() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
  }

  [[nodiscard]] auto supports_document_not_locked_status() const -> bool
  {
    return !use_gocaves && (major > 7 || (major == 7 && minor >= 6));
  }

  [[nodiscard]] auto supports_collection_update_max_expiry() const -> bool
  {
    return !use_gocaves && (major > 7 || (major == 7 && minor >= 6));
  }

  [[nodiscard]] auto supports_collection_set_max_expiry_to_no_expiry() const -> bool
  {
    return !use_gocaves && (major > 7 || (major == 7 && minor >= 6));
  }

  [[nodiscard]] auto is_capella() const -> bool
  {
    return deployment == deployment_type::capella;
  }

  [[nodiscard]] auto is_columnar() const -> bool
  {
    return edition == server_edition::columnar;
  }

  [[nodiscard]] auto supports_binary_objects_in_transactions() const -> bool
  {
    return (major == 7 && minor == 6 && micro >= 2) || (major == 7 && minor > 6) || major > 7;
  }

  [[nodiscard]] auto supports_cluster_labels() const -> bool
  {
    // See MB-63870
    return (major == 7 && minor == 6 && micro >= 4) || (major == 7 && minor > 6) || major > 7;
  }
};

} // namespace test::utils
