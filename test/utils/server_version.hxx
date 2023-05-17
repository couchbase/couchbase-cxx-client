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

enum class server_edition { unknown, enterprise, community };

enum class deployment_type { on_prem, capella, elixir };

enum class server_config_profile { unknown, serverless };

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

    static server_version parse(const std::string& str, deployment_type deployment);

    [[nodiscard]] bool is_mock() const
    {
        return use_gocaves;
    }

    [[nodiscard]] bool is_alice() const
    {
        // [6.0.0, 6.5.0)
        return major == 6 && minor < 5;
    }

    [[nodiscard]] bool is_mad_hatter() const
    {
        // [6.5.0, 7.0.0)
        return major == 6 && minor >= 5;
    }

    [[nodiscard]] bool is_cheshire_cat() const
    {
        // [7.0.0, 7.1.0)
        return major == 7 && minor < 1;
    }

    [[nodiscard]] bool is_neo() const
    {
        // [7.1.0, inf)
        return (major == 7 && minor >= 1) || major > 7;
    }

    [[nodiscard]] bool supports_gcccp() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_sync_replication() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_enhanced_durability() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_scoped_queries() const
    {
        return is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_collections() const
    {
        return (is_mad_hatter() && developer_preview) || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_storage_backend() const
    {
        return is_neo() && is_enterprise();
    }

    [[nodiscard]] bool supports_preserve_expiry() const
    {
        return !use_gocaves && (is_cheshire_cat() || is_neo());
    }

    [[nodiscard]] bool supports_preserve_expiry_for_query() const
    {
        return is_neo();
    }

    [[nodiscard]] bool supports_user_groups() const
    {
        return supports_user_management() && (is_mad_hatter() || is_cheshire_cat() || is_neo()) && is_enterprise();
    }

    [[nodiscard]] bool supports_query_index_management() const
    {
        return !use_gocaves && (is_mad_hatter() || is_cheshire_cat() || is_neo());
    }

    [[nodiscard]] bool supports_analytics() const
    {
        return !use_gocaves && is_enterprise() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
    }

    [[nodiscard]] bool supports_query() const
    {
        return !use_gocaves;
    }

    [[nodiscard]] bool supports_search() const
    {
        return !use_gocaves;
    }

    [[nodiscard]] bool supports_analytics_pending_mutations() const
    {
        return supports_analytics() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
    }

    [[nodiscard]] bool supports_analytics_link_azure_blob() const
    {
        return supports_analytics() && is_cheshire_cat() && developer_preview;
    }

    [[nodiscard]] bool supports_analytics_links() const
    {
        return supports_analytics() && ((major == 6 && minor >= 6) || major > 6);
    }

    [[nodiscard]] bool supports_minimum_durability_level() const
    {
        return (major == 6 && minor >= 6) || major > 6;
    }

    [[nodiscard]] bool supports_search_analyze() const
    {
        return supports_search() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
    }

    [[nodiscard]] bool supports_analytics_links_cert_auth() const
    {
        return supports_analytics() && is_neo();
    }

    [[nodiscard]] bool supports_eventing_functions() const
    {
        return !use_gocaves && is_enterprise() && (is_cheshire_cat() || is_neo()) && deployment == deployment_type::on_prem;
    }

    [[nodiscard]] bool is_enterprise() const
    {
        return edition == server_edition::enterprise;
    }

    [[nodiscard]] bool is_community() const
    {
        return edition == server_edition::community;
    }

    [[nodiscard]] bool supports_bucket_management() const
    {
        return !use_gocaves && deployment == deployment_type::on_prem;
    }

    [[nodiscard]] bool supports_user_management() const
    {
        return !use_gocaves && deployment == deployment_type::on_prem;
    }

    [[nodiscard]] bool requires_search_replicas() const
    {
        return deployment == deployment_type::elixir || deployment == deployment_type::capella || is_serverless_config_profile();
    }

    [[nodiscard]] bool supports_views() const
    {
        return !use_gocaves && deployment == deployment_type::on_prem && (major < 7 || (major == 7 && minor < 2));
    }

    [[nodiscard]] bool supports_memcached_buckets() const
    {
        return !is_serverless_config_profile();
    }

    [[nodiscard]] bool is_serverless_config_profile() const
    {
        return profile == server_config_profile::serverless;
    }

    [[nodiscard]] bool supports_search_disable_scoring() const
    {
        return supports_search() && (is_mad_hatter() || is_cheshire_cat() || is_neo());
    }
};

} // namespace test::utils
