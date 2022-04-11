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

struct server_version {
    unsigned long major{ 0 };
    unsigned long minor{ 0 };
    unsigned long micro{ 0 };
    unsigned long build{ 0 };
    bool developer_preview{ false };
    server_edition edition{ server_edition::unknown };

    static server_version parse(const std::string& str);

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
        return is_neo();
    }

    [[nodiscard]] bool supports_preserve_expiry() const
    {
        return is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_preserve_expiry_for_query() const
    {
        return is_neo();
    }

    [[nodiscard]] bool supports_user_groups() const
    {
        return (is_mad_hatter() || is_cheshire_cat() || is_neo()) && is_enterprise();
    }

    [[nodiscard]] bool supports_query_index_management() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_analytics() const
    {
        return is_alice() || is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_analytics_pending_mutations() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_analytics_link_azure_blob() const
    {
        return is_cheshire_cat() && developer_preview;
    }

    [[nodiscard]] bool supports_analytics_links() const
    {
        return (major == 6 && minor >= 6) || major > 6;
    }

    [[nodiscard]] bool supports_minimum_durability_level() const
    {
        return (major == 6 && minor >= 6) || major > 6;
    }

    [[nodiscard]] bool supports_search_analyze() const
    {
        return is_mad_hatter() || is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool supports_analytics_links_cert_auth() const
    {
        return is_neo();
    }

    [[nodiscard]] bool supports_eventing_functions() const
    {
        return is_cheshire_cat() || is_neo();
    }

    [[nodiscard]] bool is_enterprise() const
    {
        return edition == server_edition::enterprise;
    }

    [[nodiscard]] bool is_community() const
    {
        return edition == server_edition::community;
    }
};

} // namespace test::utils
