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

#include "config_profile.hxx"

couchbase::core::config_profiles&
couchbase::core::known_profiles()
{
    static couchbase::core::config_profiles profiles{};
    return profiles;
}

void
couchbase::core::development_profile::apply(couchbase::core::cluster_options& opts)
{
    opts.key_value_timeout = std::chrono::seconds(20);
    opts.key_value_durable_timeout = std::chrono::seconds(20);
    opts.connect_timeout = std::chrono::seconds(20);
    opts.view_timeout = std::chrono::minutes(2);
    opts.query_timeout = std::chrono::minutes(2);
    opts.analytics_timeout = std::chrono::minutes(2);
    opts.search_timeout = std::chrono::minutes(2);
    opts.management_timeout = std::chrono::minutes(2);

    // C++SDK specific
    opts.dns_config = couchbase::core::io::dns::dns_config{
        opts.dns_config.nameserver(),
        opts.dns_config.port(),
        std::chrono::seconds(20), // timeout to make DNS-SRV query
    };
    opts.resolve_timeout = std::chrono::seconds(20);  // timeout to resolve hostnames
    opts.bootstrap_timeout = std::chrono::minutes(2); // overall timeout to bootstrap
}
