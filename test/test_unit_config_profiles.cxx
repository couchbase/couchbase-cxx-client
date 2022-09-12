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

#include "test_helper.hxx"

#include "core/cluster_options.hxx"
#include "core/config_profile.hxx"
#include <stdexcept>

class test_profile : public couchbase::core::config_profile
{
  public:
    void apply(couchbase::core::cluster_options& opts) override
    {
        opts.key_value_timeout = std::chrono::milliseconds(10);
    }
};

TEST_CASE("unit: can apply wan_development profile", "[unit]")
{
    couchbase::core::cluster_options opts{};
    opts.apply_profile("wan_development");
    CHECK(opts.key_value_timeout.count() == 20000);
    CHECK(opts.connect_timeout.count() == 5000);
}
TEST_CASE("unit: all other options remain unchanged", "[unit]")
{
    couchbase::core::cluster_options default_opts{};
    couchbase::core::cluster_options opts{};
    opts.apply_profile("wan_development");
    // other than key_value_timeout and connect_timeout,
    // we'd expect default_opts to be equal to opts:
    CHECK(opts.key_value_durable_timeout == default_opts.key_value_durable_timeout);
    CHECK(opts.tracer == default_opts.tracer);
    CHECK(opts.meter == default_opts.meter);
    CHECK(opts.analytics_timeout == default_opts.analytics_timeout);
    CHECK(opts.bootstrap_timeout == default_opts.bootstrap_timeout);
    CHECK(opts.config_idle_redial_timeout == default_opts.config_idle_redial_timeout);
    CHECK(opts.config_poll_floor == default_opts.config_poll_floor);
    CHECK(opts.config_poll_interval == default_opts.config_poll_interval);
    CHECK(opts.dns_srv_timeout == default_opts.dns_srv_timeout);
    CHECK(opts.enable_clustermap_notification == opts.enable_clustermap_notification);
    CHECK(opts.enable_compression == default_opts.enable_compression);
    CHECK(opts.enable_dns_srv == default_opts.enable_dns_srv);
    CHECK(opts.enable_metrics == default_opts.enable_metrics);
    CHECK(opts.enable_mutation_tokens == default_opts.enable_mutation_tokens);
    CHECK(opts.enable_tcp_keep_alive == default_opts.enable_tcp_keep_alive);
    CHECK(opts.enable_tls == default_opts.enable_tls);
    CHECK(opts.enable_tracing == default_opts.enable_tracing);
    CHECK(opts.enable_unordered_execution == default_opts.enable_unordered_execution);
    CHECK(opts.idle_http_connection_timeout == default_opts.idle_http_connection_timeout);
    CHECK(opts.management_timeout == default_opts.management_timeout);
    CHECK(opts.max_http_connections == default_opts.max_http_connections);
    CHECK(opts.network == default_opts.network);
    CHECK(opts.query_timeout == default_opts.query_timeout);
    CHECK(opts.resolve_timeout == default_opts.resolve_timeout);
    CHECK(opts.search_timeout == default_opts.search_timeout);
    CHECK(opts.show_queries == default_opts.show_queries);
    CHECK(opts.tcp_keep_alive_interval == default_opts.tcp_keep_alive_interval);
    CHECK(opts.tls_verify == default_opts.tls_verify);
    CHECK(opts.trust_certificate == default_opts.trust_certificate);
    CHECK(opts.use_ip_protocol == default_opts.use_ip_protocol);
    CHECK(opts.user_agent_extra == default_opts.user_agent_extra);
    CHECK(opts.view_timeout == default_opts.view_timeout);
}

TEST_CASE("unit: can register and use new profile", "[unit]")
{
    couchbase::core::cluster_options opts{};
    couchbase::core::known_profiles().register_profile<test_profile>("test");
    opts.apply_profile("test");
    CHECK(opts.key_value_timeout.count() == 10);
}
TEST_CASE("unit: unknown profile name raises exception", "[unit]")
{
    couchbase::core::cluster_options opts{};
    REQUIRE_THROWS_AS(opts.apply_profile("i don't exist"), std::invalid_argument);
}

TEST_CASE("unit: can apply multiple profiles", "[unit]")
{
    couchbase::core::cluster_options opts{};
    couchbase::core::known_profiles().register_profile<test_profile>("test");
    opts.apply_profile("wan_development");
    opts.apply_profile("test");
    // set only in wan_development
    CHECK(opts.connect_timeout.count() == 5000);
    // set in both, so should be overwritten by "test"
    CHECK(opts.key_value_timeout.count() == 10);
}
