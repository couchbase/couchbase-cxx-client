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

#include "framework/test_registry.hxx"

#include "core/cluster_options.hxx"
#include "core/config_profiles.hxx"

#include <chrono>
#include <stdexcept>
#include <string>

namespace couchbase::test
{
namespace
{
class test_profile : public couchbase::core::config_profile
{
public:
  void apply(couchbase::core::cluster_options& opts) override
  {
    opts.key_value_timeout = std::chrono::milliseconds(10);
  }
};

class test_profile_with_args : public couchbase::core::config_profile
{
public:
  explicit test_profile_with_args(std::string extra)
    : extra_{ std::move(extra) }
  {
  }
  void apply(couchbase::core::cluster_options& opts) override
  {
    opts.user_agent_extra = extra_;
  }

private:
  std::string extra_;
};

// The values the wan_development profile is defined to set. See
// https://docs.google.com/document/d/1LNCYgV2Eqymp3pGmA8WKPQOLSpcRyv0P7NpMYHVcUM0
void
wan_development_sets_every_timeout_it_owns([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options opts{};
  opts.apply_profile("wan_development");

  assert_eq(opts.key_value_timeout.count(), 20'000, "key/value timeout");
  assert_eq(opts.key_value_durable_timeout.count(), 20'000, "durable key/value timeout");
  assert_eq(opts.key_value_scan_timeout.count(), 120'000, "key/value scan timeout");
  assert_eq(opts.connect_timeout.count(), 20'000, "connect timeout");
  assert_eq(opts.view_timeout.count(), 120'000, "view timeout");
  assert_eq(opts.query_timeout.count(), 120'000, "query timeout");
  assert_eq(opts.analytics_timeout.count(), 120'000, "analytics timeout");
  assert_eq(opts.search_timeout.count(), 120'000, "search timeout");
  assert_eq(opts.management_timeout.count(), 120'000, "management timeout");
  assert_eq(opts.bootstrap_timeout.count(), 120'000, "bootstrap timeout");
  assert_eq(opts.resolve_timeout.count(), 20'000, "resolve timeout");
  assert_eq(opts.dns_config.timeout().count(), 20'000, "DNS timeout");
}

void
wan_development_leaves_every_other_option_alone([[maybe_unused]] context& ctx)
{
  const couchbase::core::cluster_options defaults{};
  couchbase::core::cluster_options opts{};
  opts.apply_profile("wan_development");

  assert_true(opts.tracer == defaults.tracer, "tracer");
  assert_true(opts.meter == defaults.meter, "meter");
  assert_true(opts.config_idle_redial_timeout == defaults.config_idle_redial_timeout,
              "config idle redial timeout");
  assert_true(opts.config_poll_floor == defaults.config_poll_floor, "config poll floor");
  assert_true(opts.config_poll_interval == defaults.config_poll_interval, "config poll interval");
  assert_eq(opts.enable_clustermap_notification,
            defaults.enable_clustermap_notification,
            "clustermap notification");
  assert_eq(opts.enable_compression, defaults.enable_compression, "compression");
  assert_eq(opts.enable_dns_srv, defaults.enable_dns_srv, "DNS SRV");
  assert_eq(opts.enable_metrics, defaults.enable_metrics, "metrics");
  assert_eq(opts.enable_mutation_tokens, defaults.enable_mutation_tokens, "mutation tokens");
  assert_eq(opts.enable_tcp_keep_alive, defaults.enable_tcp_keep_alive, "TCP keep-alive");
  assert_eq(opts.enable_tls, defaults.enable_tls, "TLS");
  assert_eq(opts.enable_tracing, defaults.enable_tracing, "tracing");
  assert_eq(
    opts.enable_unordered_execution, defaults.enable_unordered_execution, "unordered execution");
  assert_true(opts.idle_http_connection_timeout == defaults.idle_http_connection_timeout,
              "idle HTTP connection timeout");
  assert_eq(opts.max_http_connections, defaults.max_http_connections, "max HTTP connections");
  assert_eq(opts.network, defaults.network, "network");
  assert_eq(opts.show_queries, defaults.show_queries, "show queries");
  assert_true(opts.tcp_keep_alive_interval == defaults.tcp_keep_alive_interval,
              "TCP keep-alive interval");
  assert_true(opts.tls_verify == defaults.tls_verify, "TLS verify mode");
  assert_eq(opts.trust_certificate, defaults.trust_certificate, "trust certificate");
  assert_true(opts.use_ip_protocol == defaults.use_ip_protocol, "IP protocol");
  assert_eq(opts.user_agent_extra, defaults.user_agent_extra, "user agent extra");
}

void
a_registered_profile_can_be_applied([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options opts{};
  couchbase::core::known_profiles().register_profile<test_profile>("test");
  opts.apply_profile("test");

  assert_eq(opts.key_value_timeout.count(), 10, "the profile's own value is applied");
}

void
an_unknown_profile_name_raises([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options opts{};
  assert_throws<std::invalid_argument>(
    [&opts]() {
      opts.apply_profile("i don't exist");
    },
    "an unregistered name is rejected rather than ignored");
}

void
a_later_profile_overrides_an_earlier_one([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options opts{};
  couchbase::core::known_profiles().register_profile<test_profile>("test");
  opts.apply_profile("wan_development");
  opts.apply_profile("test");

  assert_eq(opts.connect_timeout.count(), 20'000, "an option only wan_development sets survives");
  assert_eq(opts.key_value_timeout.count(), 10, "one both set takes the later profile's value");
}

void
a_profile_can_be_registered_with_constructor_arguments([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options opts{};
  couchbase::core::known_profiles().register_profile<test_profile_with_args>(
    std::string("test_with_args"), std::string("something_extra"));
  opts.apply_profile("test_with_args");

  assert_eq(opts.user_agent_extra,
            std::string{ "something_extra" },
            "the constructor argument reaches the applied option");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(wan_development_sets_every_timeout_it_owns) },
      { CASE(wan_development_leaves_every_other_option_alone) },
      { CASE(a_registered_profile_can_be_applied) },
      { CASE(an_unknown_profile_name_raises) },
      { CASE(a_later_profile_overrides_an_earlier_one) },
      { CASE(a_profile_can_be_registered_with_constructor_arguments) },
    },
  };
}

} // namespace couchbase::test
