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

#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/build_config.hxx>

#include <chrono>
#include <cstddef>
#include <map>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::utils::connection_string;
using couchbase::core::utils::parse_connection_string;

using address_type = connection_string::address_type;
using bootstrap_mode = connection_string::bootstrap_mode;
using nodes = std::vector<connection_string::node>;
using params = std::map<std::string, std::string>;

void
a_full_connection_string_is_parsed_into_all_its_parts([[maybe_unused]] context& ctx)
{
  auto spec = parse_connection_string(
    "couchbase://localhost:8091=http;127.0.0.1=mcd/default?dump_configuration=true");

  assert_eq(spec.scheme, "couchbase", "scheme");
  assert_eq(spec.default_port, 11210, "default port");
  assert_eq(spec.default_mode, bootstrap_mode::gcccp, "default bootstrap mode");
  assert_false(spec.tls, "TLS");
  assert_eq(spec.params, params{ { "dump_configuration", "true" } }, "parameters");
  assert_eq(spec.bootstrap_nodes,
            nodes{
              { "localhost", 8091, address_type::dns, bootstrap_mode::http },
              { "127.0.0.1", 0, address_type::ipv4, bootstrap_mode::gcccp },
            },
            "bootstrap nodes");
  assert_true(spec.options.dump_configuration, "the parameter reaches the options");
  assert_eq(spec.default_bucket_name.value_or(""), "default", "default bucket name");
}

void
the_scheme_is_taken_from_the_prefix([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://127.0.0.1").scheme, "couchbase", "couchbase");
  assert_eq(parse_connection_string("http://127.0.0.1").scheme, "http", "http");
  assert_eq(parse_connection_string("couchbase://").scheme, "couchbase", "no nodes");
  assert_eq(parse_connection_string("my+scheme://").scheme, "my+scheme", "an unknown scheme");
  assert_eq(parse_connection_string("127.0.0.1").scheme, "couchbase", "no scheme");
  assert_eq(
    parse_connection_string("127.0.0.1:8091").scheme, "couchbase", "no scheme, with a port");
}

void
the_scheme_sets_the_default_bootstrap_mode([[maybe_unused]] context& ctx)
{
  assert_eq(
    parse_connection_string("couchbase://").default_mode, bootstrap_mode::gcccp, "couchbase");
  assert_eq(parse_connection_string("https://").default_mode, bootstrap_mode::http, "https");
  assert_eq(parse_connection_string("my+scheme://").default_mode,
            bootstrap_mode::unspecified,
            "an unknown scheme");
}

void
the_scheme_sets_the_default_port([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://").default_port, 11210, "couchbase");
  assert_eq(parse_connection_string("couchbases://").default_port, 11207, "couchbases");
  assert_eq(parse_connection_string("http://").default_port, 8091, "http");
  assert_eq(parse_connection_string("https://").default_port, 18091, "https");
  assert_eq(parse_connection_string("my+scheme://").default_port, 0, "an unknown scheme");
}

void
the_scheme_decides_whether_tls_is_used([[maybe_unused]] context& ctx)
{
  assert_false(parse_connection_string("couchbase://").tls, "couchbase");
  assert_false(parse_connection_string("http://").tls, "http");
  assert_true(parse_connection_string("couchbases://").tls, "couchbases");
  assert_true(parse_connection_string("https://").tls, "https");
}

void
a_single_bootstrap_node_carries_its_address_type([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://1.2.3.4").bootstrap_nodes,
            nodes{ { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified } },
            "an IPv4 literal");
  assert_eq(parse_connection_string("couchbase://231.1.1.1").bootstrap_nodes,
            nodes{ { "231.1.1.1", 0, address_type::ipv4, bootstrap_mode::unspecified } },
            "an IPv4 literal in the class D range");
  assert_eq(parse_connection_string("couchbase://255.1.1.1").bootstrap_nodes,
            nodes{ { "255.1.1.1", 0, address_type::ipv4, bootstrap_mode::unspecified } },
            "the largest first octet that is still IPv4");
  assert_eq(parse_connection_string("couchbase://256.1.1.1").bootstrap_nodes,
            nodes{ { "256.1.1.1", 0, address_type::dns, bootstrap_mode::unspecified } },
            "an out-of-range first octet is a DNS name");
  assert_eq(parse_connection_string("couchbase://[::ffff:13.15.49.232]").bootstrap_nodes,
            nodes{ { "::ffff:13.15.49.232", 0, address_type::ipv6, bootstrap_mode::unspecified } },
            "an IPv4-mapped IPv6 literal");
  assert_eq(parse_connection_string("couchbase://[::]").bootstrap_nodes,
            nodes{ { "::", 0, address_type::ipv6, bootstrap_mode::unspecified } },
            "the unspecified IPv6 address");
  assert_eq(parse_connection_string("couchbase://[::1]").bootstrap_nodes,
            nodes{ { "::1", 0, address_type::ipv6, bootstrap_mode::unspecified } },
            "the IPv6 loopback");
  assert_eq(parse_connection_string("couchbase://[2001:db8::1]").bootstrap_nodes,
            nodes{ { "2001:db8::1", 0, address_type::ipv6, bootstrap_mode::unspecified } },
            "an abbreviated IPv6 literal");
  assert_eq(
    parse_connection_string("couchbase://[2001:db8:85a3:8d3:1319:8a2e:370:7348]").bootstrap_nodes,
    nodes{
      { "2001:db8:85a3:8d3:1319:8a2e:370:7348",
        0,
        address_type::ipv6,
        bootstrap_mode::unspecified },
    },
    "a fully written IPv6 literal");
  assert_eq(parse_connection_string("couchbase://example.com").bootstrap_nodes,
            nodes{ { "example.com", 0, address_type::dns, bootstrap_mode::unspecified } },
            "a DNS name");
  assert_eq(parse_connection_string("1.2.3.4").bootstrap_nodes,
            nodes{ { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified } },
            "an IPv4 literal with no scheme");
}

void
multiple_bootstrap_nodes_are_parsed_in_order([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://1.2.3.4,4.3.2.1").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "comma-separated");
  assert_eq(parse_connection_string("couchbase://1.2.3.4;4.3.2.1").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "semicolon-separated");
  assert_eq(parse_connection_string("couchbase://[2001:db8::1];123.123.12.4").bootstrap_nodes,
            nodes{
              { "2001:db8::1", 0, address_type::ipv6, bootstrap_mode::unspecified },
              { "123.123.12.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "mixed address families");
  assert_eq(parse_connection_string("couchbase://example.com,[::1];127.0.0.1").bootstrap_nodes,
            nodes{
              { "example.com", 0, address_type::dns, bootstrap_mode::unspecified },
              { "::1", 0, address_type::ipv6, bootstrap_mode::unspecified },
              { "127.0.0.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "both separators in one list");
  assert_eq(parse_connection_string("1.2.3.4,4.3.2.1").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "no scheme");
}

void
a_bootstrap_node_may_carry_a_custom_port([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://1.2.3.4,4.3.2.1:11210").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 11210, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "a port on the last node only");
  assert_eq(parse_connection_string("couchbase://1.2.3.4:8091,4.3.2.1").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 8091, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "a port on the first node only");
  assert_eq(parse_connection_string("couchbase://[2001:db8::1]:18091;123.123.12.4").bootstrap_nodes,
            nodes{
              { "2001:db8::1", 18091, address_type::ipv6, bootstrap_mode::unspecified },
              { "123.123.12.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "a port after an IPv6 literal");
  assert_eq(
    parse_connection_string("couchbase://example.com:123,[::1]:456;127.0.0.1:789").bootstrap_nodes,
    nodes{
      { "example.com", 123, address_type::dns, bootstrap_mode::unspecified },
      { "::1", 456, address_type::ipv6, bootstrap_mode::unspecified },
      { "127.0.0.1", 789, address_type::ipv4, bootstrap_mode::unspecified },
    },
    "a port on every node");
  assert_eq(parse_connection_string("example.com:123,[::1]:456;127.0.0.1:789").bootstrap_nodes,
            nodes{
              { "example.com", 123, address_type::dns, bootstrap_mode::unspecified },
              { "::1", 456, address_type::ipv6, bootstrap_mode::unspecified },
              { "127.0.0.1", 789, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "a port on every node, no scheme");
}

void
a_bootstrap_node_may_carry_a_custom_bootstrap_mode([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("couchbase://1.2.3.4,4.3.2.1=MCD").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::gcccp },
            },
            "mcd selects gcccp");
  assert_eq(parse_connection_string("couchbase://1.2.3.4:8091=http;4.3.2.1").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 8091, address_type::ipv4, bootstrap_mode::http },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::unspecified },
            },
            "a mode alongside a port");
  assert_eq(parse_connection_string("couchbase://1.2.3.4:8091=http;4.3.2.1=gcccp").bootstrap_nodes,
            nodes{
              { "1.2.3.4", 8091, address_type::ipv4, bootstrap_mode::http },
              { "4.3.2.1", 0, address_type::ipv4, bootstrap_mode::gcccp },
            },
            "a mode on every node");
  assert_eq(
    parse_connection_string("couchbase://[2001:db8::1]:18091=mcd;123.123.12.4").bootstrap_nodes,
    nodes{
      { "2001:db8::1", 18091, address_type::ipv6, bootstrap_mode::gcccp },
      { "123.123.12.4", 0, address_type::ipv4, bootstrap_mode::unspecified },
    },
    "a mode after an IPv6 literal and a port");
  assert_eq(
    parse_connection_string("couchbase://example.com=CcCp,[::1];127.0.0.1=Http").bootstrap_nodes,
    nodes{
      { "example.com", 0, address_type::dns, bootstrap_mode::gcccp },
      { "::1", 0, address_type::ipv6, bootstrap_mode::unspecified },
      { "127.0.0.1", 0, address_type::ipv4, bootstrap_mode::http },
    },
    "the mode name is case-insensitive");
}

void
the_path_component_names_the_default_bucket([[maybe_unused]] context& ctx)
{
  assert_eq(
    parse_connection_string("couchbase://127.0.0.1/bucket").default_bucket_name.value_or(""),
    "bucket",
    "a plain name");
  assert_eq(
    parse_connection_string("couchbase://127.0.0.1/bUcKeT").default_bucket_name.value_or(""),
    "bUcKeT",
    "the name keeps its case");
  assert_eq(parse_connection_string("couchbase://127.0.0.1/bU%1F-K__big__.mp3")
              .default_bucket_name.value_or(""),
            "bU%1F-K__big__.mp3",
            "the name is not unescaped");
  assert_false(parse_connection_string("couchbase://127.0.0.1").default_bucket_name.has_value(),
               "no path means no default bucket");
}

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
void
known_parameters_are_applied_to_the_options([[maybe_unused]] context& ctx)
{
  assert_true(parse_connection_string("couchbase://127.0.0.1").options.trust_certificate.empty(),
              "no certificate by default");
  assert_eq(parse_connection_string(
              "couchbase://127.0.0.1?security.trust_only_pem_file=/etc/tls/example.cert")
              .options.trust_certificate,
            "/etc/tls/example.cert",
            "the trusted certificate path");

  auto spec = parse_connection_string(
    "couchbase://127.0.0.1?timeout.connect_timeout=42ms&timeout.query_timeout=123ms");
  assert_eq(spec.options.bootstrap_timeout.count(), 42, "bootstrap timeout");
  assert_eq(spec.options.query_timeout.count(), 123, "query timeout");
}

void
parameters_are_recorded_verbatim_alongside_the_options_they_set([[maybe_unused]] context& ctx)
{
  auto spec = parse_connection_string(
    "couchbase://127.0.0.1?timeout.connect_timeout=42ms&timeout.query_timeout=123ms");
  assert_eq(spec.params,
            params{ { "timeout.connect_timeout", "42ms" }, { "timeout.query_timeout", "123ms" } },
            "every recognised parameter");

  spec = parse_connection_string("couchbase://127.0.0.1?timeout.connect_timeout=42ms&foo=bar");
  assert_eq(spec.params,
            params{ { "timeout.connect_timeout", "42ms" }, { "foo", "bar" } },
            "an unrecognised parameter is recorded too");
  assert_eq(spec.options.bootstrap_timeout.count(), 42, "the recognised one still applies");

  spec = parse_connection_string("couchbase://127.0.0.1?timeout.resolve_timeout=4s2ms");
  assert_eq(spec.params, params{ { "timeout.resolve_timeout", "4s2ms" } }, "a compound duration");
  assert_eq(spec.options.resolve_timeout.count(), 4002, "the compound duration is summed");

  spec = parse_connection_string("couchbase://"
                                 "127.0.0.1?user_agent_extra=couchnode%2F4.1.1%20(node%2F12.11."
                                 "1%3B%20v8%2F7.7.299.11-node.12%3B%20ssl%2F1.1.1c)");
  assert_eq(spec.options.user_agent_extra,
            "couchnode/4.1.1 (node/12.11.1; v8/7.7.299.11-node.12; ssl/1.1.1c)",
            "a percent-encoded value is decoded");
}

void
unusable_parameters_are_reported_as_warnings([[maybe_unused]] context& ctx)
{
  auto spec = parse_connection_string("couchbase://127.0.0.1?timeout.connect_timeout=42ms&foo=bar");
  assert_eq(spec.warnings,
            std::vector<std::string>{
              R"(unknown parameter "foo" in connection string (value "bar"))",
            },
            "an unknown parameter");

  spec = parse_connection_string("couchbase://127.0.0.1?enable_dns_srv=maybe&ip_protocol=yes");
  assert_eq(
    spec.warnings,
    std::vector<std::string>{
      R"(unable to parse "enable_dns_srv" parameter in connection string (value "maybe" cannot be interpreted as a boolean))",
      R"(unable to parse "ip_protocol" parameter in connection string (value "yes" is not a valid IP protocol preference))",
    },
    "values of the wrong type");

  spec = parse_connection_string(
    "couchbase://localhost:8091=http;127.0.0.1=mcd/default?enable_dns_srv=true");
  assert_eq(
    spec.warnings,
    std::vector<std::string>{
      R"(parameter "enable_dns_srv" requires single entry in bootstrap nodes list of the connection string, ignoring (value "true"))",
    },
    "a parameter that contradicts the node list");

  spec = parse_connection_string(
    "couchbase://"
    "localhost?timeout.query_timeout=10000ms&timeout.dispatch_timeout=true&timeout.resolve_"
    "timeout=11000ms");
  assert_starts_with(
    spec.warnings.at(0),
    R"(unable to parse "timeout.dispatch_timeout" parameter in connection string (value: "true"): invalid duration: true)",
    "the unparsable parameter is named");
  assert_eq(spec.options.query_timeout.count(), 10000, "a parameter before the bad one applies");
  assert_eq(spec.options.resolve_timeout.count(), 11000, "a parameter after the bad one applies");
}
#else
void
known_parameters_are_applied_to_the_options([[maybe_unused]] context& ctx)
{
  assert_true(parse_connection_string("couchbase://127.0.0.1").options.trust_certificate.empty(),
              "no certificate by default");
  assert_eq(parse_connection_string("couchbase://127.0.0.1?trust_certificate=/etc/tls/example.cert")
              .options.trust_certificate,
            "/etc/tls/example.cert",
            "the trusted certificate path");

  auto spec =
    parse_connection_string("couchbase://127.0.0.1?key_value_timeout=42&query_timeout=123");
  assert_eq(spec.options.key_value_timeout.count(), 42, "key/value timeout");
  assert_eq(spec.options.query_timeout.count(), 123, "query timeout");
}

void
parameters_are_recorded_verbatim_alongside_the_options_they_set([[maybe_unused]] context& ctx)
{
  auto spec =
    parse_connection_string("couchbase://127.0.0.1?key_value_timeout=42&query_timeout=123");
  assert_eq(spec.params,
            params{ { "key_value_timeout", "42" }, { "query_timeout", "123" } },
            "every recognised parameter");

  spec = parse_connection_string("couchbase://127.0.0.1?kv_timeout=42&foo=bar");
  assert_eq(spec.params,
            params{ { "kv_timeout", "42" }, { "foo", "bar" } },
            "an unrecognised parameter is recorded too");
  assert_eq(spec.options.key_value_timeout.count(), 42, "the recognised alias still applies");

  spec = parse_connection_string("couchbase://127.0.0.1?kv_timeout=4s2ms");
  assert_eq(spec.params, params{ { "kv_timeout", "4s2ms" } }, "a compound duration");
  assert_eq(spec.options.key_value_timeout.count(), 4002, "the compound duration is summed");

  spec = parse_connection_string("couchbase://"
                                 "127.0.0.1?user_agent_extra=couchnode%2F4.1.1%20(node%2F12.11."
                                 "1%3B%20v8%2F7.7.299.11-node.12%3B%20ssl%2F1.1.1c)");
  assert_eq(spec.options.user_agent_extra,
            "couchnode/4.1.1 (node/12.11.1; v8/7.7.299.11-node.12; ssl/1.1.1c)",
            "a percent-encoded value is decoded");
}

void
unusable_parameters_are_reported_as_warnings([[maybe_unused]] context& ctx)
{
  auto spec = parse_connection_string("couchbase://127.0.0.1?kv_timeout=42&foo=bar");
  assert_eq(spec.warnings,
            std::vector<std::string>{
              R"(unknown parameter "foo" in connection string (value "bar"))",
            },
            "an unknown parameter");

  spec = parse_connection_string("couchbase://127.0.0.1?enable_dns_srv=maybe&ip_protocol=yes");
  assert_eq(
    spec.warnings,
    std::vector<std::string>{
      R"(unable to parse "enable_dns_srv" parameter in connection string (value "maybe" cannot be interpreted as a boolean))",
      R"(unable to parse "ip_protocol" parameter in connection string (value "yes" is not a valid IP protocol preference))",
    },
    "values of the wrong type");

  spec = parse_connection_string(
    "couchbase://localhost:8091=http;127.0.0.1=mcd/default?enable_dns_srv=true");
  assert_eq(
    spec.warnings,
    std::vector<std::string>{
      R"(parameter "enable_dns_srv" requires single entry in bootstrap nodes list of the connection string, ignoring (value "true"))",
    },
    "a parameter that contradicts the node list");

  spec = parse_connection_string(
    "couchbase://localhost?query_timeout=10000&kv_timeout=true&management_timeout=11000");
  assert_starts_with(
    spec.warnings.at(0),
    R"(unable to parse "kv_timeout" parameter in connection string (value "true" is not a number))",
    "the unparsable parameter is named");
  assert_eq(spec.options.query_timeout.count(), 10000, "a parameter before the bad one applies");
  assert_eq(
    spec.options.management_timeout.count(), 11000, "a parameter after the bad one applies");
}
#endif

void
a_malformed_connection_string_is_rejected_with_a_located_error([[maybe_unused]] context& ctx)
{
  assert_eq(parse_connection_string("").error.value_or(""),
            "failed to parse connection string: empty input",
            "the empty input");
  assert_eq(parse_connection_string("couchbase://127.0.0.1/bucket/foo").error.value_or(""),
            R"(failed to parse connection string (column: 29, trailer: "/foo"))",
            "a second path segment");
  assert_eq(parse_connection_string("couchbase://[:13.15.49.232]").error.value_or(""),
            R"(failed to parse connection string (column: 14, trailer: ":13.15.49.232]"))",
            "a malformed IPv6 literal");
  assert_eq(parse_connection_string("couchbase://[2001:1:db8:85a3:8d3:1319:8a2e:370:7348]")
              .error.value_or(""),
            R"(failed to parse connection string (column: 47, trailer: ":7348]"))",
            "an IPv6 literal with too many groups");
  assert_eq(
    parse_connection_string("couchbase://2001:db8:85a3:8d3:1319:8a2e:370:7348").error.value_or(""),
    R"(failed to parse connection string (column: 18, trailer: "db8:85a3:8d3:1319:8a2e:370:7348"))",
    "an unbracketed IPv6 literal");
}

// Thirty nodes, so that a shuffle leaving the list in its original order is not something the run
// will ever see.
[[nodiscard]] auto
source_hostnames() -> std::vector<std::string>
{
  return {
    "192.168.0.10", "192.168.0.11", "192.168.0.12", "192.168.0.13", "192.168.0.14", "192.168.0.15",
    "192.168.0.16", "192.168.0.17", "192.168.0.18", "192.168.0.19", "192.168.0.20", "192.168.0.21",
    "192.168.0.22", "192.168.0.23", "192.168.0.24", "192.168.0.25", "192.168.0.26", "192.168.0.27",
    "192.168.0.28", "192.168.0.29", "192.168.0.30", "192.168.0.31", "192.168.0.32", "192.168.0.33",
    "192.168.0.34", "192.168.0.35", "192.168.0.36", "192.168.0.37", "192.168.0.38", "192.168.0.39",
  };
}

[[nodiscard]] auto
comma_separated(const std::vector<std::string>& values) -> std::string
{
  std::string joined;
  for (const auto& value : values) {
    if (!joined.empty()) {
      joined += ',';
    }
    joined += value;
  }
  return joined;
}

void
bootstrap_nodes_are_shuffled_by_default([[maybe_unused]] context& ctx)
{
  const auto hostnames = source_hostnames();
  auto connstr = parse_connection_string("couchbase://" + comma_separated(hostnames));

  assert_false(connstr.options.preserve_bootstrap_nodes_order, "the option defaults to off");
  for (std::size_t idx = 0; idx < hostnames.size(); ++idx) {
    assert_eq(connstr.bootstrap_nodes[idx].address, hostnames[idx], "parsing preserves the order");
  }

  const auto first = couchbase::core::origin({}, connstr).get_hostnames();
  assert_eq(first.size(), hostnames.size(), "no node is lost");
  assert_ne(first, hostnames, "the origin shuffles the list");

  const auto second = couchbase::core::origin({}, connstr).get_hostnames();
  assert_eq(second.size(), hostnames.size(), "no node is lost");
  assert_ne(second, hostnames, "the origin shuffles the list");

  assert_ne(first, second, "two origins shuffle independently");
}

void
preserve_bootstrap_nodes_order_keeps_the_given_order([[maybe_unused]] context& ctx)
{
  const auto hostnames = source_hostnames();
  auto connstr = parse_connection_string("couchbase://" + comma_separated(hostnames) +
                                         "?preserve_bootstrap_nodes_order=true");

  assert_true(connstr.options.preserve_bootstrap_nodes_order, "the parameter is applied");
  for (std::size_t idx = 0; idx < hostnames.size(); ++idx) {
    assert_eq(connstr.bootstrap_nodes[idx].address, hostnames[idx], "parsing preserves the order");
  }

  const auto bootstrap = couchbase::core::origin({}, connstr).get_hostnames();
  assert_eq(bootstrap.size(), hostnames.size(), "no node is lost");
  assert_eq(bootstrap, hostnames, "the origin keeps the order it was given");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_full_connection_string_is_parsed_into_all_its_parts) },
      { CASE(the_scheme_is_taken_from_the_prefix) },
      { CASE(the_scheme_sets_the_default_bootstrap_mode) },
      { CASE(the_scheme_sets_the_default_port) },
      { CASE(the_scheme_decides_whether_tls_is_used) },
      { CASE(a_single_bootstrap_node_carries_its_address_type) },
      { CASE(multiple_bootstrap_nodes_are_parsed_in_order) },
      { CASE(a_bootstrap_node_may_carry_a_custom_port) },
      { CASE(a_bootstrap_node_may_carry_a_custom_bootstrap_mode) },
      { CASE(the_path_component_names_the_default_bucket) },
      { CASE(known_parameters_are_applied_to_the_options) },
      { CASE(parameters_are_recorded_verbatim_alongside_the_options_they_set) },
      { CASE(unusable_parameters_are_reported_as_warnings) },
      { CASE(a_malformed_connection_string_is_rejected_with_a_located_error) },
      { CASE(bootstrap_nodes_are_shuffled_by_default) },
      { CASE(preserve_bootstrap_nodes_order_keeps_the_given_order) },
    },
  };
}

} // namespace couchbase::test
