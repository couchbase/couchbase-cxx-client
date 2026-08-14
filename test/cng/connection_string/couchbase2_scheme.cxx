/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Unit tests for the couchbase2:// connection-string scheme (CXXCBC-887). Pure parsing, no
// server, so these run in every mode (env-agnostic).

#include "framework/test_runner.hxx"

#include "core/utils/connection_string.hxx"

#include <cstddef>
#include <cstdint>
#include <string>

namespace couchbase::test
{
namespace
{
using ::couchbase::core::utils::connection_string;
using ::couchbase::core::utils::parse_connection_string;

void
couchbase2_defaults()
{
  const auto cs = parse_connection_string("couchbase2://localhost");
  assert_false(cs.error.has_value(), "parses without error");
  assert_eq(cs.scheme, std::string{ "couchbase2" }, "scheme is couchbase2");
  assert_true(cs.uses_protostellar(), "protocol is protostellar");
  assert_true(cs.protocol == connection_string::protocol_type::protostellar, "protocol enum");
  assert_true(cs.tls, "couchbase2 implies TLS");
  assert_true(cs.options.enable_tls, "TLS propagated to options");
  assert_eq(cs.default_port, std::uint16_t{ 18098 }, "default port is 18098");
  assert_eq(cs.bootstrap_nodes.size(), std::size_t{ 1 }, "one bootstrap node");
  // The parser leaves a portless node at 0; the default_port is applied later by the consumer
  // (same as couchbase://). So we assert the default_port field, not the node port.
  assert_eq(cs.bootstrap_nodes.at(0).port, std::uint16_t{ 0 }, "portless node stays 0");
}

void
couchbase2_explicit_port_wins()
{
  const auto cs = parse_connection_string("couchbase2://example.com:12345");
  assert_false(cs.error.has_value(), "parses without error");
  assert_eq(cs.bootstrap_nodes.size(), std::size_t{ 1 }, "one bootstrap node");
  assert_eq(cs.bootstrap_nodes.at(0).address, std::string{ "example.com" }, "address parsed");
  assert_eq(cs.bootstrap_nodes.at(0).port, std::uint16_t{ 12345 }, "explicit port overrides");
}

// A CNG gateway fronts the whole cluster and does its own routing, so there is nothing to rotate
// through: exactly one host is meaningful. Silently using the first and dropping the rest would
// hide a misconfiguration, so the parser rejects it. couchbase-jvm-clients rejects the same shape.
void
couchbase2_rejects_multiple_hosts()
{
  const auto cs = parse_connection_string("couchbase2://a.example.com,b.example.com,c.example.com");
  assert_true(cs.error.has_value(), "multiple hosts are rejected");
  assert_true(cs.error.value().find("exactly one host") != std::string::npos,
              "error names the constraint");
  // The classic schemes are unaffected -- they genuinely rotate through the list.
  const auto classic = parse_connection_string("couchbase://a.example.com,b.example.com");
  assert_false(classic.error.has_value(), "couchbase:// still accepts multiple hosts");
  assert_eq(classic.bootstrap_nodes.size(), std::size_t{ 2 }, "both nodes kept");
}

// The gateway uses neither MCBP nor HTTP config bootstrap, so a per-node mode suffix cannot be
// honoured. Rejected for the same reason as extra hosts.
void
couchbase2_rejects_mode_suffix()
{
  for (const auto* input : { "couchbase2://host=mcd", "couchbase2://host=http" }) {
    const auto cs = parse_connection_string(input);
    assert_true(cs.error.has_value(), "mode suffix is rejected");
  }
  const auto classic = parse_connection_string("couchbase://host=mcd");
  assert_false(classic.error.has_value(), "couchbase:// still accepts a mode suffix");
}

// couchbase2:// is addressed directly, so there are no SRV records to resolve. The option must be
// off regardless of the string looking SRV-eligible (single DNS host, no port), which is exactly
// the shape that enables it for couchbase://.
void
couchbase2_disables_dns_srv()
{
  const auto cs = parse_connection_string("couchbase2://localhost");
  assert_false(cs.error.has_value(), "parses without error");
  assert_false(cs.options.enable_dns_srv, "DNS SRV is off for couchbase2");

  const auto classic = parse_connection_string("couchbase://localhost");
  assert_true(classic.options.enable_dns_srv, "couchbase:// with one DNS host keeps DNS SRV on");
}

void
couchbase2_params_pass_through()
{
  const auto cs = parse_connection_string("couchbase2://host?kv_timeout=5000");
  assert_false(cs.error.has_value(), "parses with options without error");
  assert_true(cs.uses_protostellar(), "still protostellar with params");
  assert_eq(cs.bootstrap_nodes.size(), std::size_t{ 1 }, "node parsed alongside params");
}

void
classic_schemes_remain_mcbp()
{
  const auto plain = parse_connection_string("couchbase://host");
  assert_false(plain.uses_protostellar(), "couchbase is MCBP");
  assert_false(plain.tls, "couchbase has no TLS");
  assert_eq(plain.default_port, std::uint16_t{ 11210 }, "couchbase default port");

  const auto secure = parse_connection_string("couchbases://host");
  assert_false(secure.uses_protostellar(), "couchbases is MCBP");
  assert_true(secure.tls, "couchbases has TLS");
  assert_eq(secure.default_port, std::uint16_t{ 11207 }, "couchbases default port");

  const auto http = parse_connection_string("http://host");
  assert_false(http.uses_protostellar(), "http is MCBP");
  assert_eq(http.default_port, std::uint16_t{ 8091 }, "http default port");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "connection_string_couchbase2",
    {
      { "couchbase2_defaults", couchbase2_defaults },
      { "couchbase2_explicit_port_wins", couchbase2_explicit_port_wins },
      { "couchbase2_rejects_multiple_hosts", couchbase2_rejects_multiple_hosts },
      { "couchbase2_rejects_mode_suffix", couchbase2_rejects_mode_suffix },
      { "couchbase2_disables_dns_srv", couchbase2_disables_dns_srv },
      { "couchbase2_params_pass_through", couchbase2_params_pass_through },
      { "classic_schemes_remain_mcbp", classic_schemes_remain_mcbp },
    },
  };
}

} // namespace couchbase::test
