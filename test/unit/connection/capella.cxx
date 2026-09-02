/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/capella.hxx"
#include "core/tls_verify_mode.hxx"

namespace couchbase::test
{
namespace
{
using couchbase::core::effective_tls_verify_mode;
using couchbase::core::is_capella_host;
using couchbase::core::tls_verify_mode;

void
capella_hostnames_are_recognised([[maybe_unused]] context& ctx)
{
  // A typical Capella connection string host and the bare apex domain.
  assert_true(is_capella_host("cb.abcdefghij.cloud.couchbase.com"), "a tenant host");
  assert_true(is_capella_host("cloud.couchbase.com"), "the bare apex domain");
  // Deeper subdomains are still dot-bounded matches.
  assert_true(is_capella_host("a.b.c.cloud.couchbase.com"), "a deeper subdomain");
  // DNS hostnames are case-insensitive, so any casing is accepted.
  assert_true(is_capella_host("Tenant.Cloud.Couchbase.Com"), "mixed case");
  assert_true(is_capella_host("CLOUD.COUCHBASE.COM"), "upper case");
  assert_true(is_capella_host("cb.ABCDEF.Cloud.Couchbase.COM"), "mixed case with a tenant label");
  // The suffix appearing more than once must still be detected at the end.
  assert_true(is_capella_host("x.cloud.couchbase.com.cloud.couchbase.com"), "a repeated suffix");
  // A single trailing dot (the FQDN root label) must not be a bypass.
  assert_true(is_capella_host("cloud.couchbase.com."), "the apex domain with a root label");
  assert_true(is_capella_host("cb.abcdefghij.cloud.couchbase.com."),
              "a tenant host with a root label");
}

void
look_alike_hostnames_are_not_capella([[maybe_unused]] context& ctx)
{
  // No dot boundary before the suffix: a different registrable name.
  assert_false(is_capella_host("mycloud.couchbase.com"), "no dot before the suffix");
  assert_false(is_capella_host("evilcloud.couchbase.com"), "no dot before the suffix");
  assert_false(is_capella_host("xcloud.couchbase.com"), "a one-character leading label");
  // The suffix is present but not at the end (left-anchored attack).
  assert_false(is_capella_host("cloud.couchbase.com.evil.example"), "the suffix is not at the end");
  // A trailing dot after a non-suffix is still not Capella.
  assert_false(is_capella_host("cloud.couchbase.com.evil.example."),
               "the suffix is not at the end, root label included");
  // Plain Couchbase / unrelated domains.
  assert_false(is_capella_host("couchbase.com"), "the corporate domain");
  assert_false(is_capella_host("example.com"), "an unrelated domain");
  assert_false(is_capella_host("localhost"), "a bare hostname");
  assert_false(is_capella_host("127.0.0.1"), "a literal address");
  // Degenerate inputs must not match or crash.
  assert_false(is_capella_host(""), "the empty hostname");
  assert_false(is_capella_host("."), "a lone root label");
  assert_false(is_capella_host("cloud.couchbase.co"), "shorter than the suffix");
  assert_false(is_capella_host(".cloud.couchbase.co"), "suffix off by one character");
  assert_false(is_capella_host(".cloud.couchbase.com"), "an empty leftmost label");
}

void
a_non_capella_host_honours_the_requested_verify_mode([[maybe_unused]] context& ctx)
{
  assert_eq(
    effective_tls_verify_mode(tls_verify_mode::peer, false), tls_verify_mode::peer, "peer is kept");
  assert_eq(
    effective_tls_verify_mode(tls_verify_mode::none, false), tls_verify_mode::none, "none is kept");
}

void
a_capella_host_is_always_peer_verified([[maybe_unused]] context& ctx)
{
  assert_eq(
    effective_tls_verify_mode(tls_verify_mode::peer, true), tls_verify_mode::peer, "peer is kept");
  // The security-relevant case: a requested tls_verify=none is overridden.
  assert_eq(effective_tls_verify_mode(tls_verify_mode::none, true),
            tls_verify_mode::peer,
            "none is overridden");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(capella_hostnames_are_recognised) },
      { CASE(look_alike_hostnames_are_not_capella) },
      { CASE(a_non_capella_host_honours_the_requested_verify_mode) },
      { CASE(a_capella_host_is_always_peer_verified) },
    },
  };
}

} // namespace couchbase::test
