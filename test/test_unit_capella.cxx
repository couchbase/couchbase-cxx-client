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

#include "test_helper.hxx"

#include "core/capella.hxx"
#include "core/tls_verify_mode.hxx"

TEST_CASE("unit: is_capella_host recognises Couchbase Capella hostnames", "[unit]")
{
  using couchbase::core::is_capella_host;

  SECTION("Capella hosts are recognised")
  {
    // A typical Capella connection string host and the bare apex domain.
    CHECK(is_capella_host("cb.abcdefghij.cloud.couchbase.com"));
    CHECK(is_capella_host("cloud.couchbase.com"));
    // Deeper subdomains are still dot-bounded matches.
    CHECK(is_capella_host("a.b.c.cloud.couchbase.com"));
    // DNS hostnames are case-insensitive, so any casing is accepted.
    CHECK(is_capella_host("Tenant.Cloud.Couchbase.Com"));
    CHECK(is_capella_host("CLOUD.COUCHBASE.COM"));
    CHECK(is_capella_host("cb.ABCDEF.Cloud.Couchbase.COM"));
    // The suffix appearing more than once must still be detected at the end.
    CHECK(is_capella_host("x.cloud.couchbase.com.cloud.couchbase.com"));
    // A single trailing dot (the FQDN root label) must not be a bypass.
    CHECK(is_capella_host("cloud.couchbase.com."));
    CHECK(is_capella_host("cb.abcdefghij.cloud.couchbase.com."));
  }

  SECTION("look-alike and unrelated hosts are NOT treated as Capella")
  {
    // No dot boundary before the suffix: a different registrable name.
    CHECK_FALSE(is_capella_host("mycloud.couchbase.com"));
    CHECK_FALSE(is_capella_host("evilcloud.couchbase.com"));
    CHECK_FALSE(is_capella_host("xcloud.couchbase.com"));
    // The suffix is present but not at the end (left-anchored attack).
    CHECK_FALSE(is_capella_host("cloud.couchbase.com.evil.example"));
    // A trailing dot after a non-suffix is still not Capella.
    CHECK_FALSE(is_capella_host("cloud.couchbase.com.evil.example."));
    // Plain Couchbase / unrelated domains.
    CHECK_FALSE(is_capella_host("couchbase.com"));
    CHECK_FALSE(is_capella_host("example.com"));
    CHECK_FALSE(is_capella_host("localhost"));
    CHECK_FALSE(is_capella_host("127.0.0.1"));
    // Degenerate inputs must not match or crash.
    CHECK_FALSE(is_capella_host(""));
    CHECK_FALSE(is_capella_host("."));
    CHECK_FALSE(is_capella_host("cloud.couchbase.co"));  // shorter than the suffix
    CHECK_FALSE(is_capella_host(".cloud.couchbase.co")); // suffix off by one char
    CHECK_FALSE(is_capella_host(".cloud.couchbase.com"));
  }
}

TEST_CASE("unit: effective_tls_verify_mode enforces peer verification for Capella", "[unit]")
{
  using couchbase::core::effective_tls_verify_mode;
  using couchbase::core::tls_verify_mode;

  SECTION("non-Capella hosts honour the requested mode")
  {
    CHECK(effective_tls_verify_mode(tls_verify_mode::peer, false) == tls_verify_mode::peer);
    CHECK(effective_tls_verify_mode(tls_verify_mode::none, false) == tls_verify_mode::none);
  }

  SECTION("Capella hosts are always peer-verified, even when none is requested")
  {
    CHECK(effective_tls_verify_mode(tls_verify_mode::peer, true) == tls_verify_mode::peer);
    // The security-relevant case: a requested tls_verify=none is overridden.
    CHECK(effective_tls_verify_mode(tls_verify_mode::none, true) == tls_verify_mode::peer);
  }
}
