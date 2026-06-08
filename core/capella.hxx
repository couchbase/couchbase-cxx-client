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

#pragma once

#include "core/tls_verify_mode.hxx"

#include <cstddef>
#include <string_view>

namespace couchbase::core
{
/**
 * Returns true if @p hostname is the bare "cloud.couchbase.com" or a dot-bounded
 * subdomain of it (e.g. "cb.<tenant>.cloud.couchbase.com").
 *
 * The comparison is:
 *  - anchored at the END of the string (a trailing match), so
 *    "cloud.couchbase.com.evil.example" does NOT match;
 *  - tolerant of a single trailing dot (the FQDN root label), so
 *    "cloud.couchbase.com." matches -- otherwise a trailing dot would be a way
 *    to slip a Capella host past the forced peer verification (the connection
 *    string grammar accepts trailing dots and origin preserves them verbatim);
 *  - dot-bounded with a non-empty leftmost label, so "mycloud.couchbase.com"
 *    does NOT match (no '.' immediately before the suffix) and neither does the
 *    malformed ".cloud.couchbase.com" (empty leftmost label), while the bare
 *    domain itself does;
 *  - ASCII case-insensitive, because DNS hostnames are case-insensitive, so
 *    "Tenant.Cloud.Couchbase.Com" is still recognised as Capella.
 */
[[nodiscard]] inline auto
is_capella_host(std::string_view hostname) -> bool
{
  static constexpr std::string_view suffix = "cloud.couchbase.com"; // lowercase ASCII
  // Ignore a single trailing dot: "host.cloud.couchbase.com." is the same FQDN
  // as the dotless form and must not be a way to bypass Capella enforcement.
  if (!hostname.empty() && hostname.back() == '.') {
    hostname.remove_suffix(1);
  }
  if (hostname.size() < suffix.size()) {
    return false;
  }
  const auto offset = hostname.size() - suffix.size();
  for (std::size_t i = 0; i < suffix.size(); ++i) {
    auto c = hostname[offset + i];
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
    if (c != suffix[i]) {
      return false;
    }
  }
  // Either the bare apex domain, or a dot-bounded subdomain whose '.' before the
  // suffix is preceded by at least one label character (so the leading-dot form
  // ".cloud.couchbase.com", with an empty leftmost label, is rejected).
  return offset == 0 || (offset > 1 && hostname[offset - 1] == '.');
}

/**
 * Resolve the TLS peer-verification mode that is actually applied to a
 * connection, given the user-requested @p requested mode and whether any target
 * host is Couchbase Capella (@p connecting_to_capella).
 *
 * Connections to Capella are ALWAYS peer-verified: a user-supplied
 * tls_verify=none is ignored for Capella hosts (returning tls_verify_mode::peer)
 * so that certificate verification cannot be silently disabled against the
 * managed service. For every other host the requested mode is honoured verbatim.
 */
[[nodiscard]] inline auto
effective_tls_verify_mode(tls_verify_mode requested, bool connecting_to_capella) -> tls_verify_mode
{
  if (connecting_to_capella) {
    return tls_verify_mode::peer;
  }
  return requested;
}
} // namespace couchbase::core
