/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "framework/errors.hxx"

#include "core/mozilla_ca_bundle.hxx"

#include <couchbase/build_config.hxx>

#include <asio/buffer.hpp>
#include <asio/ssl.hpp>

#include <set>
#include <string>
#include <string_view>
#include <system_error>

namespace couchbase::test
{
namespace
{
using ::couchbase::core::default_ca::mozilla_ca_certs;

void
bundle_is_empty_exactly_when_embedding_is_disabled([[maybe_unused]] context& ctx)
{
#if defined(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE)
  assert_false(mozilla_ca_certs().empty(), "an embedded bundle carries certificates");
#else
  assert_true(mozilla_ca_certs().empty(), "a build without embedding carries no certificates");
#endif
}

// MCBP loads each entry through this call and only logs a rejection, so an entry the TLS library
// cannot parse would silently drop out of the default trust set.
void
every_embedded_certificate_loads_as_a_trust_anchor([[maybe_unused]] context& ctx)
{
  for (const auto& cert : mozilla_ca_certs()) {
    asio::ssl::context tls{ asio::ssl::context::tls_client };
    std::error_code ec;
    tls.add_certificate_authority(asio::buffer(cert.body.data(), cert.body.size()), ec);
    assert_success(ec, "the TLS library accepts \"" + std::string{ cert.authority } + "\"");
  }
}

void
embedded_certificates_are_distinct([[maybe_unused]] context& ctx)
{
  std::set<std::string_view> bodies;
  for (const auto& cert : mozilla_ca_certs()) {
    assert_true(bodies.insert(cert.body).second,
                "\"" + std::string{ cert.authority } + "\" is embedded once");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(bundle_is_empty_exactly_when_embedding_is_disabled), {}, timeout::instant },
      { CASE(every_embedded_certificate_loads_as_a_trust_anchor), {}, timeout::instant },
      { CASE(embedded_certificates_are_distinct), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
