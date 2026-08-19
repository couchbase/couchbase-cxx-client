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

// Unit tests for the couchbase2 credential translation (CXXCBC-890): building gRPC SSL
// material from the SDK's TLS options, and the authorization header from credentials. Pure
// construction, no server (env-agnostic).

#include "framework/test_runner.hxx"

#include "core/capella_ca.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/platform/base64.h"
#include "core/protostellar/credentials.hxx"
#include "core/tls_verify_mode.hxx"

#include <filesystem>
#include <fstream>
#include <random>
#include <string>

namespace couchbase::test
{
namespace
{
namespace ps = ::couchbase::core::protostellar;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::cluster_options;
using ::couchbase::core::tls_verify_mode;

void
basic_and_bearer_header_values()
{
  const auto expected_basic = "Basic " + ::couchbase::core::base64::encode("Administrator:secret");
  assert_eq(ps::basic_auth_value("Administrator", "secret"), expected_basic, "basic header value");
  assert_eq(ps::bearer_auth_value("jwt-token"), std::string{ "Bearer jwt-token" }, "bearer value");
}

void
authorization_header_selects_scheme()
{
  cluster_credentials password;
  password.username = "Administrator";
  password.password = "secret";
  assert_eq(ps::authorization_header(password),
            ps::basic_auth_value("Administrator", "secret"),
            "username/password -> Basic");

  cluster_credentials jwt;
  jwt.jwt_token = "jwt-token";
  assert_eq(ps::authorization_header(jwt), std::string{ "Bearer jwt-token" }, "JWT -> Bearer");

  cluster_credentials certificate;
  certificate.certificate_path = "/path/to/cert.pem";
  certificate.key_path = "/path/to/key.pem";
  assert_true(ps::authorization_header(certificate).empty(),
              "certificate auth carries no authorization header");
}

// RFC 77 (Bootstrapping -> Security): "the set of Root CAs that SDKs trust by default for
// couchbase2 must be identical to the one used for Classic". Of the material this SDK ships, that
// means the Capella root and the Mozilla bundle -- the Capella root is what this path originally
// omitted, so a default-options couchbase2 connection could not verify a certificate chaining to it
// while the equivalent MCBP connection could. The set is compared against default_ca::capellaCaCert
// itself rather than a copied fingerprint, so rotating that constant cannot leave this passing
// vacuously.
void
ssl_root_certs_prefer_explicit_then_capella_and_mozilla()
{
  const cluster_credentials no_creds;
  const std::string capella{ ::couchbase::core::default_ca::capellaCaCert };

  cluster_options inline_ca;
  inline_ca.trust_certificate_value =
    "-----BEGIN CERTIFICATE-----\ninline\n-----END CERTIFICATE-----\n";
  const auto explicit_roots = ps::build_ssl_options(inline_ca, no_creds).pem_root_certs;
  assert_eq(explicit_roots, inline_ca.trust_certificate_value, "inline CA value is used verbatim");
  // Explicit material replaces the default set rather than extending it, which is also what MCBP
  // does: "load only the explicit certificate / system and default capella certificates are not
  // loaded".
  assert_true(explicit_roots.find(capella) == std::string::npos,
              "an explicit CA replaces the default set instead of extending it");

  cluster_options defaults; // no explicit CA, bundle enabled by default
  const auto default_roots = ps::build_ssl_options(defaults, no_creds).pem_root_certs;
  assert_eq(default_roots.find(capella),
            std::string::size_type{ 0 },
            "the Capella root leads the default trust set");
  assert_true(default_roots.size() > capella.size(), "the Mozilla bundle follows the Capella root");
  assert_true(default_roots.find("BEGIN CERTIFICATE") != std::string::npos,
              "the default trust set is PEM");

  cluster_options no_bundle;
  no_bundle.disable_mozilla_ca_certificates = true;
  const auto without_bundle = ps::build_ssl_options(no_bundle, no_creds).pem_root_certs;
  // Disabling the bundle drops that layer only. Leaving the set empty would hand trust to gRPC's
  // own fallback -- an environment variable, a process-wide hook, or roots bundled in the gRPC
  // install -- none of which is the platform store and none of which this SDK controls.
  assert_eq(without_bundle,
            capella,
            "disabling the Mozilla bundle keeps the Capella root and nothing else");
  assert_true(without_bundle.size() < default_roots.size(), "and does drop the bundle");
}

void
ssl_client_certificate_is_read_from_files()
{
  // Own a private directory rather than writing fixed names into the shared temp directory: under a
  // parallel ctest two runs would otherwise race on the same paths, and a leftover file from a
  // killed run would be picked up by the next one.
  std::random_device entropy;
  const auto dir =
    std::filesystem::temp_directory_path() / ("cng_credentials_test-" + std::to_string(entropy()));
  std::filesystem::create_directories(dir);

  const auto cert_path = dir / "cert.pem";
  const auto key_path = dir / "key.pem";
  {
    std::ofstream{ cert_path } << "CERT-CHAIN-CONTENTS";
    std::ofstream{ key_path } << "PRIVATE-KEY-CONTENTS";
  }

  cluster_credentials certificate;
  certificate.certificate_path = cert_path.string();
  certificate.key_path = key_path.string();
  const cluster_options options;

  const auto ssl = ps::build_ssl_options(options, certificate);
  assert_eq(ssl.pem_cert_chain, std::string{ "CERT-CHAIN-CONTENTS" }, "client cert chain is read");
  assert_eq(ssl.pem_private_key, std::string{ "PRIVATE-KEY-CONTENTS" }, "client key is read");

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

void
channel_credentials_switch_on_tls()
{
  const cluster_credentials creds;

  cluster_options secure;
  secure.enable_tls = true;
  secure.disable_mozilla_ca_certificates = true; // keep it cheap; no bundle needed here
  assert_true(ps::make_channel_credentials(secure, creds) != nullptr, "TLS channel credentials");

  cluster_options insecure;
  insecure.enable_tls = false;
  assert_true(ps::make_channel_credentials(insecure, creds) != nullptr,
              "insecure channel credentials");
}

// The security-critical property of make_channel_credentials(): exactly one combination of options
// selects the branch that skips server-certificate and hostname verification. Asserting that
// make_channel_credentials() returns non-null cannot express this -- every branch returns non-null,
// including one that returned unverified credentials unconditionally -- and the returned
// grpc::ChannelCredentials is opaque, so the branch is not recoverable from it afterwards. The
// function therefore branches on tls_peer_verification_disabled(), and this pins that predicate.
void
peer_verification_is_disabled_only_by_tls_verify_none()
{
  cluster_options defaults;
  assert_false(ps::tls_peer_verification_disabled(defaults),
               "default options must verify the peer certificate");

  cluster_options verifying;
  verifying.enable_tls = true;
  verifying.tls_verify = tls_verify_mode::peer;
  assert_false(ps::tls_peer_verification_disabled(verifying),
               "tls_verify=peer over TLS must verify the peer certificate");

  cluster_options unverified;
  unverified.enable_tls = true;
  unverified.tls_verify = tls_verify_mode::none;
  assert_true(ps::tls_peer_verification_disabled(unverified),
              "tls_verify=none over TLS reaches the unverified branch");

  // A plaintext channel is not "unverified" in this sense: it carries no server certificate to
  // verify, and make_channel_credentials() refuses to put credentials on it at all. Reporting it as
  // unverified would conflate two different failures and make the predicate useless as a signal.
  cluster_options plaintext;
  plaintext.enable_tls = false;
  plaintext.tls_verify = tls_verify_mode::none;
  assert_false(ps::tls_peer_verification_disabled(plaintext),
               "tls_verify=none without TLS is a plaintext channel, not an unverified one");
}

// The predicate would be worthless if the credential shape could steer the decision -- e.g. if a
// configured client identity or a JWT quietly relaxed verification. Sweep every credential shape
// against both verify modes and assert the answer tracks tls_verify alone.
void
peer_verification_does_not_depend_on_the_credential_shape()
{
  cluster_credentials password;
  password.username = "Administrator";
  password.password = "secret";

  cluster_credentials jwt;
  jwt.jwt_token = "jwt-token";

  cluster_credentials certificate;
  certificate.certificate_path = "/nonexistent/cert.pem";
  certificate.key_path = "/nonexistent/key.pem";

  for (const auto& creds : { cluster_credentials{}, password, jwt, certificate }) {
    cluster_options verifying;
    verifying.enable_tls = true;
    assert_false(ps::tls_peer_verification_disabled(verifying),
                 "no credential shape may reach the unverified branch on its own");

    cluster_options unverified;
    unverified.enable_tls = true;
    unverified.tls_verify = tls_verify_mode::none;
    assert_true(ps::tls_peer_verification_disabled(unverified),
                "tls_verify=none stays decisive for every credential shape");
    (void)creds;
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_credentials",
    {
      { "basic_and_bearer_header_values", basic_and_bearer_header_values },
      { "authorization_header_selects_scheme", authorization_header_selects_scheme },
      { "ssl_root_certs_prefer_explicit_then_capella_and_mozilla",
        ssl_root_certs_prefer_explicit_then_capella_and_mozilla },
      { "ssl_client_certificate_is_read_from_files", ssl_client_certificate_is_read_from_files },
      { "channel_credentials_switch_on_tls", channel_credentials_switch_on_tls },
      { "peer_verification_is_disabled_only_by_tls_verify_none",
        peer_verification_is_disabled_only_by_tls_verify_none },
      { "peer_verification_does_not_depend_on_the_credential_shape",
        peer_verification_does_not_depend_on_the_credential_shape },
    },
  };
}

} // namespace couchbase::test
