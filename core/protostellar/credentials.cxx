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

#include "core/protostellar/credentials.hxx"

#include "core/capella_ca.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/mozilla_ca_bundle.hxx"
#include "core/platform/base64.h"
#include "core/protostellar/dispatcher.hxx"

#include <grpcpp/create_channel.h>

#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace couchbase::core::protostellar
{
namespace
{
// Read a PEM file that the caller has explicitly configured. Fails closed: a configured but
// unreadable/empty trust or certificate file is an error, never a silent fall-back to the system
// trust store (which would broaden the accepted-issuer set beyond the operator's intent).
auto
read_required_file(const std::string& path) -> std::string
{
  const std::ifstream stream{ path, std::ios::binary };
  if (!stream) {
    throw std::runtime_error("couchbase2: cannot read configured TLS file: " + path);
  }
  std::ostringstream buffer;
  buffer << stream.rdbuf();
  auto contents = buffer.str();
  if (contents.empty()) {
    throw std::runtime_error("couchbase2: configured TLS file is empty: " + path);
  }
  return contents;
}

// The default trust material, concatenated into a single PEM as gRPC's pem_root_certs expects: the
// Couchbase Capella root, plus the bundled Mozilla roots unless the operator disabled them. RFC 77
// (Bootstrapping -> Security) requires that "the set of Root CAs that SDKs trust by default for
// couchbase2 must be identical to the one used for Classic", and the Capella root is what this path
// was missing -- MCBP's configure_tls_context() adds it unconditionally whenever no explicit trust
// material is configured.
//
// Why the platform trust store is not also folded in, unlike MCBP's set_default_verify_paths():
// gRPC accepts trust material only as one PEM blob and treats a non-empty pem_root_certs as the
// entire store -- "If this parameter is empty, the default roots will be used" -- so there is
// nothing to layer onto and no incremental API to layer with. Snapshotting the platform store into
// that blob would not reproduce MCBP anyway: under OpenSSL/BoringSSL "system roots" is not a
// defined set the way the JVM's trust store or .NET's X509Chain is, but whatever the host happens
// to ship, relocatable at run time through SSL_CERT_FILE/SSL_CERT_DIR, and BoringSSL compiles in
// different default paths again. The two sets this SDK ships and can therefore guarantee are the
// Capella root and the Mozilla bundle, so those are what the default trusts. A private or corporate
// CA is configured explicitly, which replaces this set outright -- exactly as it does for MCBP.
//
// For reference, the other SDKs land in the same place by different routes: Java trusts its Capella
// root plus the JVM trust store (SecurityConfig::defaultCaCertificates), .NET accepts whatever the
// platform validated and adds its Capella root to the chain's ExtraStore, and Go trusts only
// x509.SystemCertPool() and ships no Capella root at all.
auto
default_root_certs(bool include_mozilla_bundle) -> std::string
{
  std::string pem{ default_ca::capellaCaCert };
  if (!pem.empty() && pem.back() != '\n') {
    pem.push_back('\n');
  }
  if (include_mozilla_bundle) {
    for (const auto& cert : default_ca::mozilla_ca_certs()) {
      pem.append(cert.body);
      if (pem.back() != '\n') {
        pem.push_back('\n');
      }
    }
  }
  return pem;
}

// The configured client certificate/key pair for mTLS, as {private_key, certificate_chain}.
// Returns nullopt when neither is set; throws when only one is set, since a half-configured
// identity produces an opaque connect-time failure rather than a clear misconfiguration error.
auto
read_client_identity(const cluster_credentials& credentials)
  -> std::optional<std::pair<std::string, std::string>>
{
  const bool have_cert = !credentials.certificate_path.empty();
  const bool have_key = !credentials.key_path.empty();
  if (!have_cert && !have_key) {
    return std::nullopt;
  }
  if (have_cert != have_key) {
    throw std::invalid_argument("couchbase2: client certificate authentication requires both a "
                                "certificate and a private key; only one was configured");
  }
  return std::pair{ read_required_file(credentials.key_path),
                    read_required_file(credentials.certificate_path) };
}
} // namespace

auto
build_ssl_options(const cluster_options& options, const cluster_credentials& credentials)
  -> grpc::SslCredentialsOptions
{
  grpc::SslCredentialsOptions ssl;
  if (!options.trust_certificate_value.empty()) {
    ssl.pem_root_certs = options.trust_certificate_value;
  } else if (!options.trust_certificate.empty()) {
    ssl.pem_root_certs = read_required_file(options.trust_certificate);
  } else {
    // Never left empty: gRPC's own fallback is the GRPC_DEFAULT_SSL_ROOTS_FILE_PATH environment
    // variable, else a process-wide override hook, else roots bundled in the gRPC installation --
    // none of which is the platform trust store, and all of which are outside this SDK's control.
    // Disabling the Mozilla bundle drops that layer only; the Capella root stays, matching MCBP,
    // where disable_mozilla_ca_certificates likewise gates just the bundle loop.
    ssl.pem_root_certs = default_root_certs(!options.disable_mozilla_ca_certificates);
  }

  if (auto identity = read_client_identity(credentials)) {
    ssl.pem_private_key = identity->first;
    ssl.pem_cert_chain = identity->second;
  }
  return ssl;
}

auto
basic_auth_value(const std::string& username, const std::string& password) -> std::string
{
  return "Basic " + base64::encode(username + ":" + password);
}

auto
bearer_auth_value(const std::string& jwt_token) -> std::string
{
  return "Bearer " + jwt_token;
}

auto
authorization_header(const cluster_credentials& credentials) -> std::string
{
  if (credentials.uses_jwt()) {
    return bearer_auth_value(credentials.jwt_token);
  }
  if (credentials.uses_password()) {
    return basic_auth_value(credentials.username, credentials.password);
  }
  return {};
}

auto
make_channel_credentials(const cluster_options& options, const cluster_credentials& credentials)
  -> std::shared_ptr<grpc::ChannelCredentials>
{
  if (!options.enable_tls) {
    if (credentials.uses_password() || credentials.uses_jwt() ||
        !credentials.certificate_path.empty() || !credentials.key_path.empty()) {
      throw std::invalid_argument(
        "couchbase2: refusing to send credentials over a plaintext channel; enable TLS");
    }
    return grpc::InsecureChannelCredentials();
  }
  return grpc::SslCredentials(build_ssl_options(options, credentials));
}

auto
make_channel(const std::string& endpoint,
             const cluster_options& options,
             const cluster_credentials& credentials) -> std::shared_ptr<grpc::Channel>
{
  return grpc::CreateCustomChannel(
    endpoint, make_channel_credentials(options, credentials), default_channel_arguments());
}

} // namespace couchbase::core::protostellar
