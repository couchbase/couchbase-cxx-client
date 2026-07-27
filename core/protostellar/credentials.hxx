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

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

namespace couchbase::core
{
struct cluster_options;
struct cluster_credentials;
} // namespace couchbase::core

namespace couchbase::core::protostellar
{

// Translate the SDK's TLS options and credentials into gRPC's SSL material: root certs come from an
// inline PEM value, else a CA file, else the SDK's own default set -- the Capella root plus the
// bundled Mozilla roots unless those are disabled. The client chain/key are set together for
// certificate (mTLS) authentication.
//
// The default set matches what MCBP's configure_tls_context() contributes from material this SDK
// ships, as RFC 77 (Bootstrapping -> Security) requires. It does not additionally fold in the
// platform trust store that MCBP reaches through set_default_verify_paths(): gRPC takes roots only
// as a single PEM that replaces its store outright, and under OpenSSL/BoringSSL the platform set is
// not a defined collection but whatever the host ships, re-pointable at run time via
// SSL_CERT_FILE/SSL_CERT_DIR. See credentials.cxx for the full reasoning and how the other SDKs
// handle it.
[[nodiscard]] auto
build_ssl_options(const cluster_options& options, const cluster_credentials& credentials)
  -> grpc::SslCredentialsOptions;

// "Authorization" header values: Basic base64(user:password) and Bearer <jwt>.
[[nodiscard]] auto
basic_auth_value(const std::string& username, const std::string& password) -> std::string;
[[nodiscard]] auto
bearer_auth_value(const std::string& jwt_token) -> std::string;

// The per-RPC authorization header the credentials imply: Bearer for a JWT, Basic for
// username/password, empty when neither applies (e.g. certificate auth, where identity is the
// client cert). couchbase2 attaches this as call metadata rather than as gRPC CallCredentials,
// so it works on both secure and insecure channels.
[[nodiscard]] auto
authorization_header(const cluster_credentials& credentials) -> std::string;

// Channel credentials for a couchbase2 endpoint: TLS (from build_ssl_options) when TLS is
// enabled, otherwise insecure.
[[nodiscard]] auto
make_channel_credentials(const cluster_options& options, const cluster_credentials& credentials)
  -> std::shared_ptr<grpc::ChannelCredentials>;

// Build a channel to a couchbase2 endpoint honouring the TLS options, with the default channel
// arguments (keepalive).
[[nodiscard]] auto
make_channel(const std::string& endpoint,
             const cluster_options& options,
             const cluster_credentials& credentials) -> std::shared_ptr<grpc::Channel>;

} // namespace couchbase::core::protostellar
