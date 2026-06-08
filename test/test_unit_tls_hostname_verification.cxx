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

#include "core/io/streams.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tls_verify_mode.hxx"

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ssl.hpp>

#include <cstring>
#include <memory>
#include <string>

namespace
{
// Self-signed certificate (also acts as its own trust anchor) whose only
// subjectAltName is DNS:correct.example. Test fixture only. The validity window
// is deliberately very wide (2000-2099) so the test does not depend on the wall
// clock or fail on machines with skewed clocks.
constexpr const char* server_cert = R"(-----BEGIN CERTIFICATE-----
MIIDMzCCAhugAwIBAgIULstz/HoRnjCkbtarc8M1y1COtUwwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPY29ycmVjdC5leGFtcGxlMCAXDTAwMDEwMTAwMDAwMFoY
DzIwOTkxMjMxMjM1OTU5WjAaMRgwFgYDVQQDDA9jb3JyZWN0LmV4YW1wbGUwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDjVLUmfrjLbEiWOR9p4WrGmjdo
cXLYy01I1Dkcw50pPEtg2lTlgZ2ctB9mlHdborFOQwdSP8KRA5TZCHc793IcICrS
sG2OESAsbVwYWpJH5vB+bGJqvM3xAOBjsEH1UkIv1+yDjnZ54XOQv/6C2YQHj7xS
Kd7xCuNmoUfyli8MI/gocOtnWoCW1vwaUs2BYO3cvbuNmvWjadYWFn++1gQKek5X
+z920+sM7mP1/3rZAyooFhdVhFkzJBfy176kRS6/9XUF5iHWEi42nZ7XnUubKwFn
NI0fgvdXtkMfans1pTOyN/Zzq6Lf2MgrFOt7rq/KCCwaSqht1949W0kgqccZAgMB
AAGjbzBtMB0GA1UdDgQWBBRXenifLMKCVfYbArW04sssiF3q2zAfBgNVHSMEGDAW
gBRXenifLMKCVfYbArW04sssiF3q2zAaBgNVHREEEzARgg9jb3JyZWN0LmV4YW1w
bGUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAfr/eT84yqwPD
Xa7IthMFUkqmUBVOqjOMaNvgUVS33DEHsseke8HJkT9nKMcmxzEHXiqeRxwjVDcV
TDnEOC5MQs9eOwATU4ZNyAYsCxwf1jgqiyX3kKqjVriB5B1JdfWcxfTldpEkcsNM
Z5bm5K2QndhCidFK4/zAZF3p4FxlY6J2G22QI3WDER3sA6lt/IiUYM/N3wb8rlG8
b1PzHPsHE437O5L5RuZt51DXxEcIUrv8oTIg/4kkzbD7sQVN59myt4n+ef2+5MDs
RhdSXCpuXr04sNbyw//wxf53l4/qTj2lJ4EQZqDPckxKhtIyM2C2z6ju+M6DXknn
omL1F449Qw==
-----END CERTIFICATE-----
)";

constexpr const char* server_key = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDjVLUmfrjLbEiW
OR9p4WrGmjdocXLYy01I1Dkcw50pPEtg2lTlgZ2ctB9mlHdborFOQwdSP8KRA5TZ
CHc793IcICrSsG2OESAsbVwYWpJH5vB+bGJqvM3xAOBjsEH1UkIv1+yDjnZ54XOQ
v/6C2YQHj7xSKd7xCuNmoUfyli8MI/gocOtnWoCW1vwaUs2BYO3cvbuNmvWjadYW
Fn++1gQKek5X+z920+sM7mP1/3rZAyooFhdVhFkzJBfy176kRS6/9XUF5iHWEi42
nZ7XnUubKwFnNI0fgvdXtkMfans1pTOyN/Zzq6Lf2MgrFOt7rq/KCCwaSqht1949
W0kgqccZAgMBAAECggEALqys790rVGrkYWGTk1HqsiGmOC242o2tTcNzAXahTT7Z
rCZPsXqCEZNC8jUP55LZDBxDg83fBRai6EeucXO97EvndvAt4jIedLi0ZLSt3ZDr
Nk3LDCa9MtsO9zDQbg3IVJnk7+LfbOlO6LyexR9jVgkbLZR2t29YnrEE/Gf8+2UB
EhVSHxzVzhi7ajx/Fv3xkLavnrgxSarTcSl4Hp9YVI6LS0GAxnUbT9qsEQRoCVFk
j6NkEoQdUn820WCAO9bxIvEgO9mGfnr7ylNaKi71opUGBXFhnnyqpwVceWd1kaWD
qBemnLN5vCTMIlpu3S33zNOKVedAVIa/kafkaoUnPQKBgQD6+X/1ApargGDYBHxI
08+gfhJqDjrMn0gfNbKngAHzH+/VfljWDPm9e9WLSDea0t8+piGmwP4iBHwy/nIQ
vbKqjlR99Gj24Pkvmq0FZ1ou9FjtZGXV7aNMG6kshbKPegP1EII5bwHYZMJiaKIq
ckzCCbSK66Qm/WXWt2ul/0K+tQKBgQDn4gJ55KY2JY/n4VlJkkBX9/rhcyB7eXy7
86ZsWKfrMNn0BOWpM45lHxj9mAfT5B1n9NmFCfbL1aGCl46a/N8VkY3hHkm9leGN
68gaqaA65XfOSKa7krnn3V3YmyzWMvISnou9phxIJAvH5OIAIJKl8Hkp20GJ3e9G
qlFoMRLBVQKBgEKmF3D9av3Ibe9v4YGFnlHEqSc4+Cx28DQ5kmQg/mOOS6aqkvTl
JT1IsYD3gKzA60A75hvejJ6ECmeQYsJHXjck7RM14NoPDJ2zudcBh1WI1kTUsKaL
IR6JCfgk2TJ4+KwP4kVWUWsh9u0jVE1pZTDyWtu5kDI6gNzwgMnoa9UxAoGAZFjJ
K4jIaPw+V2GM6yqwT6FP34qbzvNXCFs7dP20xTHh0BjibiOShq47eVr2YDsCgr9R
9qHGPJWZjFMb8nRl8gaIOJiL3tBiyLD1apxna7Vr8Eg+Z0Pq0a1ZdGhKsfNgELCt
1odxC8MVmg6xws5VyBvVw0hQB2KUrqb8DbPW4vUCgYBicTMwPN2th81I9gv/QKA/
OZwqMcnMUzqGRQ1fyr7rWxnw1WkO7y0x8za2U8c9APB2CYM9g5ubDc203DzB1pwM
/VSERuhUdAzadRW9ax5En7eWUP/ZSvT0qee0B0LrjCbXF/L8uBc5Ia4eIS0eeIKK
+pMEQRYQWT9SSDMZtw1VbQ==
-----END PRIVATE KEY-----
)";

struct handshake_outcome {
  std::error_code client; // client-side handshake result (the behaviour under test)
  std::error_code server; // server-side handshake result (for cross-checking)
};

// Drives a real loopback TLS handshake through the production client stream,
// couchbase::core::io::tls_stream_impl. The client trusts the (correct) server
// certificate and is configured with `verify_mode`; tls_stream_impl::async_connect()
// is then asked to reach `request_hostname`, which is the method that wires in
// SNI and server-identity (SAN/CN) verification before handshaking.
//
// The server side is plain Asio because the SDK only implements the TLS client.
//
// With tls_verify_mode::peer, a `request_hostname` that does not match the
// certificate SAN (DNS:correct.example) MUST cause the client handshake to fail
// (and, as a consequence, the server handshake too). With tls_verify_mode::none,
// verification is disabled and even a mismatched hostname MUST complete.
auto
handshake_with_hostname(const std::string& request_hostname,
                        couchbase::core::tls_verify_mode verify_mode =
                          couchbase::core::tls_verify_mode::peer) -> handshake_outcome
{
  asio::io_context io;

  asio::ssl::context server_ctx{ asio::ssl::context::tls_server };
  server_ctx.use_certificate_chain(asio::buffer(server_cert, std::strlen(server_cert)));
  server_ctx.use_private_key(asio::buffer(server_key, std::strlen(server_key)),
                             asio::ssl::context::pem);

  // Build the client TLS context the way the SDK does, then hand it to the
  // production stream class via tls_context_provider so the test exercises the
  // real tls_stream_impl::async_connect() -> configure_tls_handshake() path.
  auto client_ctx = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client);
  client_ctx->add_certificate_authority(asio::buffer(server_cert, std::strlen(server_cert)));
  client_ctx->set_verify_mode(verify_mode == couchbase::core::tls_verify_mode::peer
                                ? asio::ssl::verify_peer
                                : asio::ssl::verify_none);
  couchbase::core::tls_context_provider provider{ client_ctx };
  couchbase::core::io::tls_stream_impl client_stream{ io, provider };

  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();

  std::error_code server_ec{ asio::error::would_block };
  asio::ssl::stream<asio::ip::tcp::socket> server_stream{ io, server_ctx };
  acceptor.async_accept(server_stream.lowest_layer(), [&](std::error_code accept_ec) {
    // A failing accept would make the client result meaningless, so surface it
    // here rather than letting it masquerade as a client handshake failure.
    REQUIRE_FALSE(accept_ec);
    server_stream.async_handshake(asio::ssl::stream_base::server, [&](std::error_code ec) {
      server_ec = ec;
    });
  });

  std::error_code client_ec{ asio::error::would_block };
  // The behaviour under test: the production connect path resolves SNI and
  // hostname verification for the name the caller intended to reach and reports
  // the combined connect/configure/handshake result.
  client_stream.async_connect(asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), port },
                              request_hostname,
                              [&](std::error_code ec) {
                                client_ec = ec;
                              });

  io.run();
  return { client_ec, server_ec };
}
} // namespace

TEST_CASE("unit: TLS handshake verifies the server hostname against the certificate", "[unit]")
{
  SECTION("hostname that matches the certificate SAN is accepted")
  {
    const auto outcome = handshake_with_hostname("correct.example");
    CHECK_FALSE(outcome.client);
    CHECK_FALSE(outcome.server); // both sides complete the handshake
  }

  SECTION("hostname that does NOT match the certificate SAN is rejected (CWE-297)")
  {
    const auto outcome = handshake_with_hostname("not-the-real-node.example");
    CHECK(outcome.client); // a name-mismatched certificate must not be accepted
  }
}

TEST_CASE("unit: tls_verify=none disables server-identity verification", "[unit]")
{
  // tls_verify=none maps to asio::ssl::verify_none. This is the documented
  // (insecure) escape hatch: certificate and hostname verification are switched
  // off, so even a hostname that does NOT match the certificate SAN must still
  // complete the handshake. This complements the verify_peer cases above and
  // guards against the verify-none branch silently breaking.
  SECTION("a mismatched hostname is accepted when verification is disabled")
  {
    const auto outcome =
      handshake_with_hostname("not-the-real-node.example", couchbase::core::tls_verify_mode::none);
    CHECK_FALSE(outcome.client); // verification disabled => handshake succeeds
    CHECK_FALSE(outcome.server);
  }

  SECTION("a matching hostname is also accepted when verification is disabled")
  {
    const auto outcome =
      handshake_with_hostname("correct.example", couchbase::core::tls_verify_mode::none);
    CHECK_FALSE(outcome.client);
    CHECK_FALSE(outcome.server);
  }
}

TEST_CASE("unit: TLS handshake configuration rejects an empty hostname", "[unit]")
{
  // An empty hostname must fail closed rather than silently skipping SNI and
  // hostname verification (which would regress to CA-only validation, CWE-297).
  asio::io_context io;
  asio::ssl::context client_ctx{ asio::ssl::context::tls_client };
  asio::ssl::stream<asio::ip::tcp::socket> client_stream{ io, client_ctx };

  const auto ec = couchbase::core::io::configure_tls_handshake(client_stream, "");
  CHECK(ec);
}
