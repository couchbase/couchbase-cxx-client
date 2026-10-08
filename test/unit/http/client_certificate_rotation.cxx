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

// Client certificate rotation on pooled HTTP sessions, against a loopback TLS endpoint that
// requires a client certificate and records the one each connection presents.
//
// Every key and certificate is ephemeral test material: generated in this process when a case
// runs and never checked in. cluster_fixture writes them to a temporary directory that it removes.

#include "framework/context.hxx"
#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/cluster.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/cluster_options.hxx"
#include "core/core_sdk_shim.hxx"
#include "core/free_form_http_request.hxx"
#include "core/http_component.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/http_noop.hxx"
#include "core/origin.hxx"
#include "core/pending_operation.hxx"
#include "core/pending_operation_connection_info.hxx"
#include "core/service_type.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;

struct pkey_deleter {
  void operator()(EVP_PKEY* p) const
  {
    EVP_PKEY_free(p);
  }
};
struct x509_deleter {
  void operator()(X509* p) const
  {
    X509_free(p);
  }
};
using pkey_ptr = std::unique_ptr<EVP_PKEY, pkey_deleter>;
using x509_ptr = std::unique_ptr<X509, x509_deleter>;

struct identity {
  pkey_ptr key;
  x509_ptr cert;
};

auto
make_key() -> pkey_ptr
{
  EVP_PKEY_CTX* kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
  EVP_PKEY* key = nullptr;
  const bool ok = kctx != nullptr && EVP_PKEY_keygen_init(kctx) == 1 &&
                  EVP_PKEY_CTX_set_ec_paramgen_curve_nid(kctx, NID_X9_62_prime256v1) == 1 &&
                  EVP_PKEY_keygen(kctx, &key) == 1;
  EVP_PKEY_CTX_free(kctx);
  if (!ok) {
    fail("an ephemeral EC key is generated");
  }
  return pkey_ptr{ key };
}

// A certificate for common_name, valid for [not_before, not_after) in seconds since the epoch and
// signed by issuer, or self-signed when issuer is null.
auto
make_identity(const std::string& common_name,
              std::int64_t not_before,
              std::int64_t not_after,
              const identity* issuer) -> identity
{
  identity id{ make_key(), x509_ptr{ X509_new() } };
  X509* cert = id.cert.get();
  X509_set_version(cert, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  ASN1_TIME_set(X509_getm_notBefore(cert), static_cast<time_t>(not_before));
  ASN1_TIME_set(X509_getm_notAfter(cert), static_cast<time_t>(not_after));
  X509_set_pubkey(cert, id.key.get());
  X509_NAME* name = X509_NAME_new();
  X509_NAME_add_entry_by_txt(name,
                             "CN",
                             MBSTRING_ASC,
                             reinterpret_cast<const unsigned char*>(common_name.c_str()),
                             -1,
                             -1,
                             0);
  X509_set_subject_name(cert, name);
  X509_NAME* issuer_name =
    issuer == nullptr ? name : X509_NAME_dup(X509_get_subject_name(issuer->cert.get()));
  X509_set_issuer_name(cert, issuer_name);
  if (issuer_name != name) {
    X509_NAME_free(issuer_name);
  }
  X509_NAME_free(name);
  if (issuer == nullptr) {
    BASIC_CONSTRAINTS* constraints = BASIC_CONSTRAINTS_new();
    constraints->ca = 1;
    X509_add1_ext_i2d(cert, NID_basic_constraints, constraints, 1, 0);
    BASIC_CONSTRAINTS_free(constraints);
  }
  if (X509_sign(cert, issuer == nullptr ? id.key.get() : issuer->key.get(), EVP_sha256()) == 0) {
    fail("an ephemeral certificate is signed");
  }
  return id;
}

constexpr std::int64_t year_2000 = 946684800;
constexpr std::int64_t year_2001 = 978307200;
constexpr std::int64_t year_2099 = 4070908800;

auto
common_name_of(const X509* cert) -> std::string
{
  if (cert == nullptr) {
    return {};
  }
  const auto* subject = X509_get_subject_name(cert);
  const int index = X509_NAME_get_index_by_NID(subject, NID_commonName, -1);
  if (index < 0) {
    return {};
  }
  const auto* data = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(subject, index));
  return { reinterpret_cast<const char*>(ASN1_STRING_get0_data(data)),
           static_cast<std::size_t>(ASN1_STRING_length(data)) };
}

// Writes the certificate and key of client to PEM files in dir, which a certificate authenticator
// names by path.
auto
write_pem(const identity& client, const std::filesystem::path& dir)
  -> couchbase::core::cluster_credentials
{
  const auto name = common_name_of(client.cert.get());
  couchbase::core::cluster_credentials creds{};
  creds.certificate_path = (dir / (name + ".pem")).string();
  creds.key_path = (dir / (name + ".key")).string();
  BIO* cert = BIO_new_file(creds.certificate_path.c_str(), "w");
  BIO* key = BIO_new_file(creds.key_path.c_str(), "w");
  const bool written =
    cert != nullptr && key != nullptr && PEM_write_bio_X509(cert, client.cert.get()) == 1 &&
    PEM_write_bio_PrivateKey(key, client.key.get(), nullptr, nullptr, 0, nullptr, nullptr) == 1;
  BIO_free(cert);
  BIO_free(key);
  if (!written) {
    fail("the ephemeral certificate and key are written to " + dir.string());
  }
  return creds;
}

auto
peer_certificate(SSL* ssl) -> x509_ptr
{
#if defined(OPENSSL_IS_BORINGSSL) || OPENSSL_VERSION_NUMBER < 0x30000000L
  return x509_ptr{ SSL_get_peer_certificate(ssl) };
#else
  return x509_ptr{ SSL_get1_peer_certificate(ssl) };
#endif
}

auto
client_context(const identity& client) -> std::shared_ptr<asio::ssl::context>
{
  auto ctx = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client);
  // The server's identity is not under test; tls_hostname_verification covers it.
  ctx->set_verify_mode(asio::ssl::verify_none);
  SSL_CTX_use_certificate(ctx->native_handle(), client.cert.get());
  SSL_CTX_use_PrivateKey(ctx->native_handle(), client.key.get());
  return ctx;
}

// A client context that presents no certificate, as a session with password credentials does.
auto
anonymous_context() -> std::shared_ptr<asio::ssl::context>
{
  auto ctx = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client);
  ctx->set_verify_mode(asio::ssl::verify_none);
  return ctx;
}

// A loopback HTTPS endpoint that requires a client certificate signed by ca. Where the TLS library
// has TLS 1.3 it speaks only that, so a refused certificate reaches the client after the client's
// handshake has completed. With tls_1_2 it speaks only TLS 1.2, and refuses during the handshake.
// presented lists the common name of each accepted certificate, in order. rejected counts refused
// handshakes, and on_reject runs after each. With close_on_reject the endpoint then closes the
// connection, as a server does. answered_from is the client
// address of the connection that answered last. While hold is set, a request is answered only on
// release().
class certificate_endpoint
{
public:
  certificate_endpoint(asio::io_context& io, const identity& ca, bool tls_1_2 = false)
    : io_{ io }
    , server_{ make_identity("server", year_2000, year_2099, &ca) }
  {
    SSL_CTX_use_certificate(ctx_.native_handle(), server_.cert.get());
    SSL_CTX_use_PrivateKey(ctx_.native_handle(), server_.key.get());
    X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx_.native_handle()), ca.cert.get());
    ctx_.set_verify_mode(asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert);
    if (tls_1_2) {
      SSL_CTX_set_max_proto_version(ctx_.native_handle(), TLS1_2_VERSION);
    }
#if defined(TLS1_3_VERSION)
    else {
      SSL_CTX_set_min_proto_version(ctx_.native_handle(), TLS1_3_VERSION);
    }
#endif
    accept();
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return acceptor_.local_endpoint().port();
  }

  void close()
  {
    std::error_code ignored;
    acceptor_.close(ignored);
    for (auto& s : streams_) {
      s->lowest_layer().close(ignored);
    }
  }

  void release()
  {
    if (auto respond = std::exchange(held_, {}); respond) {
      respond();
    }
  }

  std::vector<std::string> presented{};
  std::string answered_from{};
  int rejected{ 0 };
  std::function<void()> on_reject{};
  bool close_on_reject{ false };
  bool hold{ false };
  std::function<void()> on_request{};

private:
  using stream = asio::ssl::stream<asio::ip::tcp::socket>;

  void accept()
  {
    auto s = std::make_shared<stream>(io_, ctx_);
    acceptor_.async_accept(s->lowest_layer(), [this, s](std::error_code ec) {
      if (ec) {
        return;
      }
      streams_.push_back(s);
      s->async_handshake(asio::ssl::stream_base::server, [this, s](std::error_code hs) {
        if (hs) {
          ++rejected;
          if (on_reject) {
            on_reject();
          }
          if (close_on_reject) {
            std::error_code ignored;
            s->lowest_layer().close(ignored);
          }
          return;
        }
        presented.push_back(common_name_of(peer_certificate(s->native_handle()).get()));
        serve(s);
      });
      accept();
    });
  }

  void serve(const std::shared_ptr<stream>& s)
  {
    auto buf = std::make_shared<asio::streambuf>();
    asio::async_read_until(*s, *buf, "\r\n\r\n", [this, s, buf](std::error_code ec, std::size_t) {
      if (ec) {
        return;
      }
      auto respond = [this, s]() {
        static const std::string ok{ "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" };
        std::error_code ignored;
        const auto client = s->lowest_layer().remote_endpoint(ignored);
        answered_from = client.address().to_string() + ":" + std::to_string(client.port());
        asio::async_write(*s, asio::buffer(ok), [this, s](std::error_code wec, std::size_t) {
          if (!wec) {
            serve(s);
          }
        });
      };
      if (hold) {
        held_ = respond;
      } else {
        respond();
      }
      if (on_request) {
        on_request();
      }
    });
  }

  asio::io_context& io_;
  identity server_;
  asio::ssl::context ctx_{ asio::ssl::context::tls_server };
  asio::ip::tcp::acceptor acceptor_{ io_,
                                     asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"),
                                                              0 } };
  std::list<std::shared_ptr<stream>> streams_{};
  std::function<void()> held_{};
};

void
run_until(asio::io_context& io, const std::function<bool()>& done, std::chrono::milliseconds budget)
{
  const auto deadline = std::chrono::steady_clock::now() + scaled_budget(budget);
  while (!done() && std::chrono::steady_clock::now() < deadline) {
    io.restart();
    io.run_one_for(10ms);
  }
}

// Points manager at port for the query service.
void
serve_query_from(couchbase::core::io::http_session_manager& manager,
                 std::uint16_t port,
                 const couchbase::core::cluster_options& options)
{
  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.hostname = "127.0.0.1";
  node.services_tls.query = port;
  config.nodes.push_back(node);
  manager.set_configuration(config, options);
}

// Sends a query-service request through manager and waits for its completion.
auto
request(asio::io_context& io,
        couchbase::core::io::http_session_manager& manager,
        std::chrono::milliseconds timeout = 2s) -> std::optional<std::error_code>
{
  std::optional<std::error_code> result{};
  couchbase::core::operations::http_noop_request req{};
  req.type = couchbase::core::service_type::query;
  req.timeout = timeout;
  manager.execute(req, [&result](couchbase::core::operations::http_noop_response&& resp) {
    result = resp.ctx.ec;
  });
  run_until(
    io,
    [&result]() {
      return result.has_value();
    },
    timeout + 2s);
  return result;
}

auto
certificate_credentials() -> couchbase::core::cluster_credentials
{
  couchbase::core::cluster_credentials creds{};
  creds.certificate_path = "client-a";
  return creds;
}

// A real http_session_manager with TLS enabled, pointed at endpoint for the query service.
struct rotation_fixture {
  asio::io_context io{};
  identity ca{ make_identity("rotation-test-ca", year_2000, year_2099, nullptr) };
  certificate_endpoint endpoint{ io, ca };
  identity client_a{ make_identity("client-a", year_2000, year_2099, &ca) };
  identity client_b{ make_identity("client-b", year_2000, year_2099, &ca) };
  identity client_expired{ make_identity("client-expired", year_2000, year_2001, &ca) };
  couchbase::core::tls_context_provider tls{ client_context(client_a) };
  couchbase::core::cluster_options options{};
  std::optional<couchbase::core::origin> origin{};
  std::shared_ptr<couchbase::core::io::http_session_manager> manager{};
  // The last completion's last_dispatched_from.
  std::string dispatched_from{};

  explicit rotation_fixture(couchbase::core::cluster_credentials creds = certificate_credentials(),
                            bool tls_1_2 = false)
    : endpoint{ io, ca, tls_1_2 }
  {
    options.enable_tls = true;
    options.network = "default";
    options.idle_http_connection_timeout = 30s;
    origin.emplace(creds, "127.0.0.1", endpoint.port(), options);

    manager =
      std::make_shared<couchbase::core::io::http_session_manager>("client-id", io, tls, *origin);
    auto labels = std::make_shared<couchbase::core::cluster_label_listener>();
    manager->set_tracer(couchbase::core::tracing::tracer_wrapper::create(
      std::make_shared<couchbase::core::tracing::noop_tracer>(), labels));
    manager->set_meter(couchbase::core::metrics::meter_wrapper::create(
      std::make_shared<couchbase::core::metrics::noop_meter>(), labels));
    manager->set_app_telemetry_meter(std::make_shared<couchbase::core::app_telemetry_meter>());

    serve_query_from(*manager, endpoint.port(), options);
  }

  // What cluster::update_credentials does for a certificate authenticator.
  void rotate_to(const identity& client)
  {
    tls.set_ctx(client_context(client));
    manager->retire_sessions();
  }

  void run_until(const std::function<bool()>& done, std::chrono::milliseconds budget)
  {
    test::run_until(io, done, budget);
  }

  auto request(std::chrono::milliseconds timeout = 2s) -> std::optional<std::error_code>
  {
    return test::request(io, *manager, timeout);
  }

  void send(std::chrono::milliseconds timeout, std::function<void(std::error_code)> on_done)
  {
    couchbase::core::operations::http_noop_request req{};
    req.type = couchbase::core::service_type::query;
    req.timeout = timeout;
    manager->execute(
      req,
      [this, on_done = std::move(on_done)](couchbase::core::operations::http_noop_response&& resp) {
        dispatched_from = resp.ctx.last_dispatched_from.value_or("");
        on_done(resp.ctx.ec);
      });
  }

  ~rotation_fixture()
  {
    bool stopped = false;
    manager->close([&stopped]() {
      stopped = true;
    });
    endpoint.close();
    run_until(
      [&stopped]() {
        return stopped;
      },
      2s);
  }
};

auto
names(std::initializer_list<const char*> list) -> std::vector<std::string>
{
  return { list.begin(), list.end() };
}

// An idle session opened with the previous certificate is not reused once the certificate changes.
void
a_request_after_rotation_presents_the_new_certificate([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  const auto first = f.request();
  assert_true(first.has_value(), "the first request completes");
  assert_success(*first, "the first request succeeds");

  f.rotate_to(f.client_b);
  const auto second = f.request();
  assert_true(second.has_value(), "the request after rotation completes");
  assert_success(*second, "the request after rotation succeeds");
  assert_true(f.endpoint.presented == names({ "client-a", "client-b" }),
              "the request after rotation is sent on a new connection with the new certificate");
}

// A session in use at rotation finishes its request and is then not returned to the pool.
void
a_session_busy_at_rotation_finishes_and_is_not_reused([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  f.endpoint.hold = true;
  std::optional<std::error_code> in_flight{};
  f.endpoint.on_request = [&f]() {
    f.endpoint.on_request = {};
    f.endpoint.hold = false;
    f.rotate_to(f.client_b);
    f.endpoint.release();
  };
  f.send(2s, [&in_flight](std::error_code ec) {
    in_flight = ec;
  });
  f.run_until(
    [&in_flight]() {
      return in_flight.has_value();
    },
    4s);
  assert_true(in_flight.has_value(), "the request in flight at rotation completes");
  assert_success(*in_flight, "the request in flight at rotation succeeds");

  const auto next = f.request();
  assert_true(next.has_value(), "the next request completes");
  assert_success(*next, "the next request succeeds");
  assert_true(f.endpoint.presented == names({ "client-a", "client-b" }),
              "the session busy at rotation is not reused for the next request");
}

// Regression: retire_sessions() took the idle sessions off every list before their posted stop()
// ran, so a close() right after it had nothing to wait for and completed with those stops queued.
void
a_close_right_after_rotation_waits_for_the_retired_session([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  const auto first = f.request();
  assert_true(first.has_value(), "the first request completes");
  assert_success(*first, "the first request succeeds");

  bool stopped = false;
  f.rotate_to(f.client_b);
  f.manager->close([&stopped]() {
    stopped = true;
  });
  assert_false(stopped, "close() does not complete while the retired session's stop is queued");
  f.run_until(
    [&stopped]() {
      return stopped;
    },
    2s);
  assert_true(stopped, "close() completes once the retired session has stopped");
}

// Regression: check_out() popped a retired session off the idle list and dropped it, so a close()
// that followed had nothing to wait for while that session's stop was still queued.
void
a_close_after_a_checkout_skips_a_retired_session_still_waits_for_it([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  const auto first = f.request();
  assert_true(first.has_value(), "the first request completes");
  assert_success(*first, "the first request succeeds");

  f.rotate_to(f.client_b);
  auto [ec, fresh] = f.manager->check_out(couchbase::core::service_type::query, {});
  assert_success(ec, "check_out succeeds after the rotation");
  // Unconnected, so check_in only unlists it, and close() has only the retired session to wait for.
  f.manager->check_in(couchbase::core::service_type::query, fresh);

  bool stopped = false;
  f.manager->close([&stopped]() {
    stopped = true;
  });
  assert_false(stopped, "close() does not complete while the retired session's stop is queued");
  f.run_until(
    [&stopped]() {
      return stopped;
    },
    2s);
  assert_true(stopped, "close() completes once the retired session has stopped");
}

// A session still connecting at rotation completes its request and is then not reused.
void
a_session_connecting_at_rotation_completes_its_request([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  std::optional<std::error_code> connecting{};
  f.send(2s, [&connecting](std::error_code ec) {
    connecting = ec;
  });
  f.rotate_to(f.client_b);
  f.run_until(
    [&connecting]() {
      return connecting.has_value();
    },
    4s);
  assert_true(connecting.has_value(), "the request connecting at rotation completes");
  assert_success(*connecting, "the request connecting at rotation succeeds");

  const auto next = f.request();
  assert_true(next.has_value(), "the next request completes");
  assert_success(*next, "the next request succeeds");
  assert_true(f.endpoint.presented == names({ "client-a", "client-b" }),
              "the session connecting at rotation is not reused for the next request");
}

// Removes the directory on destruction, also when an assertion has failed.
struct temporary_directory {
  explicit temporary_directory(std::filesystem::path p)
    : path{ std::move(p) }
  {
    std::filesystem::create_directories(path);
  }
  temporary_directory(const temporary_directory&) = delete;
  auto operator=(const temporary_directory&) -> temporary_directory& = delete;
  ~temporary_directory()
  {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }

  std::filesystem::path path;
};

// A core::cluster opened with client_a, with its HTTP session manager pointed at endpoint for the
// query service. Its KV port accepts connections and never answers, so the cluster stays open,
// bootstrapping.
struct cluster_fixture {
  asio::io_context io{};
  identity ca{ make_identity("rotation-test-ca", year_2000, year_2099, nullptr) };
  certificate_endpoint endpoint{ io, ca };
  asio::ip::tcp::acceptor silent_kv{ io, { asio::ip::make_address("127.0.0.1"), 0 } };
  temporary_directory dir{ std::filesystem::temp_directory_path() /
                           ("client_certificate_rotation_" + std::to_string(endpoint.port())) };
  couchbase::core::cluster_credentials client_a{
    write_pem(make_identity("client-a", year_2000, year_2099, &ca), dir.path)
  };
  couchbase::core::cluster_credentials client_b{
    write_pem(make_identity("client-b", year_2000, year_2099, &ca), dir.path)
  };
  couchbase::core::cluster_credentials client_expired{
    write_pem(make_identity("client-expired", year_2000, year_2001, &ca), dir.path)
  };
  couchbase::core::cluster cluster{ io };
  std::shared_ptr<couchbase::core::io::http_session_manager> manager{};

  cluster_fixture()
  {
    couchbase::core::cluster_options options{};
    options.enable_tls = true;
    options.tls_verify = couchbase::core::tls_verify_mode::none;
    options.network = "default";
    options.idle_http_connection_timeout = 30s;
    options.enable_dns_srv = false;
    cluster.open({ client_a, "127.0.0.1", silent_kv.local_endpoint().port(), options },
                 [](std::error_code) {
                 });
    std::error_code ec;
    std::tie(ec, manager) = cluster.http_session_manager();
    assert_success(ec, "the opening cluster has a session manager");
    serve_query_from(*manager, endpoint.port(), options);
  }

  ~cluster_fixture()
  {
    bool closed = false;
    cluster.close([&closed]() {
      closed = true;
    });
    endpoint.close();
    run_until(
      io,
      [&closed]() {
        return closed;
      },
      4s);
  }

  auto request() -> std::optional<std::error_code>
  {
    return test::request(io, *manager);
  }
};

// cluster::update_credentials() with a certificate authenticator retires the idle HTTP sessions.
void
update_credentials_retires_the_idle_sessions([[maybe_unused]] context& ctx)
{
  cluster_fixture f;
  const auto first = f.request();
  assert_true(first.has_value(), "the first request completes");
  assert_success(*first, "the first request succeeds");
  assert_success(f.cluster.update_credentials(f.client_b).ec, "the credentials are updated");
  const auto second = f.request();
  assert_true(second.has_value(), "the request after the update completes");
  assert_success(*second, "the request after the update succeeds");
  assert_true(f.endpoint.presented == names({ "client-a", "client-b" }),
              "the request after the update is sent on a new connection with the new certificate");
}

// Regression: close() moved session_manager_ out on the io thread while update_credentials() used
// it on the caller's thread, and update_credentials() dereferenced the moved-out manager.
// update_credentials() replaces the origin's credentials just after its closed check, so close()
// starts while the update is past that check and still building the TLS context.
void
update_credentials_racing_close_completes([[maybe_unused]] context& ctx)
{
  for (int i = 0; i < 20; ++i) {
    cluster_fixture f;
    std::error_code result{};
    std::thread updater([&f, &result]() {
      result = f.cluster.update_credentials(f.client_b).ec;
    });
    while (f.cluster.origin().second.credentials().certificate_path !=
           f.client_b.certificate_path) {
      std::this_thread::yield();
    }
    bool closed = false;
    f.cluster.close([&closed]() {
      closed = true;
    });
    run_until(
      f.io,
      [&closed]() {
        return closed;
      },
      4s);
    updater.join();
    assert_true(closed, "close() completes while update_credentials() runs");
    assert_success(result, "update_credentials() that got past its closed check completes");
  }
}

// A certificate the server refuses fails requests with a timeout rather than an immediate error,
// and rotating to a valid certificate recovers.
void
a_rejected_certificate_retries_until_the_request_times_out([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  const auto first = f.request();
  assert_true(first.has_value(), "the first request completes");
  assert_success(*first, "the first request succeeds");

  f.rotate_to(f.client_expired);
  const auto refused = f.request(scaled_budget(2s));
  assert_true(refused.has_value(), "the request with the expired certificate completes");
  assert_true(*refused == couchbase::errc::common::unambiguous_timeout ||
                *refused == couchbase::errc::common::ambiguous_timeout,
              "the request with the expired certificate ends in a timeout");
  assert_true(f.endpoint.rejected > 1, "the session keeps reconnecting while the server refuses");

  f.rotate_to(f.client_b);
  const auto recovered = f.request();
  assert_true(recovered.has_value(), "the request after rotating to a valid certificate completes");
  assert_success(*recovered, "the request after rotating to a valid certificate succeeds");
  assert_eq(f.endpoint.presented.back(),
            std::string{ "client-b" },
            "the request after recovery presents the valid certificate");
}

// Under TLS 1.2 the server refuses the certificate during the handshake, and the connect is
// retried. The request still ends in a timeout rather than an immediate error, and rotating to a
// valid certificate recovers.
void
a_certificate_refused_in_the_handshake_retries_until_the_request_times_out(
  [[maybe_unused]] context& ctx)
{
  rotation_fixture f{ certificate_credentials(), true };
  f.rotate_to(f.client_expired);
  const auto refused = f.request(scaled_budget(2s));
  assert_true(refused.has_value(), "the request with the expired certificate completes");
  assert_true(*refused == couchbase::errc::common::unambiguous_timeout ||
                *refused == couchbase::errc::common::ambiguous_timeout,
              "the request with the expired certificate ends in a timeout");
  assert_true(f.endpoint.rejected > 1, "the connect is retried while the server refuses");

  f.rotate_to(f.client_b);
  const auto recovered = f.request();
  assert_true(recovered.has_value(), "the request after rotating to a valid certificate completes");
  assert_success(*recovered, "the request after rotating to a valid certificate succeeds");
  assert_eq(f.endpoint.presented.back(),
            std::string{ "client-b" },
            "the request after recovery presents the valid certificate");
}

// A server that closes the connection after refusing the certificate leaves its alert to be read
// first, so the request is resent rather than failed at once.
void
a_refusal_followed_by_a_close_retries_until_the_request_times_out([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  f.endpoint.close_on_reject = true;
  f.rotate_to(f.client_expired);
  const auto refused = f.request(scaled_budget(2s));
  assert_true(refused.has_value(), "the request with the expired certificate completes");
  assert_true(*refused == couchbase::errc::common::unambiguous_timeout ||
                *refused == couchbase::errc::common::ambiguous_timeout,
              "the request with the expired certificate ends in a timeout");
  assert_true(f.endpoint.rejected > 1, "the session keeps reconnecting while the server refuses");
}

// A request that keeps being refused picks up a rotation to a valid certificate and succeeds.
void
a_refused_request_succeeds_once_the_certificate_is_rotated([[maybe_unused]] context& ctx)
{
  rotation_fixture f;
  f.rotate_to(f.client_expired);
  f.endpoint.on_reject = [&f]() {
    f.endpoint.on_reject = {};
    f.rotate_to(f.client_b);
  };
  std::optional<std::error_code> result{};
  f.send(scaled_budget(4s), [&result](std::error_code ec) {
    result = ec;
  });
  f.run_until(
    [&result]() {
      return result.has_value();
    },
    6s);
  assert_true(result.has_value(), "the request completes");
  assert_success(*result, "the request that was refused succeeds after the rotation");
  assert_true(f.endpoint.rejected > 0, "the server refused the expired certificate first");
  assert_true(f.endpoint.presented == std::vector<std::string>{ "client-b" },
              "the request is answered on a connection that presents the rotated certificate");
  assert_eq(f.dispatched_from,
            f.endpoint.answered_from,
            "the request reports the connection that answered it, not the refused one");
}

#if defined(TLS1_3_VERSION)
// A session with password credentials has no certificate that a rotation could replace, so a
// refusal fails the request at once. Below TLS 1.3 the refusal fails the handshake instead, and
// the connect is retried until the deadline.
void
a_refused_session_without_a_certificate_is_not_resent([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "password";
  rotation_fixture f{ creds };
  f.tls.set_ctx(anonymous_context());
  const auto result = f.request(scaled_budget(2s));
  assert_true(result.has_value(), "the request completes");
  assert_true(*result && *result != couchbase::errc::common::unambiguous_timeout &&
                *result != couchbase::errc::common::ambiguous_timeout,
              "the request fails without waiting for its deadline");
  assert_eq(f.endpoint.rejected, 1, "the refused request is not sent again");
}
#endif

// A free-form request refused for its certificate is sent again with the certificate rotated by
// cluster::update_credentials(), and reports the connection that answered it.
void
a_refused_free_form_request_succeeds_once_the_certificate_is_rotated([[maybe_unused]] context& ctx)
{
  for (const bool streamed : { false, true }) {
    const std::string label = streamed ? "streamed: " : "buffered: ";
    cluster_fixture f;
    assert_success(f.cluster.update_credentials(f.client_expired).ec,
                   label + "the credentials are updated");
    std::error_code rotated{};
    f.endpoint.on_reject = [&f, &rotated]() {
      f.endpoint.on_reject = {};
      rotated = f.cluster.update_credentials(f.client_b).ec;
    };
    couchbase::core::http_component component{ f.io, couchbase::core::core_sdk_shim{ f.cluster } };
    couchbase::core::http_request request{};
    request.service = couchbase::core::service_type::query;
    request.method = "GET";
    request.path = "/";
    request.timeout = scaled_budget(2s);
    std::optional<std::error_code> result{};
    auto op =
      streamed
        ? component.do_http_request(request,
                                    [&result](couchbase::core::http_response, std::error_code ec) {
                                      result = ec;
                                    })
        : component.do_http_request_buffered(
            request, [&result](couchbase::core::buffered_http_response, std::error_code ec) {
              result = ec;
            });
    assert_true(op.has_value(), label + "the request is dispatched");
    run_until(
      f.io,
      [&result]() {
        return result.has_value();
      },
      3s);
    assert_true(result.has_value(), label + "the request completes");
    assert_success(rotated, label + "the credentials are rotated on the refusal");
    assert_success(*result, label + "the request that was refused succeeds after the rotation");
    assert_true(f.endpoint.rejected > 0,
                label + "the server refused the expired certificate first");
    assert_true(f.endpoint.presented == names({ "client-b" }),
                label + "the request is answered on a connection that presents client-b");
    const auto info =
      std::dynamic_pointer_cast<couchbase::core::pending_operation_connection_info>(*op);
    assert_true(info != nullptr, label + "the operation reports its connection");
    assert_eq(info->dispatched_from(),
              f.endpoint.answered_from,
              label + "the request reports the connection that answered it, not the refused one");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_request_after_rotation_presents_the_new_certificate), {}, timeout::network },
      { CASE(a_session_busy_at_rotation_finishes_and_is_not_reused), {}, timeout::network },
      { CASE(a_session_connecting_at_rotation_completes_its_request), {}, timeout::network },
      { CASE(a_close_right_after_rotation_waits_for_the_retired_session), {}, timeout::network },
      { CASE(a_close_after_a_checkout_skips_a_retired_session_still_waits_for_it),
        {},
        timeout::network },
      { CASE(update_credentials_retires_the_idle_sessions), {}, timeout::network },
      { CASE(update_credentials_racing_close_completes), {}, timeout::network },
      // Two requests and one that waits out its timeout.
      { CASE(a_rejected_certificate_retries_until_the_request_times_out), {}, timeout::slow },
      { CASE(a_certificate_refused_in_the_handshake_retries_until_the_request_times_out),
        {},
        timeout::slow },
      { CASE(a_refusal_followed_by_a_close_retries_until_the_request_times_out),
        {},
        timeout::slow },
      { CASE(a_refused_request_succeeds_once_the_certificate_is_rotated), {}, timeout::network },
      { CASE(a_refused_free_form_request_succeeds_once_the_certificate_is_rotated),
        {},
        timeout::network },
#if defined(TLS1_3_VERSION)
      { CASE(a_refused_session_without_a_certificate_is_not_resent), {}, timeout::network },
#endif
    },
  };
}

} // namespace couchbase::test
