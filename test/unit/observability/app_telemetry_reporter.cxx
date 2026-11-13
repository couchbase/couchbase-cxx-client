/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2026-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "framework/test_registry.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/app_telemetry_reporter.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/crypto/cbcrypto.h"
#include "core/origin.hxx"
#include "core/platform/base64.h"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/connection_string.hxx"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#include <spdlog/fmt/bundled/core.h>

#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::test
{
template<typename T>
struct operand_printer<std::optional<T>, std::enable_if_t<operand_printer<T>::available>> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const std::optional<T>& value) -> std::string
  {
    if (!value.has_value()) {
      return "none";
    }
    return operand_printer<T>::to_text(*value);
  }
};

namespace
{
using namespace std::chrono_literals;

struct captured_exchange {
  std::string request_line{};
  std::map<std::string, std::string> headers{};
  std::optional<std::uint8_t> telemetry_status{};
  std::optional<std::string> telemetry_report{};
};

auto
lowercase_ascii(std::string_view input) -> std::string
{
  std::string out;
  out.reserve(input.size());
  for (const char ch : input) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

auto
trim_http_whitespace(std::string_view input) -> std::string
{
  while (!input.empty() && (input.front() == ' ' || input.front() == '\t')) {
    input.remove_prefix(1);
  }
  while (!input.empty() && (input.back() == ' ' || input.back() == '\t')) {
    input.remove_suffix(1);
  }
  return std::string{ input };
}

auto
websocket_accept_key(std::string_view session_key) -> std::string
{
  std::string salted{ session_key };
  salted.append("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
  return couchbase::core::base64::encode(
    couchbase::core::crypto::digest(couchbase::core::crypto::Algorithm::ALG_SHA1, salted), false);
}

// Loopback WebSocket telemetry collector that records the upgrade request, completes the RFC 6455
// handshake, sends a GET_TELEMETRY command once the client session is ready, and records the binary
// telemetry report returned by app_telemetry_reporter.
class telemetry_collector
{
public:
  telemetry_collector(asio::io_context& io,
                      captured_exchange& result,
                      std::function<void()> on_complete)
    : acceptor_{ io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 } }
    , socket_{ io }
    , result_{ result }
    , on_complete_{ std::move(on_complete) }
  {
    acceptor_.async_accept(socket_, [this](std::error_code ec) {
      if (ec) {
        return;
      }
      read_handshake();
    });
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return acceptor_.local_endpoint().port();
  }

  void close()
  {
    std::error_code ignored;
    acceptor_.close(ignored);
    socket_.close(ignored);
  }

private:
  void finish()
  {
    if (const auto cb = std::exchange(on_complete_, nullptr); cb) {
      cb();
    }
  }

  void read_handshake()
  {
    asio::async_read_until(
      socket_, handshake_buffer_, "\r\n\r\n", [this](std::error_code ec, std::size_t) {
        if (ec) {
          finish();
          return;
        }
        const auto data = handshake_buffer_.data();
        const std::string raw{ static_cast<const char*>(data.data()), data.size() };
        const auto header_end = raw.find("\r\n\r\n");
        if (header_end == std::string::npos) {
          finish();
          return;
        }

        parse_request(std::string_view{ raw }.substr(0, header_end));
        const auto consumed = header_end + 4;
        for (std::size_t i = consumed; i < raw.size(); ++i) {
          frame_buffer_.push_back(static_cast<std::uint8_t>(raw[i]));
        }
        handshake_buffer_.consume(raw.size());

        const auto key_it = result_.headers.find("sec-websocket-key");
        if (key_it == result_.headers.end()) {
          finish();
          return;
        }

        handshake_response_ = fmt::format("HTTP/1.1 101 Switching Protocols\r\n"
                                          "Upgrade: websocket\r\n"
                                          "Connection: Upgrade\r\n"
                                          "Sec-WebSocket-Accept: {}\r\n"
                                          "\r\n",
                                          websocket_accept_key(key_it->second));
        asio::async_write(socket_,
                          asio::buffer(handshake_response_),
                          [this](std::error_code write_ec, std::size_t) {
                            if (write_ec) {
                              finish();
                              return;
                            }
                            process_frames();
                          });
      });
  }

  void parse_request(std::string_view header_block)
  {
    const auto first_eol = header_block.find("\r\n");
    result_.request_line = std::string{ header_block.substr(0, first_eol) };
    if (first_eol == std::string_view::npos) {
      return;
    }

    auto rest = header_block.substr(first_eol + 2);
    while (!rest.empty()) {
      const auto eol = rest.find("\r\n");
      const auto line = eol == std::string_view::npos ? rest : rest.substr(0, eol);
      if (const auto colon = line.find(':'); colon != std::string_view::npos) {
        result_.headers[lowercase_ascii(line.substr(0, colon))] =
          trim_http_whitespace(line.substr(colon + 1));
      }
      if (eol == std::string_view::npos) {
        break;
      }
      rest.remove_prefix(eol + 2);
    }
  }

  void read_more_frames()
  {
    socket_.async_read_some(
      asio::buffer(read_chunk_), [this](std::error_code ec, std::size_t bytes_transferred) {
        if (ec) {
          finish();
          return;
        }
        frame_buffer_.insert(frame_buffer_.end(),
                             read_chunk_.begin(),
                             read_chunk_.begin() + static_cast<std::ptrdiff_t>(bytes_transferred));
        process_frames();
      });
  }

  void process_frames()
  {
    while (frame_buffer_.size() >= 2) {
      const std::uint8_t opcode = frame_buffer_[0] & 0x0fU;
      const bool masked = (frame_buffer_[1] & 0x80U) != 0;
      const std::uint8_t len7 = frame_buffer_[1] & 0x7fU;

      std::size_t header_len = 2;
      std::size_t payload_len = len7;
      if (len7 == 126) {
        if (frame_buffer_.size() < 4) {
          break;
        }
        header_len = 4;
        payload_len = (static_cast<std::size_t>(frame_buffer_[2]) << 8U) |
                      static_cast<std::size_t>(frame_buffer_[3]);
      } else if (len7 == 127) {
        if (frame_buffer_.size() < 10) {
          break;
        }
        header_len = 10;
        payload_len = 0;
        for (std::size_t i = 2; i < 10; ++i) {
          payload_len = (payload_len << 8U) | static_cast<std::size_t>(frame_buffer_[i]);
        }
      }

      const std::size_t mask_len = masked ? 4 : 0;
      const std::size_t total_len = header_len + mask_len + payload_len;
      if (frame_buffer_.size() < total_len) {
        break;
      }

      std::vector<std::uint8_t> payload(
        frame_buffer_.begin() + static_cast<std::ptrdiff_t>(header_len + mask_len),
        frame_buffer_.begin() + static_cast<std::ptrdiff_t>(total_len));
      if (masked) {
        for (std::size_t i = 0; i < payload.size(); ++i) {
          payload[i] ^= frame_buffer_[header_len + (i % 4)];
        }
      }
      frame_buffer_.erase(frame_buffer_.begin(),
                          frame_buffer_.begin() + static_cast<std::ptrdiff_t>(total_len));

      if (opcode == 0x09U && !telemetry_requested_) {
        telemetry_requested_ = true;
        // Unmasked server-to-client binary frame (0x82, len=1) carrying GET_TELEMETRY (0x00).
        static constexpr std::array<std::uint8_t, 3> get_telemetry_frame{
          0x82U,
          0x01U,
          0x00U,
        };
        asio::async_write(socket_,
                          asio::buffer(get_telemetry_frame),
                          [this](std::error_code write_ec, std::size_t) {
                            if (write_ec) {
                              finish();
                              return;
                            }
                            process_frames();
                          });
        return;
      }

      if (opcode == 0x02U) {
        if (!payload.empty()) {
          result_.telemetry_status = payload[0];
          result_.telemetry_report.emplace(reinterpret_cast<const char*>(payload.data() + 1),
                                           payload.size() - 1);
        }
        finish();
        return;
      }
    }
    read_more_frames();
  }

  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket socket_;
  asio::streambuf handshake_buffer_{};
  std::string handshake_response_{};
  std::array<std::uint8_t, 512> read_chunk_{};
  std::vector<std::uint8_t> frame_buffer_{};
  bool telemetry_requested_{ false };
  captured_exchange& result_;
  std::function<void()> on_complete_;
};

auto
header_value(const captured_exchange& exchange, const std::string& lower_name)
  -> std::optional<std::string>
{
  const auto it = exchange.headers.find(lower_name);
  if (it == exchange.headers.end()) {
    return std::nullopt;
  }
  return it->second;
}

void
assert_report_contains(const std::optional<std::string>& report,
                       std::string_view needle,
                       std::string_view message,
                       source_location loc = source_location::current())
{
  if (!report.has_value()) {
    fail("a telemetry report payload is delivered", loc);
  }
  assert_contains(*report, needle, message, loc);
}

auto
cluster_config_with_collector(std::uint16_t management_port, std::string path)
  -> couchbase::core::topology::configuration
{
  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.hostname = "127.0.0.1";
  node.node_uuid = "node-1";
  node.services_plain.management = management_port;
  node.app_telemetry_path = std::move(path);
  config.nodes.push_back(std::move(node));
  return config;
}

auto
collect_telemetry(
  const std::function<couchbase::core::origin(std::uint16_t)>& make_origin,
  const std::function<couchbase::core::topology::configuration(std::uint16_t)>& make_config)
  -> captured_exchange
{
  captured_exchange result{};
  asio::io_context io{};
  asio::steady_timer give_up{ io };
  const auto meter = std::make_shared<couchbase::core::app_telemetry_meter>();
  couchbase::core::tls_context_provider tls{};

  std::unique_ptr<couchbase::core::app_telemetry_reporter> reporter{};
  telemetry_collector collector{ io, result, [&]() {
                                  give_up.cancel();
                                  if (reporter) {
                                    reporter->stop();
                                  }
                                  collector.close();
                                } };

  auto origin = make_origin(collector.port());
  reporter = std::make_unique<couchbase::core::app_telemetry_reporter>(meter, origin, io, tls);
  reporter->update_config(make_config(collector.port()));
  meter->value_recorder("node-1", "default")
    ->update_counter(couchbase::core::app_telemetry_counter::kv_r_total);

  give_up.expires_after(4s);
  give_up.async_wait([&](std::error_code ec) {
    if (ec != asio::error::operation_aborted) {
      if (reporter) {
        reporter->stop();
      }
      collector.close();
      io.stop();
    }
  });
  io.run();

  reporter->stop();
  collector.close();
  give_up.cancel();
  io.restart();
  io.run_for(1s);
  return result;
}

auto
password_credentials() -> couchbase::core::cluster_credentials
{
  couchbase::core::cluster_credentials creds{};
  creds.username = "alice";
  creds.password = "secret";
  return creds;
}

auto
jwt_credentials() -> couchbase::core::cluster_credentials
{
  couchbase::core::cluster_credentials creds{};
  creds.jwt_token = "eyJhbGciOiJFUzI1NiJ9.payload.signature";
  return creds;
}

void
user_set_endpoint_with_password_credentials_omits_authorization_and_delivers_telemetry(
  context& /* ctx */)
{
  const auto result = collect_telemetry(
    [](std::uint16_t port) {
      couchbase::core::cluster_options options{};
      options.enable_app_telemetry = true;
      options.app_telemetry_endpoint = fmt::format("ws://127.0.0.1:{}/collect", port);
      return couchbase::core::origin{ password_credentials(), "127.0.0.1", 11210, options };
    },
    [](std::uint16_t /* port */) {
      return cluster_config_with_collector(18091, "/_p/appTelemetry");
    });

  assert_eq(result.request_line,
            std::string{ "GET /collect HTTP/1.1" },
            "the client connects to the user-set path");
  assert_eq(header_value(result, "authorization"),
            std::optional<std::string>{},
            "no Authorization header is sent to a user-set endpoint for password credentials");
  assert_eq(result.telemetry_status,
            std::optional<std::uint8_t>{ 0 },
            "GET_TELEMETRY succeeds on the unauthenticated user-set endpoint");
  assert_report_contains(result.telemetry_report,
                         "sdk_kv_r_total{",
                         "recorded metrics reach the unauthenticated user-set endpoint");
}

void
user_set_endpoint_with_jwt_credentials_omits_authorization_and_delivers_telemetry(
  context& /* ctx */)
{
  const auto result = collect_telemetry(
    [](std::uint16_t port) {
      couchbase::core::cluster_options options{};
      options.enable_app_telemetry = true;
      options.app_telemetry_endpoint = fmt::format("ws://127.0.0.1:{}/collect", port);
      return couchbase::core::origin{ jwt_credentials(), "127.0.0.1", 11210, options };
    },
    [](std::uint16_t /* port */) {
      return cluster_config_with_collector(18091, "/_p/appTelemetry");
    });

  assert_eq(result.request_line,
            std::string{ "GET /collect HTTP/1.1" },
            "the client connects to the user-set path");
  assert_eq(header_value(result, "authorization"),
            std::optional<std::string>{},
            "no Authorization header is sent to a user-set endpoint for JWT credentials");
  assert_eq(result.telemetry_status,
            std::optional<std::uint8_t>{ 0 },
            "GET_TELEMETRY succeeds on the unauthenticated user-set endpoint");
  assert_report_contains(result.telemetry_report,
                         "sdk_kv_r_total{",
                         "recorded metrics reach the unauthenticated user-set endpoint");
}

void
connection_string_endpoint_omits_authorization_and_delivers_telemetry(context& /* ctx */)
{
  const auto result = collect_telemetry(
    [](std::uint16_t port) {
      const auto connstr = couchbase::core::utils::parse_connection_string(fmt::format(
        "couchbase://127.0.0.1?app_telemetry_endpoint=ws://127.0.0.1:{}/collect", port));
      return couchbase::core::origin{ password_credentials(), connstr };
    },
    [](std::uint16_t /* port */) {
      return cluster_config_with_collector(18091, "/_p/appTelemetry");
    });

  assert_eq(result.request_line,
            std::string{ "GET /collect HTTP/1.1" },
            "the client connects to the connection-string endpoint");
  assert_eq(header_value(result, "authorization"),
            std::optional<std::string>{},
            "no Authorization header is sent to a connection-string app_telemetry_endpoint");
  assert_eq(result.telemetry_status,
            std::optional<std::uint8_t>{ 0 },
            "GET_TELEMETRY succeeds on the unauthenticated connection-string endpoint");
  assert_report_contains(result.telemetry_report,
                         "sdk_kv_r_total{",
                         "recorded metrics reach the connection-string endpoint");
}

void
cluster_config_endpoint_with_password_credentials_sends_basic_authorization(context& /* ctx */)
{
  const auto result = collect_telemetry(
    [](std::uint16_t /* port */) {
      couchbase::core::cluster_options options{};
      options.enable_app_telemetry = true;
      return couchbase::core::origin{ password_credentials(), "127.0.0.1", 11210, options };
    },
    [](std::uint16_t port) {
      return cluster_config_with_collector(port, "/_p/appTelemetry");
    });

  assert_eq(result.request_line,
            std::string{ "GET /_p/appTelemetry HTTP/1.1" },
            "the client connects to the cluster-configured path");
  assert_eq(header_value(result, "authorization"),
            std::optional<std::string>{ "Basic YWxpY2U6c2VjcmV0" },
            "a cluster-configured endpoint receives Basic authorization");
  assert_eq(result.telemetry_status,
            std::optional<std::uint8_t>{ 0 },
            "GET_TELEMETRY succeeds on the cluster-configured endpoint");
  assert_report_contains(result.telemetry_report,
                         "sdk_kv_r_total{",
                         "recorded metrics reach the cluster-configured endpoint");
}

void
cluster_config_endpoint_with_jwt_credentials_sends_bearer_authorization(context& /* ctx */)
{
  const auto result = collect_telemetry(
    [](std::uint16_t /* port */) {
      couchbase::core::cluster_options options{};
      options.enable_app_telemetry = true;
      return couchbase::core::origin{ jwt_credentials(), "127.0.0.1", 11210, options };
    },
    [](std::uint16_t port) {
      return cluster_config_with_collector(port, "/_p/appTelemetry");
    });

  assert_eq(result.request_line,
            std::string{ "GET /_p/appTelemetry HTTP/1.1" },
            "the client connects to the cluster-configured path");
  assert_eq(header_value(result, "authorization"),
            std::optional<std::string>{ "Bearer eyJhbGciOiJFUzI1NiJ9.payload.signature" },
            "a cluster-configured endpoint receives Bearer authorization");
  assert_eq(result.telemetry_status,
            std::optional<std::uint8_t>{ 0 },
            "GET_TELEMETRY succeeds on the cluster-configured endpoint");
  assert_report_contains(result.telemetry_report,
                         "sdk_kv_r_total{",
                         "recorded metrics reach the cluster-configured endpoint");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(
        user_set_endpoint_with_password_credentials_omits_authorization_and_delivers_telemetry) },
      { CASE(user_set_endpoint_with_jwt_credentials_omits_authorization_and_delivers_telemetry) },
      { CASE(connection_string_endpoint_omits_authorization_and_delivers_telemetry) },
      { CASE(cluster_config_endpoint_with_password_credentials_sends_basic_authorization) },
      { CASE(cluster_config_endpoint_with_jwt_credentials_sends_bearer_authorization) },
    },
  };
}

} // namespace couchbase::test
