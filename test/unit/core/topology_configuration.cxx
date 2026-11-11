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
#include "utils/topology_fixtures.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/bucket.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/document_id.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/origin.hxx"
#include "core/orphan_reporter.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/cmd_cluster_map_change_notification.hxx"
#include "core/protocol/cmd_get_cluster_config.hxx"
#include "core/protocol/magic.hxx"
#include "core/protocol/server_opcode.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/read_preference.hxx>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::read_preference;
using couchbase::core::topology::configuration;
using ::test::utils::config_with_vbmap;
using ::test::utils::vbucket_map;

void
an_absent_vbucket_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(std::nullopt, 1);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "there is no map to look in");
}

void
a_vbucket_beyond_the_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(1, 0).has_value(), "the map holds one row");
}

void
a_chain_index_equal_to_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 2).has_value(), "the chain holds two entries");
}

void
a_chain_index_beyond_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 42).has_value(), "the chain holds two entries");
}

void
an_empty_chain_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ {} }, 0);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "the row lists no node at all");
}

void
an_unassigned_chain_slot_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, -1 } }, 1);
  assert_false(config.server_by_vbucket(0, 1).has_value(), "-1 marks the slot unassigned");
}

void
an_assigned_chain_slot_names_its_node_index([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 3, 7 } }, 1);
  assert_true(config.server_by_vbucket(0, 0) == 3, "the active copy's node");
  assert_true(config.server_by_vbucket(0, 1) == 7, "the first replica's node");
}

void
replica_reads_stop_at_the_end_of_a_short_vbucket_row([[maybe_unused]] context& ctx)
{
  // A configuration may advertise more replicas than its vbucket-map rows
  // list: the two fields are parsed independently. Callers derive the replica
  // index from num_replicas, so the surplus indexes must not be read.
  const auto config =
    std::make_shared<configuration>(config_with_vbmap(vbucket_map{ { 0, 1 } }, 2, 3));
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };

  const auto nodes = couchbase::core::impl::effective_nodes(
    id, config, read_preference::no_preference, /* preferred_server_group */ "");

  assert_eq(nodes.size(), std::size_t{ 2 }, "one node per entry the row actually lists");
  assert_false(nodes[0].is_replica, "the first entry is the active copy");
  assert_eq(nodes[0].index, std::size_t{ 0 }, "the active copy's node");
  assert_true(nodes[1].is_replica, "the second entry is a replica");
  assert_eq(nodes[1].index, std::size_t{ 1 }, "the first replica's node");
}

void
a_vbucket_map_without_rows_maps_no_key([[maybe_unused]] context& ctx)
{
  // The vbucket count comes from the map itself, so a map present but without
  // rows must not be asked which vbucket a key belongs to.
  const auto config = config_with_vbmap(vbucket_map{}, 1);

  const auto [vbucket, server] = config.map_key("foo", 0);
  assert_eq(vbucket, 0, "no row can be chosen");
  assert_false(server.has_value(), "no row means no server");

  const std::vector<std::byte> binary_key{ std::byte{ 'f' }, std::byte{ 'o' }, std::byte{ 'o' } };
  const auto [binary_vbucket, binary_server] = config.map_key(binary_key, 0);
  assert_eq(binary_vbucket, 0, "no row can be chosen");
  assert_false(binary_server.has_value(), "no row means no server");
}

auto
bytes_from_string(std::string_view text) -> std::vector<std::byte>
{
  std::vector<std::byte> out;
  out.reserve(text.size());
  for (const char ch : text) {
    out.push_back(static_cast<std::byte>(ch));
  }
  return out;
}

void
an_alternate_address_without_a_ports_object_parses_with_the_hostname_alone(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view json = R"({
    "rev": 1,
    "nodesExt": [
      {
        "thisNode": true,
        "hostname": "10.0.0.1",
        "services": { "kv": 11210 },
        "alternateAddresses": {
          "external": {
            "hostname": "ext.example.com"
          }
        }
      }
    ]
  })";

  const auto config = couchbase::core::protocol::parse_config(json, "10.0.0.1", 11210);
  assert_eq(config.nodes.size(), std::size_t{ 1 }, "one node parsed");
  const auto it = config.nodes[0].alt.find("external");
  assert_true(it != config.nodes[0].alt.end(), "external alternate address is present");
  assert_eq(it->second.hostname, "ext.example.com", "external hostname parsed");
  assert_eq(it->second.services_plain.key_value.value_or(0),
            std::uint16_t{ 11210 },
            "external kv port inherits primary kv port when ports object is omitted");
  assert_eq(config.nodes[0].port_or("external", couchbase::core::service_type::key_value, false, 0),
            std::uint16_t{ 11210 },
            "port_or returns inherited primary kv port for external network");
  assert_eq(config.nodes[0]
              .endpoint("external", couchbase::core::service_type::key_value, false)
              .value_or(""),
            "ext.example.com:11210",
            "effective external endpoint uses alternate hostname and inherited kv port");

  constexpr std::string_view json_with_override_port = R"({
    "rev": 1,
    "nodesExt": [
      {
        "thisNode": true,
        "hostname": "10.0.0.1",
        "services": { "kv": 11210 },
        "alternateAddresses": {
          "external": {
            "hostname": "ext.example.com",
            "ports": { "kv": 31210 }
          }
        }
      }
    ]
  })";
  const auto override_cfg =
    couchbase::core::protocol::parse_config(json_with_override_port, "10.0.0.1", 11210);
  assert_eq(override_cfg.nodes[0]
              .endpoint("default", couchbase::core::service_type::key_value, false)
              .value_or(""),
            "10.0.0.1:11210",
            "default network endpoint uses primary hostname and primary kv port");
  assert_eq(override_cfg.nodes[0]
              .endpoint("external", couchbase::core::service_type::key_value, false)
              .value_or(""),
            "ext.example.com:31210",
            "external network endpoint uses alternate hostname and overridden alternate kv port");
}

void
get_cluster_config_rejects_malformed_or_schema_invalid_json_without_throwing(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::get_cluster_config_response_body;

  couchbase::core::protocol::header_buffer hdr{};
  hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  hdr[1] = static_cast<std::byte>(get_cluster_config_response_body::opcode);

  get_cluster_config_response_body wrong_schema{};
  assert_false(wrong_schema.parse(key_value_status_code::success,
                                  hdr,
                                  0,
                                  0,
                                  0,
                                  bytes_from_string(R"({"nodesExt":"not-an-array"})"),
                                  {}),
               "schema-invalid cluster config returns false without throwing");

  get_cluster_config_response_body invalid_json{};
  assert_false(invalid_json.parse(
                 key_value_status_code::success, hdr, 0, 0, 0, bytes_from_string("{not-json"), {}),
               "malformed JSON cluster config returns false");

  get_cluster_config_response_body truncated_prefix{};
  assert_false(truncated_prefix.parse(
                 key_value_status_code::success, hdr, 0, 20, 0, bytes_from_string("{}"), {}),
               "header prefix exceeding body size returns false");
}

void
cluster_map_change_notification_rejects_truncated_headers_or_invalid_config_json(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::cluster_map_change_notification_request_body;

  couchbase::core::protocol::header_buffer hdr{};
  hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::server_request);
  hdr[1] = static_cast<std::byte>(cluster_map_change_notification_request_body::opcode);
  hdr[4] = std::byte{ 4 }; // ext_size = 4

  cluster_map_change_notification_request_body truncated{};
  assert_false(truncated.parse(hdr, { std::byte{ 0x00 } }, {}),
               "notification shorter than extras and key prefix is rejected");

  hdr[4] = std::byte{ 0 };
  cluster_map_change_notification_request_body bad_json{};
  assert_false(bad_json.parse(hdr, bytes_from_string(R"({"nodesExt":"not-an-array"})"), {}),
               "notification with invalid config JSON returns false without throwing");
}

void
parse_config_handles_null_optional_fields_legacy_nodes_and_rejects_invalid_types(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::get_cluster_config_response_body;

  // 1. Null optional fields, $HOST in alternateAddresses, and hex collectionsManifestUid.
  constexpr std::string_view json_with_nulls = R"({
    "rev": 2,
    "uuid": null,
    "clusterName": null,
    "clusterUUID": null,
    "collectionsManifestUid": "1a",
    "nodesExt": [
      {
        "thisNode": true,
        "hostname": "$HOST",
        "serverGroup": null,
        "appTelemetryPath": null,
        "nodeUUID": null,
        "services": { "kv": 11210 },
        "alternateAddresses": {
          "external": {
            "hostname": "$HOST",
            "ports": null
          }
        }
      }
    ]
  })";
  const auto cfg1 = couchbase::core::protocol::parse_config(json_with_nulls, "192.0.2.10", 11210);
  assert_eq(cfg1.nodes.size(), std::size_t{ 1 }, "one node parsed");
  assert_eq(cfg1.nodes[0].hostname, "192.0.2.10", "$HOST replaced on node.hostname");
  assert_eq(cfg1.nodes[0].alt.at("external").hostname,
            "192.0.2.10",
            "$HOST replaced on alternate address hostname");
  assert_eq(cfg1.collections_manifest_uid.value_or(0),
            std::uint64_t{ 0x1a },
            "collectionsManifestUid parsed as hex");

  // 2. Legacy vBucketServerMap + nodes without couchApiBase and with port 65535.
  constexpr std::string_view legacy_json = R"({
    "rev": 3,
    "nodeLocator": "vbucket",
    "nodes": [
      {
        "thisNode": true,
        "hostname": "10.0.0.1:8091",
        "ports": {
          "httpsMgmt": 65535,
          "httpsCAPI": 18092
        }
      }
    ],
    "vBucketServerMap": {
      "numReplicas": 0,
      "serverList": ["10.0.0.1:11210"],
      "vBucketMap": [[0]]
    }
  })";
  const auto cfg2 = couchbase::core::protocol::parse_config(legacy_json, "10.0.0.1", 11210);
  assert_eq(cfg2.nodes.size(), std::size_t{ 1 }, "legacy node parsed");
  assert_eq(
    cfg2.nodes[0].services_plain.key_value.value_or(0), std::uint16_t{ 11210 }, "legacy kv port");
  assert_eq(
    cfg2.nodes[0].services_plain.management.value_or(0), std::uint16_t{ 8091 }, "legacy mgmt port");
  assert_eq(cfg2.nodes[0].services_tls.management.value_or(0),
            std::uint16_t{ 65535 },
            "legacy httpsMgmt port 65535 accepted");

  // 3. Non-object services/ports or negative collectionsManifestUid are rejected.
  couchbase::core::protocol::header_buffer hdr{};
  hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  hdr[1] = static_cast<std::byte>(get_cluster_config_response_body::opcode);

  get_cluster_config_response_body bad_services{};
  assert_false(
    bad_services.parse(key_value_status_code::success,
                       hdr,
                       0,
                       0,
                       0,
                       bytes_from_string(R"({"nodesExt":[{"hostname":"h","services":"bad"}]})"),
                       {}),
    "non-object services field is rejected");

  get_cluster_config_response_body bad_uid{};
  assert_false(bad_uid.parse(key_value_status_code::success,
                             hdr,
                             0,
                             0,
                             0,
                             bytes_from_string(R"({"collectionsManifestUid":"-1","nodesExt":[]})"),
                             {}),
               "negative collectionsManifestUid is rejected");

  for (const std::string_view bad_port_json : {
         std::string_view{ R"({"nodesExt":[{"hostname":"h","services":{"kv":0}}]})" },
         std::string_view{ R"({"nodesExt":[{"hostname":"h","services":{"kv":-1}}]})" },
         std::string_view{ R"({"nodesExt":[{"hostname":"h","services":{"kv":65536}}]})" },
         std::string_view{ R"({"nodesExt":[{"hostname":"h","alternateAddresses":)"
                           R"({"external":{"hostname":"e","ports":{"kv":0}}}}]})" },
         std::string_view{ R"({"nodesExt":[{"hostname":"h","alternateAddresses":)"
                           R"({"external":{"hostname":"e","ports":{"kv":-1}}}}]})" },
         std::string_view{ R"({"nodesExt":[{"hostname":"h","alternateAddresses":)"
                           R"({"external":{"hostname":"e","ports":{"kv":65536}}}}]})" },
       }) {
    get_cluster_config_response_body bad_port{};
    assert_false(
      bad_port.parse(
        key_value_status_code::success, hdr, 0, 0, 0, bytes_from_string(bad_port_json), {}),
      "out-of-range numeric port in services or alternateAddresses.ports is rejected");
  }
}

void
bucket_bootstrap_without_this_node_stops_session_and_completes_with_error(
  [[maybe_unused]] context& ctx)
{
  asio::io_context server_io;
  asio::ip::tcp::acceptor acceptor(server_io,
                                   asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
  const auto port = acceptor.local_endpoint().port();

  std::atomic_bool session_closed{ false };
  std::thread server_thread([&server_io, &acceptor, &session_closed]() {
    asio::ip::tcp::socket sock(acceptor.get_executor());
    std::error_code ec;
    acceptor.accept(sock, ec);
    if (ec) {
      return;
    }
    auto send_resp = [&sock](std::uint8_t opcode,
                             std::uint16_t status,
                             std::uint32_t opaque,
                             std::string_view value = {},
                             std::uint8_t datatype = 0) {
      std::vector<std::uint8_t> pkt(24 + value.size(), 0);
      pkt[0] = 0x81;
      pkt[1] = opcode;
      pkt[5] = datatype;
      pkt[6] = static_cast<std::uint8_t>((status >> 8U) & 0xffU);
      pkt[7] = static_cast<std::uint8_t>(status & 0xffU);
      const auto blen = static_cast<std::uint32_t>(value.size());
      pkt[8] = static_cast<std::uint8_t>((blen >> 24U) & 0xffU);
      pkt[9] = static_cast<std::uint8_t>((blen >> 16U) & 0xffU);
      pkt[10] = static_cast<std::uint8_t>((blen >> 8U) & 0xffU);
      pkt[11] = static_cast<std::uint8_t>(blen & 0xffU);
      pkt[12] = static_cast<std::uint8_t>((opaque >> 24U) & 0xffU);
      pkt[13] = static_cast<std::uint8_t>((opaque >> 16U) & 0xffU);
      pkt[14] = static_cast<std::uint8_t>((opaque >> 8U) & 0xffU);
      pkt[15] = static_cast<std::uint8_t>(opaque & 0xffU);
      for (std::size_t i = 0; i < value.size(); ++i) {
        pkt[24 + i] = static_cast<std::uint8_t>(value[i]);
      }
      std::error_code write_ec;
      asio::write(sock, asio::buffer(pkt), write_ec);
    };

    while (true) {
      std::array<std::uint8_t, 24> hdr{};
      asio::read(sock, asio::buffer(hdr), ec);
      if (ec) {
        session_closed = true;
        break;
      }
      const std::uint8_t opcode = hdr[1];
      const std::uint32_t bodylen =
        (static_cast<std::uint32_t>(hdr[8]) << 24U) | (static_cast<std::uint32_t>(hdr[9]) << 16U) |
        (static_cast<std::uint32_t>(hdr[10]) << 8U) | static_cast<std::uint32_t>(hdr[11]);
      const std::uint32_t opaque = (static_cast<std::uint32_t>(hdr[12]) << 24U) |
                                   (static_cast<std::uint32_t>(hdr[13]) << 16U) |
                                   (static_cast<std::uint32_t>(hdr[14]) << 8U) |
                                   static_cast<std::uint32_t>(hdr[15]);
      if (bodylen > 0) {
        std::vector<std::uint8_t> body(bodylen);
        asio::read(sock, asio::buffer(body), ec);
        if (ec) {
          session_closed = true;
          break;
        }
      }
      if (opcode == 0x1f) { // hello
        send_resp(opcode, 0, opaque);
      } else if (opcode == 0x20) { // sasl_list_mechs
        send_resp(opcode, 0, opaque, "PLAIN");
      } else if (opcode == 0x21) { // sasl_auth
        send_resp(opcode, 0, opaque);
      } else if (opcode == 0x89) { // select_bucket
        send_resp(opcode, 0, opaque);
      } else if (opcode == 0xb5) { // get_cluster_config
        // Config has no thisNode and a different hostname, so index() will throw.
        send_resp(
          opcode,
          0,
          opaque,
          R"({"rev":1,"nodesExt":[{"hostname":"other.example.com","services":{"kv":11210}}]})",
          0x01);
        asio::steady_timer timer(acceptor.get_executor(), scaled_budget(timeout::fast / 2));
        timer.async_wait([&sock](std::error_code timer_ec) {
          if (!timer_ec) {
            std::error_code close_ec;
            sock.close(close_ec);
          }
        });
        std::array<std::uint8_t, 1> eof_buf{};
        sock.async_read_some(asio::buffer(eof_buf), [&](std::error_code read_ec, std::size_t) {
          if (read_ec == asio::error::eof || read_ec == asio::error::connection_reset) {
            session_closed = true;
          }
          timer.cancel();
        });
        server_io.run();
        break;
      }
    }
  });

  asio::io_context client_io;
  couchbase::core::tls_context_provider tls{};
  couchbase::core::cluster_credentials auth{};
  auth.username = "user";
  auth.password = "pass";
  auth.allowed_sasl_mechanisms = { "PLAIN" };
  couchbase::core::cluster_options opts{};
  auto b = std::make_shared<couchbase::core::bucket>(
    "client-id",
    client_io,
    tls,
    couchbase::core::tracing::tracer_wrapper::create(nullptr, nullptr),
    couchbase::core::metrics::meter_wrapper::create(nullptr, nullptr),
    std::make_shared<couchbase::core::orphan_reporter>(client_io,
                                                       couchbase::core::orphan_reporter_options{}),
    std::make_shared<couchbase::core::app_telemetry_meter>(),
    "default",
    couchbase::core::origin{ auth, "127.0.0.1", port, opts },
    std::vector<couchbase::core::protocol::hello_feature>{},
    nullptr);

  std::error_code bootstrap_ec{};
  b->bootstrap([&bootstrap_ec, &b](std::error_code ec, const configuration&) {
    bootstrap_ec = ec;
    b->close();
  });

  client_io.run();
  server_thread.join();

  assert_eq(bootstrap_ec,
            couchbase::errc::network::configuration_not_available,
            "missing thisNode in bucket bootstrap config reports configuration_not_available");
  assert_true(session_closed.load(),
              "failed bootstrap session closes its socket instead of remaining alive");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_absent_vbucket_map_names_no_server), {}, timeout::instant },
      { CASE(a_vbucket_beyond_the_map_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_equal_to_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_beyond_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(an_empty_chain_names_no_server), {}, timeout::instant },
      { CASE(an_unassigned_chain_slot_names_no_server), {}, timeout::instant },
      { CASE(an_assigned_chain_slot_names_its_node_index), {}, timeout::instant },
      { CASE(replica_reads_stop_at_the_end_of_a_short_vbucket_row), {}, timeout::instant },
      { CASE(a_vbucket_map_without_rows_maps_no_key), {}, timeout::instant },
      { CASE(an_alternate_address_without_a_ports_object_parses_with_the_hostname_alone),
        {},
        timeout::instant },
      { CASE(get_cluster_config_rejects_malformed_or_schema_invalid_json_without_throwing),
        {},
        timeout::instant },
      { CASE(cluster_map_change_notification_rejects_truncated_headers_or_invalid_config_json),
        {},
        timeout::instant },
      { CASE(parse_config_handles_null_optional_fields_legacy_nodes_and_rejects_invalid_types),
        {},
        timeout::instant },
      { CASE(bucket_bootstrap_without_this_node_stops_session_and_completes_with_error),
        {},
        timeout::fast },
    },
  };
}

} // namespace couchbase::test
