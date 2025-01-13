/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include <couchbase/cluster_options.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/transcoder_traits.hxx>

#include <core/utils/duration_parser.hxx>

#include <CLI/CLI.hpp>

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/core.h>

#include <string>
#include <vector>

namespace std::chrono
{
inline bool
lexical_cast(const std::string& input, std::chrono::milliseconds& value)
{
  try {
    value = std::chrono::duration_cast<std::chrono::milliseconds>(
      couchbase::core::utils::parse_duration(input));
  } catch (const couchbase::core::utils::duration_parse_error&) {
    try {
      value = std::chrono::milliseconds(std::stoull(input, nullptr, 10));
    } catch (const std::invalid_argument&) {
      // cannot parse input as duration: not a number
      return false;
    } catch (const std::out_of_range&) {
      // cannot parse input as duration: out of range
      return false;
    }
  }
  return true;
}

inline std::ostream&
operator<<(std::ostream& os, std::chrono::milliseconds duration)
{
  os << fmt::format("{}", duration);
  return os;
}
} // namespace std::chrono

namespace cbc
{
constexpr std::string_view default_bucket_name{ "default" };

struct passthrough_transcoder {
  using document_type = std::pair<std::vector<std::byte>, std::uint32_t>;

  static auto decode(const couchbase::codec::encoded_value& encoded) -> document_type
  {
    return { encoded.data, encoded.flags };
  }
};

struct connection_options {
  std::string connection_string{};
  std::string username{};
  std::string password{};
  std::string certificate_path{};
  std::string key_path{};
  bool ldap_compatible{ false };
  std::string configuration_profile{};
};

struct security_options {
  bool disable_tls{ false };
  std::string trust_certificate_path{};
  std::string tls_verify_mode{};
};

struct logger_options {
  std::string level{};
  std::string output_path{};
  std::string protocol_path{};
};

struct timeout_options {
  std::chrono::milliseconds bootstrap_timeout{};
  std::chrono::milliseconds connect_timeout{};
  std::chrono::milliseconds resolve_timeout{};
  std::chrono::milliseconds key_value_timeout{};
  std::chrono::milliseconds key_value_durable_timeout{};
  std::chrono::milliseconds query_timeout{};
  std::chrono::milliseconds search_timeout{};
  std::chrono::milliseconds eventing_timeout{};
  std::chrono::milliseconds analytics_timeout{};
  std::chrono::milliseconds view_timeout{};
  std::chrono::milliseconds management_timeout{};
};

struct compression_options {
  bool disable{ false };
  std::size_t minimum_size{};
  double minimum_ratio{};
};

struct dns_srv_options {
  std::chrono::milliseconds timeout{};
  std::string nameserver{};
  std::uint16_t port{};
};

struct network_options {
  std::string network{};
  std::chrono::milliseconds tcp_keep_alive_interval{};
  std::chrono::milliseconds config_poll_interval{};
  std::chrono::milliseconds idle_http_connection_timeout{};
};

struct transactions_options {
  std::string durability_level{};
  std::chrono::milliseconds timeout{};
  std::string metadata_bucket{};
  std::string metadata_scope{};
  std::string metadata_collection{};
  std::string query_scan_consistency{};
  bool cleanup_ignore_lost_attempts{};
  bool cleanup_ignore_client_attempts{};
  std::chrono::milliseconds cleanup_window{};
};

struct metrics_options {
  bool disable{ false };
  std::chrono::milliseconds emit_interval{};
};

struct tracing_options {
  bool disable{ false };

  std::chrono::milliseconds orphaned_emit_interval{};
  std::size_t orphaned_sample_size{};

  std::chrono::milliseconds threshold_emit_interval{};
  std::size_t threshold_sample_size{};
  std::chrono::milliseconds threshold_key_value{};
  std::chrono::milliseconds threshold_query{};
  std::chrono::milliseconds threshold_search{};
  std::chrono::milliseconds threshold_analytics{};
  std::chrono::milliseconds threshold_management{};
  std::chrono::milliseconds threshold_eventing{};
  std::chrono::milliseconds threshold_view{};
};

struct behavior_options {
  std::string user_agent_extra{};
  std::string network{};
  bool show_queries{};
  bool disable_clustermap_notifications{};
  bool disable_mutation_tokens{};
  bool disable_unordered_execution{};
  bool dump_configuration{};
};

struct common_options {
  connection_options connection{};
  security_options security{};
  logger_options logger{};
  timeout_options timeouts{};
  compression_options compression{};
  dns_srv_options dns_srv{};
  network_options network{};
  transactions_options transactions{};
  metrics_options metrics{};
  tracing_options tracing{};
  behavior_options behavior{};
};

void
add_common_options(CLI::App* app, common_options& options);

void
apply_logger_options(const logger_options& options);

auto
build_cluster_options(const common_options& options) -> couchbase::cluster_options;

struct keyspace_with_id {
  std::string bucket_name;
  std::string scope_name;
  std::string collection_name;
  std::string id;
};

auto
extract_inlined_keyspace(const std::string& id) -> std::optional<keyspace_with_id>;

auto
available_query_scan_consistency_modes() -> std::vector<std::string>;

auto
available_analytics_scan_consistency_modes() -> std::vector<std::string>;

[[noreturn]] void
fail(std::string_view message);
} // namespace cbc

template<>
struct couchbase::codec::is_transcoder<cbc::passthrough_transcoder> : public std::true_type {
};
