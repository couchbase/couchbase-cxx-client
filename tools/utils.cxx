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

#include "utils.hxx"
#include "CLI/CLI.hpp"
#include "core/logger/configuration.hxx"
#include "core/logger/logger.hxx"
#include "core/meta/version.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <couchbase/cluster_options.hxx>
#include <couchbase/configuration_profiles_registry.hxx>
#include <couchbase/fmt/analytics_scan_consistency.hxx>
#include <couchbase/fmt/durability_level.hxx>
#include <couchbase/fmt/query_scan_consistency.hxx>
#include <couchbase/scope.hxx>
#include <cstdlib>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wuseless-cast"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#endif
#include <opentelemetry/sdk/common/global_log_handler.h>

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span_startoptions.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/samplers/always_off.h>
#include <opentelemetry/sdk/trace/samplers/always_on.h>
#include <opentelemetry/sdk/trace/samplers/parent.h>
#include <opentelemetry/sdk/trace/samplers/trace_id_ratio.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/meter.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/view_registry_factory.h>
#include <opentelemetry/sdk/resource/resource.h>

#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>

#include <couchbase/metrics/otel_meter.hxx>
#include <couchbase/tracing/otel_tracer.hxx>

#if defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <spdlog/details/os.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/spdlog.h>
#include <tao/json/value.hpp>

#include <chrono>
#include <memory>
#include <regex>
#include <stdexcept>

#ifdef _WIN32
#include <io.h>
#define ACCESS _access
#define W_OK 2
#else
#include <unistd.h>
#define ACCESS access
#endif

namespace cbc
{
namespace
{

class null_opentelemetry_logger : public opentelemetry::sdk::common::internal_log::LogHandler
{
public:
  void Handle(
    [[maybe_unused]] opentelemetry::sdk::common::internal_log::LogLevel level,
    [[maybe_unused]] const char* file,
    [[maybe_unused]] int line,
    [[maybe_unused]] const char* msg,
    [[maybe_unused]] const opentelemetry::sdk::common::AttributeMap& attributes) noexcept override
  {
    // CB_LOG_TRACE(
    //   "OpenTelemetry({}): {}", opentelemetry::sdk::common::internal_log::LevelToString(level),
    //   msg);
  }
};

auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>
{
  if (name.empty()) {
    return std::nullopt;
  }

#if defined(_WIN32)
  char* buf = nullptr;
  size_t len = 0;
  if (_dupenv_s(&buf, &len, name.c_str()) == 0 && buf != nullptr) {
    std::string value(buf);
    free(buf);
    if (!value.empty()) {
      return value;
    }
  }
  return std::nullopt;

#else
  if (const char* val = std::getenv(name.c_str())) { // NOLINT(concurrency-mt-unsafe)
    if (val[0] != '\0') {
      return std::string(val);
    }
  }
  return std::nullopt;
#endif
}

auto
getenv_or_default(const std::string& var_name, const std::string& default_value) -> std::string
{
  return safe_getenv(var_name).value_or(default_value);
}

auto
default_cluster_options() -> const auto&
{
  static const auto default_cluster_options_ =
    couchbase::cluster_options("Administrator", "password").build();
  return default_cluster_options_;
}

void
add_options(CLI::App* app, connection_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Connection", "Specify cluster location and credentials.");

  group
    ->add_option(
      "--connection-string",
      options.connection_string,
      "Connection string for the cluster. Also see CBC_CONNECTION_STRING environment variable.")
    ->default_val(getenv_or_default("CBC_CONNECTION_STRING", "couchbase://localhost"));
  group
    ->add_option("--username",
                 options.username,
                 "Username for the cluster. Also see CBC_USERNAME environment variable.")
    ->default_val(getenv_or_default("CBC_USERNAME", defaults.username));
  group
    ->add_option("--password",
                 options.password,
                 "Username for the cluster. Also see CBC_PASSWORD environment variable.")
    ->default_val(getenv_or_default("CBC_PASSWORD", defaults.password));
  group
    ->add_option("--certificate-path", options.certificate_path, "Path to the client certificate.")
    ->transform(CLI::ExistingFile);
  group->add_option("--key-path", options.key_path, "Path to the client key.")
    ->transform(CLI::ExistingFile);
  group->add_flag("--ldap-compatible",
                  options.ldap_compatible,
                  "Whether to select authentication mechanism that is compatible with LDAP.");
  group
    ->add_option("--configuration-profile",
                 options.configuration_profile,
                 "Apply configuration profile (might override other switches).")
    ->transform(CLI::IsMember(couchbase::configuration_profiles_registry::available_profiles()));
}

void
add_options(CLI::App* app, logger_options& options)
{
  const std::vector<std::string> allowed_log_levels{
    "trace", "debug", "info", "warning", "error", "critical", "off",
  };
  auto* group = app->add_option_group("Logger", "Set logger verbosity and file output.");

  group
    ->add_option(
      "--log-level", options.level, "Log level. Also see CBC_LOG_LEVEL environment variable.")
    ->default_val(getenv_or_default("CBC_LOG_LEVEL", "off"))
    ->transform(CLI::IsMember(allowed_log_levels));
  group
    ->add_option("--log-output",
                 options.output_path,
                 "File to write logs (when is not set, logs will be written to STDERR).")
    ->transform(CLI::ExistingFile | CLI::NonexistentPath);
  group->add_option("--log-protocol", options.protocol_path, "File to write protocol logs.")
    ->transform(CLI::ExistingFile | CLI::NonexistentPath);
}

void
add_options(CLI::App* app, security_options& options)
{
  const std::vector<std::string> allowed_tls_verficiation_modes{
    "peer",
    "none",
  };
  auto* group = app->add_option_group("Security", "Set security and TLS options.");

  group->add_flag("--disable-tls", options.disable_tls, "Whether to disable TLS completely.");
  group
    ->add_option("--trust-certificate-path",
                 options.trust_certificate_path,
                 "Path to the trust certificate bundle.")
    ->transform(CLI::ExistingFile);
  group
    ->add_option(
      "--tls-verify-mode", options.tls_verify_mode, "Verification mode for TLS connections.")
    ->default_val("peer")
    ->transform(CLI::IsMember(allowed_tls_verficiation_modes));
}

void
add_options(CLI::App* app, timeout_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Timeouts", "Set security and TLS options.");

  group
    ->add_option(
      "--bootstrap-timeout", options.bootstrap_timeout, "Timeout for overall bootstrap of the SDK.")
    ->default_val(defaults.timeouts.bootstrap_timeout)
    ->type_name("DURATION");
  group->add_option("--connect-timeout", options.connect_timeout, "Timeout for socket connection.")
    ->default_val(defaults.timeouts.connect_timeout)
    ->type_name("DURATION");
  group
    ->add_option("--resolve-timeout",
                 options.resolve_timeout,
                 "Timeout to resolve DNS address for the sockets.")
    ->default_val(defaults.timeouts.resolve_timeout)
    ->type_name("DURATION");
  group
    ->add_option(
      "--key-value-timeout", options.key_value_timeout, "Timeout for Key/Value operations.")
    ->default_val(defaults.timeouts.key_value_timeout)
    ->type_name("DURATION");
  group
    ->add_option("--key-value-durable-timeout",
                 options.key_value_durable_timeout,
                 "Timeout for Key/Value durable operations.")
    ->default_val(defaults.timeouts.key_value_durable_timeout)
    ->type_name("DURATION");
  group->add_option("--query-timeout", options.query_timeout, "Timeout for Query service.")
    ->default_val(defaults.timeouts.query_timeout)
    ->type_name("DURATION");
  group->add_option("--search-timeout", options.search_timeout, "Timeout for Search service.")
    ->default_val(defaults.timeouts.search_timeout)
    ->type_name("DURATION");
  group->add_option("--eventing-timeout", options.eventing_timeout, "Timeout for Eventing service.")
    ->default_val(defaults.timeouts.eventing_timeout)
    ->type_name("DURATION");
  group
    ->add_option("--analytics-timeout", options.analytics_timeout, "Timeout for Analytics service.")
    ->default_val(defaults.timeouts.analytics_timeout);
  group->add_option("--view-timeout", options.view_timeout, "Timeout for View service.")
    ->default_val(defaults.timeouts.view_timeout)
    ->type_name("DURATION");
  group
    ->add_option(
      "--management-timeout", options.management_timeout, "Timeout for management operations.")
    ->default_val(defaults.timeouts.management_timeout)
    ->type_name("DURATION");
}

void
add_options(CLI::App* app, compression_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Compression", "Set compression options.");

  group->add_flag("--disable-compression", options.disable, "Whether to disable compression.");
  group
    ->add_option("--compression-minimum-size",
                 options.minimum_size,
                 "The minimum size of the document (in bytes), that will be compressed.")
    ->default_val(defaults.compression.min_size);
  group
    ->add_option("--compression-minimum-ratio",
                 options.minimum_ratio,
                 "The minimum compression ratio to allow compressed form to be used.")
    ->default_val(defaults.compression.min_ratio);
}

void
add_options(CLI::App* app, dns_srv_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("DNS-SRV", "Set DNS-SRV options.");

  group->add_option("--dns-srv-timeout", options.timeout, "Timeout for DNS SRV requests.")
    ->default_val(defaults.dns.timeout)
    ->type_name("DURATION");
  group->add_option("--dns-srv-nameserver",
                    options.nameserver,
                    "Hostname of the DNS server where the DNS SRV requests will be sent.");
  group->add_option("--dns-srv-port",
                    options.port,
                    "Port of the DNS server where the DNS SRV requests will be sent.");
}

void
add_options(CLI::App* app, network_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Network", "Set network options.");

  group
    ->add_option(
      "--tcp-keep-alive-interval", options.tcp_keep_alive_interval, "Interval for TCP keep alive.")
    ->default_val(defaults.network.tcp_keep_alive_interval)
    ->type_name("DURATION");
  group
    ->add_option("--config-poll-interval",
                 options.config_poll_interval,
                 "How often the library should poll for new configuration.")
    ->default_val(defaults.network.config_poll_interval)
    ->type_name("DURATION");
  group
    ->add_option("--idle-http-connection-timeout",
                 options.idle_http_connection_timeout,
                 "Period to wait before calling HTTP connection idle.")
    ->default_val(defaults.network.idle_http_connection_timeout)
    ->type_name("DURATION");
}

void
add_options(CLI::App* app, transactions_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Transactions", "Set transactions options.");
  const std::vector<std::string> available_durability_levels{
    fmt::format("{}", couchbase::durability_level::none),
    fmt::format("{}", couchbase::durability_level::majority),
    fmt::format("{}", couchbase::durability_level::majority_and_persist_to_active),
    fmt::format("{}", couchbase::durability_level::persist_to_majority),
  };

  group
    ->add_option("--transactions-durability-level",
                 options.durability_level,
                 "Durability level of the transaction.")
    ->default_val(fmt::format("{}", defaults.transactions.level))
    ->transform(CLI::IsMember(available_durability_levels));
  group->add_option("--transactions-timeout", options.timeout, "Timeout of the transaction.")
    ->default_val(defaults.transactions.timeout)
    ->type_name("DURATION");
  group->add_option("--transactions-metadata-bucket",
                    options.metadata_bucket,
                    "Bucket name where transaction metadata is stored.");
  group
    ->add_option("--transactions-metadata-scope",
                 options.metadata_scope,
                 "Scope name where transaction metadata is stored.")
    ->default_val(couchbase::scope::default_name);
  group
    ->add_option("--transactions-metadata-collection",
                 options.metadata_collection,
                 "Collection name where transaction metadata is stored.")
    ->default_val(couchbase::collection::default_name);
  group
    ->add_option("--transactions-query-scan-consistency",
                 options.query_scan_consistency,
                 "Scan consistency for queries in transactions.")
    ->default_val(fmt::format("{}", defaults.transactions.query_config.scan_consistency))
    ->transform(CLI::IsMember(available_query_scan_consistency_modes()));
  group->add_option("--transactions-cleanup-window", options.cleanup_window, "Cleanup window.")
    ->default_val(defaults.transactions.cleanup_config.cleanup_window)
    ->type_name("DURATION");
  group->add_flag("--transactions-cleanup-ignore-lost-attempts",
                  options.cleanup_ignore_lost_attempts,
                  "Do not cleanup lost attempts.");
  group->add_flag("--transactions-cleanup-ignore-client-attempts",
                  options.cleanup_ignore_client_attempts,
                  "Do not cleanup client attempts.");
}

void
add_options(CLI::App* app, metrics_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Metrics", "Set metrics options.");

  group->add_flag(
    "--disable-metrics", options.disable, "Disable collecting and reporting metrics.");
  group
    ->add_option("--metrics-orphaned-emit-interval",
                 options.emit_interval,
                 "Interval to emit metrics report on INFO log level.")
    ->default_val(defaults.metrics.emit_interval)
    ->type_name("DURATION");

  group->add_flag("--metrics-use-opentelemetry",
                  options.opentelemetry.use_opentelemetry,
                  "Use OpenTelemetry and export metrics with HTTP transport.");

  group->add_option("--metrics-opentelemetry-endpoint",
                    options.opentelemetry.endpoint,
                    "The OTLP HTTP endpoint for OpenTelemetry to export to.");

  group
    ->add_option("--metrics-opentelemetry-reader-export-interval",
                 options.opentelemetry.reader_export_interval,
                 "Interval between metric exports in milliseconds.")
    ->default_val(std::chrono::milliseconds(1000));

  group
    ->add_option("--metrics-opentelemetry-reader-export-timeout",
                 options.opentelemetry.reader_export_timeout,
                 "Timeout for metric export operations in milliseconds.")
    ->type_name("DURATION")
    ->default_val(options.opentelemetry.reader_export_timeout);

  std::vector<std::string> allowed_temporalities{
    "unspecified", "delta", "cumulative", "low_memory"
  };
  group
    ->add_option("--metrics-opentelemetry-exporter-aggregation-temporality",
                 options.opentelemetry.exporter_aggregation_temporality,
                 "Aggregation temporality.")
    ->default_val("delta")
    ->check(CLI::IsMember(allowed_temporalities));

  group
    ->add_option("--metrics-opentelemetry-exporter-timeout",
                 options.opentelemetry.exporter_timeout,
                 "Overall exporter timeout in milliseconds.")
    ->type_name("DURATION")
    ->default_val(options.opentelemetry.exporter_timeout);

  group->add_flag("--metrics-opentelemetry-use-ssl-credentials",
                  options.opentelemetry.use_ssl_credentials,
                  "Use SSL/TLS credentials for the OTLP HTTP connection.");

  group->add_option("--metrics-opentelemetry-ssl-credentials-cacert",
                    options.opentelemetry.ssl_credentials_cacert,
                    "Path to CA certificate file for SSL/TLS verification.");

  group
    ->add_option("--metrics-opentelemetry-headers",
                 options.opentelemetry.headers,
                 "Custom HTTP headers as key=value pairs (can be specified multiple times).")
    ->type_name("KEY=VALUE");

  group->add_option("--metrics-opentelemetry-compression",
                    options.opentelemetry.compression,
                    "Compression algorithm to use (e.g., gzip).");
}

void
add_options(CLI::App* app, tracing_options& options)
{
  const auto& defaults = default_cluster_options();
  auto* group = app->add_option_group("Tracing", "Set tracing options.");

  group->add_flag(
    "--disable-tracing", options.disable, "Disable collecting and reporting trace information.");
  group
    ->add_option("--tracing-orphaned-emit-interval",
                 options.orphaned_emit_interval,
                 "Interval to emit report about orphan operations.")
    ->default_val(defaults.tracing.orphaned_emit_interval)
    ->type_name("DURATION");
  group
    ->add_option("--tracing-orphaned-sample-size",
                 options.orphaned_sample_size,
                 "Size of the sample of the orphan report.")
    ->default_val(defaults.tracing.orphaned_sample_size);
  group
    ->add_option("--tracing-threshold-emit-interval",
                 options.threshold_emit_interval,
                 "Interval to emit report about operations exceeding threshold.")
    ->default_val(defaults.tracing.threshold_emit_interval)
    ->type_name("DURATION");
  group
    ->add_option("--tracing-threshold-sample-size",
                 options.threshold_sample_size,
                 "Size of the sample of the threshold report.")
    ->default_val(defaults.tracing.threshold_sample_size);
  group
    ->add_option("--tracing-threshold-key-value",
                 options.threshold_key_value,
                 "Threshold for Key/Value service.")
    ->default_val(defaults.tracing.key_value_threshold)
    ->type_name("DURATION");
  group
    ->add_option(
      "--tracing-threshold-query", options.threshold_query, "Threshold for Query service.")
    ->default_val(defaults.tracing.query_threshold)
    ->type_name("DURATION");
  group
    ->add_option(
      "--tracing-threshold-search", options.threshold_search, "Threshold for Query service.")
    ->default_val(defaults.tracing.search_threshold)
    ->type_name("DURATION");
  group
    ->add_option(
      "--tracing-threshold-analytics", options.threshold_analytics, "Threshold for Query service.")
    ->default_val(defaults.tracing.analytics_threshold)
    ->type_name("DURATION");
  group
    ->add_option("--tracing-threshold-management",
                 options.threshold_management,
                 "Threshold for Query service.")
    ->default_val(defaults.tracing.management_threshold)
    ->type_name("DURATION");
  group
    ->add_option(
      "--tracing-threshold-eventing", options.threshold_eventing, "Threshold for Query service.")
    ->default_val(defaults.tracing.eventing_threshold)
    ->type_name("DURATION");
  group
    ->add_option("--tracing-threshold-view", options.threshold_view, "Threshold for Query service.")
    ->default_val(defaults.tracing.view_threshold)
    ->type_name("DURATION");

  group->add_flag("--tracing-use-opentelemetry",
                  options.opentelemetry.use_opentelemetry,
                  "Use OpenTelemetry and export tracing spans with HTTP transport.");

  group->add_option("--tracing-opentelemetry-endpoint",
                    options.opentelemetry.endpoint,
                    "The OTLP HTTP endpoint for OpenTelemetry to export to.");

  std::vector<std::string> allowed_samplers{
    "always_on",
    "always_off",
    "trace_id_ratio_based",
    "parent_based",
  };
  group
    ->add_option("--tracing-opentelemetry-sampler",
                 options.opentelemetry.sampler,
                 "Sampler type for OpenTelemetry tracing.")
    ->transform(CLI::IsMember(allowed_samplers))
    ->default_str("parent_based");

  group
    ->add_option("--tracing-opentelemetry-sampling-ratio",
                 options.opentelemetry.sampling_ratio,
                 "Sampling ratio for trace_id_ratio_based sampler (0.0 to 1.0).")
    ->check(CLI::Range(0.0, 1.0))
    ->default_str("1.0");

  group->add_flag("--tracing-opentelemetry-use-batch-processor{true},!--tracing-opentelemetry-use-"
                  "simple-processor{false}",
                  options.opentelemetry.use_batch_processor,
                  "Use batch span processor (default) or simple span processor.");

  group
    ->add_option("--tracing-opentelemetry-batch-schedule-delay",
                 options.opentelemetry.batch_schedule_delay,
                 "Batch processor schedule delay in milliseconds.")
    ->default_val(options.opentelemetry.batch_schedule_delay)
    ->type_name("DURATION");

  group
    ->add_option("--tracing-opentelemetry-batch-max-queue-size",
                 options.opentelemetry.batch_max_queue_size,
                 "Maximum queue size for batch processor.")
    ->default_val(options.opentelemetry.batch_max_queue_size);

  group
    ->add_option("--tracing-opentelemetry-batch-max-export-batch-size",
                 options.opentelemetry.batch_max_export_batch_size,
                 "Maximum export batch size for batch processor.")
    ->default_val(options.opentelemetry.batch_max_export_batch_size);

  group
    ->add_option("--tracing-opentelemetry-exporter-timeout",
                 options.opentelemetry.exporter_timeout,
                 "Exporter timeout in milliseconds.")
    ->default_val(options.opentelemetry.exporter_timeout)
    ->type_name("DURATION");

  group->add_flag("--tracing-opentelemetry-use-ssl-credentials",
                  options.opentelemetry.use_ssl_credentials,
                  "Use SSL/TLS credentials for secure connection.");

  group->add_option("--tracing-opentelemetry-ssl-credentials-cacert",
                    options.opentelemetry.ssl_credentials_cacert,
                    "Path to CA certificate file for SSL/TLS verification.");

  group
    ->add_option("--tracing-opentelemetry-headers",
                 options.opentelemetry.headers,
                 "HTTP headers (e.g. for authentication) (format: key=value).")
    ->type_name("KEY=VALUE")
    ->take_all();

  group->add_option("--tracing-opentelemetry-compression",
                    options.opentelemetry.compression,
                    "Compression algorithm for exporter (e.g., 'gzip').");
}

auto
full_user_agent(const std::string& extra) -> std::string
{
  constexpr auto uuid{ "00000000-0000-0000-0000-000000000000" };
  auto hello = couchbase::core::meta::user_agent_for_mcbp(uuid, uuid, extra);
  auto json = couchbase::core::utils::json::parse(hello.data(), hello.size());
  return json["a"].get_string();
}

void
add_options(CLI::App* app, behavior_options& options)
{
  const std::string default_user_agent_extra{ "cbc" };
  const std::string default_network{ "auto" };
  auto* group =
    app->add_option_group("Behavior", "Set options related to general library behavior.");

  group
    ->add_option("--user-agent-extra",
                 options.user_agent_extra,
                 fmt::format("Append extra string SDK identifiers (full user-agent is \"{}\").",
                             full_user_agent(default_user_agent_extra)))
    ->default_val(default_user_agent_extra);
  group->add_option("--network", options.network, "Network (a.k.a. Alternate Addresses) to use.")
    ->default_val(default_network);
  group->add_flag("--show-queries", options.show_queries, "Log queries on INFO level.");
  group->add_flag("--disable-clustermap-notifications",
                  options.disable_clustermap_notifications,
                  "Do not allow server to send notifications when cluster configuration changes.");
  group->add_flag("--disable-mutation-tokens",
                  options.disable_mutation_tokens,
                  "Do not request Key/Value service to send mutation tokens.");
  group->add_flag("--disable-unordered-execution",
                  options.disable_unordered_execution,
                  "Disable unordered execution for Key/Value service.");
  group->add_flag("--dump-configuration",
                  options.dump_configuration,
                  "Dump every new configuration on TRACE log level.");
}

auto
create_cluster_options(const connection_options& options) -> couchbase::cluster_options
{
  if (!options.certificate_path.empty() && !options.key_path.empty()) {
    return couchbase::cluster_options{ couchbase::certificate_authenticator(
      options.certificate_path, options.key_path) };
  }
  if (!options.certificate_path.empty()) {
    fail("--key-path must be provided when --certificate-path is set.");
  }
  if (!options.key_path.empty()) {
    fail("--certificate-path must be provided when --key-path is set.");
  }
  if (options.ldap_compatible) {
    return couchbase::cluster_options(
      couchbase::password_authenticator::ldap_compatible(options.username, options.password));
  }
  return couchbase::cluster_options{ couchbase::password_authenticator(options.username,
                                                                       options.password) };
}

void
apply_options(couchbase::cluster_options& options, const security_options& security)
{
  options.security().enabled(!security.disable_tls);
  if (!security.trust_certificate_path.empty()) {
    options.security().trust_certificate(security.trust_certificate_path);
  }
  if (security.tls_verify_mode == "none") {
    options.security().tls_verify(couchbase::tls_verify_mode::none);
  } else if (security.tls_verify_mode == "peer") {
    options.security().tls_verify(couchbase::tls_verify_mode::peer);
  } else if (!security.tls_verify_mode.empty()) {
    fail(fmt::format("unexpected value '{}' for --tls-verify-mode", security.tls_verify_mode));
  }
}

void
apply_options(couchbase::cluster_options& options, const timeout_options& timeouts)
{
  options.timeouts().bootstrap_timeout(timeouts.bootstrap_timeout);
  options.timeouts().connect_timeout(timeouts.connect_timeout);
  options.timeouts().resolve_timeout(timeouts.resolve_timeout);
  options.timeouts().key_value_timeout(timeouts.key_value_timeout);
  options.timeouts().key_value_durable_timeout(timeouts.key_value_durable_timeout);
  options.timeouts().query_timeout(timeouts.query_timeout);
  options.timeouts().search_timeout(timeouts.search_timeout);
  options.timeouts().eventing_timeout(timeouts.eventing_timeout);
  options.timeouts().analytics_timeout(timeouts.analytics_timeout);
  options.timeouts().view_timeout(timeouts.view_timeout);
  options.timeouts().management_timeout(timeouts.management_timeout);
}

void
apply_options(couchbase::cluster_options& options, const compression_options& compression)
{
  options.compression().enabled(!compression.disable);
  options.compression().min_size(compression.minimum_size);
  options.compression().min_ratio(compression.minimum_ratio);
}

void
apply_options(couchbase::cluster_options& options, const dns_srv_options& dns_srv)
{
  options.dns().timeout(dns_srv.timeout);
  if (!dns_srv.nameserver.empty()) {
    if (dns_srv.port > 0) {
      options.dns().nameserver(dns_srv.nameserver, dns_srv.port);
    } else {
      options.dns().nameserver(dns_srv.nameserver);
    }
  }
}

void
apply_options(couchbase::cluster_options& options, const network_options& network)
{
  options.network().preferred_network(network.network);
  options.network().tcp_keep_alive_interval(network.tcp_keep_alive_interval);
  options.network().config_poll_interval(network.config_poll_interval);
  options.network().idle_http_connection_timeout(network.idle_http_connection_timeout);
}

void
apply_options(couchbase::cluster_options& options, const transactions_options& transactions)
{
  if (transactions.durability_level == "none") {
    options.transactions().durability_level(couchbase::durability_level::none);
  } else if (transactions.durability_level == "majority") {
    options.transactions().durability_level(couchbase::durability_level::majority);
  } else if (transactions.durability_level == "majority_and_persist_to_active") {
    options.transactions().durability_level(
      couchbase::durability_level::majority_and_persist_to_active);
  } else if (transactions.durability_level == "persist_to_majority") {
    options.transactions().durability_level(couchbase::durability_level::persist_to_majority);
  } else if (!transactions.durability_level.empty()) {
    fail(fmt::format("unexpected value '{}' for --transactions-durability-level",
                     transactions.durability_level));
  }
  options.transactions().timeout(transactions.timeout);
  if (!transactions.metadata_bucket.empty()) {
    options.transactions().metadata_collection({ transactions.metadata_bucket,
                                                 transactions.metadata_scope,
                                                 transactions.metadata_collection });
  }
  if (transactions.query_scan_consistency == "not_bounded") {
    options.transactions().query_config().scan_consistency(
      couchbase::query_scan_consistency::not_bounded);
  } else if (transactions.query_scan_consistency == "request_plus") {
    options.transactions().query_config().scan_consistency(
      couchbase::query_scan_consistency::request_plus);
  } else if (!transactions.query_scan_consistency.empty()) {
    fail(fmt::format("unexpected value '{}' for --transactions-query-scan-consistency",
                     transactions.query_scan_consistency));
  }
  options.transactions().cleanup_config().cleanup_lost_attempts(
    !transactions.cleanup_ignore_lost_attempts);
  options.transactions().cleanup_config().cleanup_client_attempts(
    !transactions.cleanup_ignore_client_attempts);
  options.transactions().cleanup_config().cleanup_window(transactions.cleanup_window);
}

void
apply_opentelemetry_meter_options(couchbase::cluster_options& options,
                                  const opentelemetry_metrics_options& metrics)
{
  opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogHandler(
    std::make_shared<null_opentelemetry_logger>());

  auto resource = opentelemetry::sdk::resource::Resource::Create({
    { "service.name", "cbc" },
    { "service.version", couchbase::core::meta::sdk_semver() },
  });

  opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions exporter_options{};
  if (auto endpoint = metrics.endpoint; endpoint) {
    exporter_options.url = endpoint.value();
  }
  exporter_options.content_type = opentelemetry::exporter::otlp::HttpRequestContentType::kJson;
  if (auto temporality = metrics.exporter_aggregation_temporality;
      temporality.empty() || temporality == "unspecified") {
    exporter_options.aggregation_temporality =
      opentelemetry::exporter::otlp::PreferredAggregationTemporality::kUnspecified;
  } else if (temporality == "delta") {
    exporter_options.aggregation_temporality =
      opentelemetry::exporter::otlp::PreferredAggregationTemporality::kDelta;
  } else if (temporality == "cumulative") {
    exporter_options.aggregation_temporality =
      opentelemetry::exporter::otlp::PreferredAggregationTemporality::kCumulative;
  } else if (temporality == "low_memory") {
    exporter_options.aggregation_temporality =
      opentelemetry::exporter::otlp::PreferredAggregationTemporality::kLowMemory;
  }

  exporter_options.timeout = metrics.exporter_timeout;
  if (!metrics.headers.empty()) {
    for (const auto& [key, value] : metrics.headers) {
      exporter_options.http_headers.insert({ key, value });
    }
  }
  if (auto compression = metrics.compression; compression) {
    exporter_options.compression = compression.value();
  }

  auto exporter =
    opentelemetry::exporter::otlp::OtlpHttpMetricExporterFactory::Create(exporter_options);

  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_options{};
  reader_options.export_interval_millis = metrics.reader_export_interval;
  reader_options.export_timeout_millis = metrics.reader_export_timeout;

  std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> reader{
    new opentelemetry::sdk::metrics::PeriodicExportingMetricReader(std::move(exporter),
                                                                   reader_options)
  };

  auto context = opentelemetry::sdk::metrics::MeterContextFactory::Create(
    opentelemetry::sdk::metrics::ViewRegistryFactory::Create(), resource);
  context->AddMetricReader(std::move(reader));

  auto provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(context));
  opentelemetry::metrics::Provider::SetMeterProvider(
    std::shared_ptr<opentelemetry::metrics::MeterProvider>(std::move(provider)));

  options.metrics().meter(std::make_shared<couchbase::metrics::otel_meter>(
    opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(
      "cbc", couchbase::core::meta::sdk_semver())));
}

void
apply_options(couchbase::cluster_options& options, const metrics_options& metrics)
{
  if (metrics.opentelemetry.use_opentelemetry) {
    apply_opentelemetry_meter_options(options, metrics.opentelemetry);
  }

  options.metrics().enable(!metrics.disable);
  options.metrics().emit_interval(metrics.emit_interval);
}

void
apply_opentelemetry_tracer_options(couchbase::cluster_options& options,
                                   const opentelemetry_tracing_options& tracing)
{
  opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogHandler(
    std::make_shared<null_opentelemetry_logger>());

  opentelemetry::exporter::otlp::OtlpHttpExporterOptions exporter_options{};
  if (auto endpoint = tracing.endpoint; endpoint) {
    exporter_options.url = endpoint.value();
  }
  exporter_options.timeout = tracing.exporter_timeout;
  exporter_options.content_type = opentelemetry::exporter::otlp::HttpRequestContentType::kJson;
  if (!tracing.headers.empty()) {
    for (const auto& [key, value] : tracing.headers) {
      exporter_options.http_headers.insert({ key, value });
    }
  }
  if (auto compression = tracing.compression; compression) {
    exporter_options.compression = compression.value();
  }

  auto exporter = opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(exporter_options);

  std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor;
  if (tracing.use_batch_processor) {
    opentelemetry::sdk::trace::BatchSpanProcessorOptions batch_options{};
    batch_options.schedule_delay_millis = tracing.batch_schedule_delay;
    batch_options.max_queue_size = tracing.batch_max_queue_size;
    batch_options.max_export_batch_size = tracing.batch_max_export_batch_size;
    processor = opentelemetry::sdk::trace::BatchSpanProcessorFactory::Create(std::move(exporter),
                                                                             batch_options);
  } else {
    processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  }

  std::unique_ptr<opentelemetry::sdk::trace::Sampler> sampler;
  if (!tracing.sampler || tracing.sampler == "parent_based") {
    sampler = std::make_unique<opentelemetry::sdk::trace::ParentBasedSampler>(
      std::make_shared<opentelemetry::sdk::trace::AlwaysOnSampler>());
  } else if (tracing.sampler == "always_on") {
    sampler = std::make_unique<opentelemetry::sdk::trace::AlwaysOnSampler>();
  } else if (tracing.sampler == "always_off") {
    sampler = std::make_unique<opentelemetry::sdk::trace::AlwaysOffSampler>();
  } else if (tracing.sampler == "trace_id_ratio_based") {
    sampler =
      std::make_unique<opentelemetry::sdk::trace::TraceIdRatioBasedSampler>(tracing.sampling_ratio);
  }

  auto resource = opentelemetry::sdk::resource::Resource::Create({
    { "service.name", "cbc" },
    { "service.version", couchbase::core::meta::sdk_semver() },
  });

  auto provider = opentelemetry::sdk::trace::TracerProviderFactory::Create(
    std::move(processor), resource, std::move(sampler));

  opentelemetry::trace::Provider::SetTracerProvider(
    std::shared_ptr<opentelemetry::trace::TracerProvider>{ std::move(provider) });

  options.tracing().tracer(std::make_shared<couchbase::tracing::otel_request_tracer>(
    opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      "cbc", couchbase::core::meta::sdk_semver())));
}

void
apply_options(couchbase::cluster_options& options, const tracing_options& tracing)
{
  if (tracing.opentelemetry.use_opentelemetry) {
    apply_opentelemetry_tracer_options(options, tracing.opentelemetry);
  }

  options.tracing().enable(!tracing.disable);
  options.tracing().orphaned_emit_interval(tracing.orphaned_emit_interval);
  options.tracing().orphaned_sample_size(tracing.orphaned_sample_size);
  options.tracing().threshold_emit_interval(tracing.threshold_emit_interval);
  options.tracing().threshold_sample_size(tracing.threshold_sample_size);
  options.tracing().key_value_threshold(tracing.threshold_key_value);
  options.tracing().query_threshold(tracing.threshold_query);
  options.tracing().search_threshold(tracing.threshold_search);
  options.tracing().analytics_threshold(tracing.threshold_analytics);
  options.tracing().management_threshold(tracing.threshold_management);
  options.tracing().eventing_threshold(tracing.threshold_eventing);
  options.tracing().view_threshold(tracing.threshold_view);
}

void
apply_options(couchbase::cluster_options& options, const behavior_options& behavior)
{
  options.behavior().append_to_user_agent(behavior.user_agent_extra);
  options.behavior().show_queries(behavior.show_queries);
  options.behavior().dump_configuration(behavior.dump_configuration);
  options.behavior().enable_clustermap_notification(!behavior.disable_clustermap_notifications);
  options.behavior().enable_mutation_tokens(!behavior.disable_mutation_tokens);
  options.behavior().enable_unordered_execution(!behavior.disable_unordered_execution);
}
} // namespace

void
add_common_options(CLI::App* app, common_options& options)
{
  add_options(app, options.logger);
  add_options(app, options.connection);
  add_options(app, options.security);
  add_options(app, options.timeouts);
  add_options(app, options.compression);
  add_options(app, options.dns_srv);
  add_options(app, options.network);
  add_options(app, options.transactions);
  add_options(app, options.metrics);
  add_options(app, options.tracing);
  add_options(app, options.behavior);
}

void
apply_logger_options(const logger_options& options)
{
  auto level = couchbase::core::logger::level_from_str(options.level);

  if (level != couchbase::core::logger::level::off) {
    couchbase::core::logger::configuration configuration{};

    if (options.output_path.empty()) {
      configuration.console = true;
      configuration.unit_test = true;
    } else {
      configuration.filename = options.output_path;
    }
    configuration.log_level = level;
    couchbase::core::logger::create_file_logger(configuration);
  }

  if (!options.protocol_path.empty()) {
    couchbase::core::logger::configuration configuration{};
    configuration.filename = options.protocol_path;
    couchbase::core::logger::create_protocol_logger(configuration);
  }

  spdlog::set_level(spdlog::level::from_str(options.level));
  couchbase::core::logger::set_log_levels(level);
}

auto
build_cluster_options(const common_options& options) -> couchbase::cluster_options
{
  auto cluster_options = create_cluster_options(options.connection);

  apply_options(cluster_options, options.security);
  apply_options(cluster_options, options.timeouts);
  apply_options(cluster_options, options.compression);
  apply_options(cluster_options, options.dns_srv);
  apply_options(cluster_options, options.network);
  apply_options(cluster_options, options.transactions);
  apply_options(cluster_options, options.metrics);
  apply_options(cluster_options, options.tracing);
  apply_options(cluster_options, options.behavior);

  if (!options.connection.configuration_profile.empty()) {
    cluster_options.apply_profile(options.connection.configuration_profile);
  }

  return cluster_options;
}

auto
extract_inlined_keyspace(const std::string& id) -> std::optional<keyspace_with_id>
{
  static const std::regex inlined_keyspace_regex{ R"(^(.*?):(.*?)\.(.*?):(.*)$)" };

  if (std::smatch match; std::regex_match(id, match, inlined_keyspace_regex)) {
    keyspace_with_id ks_id{};
    ks_id.bucket_name = match[1];
    ks_id.scope_name = match[2];
    ks_id.collection_name = match[3];
    ks_id.id = match[4];
    return ks_id;
  }

  return {};
}

auto
extract_inlined_value(const std::string& id, const std::optional<std::string>& separator)
  -> std::optional<document_id_with_value>
{
  if (separator.has_value()) {
    auto pos = id.find(separator.value());
    if (pos == std::string::npos || pos == 0) {
      return {};
    }
    document_id_with_value result;
    result.id = id.substr(0, pos);
    result.value.data =
      couchbase::core::utils::to_binary(id.data() + (pos + 1), id.size() - (pos + 1));
    try {
      auto json = couchbase::core::utils::json::parse_binary(result.value.data);
      result.value.flags = couchbase::codec::codec_flags::json_common_flags;
    } catch (const tao::pegtl::parse_error&) {
      result.value.flags = couchbase::codec::codec_flags::binary_common_flags;
    }
    return result;
  }

  return {};
}

auto
extract_inlined_cas(const std::string& id, const std::optional<std::string>& separator)
  -> std::optional<document_id_with_cas>
{
  if (separator.has_value()) {
    auto pos = id.find(separator.value());
    if (pos == std::string::npos || pos == 0) {
      return {};
    }
    document_id_with_cas result;
    result.id = id.substr(0, pos);
    try {
      result.cas =
        couchbase::cas{ std::stoull(id.substr(pos + 1), nullptr, 0 /* auto detect base */) };
    } catch (const std::logic_error&) {
      return {};
    }
    return result;
  }

  return {};
}

auto
available_query_scan_consistency_modes() -> std::vector<std::string>
{
  return {
    fmt::format("{}", couchbase::query_scan_consistency::not_bounded),
    fmt::format("{}", couchbase::query_scan_consistency::request_plus),
  };
}

auto
available_analytics_scan_consistency_modes() -> std::vector<std::string>
{
  return {
    fmt::format("{}", couchbase::analytics_scan_consistency::not_bounded),
    fmt::format("{}", couchbase::analytics_scan_consistency::request_plus),
  };
}

[[noreturn]] void
fail(std::string_view message)
{
  fmt::print(stderr, "ERROR: {}\n", message);

#if defined(__APPLE__) && (__MAC_OS_X_VERSION_MAX_ALLOWED < 150000)
  std::_Exit(EXIT_FAILURE);
#else
  std::quick_exit(EXIT_FAILURE);
#endif
}

auto
cas_to_time_point(couchbase::cas cas) -> std::chrono::system_clock::time_point
{
  return std::chrono::system_clock::time_point{ std::chrono::milliseconds(cas.value()) / 1000000 };
}
} // namespace cbc
