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

#include <core/logger/configuration.hxx>
#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>

#include <couchbase/fmt/durability_level.hxx>
#include <couchbase/fmt/query_scan_consistency.hxx>
#include <couchbase/fmt/tls_verify_mode.hxx>

#include <spdlog/details/os.h>
#include <spdlog/spdlog.h>

#include <regex>

namespace cbc
{
docopt::Options
parse_options(const std::string& doc, const std::vector<std::string>& argv, bool options_first)
{
    try {
        return docopt::docopt_parse(doc, argv, false, true, options_first);
    } catch (const docopt::DocoptExitHelp&) {
        fmt::print(stdout, doc);
        std::exit(0);
    } catch (const docopt::DocoptExitVersion&) {
        fmt::print(stdout, "cbc {}\n", couchbase::core::meta::sdk_semver());
        std::exit(0);
    } catch (const docopt::DocoptLanguageError& error) {
        fmt::print(stdout, "Docopt usage string could not be parsed. Please report this error.\n{}\n", error.what());
        std::exit(-1);
    } catch (const docopt::DocoptArgumentError&) {
        fmt::print(stdout, doc);
        fflush(stdout);
        throw;
    }
}

static auto
getenv_or_default(std::string_view var_name, const std::string& default_value) -> std::string
{
    if (auto val = spdlog::details::os::getenv(var_name.data()); !val.empty()) {
        return val;
    }
    return default_value;
}

auto
default_cluster_options() -> const couchbase::cluster_options&
{
    static const couchbase::cluster_options default_cluster_options_(getenv_or_default("CBC_USERNAME", "Administrator"),
                                                                     getenv_or_default("CBC_PASSWORD", "password"));
    return default_cluster_options_;
}

auto
default_connection_string() -> const std::string&
{
    static const std::string default_connection_string_ = getenv_or_default("CBC_CONNECTION_STRING", "couchbase://localhost");

    return default_connection_string_;
}

auto
usage_block_for_cluster_options() -> std::string
{
    const auto default_user_agent_extra{ "cbc" };
    const auto default_options = default_cluster_options().build();
    const auto connection_string = default_connection_string();

    return fmt::format(
      R"(
Connection options:
  --connection-string=STRING      Connection string for the cluster. CBC_CONNECTION_STRING. [default: {connection_string}]
  --username=STRING               Username for the cluster. CBC_USERNAME. [default: {username}]
  --password=STRING               Password for the cluster. CBC_PASSWORD. [default: {password}]
  --certificate-path=STRING       Path to the certificate.
  --key-path=STRING               Path to the key.
  --ldap-compatible               Whether to select authentication mechanism that is compatible with LDAP.
  --configuration-profile=STRING  Apply configuration profile. (available profiles: {available_configuration_profiles})

Security options:
  --disable-tls                    Whether to disable TLS.
  --trust-certificate-path=STRING  Path to the trust certificate bundle.
  --tls-verify-mode=MODE           Path to the certificate (allowed values: peer, none). [default: {tls_verify_mode}]

Timeout options:
  --bootstrap-timeout=DURATION          Timeout for overall bootstrap of the SDK. [default: {bootstrap_timeout}]
  --connect-timeout=DURATION            Timeout for socket connection. [default: {connect_timeout}]
  --resolve-timeout=DURATION            Timeout to resolve DNS address for the sockets. [default: {resolve_timeout}]
  --key-value-timeout=DURATION          Timeout for Key/Value operations. [default: {key_value_timeout}]
  --key-value-durable-timeout=DURATION  Timeout for Key/Value durable operations. [default: {key_value_durable_timeout}]
  --query-timeout=DURATION              Timeout for Query service. [default: {query_timeout}]
  --search-timeout=DURATION             Timeout for Search service. [default: {search_timeout}]
  --eventing-timeout=DURATION           Timeout for Eventing service. [default: {eventing_timeout}]
  --analytics-timeout=DURATION          Timeout for Analytics service. [default: {analytics_timeout}]
  --view-timeout=DURATION               Timeout for View service. [default: {view_timeout}]
  --management-timeout=DURATION         Timeout for management operations. [default: {management_timeout}]

Compression options:
  --disable-compression               Whether to disable compression.
  --compression-minimum-size=INTEGER  The minimum size of the document (in bytes), that will be compressed. [default: {compression_minimum_size}]
  --compression-minimum-ratio=FLOAT   The minimum compression ratio to allow compressed form to be used. [default: {compression_minimum_ratio}]

DNS-SRV options:
  --dns-srv-timeout=DURATION   Timeout for DNS SRV requests. [default: {dns_srv_timeout}]
  --dns-srv-nameserver=STRING  Hostname of the DNS server where the DNS SRV requests will be sent.
  --dns-srv-port=INTEGER       Port of the DNS server where the DNS SRV requests will be sent.

Network options:
  --tcp-keep-alive-interval=DURATION       Interval for TCP keep alive. [default: {tcp_keep_alive_interval}]
  --config-poll-interval=DURATION          How often the library should poll for new configuration. [default: {config_poll_interval}]
  --idle-http-connection-timeout=DURATION  Period to wait before calling HTTP connection idle. [default: {idle_http_connection_timeout}]

Transactions options:
  --transactions-durability-level=LEVEL          Durability level of the transaction (allowed values: none, majority, majority_and_persist_to_active, persist_to_majority). [default: {transactions_durability_level}]
  --transactions-expiration-time=DURATION        Expiration time of the transaction. [default: {transactions_expiration_time}]
  --transactions-key-value-timeout=DURATION      Override Key/Value timeout just for the transaction.
  --transactions-metadata-bucket=STRING          Bucket name where transaction metadata is stored.
  --transactions-metadata-scope=STRING           Scope name where transaction metadata is stored. [default: {transactions_metadata_scope}]
  --transactions-metadata-collection=STRING      Collection name where transaction metadata is stored. [default: {transactions_metadata_collection}]
  --transactions-query-scan-consistency=MODE     Scan consistency for queries in transactions (allowed values: not_bounded, request_plus). [default: {transactions_query_scan_consistency}]
  --transactions-cleanup-ignore-lost-attempts    Do not cleanup lost attempts.
  --transactions-cleanup-ignore-client-attempts  Do not cleanup client attempts.
  --transactions-cleanup-window=DURATION         Cleanup window. [default: {transactions_cleanup_window}]

Metrics options:
  --disable-metrics                 Disable collecting and reporting metrics.
  --metrics-emit-interval=DURATION  Interval to emit metrics report on INFO log level. [default: {metrics_emit_interval}]

Tracing options:
  --disable-tracing                           Disable collecting and reporting trace information.
  --tracing-orphaned-emit-interval=DURATION   Interval to emit report about orphan operations. [default: {tracing_orphaned_emit_interval}]
  --tracing-orphaned-sample-size=INTEGER      Size of the sample of the orphan report. [default: {tracing_orphaned_sample_size}]
  --tracing-threshold-emit-interval=DURATION  Interval to emit report about operations exceeding threshold. [default: {tracing_threshold_emit_interval}]
  --tracing-threshold-sample-size=INTEGER     Size of the sample of the threshold report. [default: {tracing_threshold_sample_size}]
  --tracing-threshold-key-value=DURATION      Threshold for Key/Value service. [default: {tracing_threshold_key_value}]
  --tracing-threshold-query=DURATION          Threshold for Query service. [default: {tracing_threshold_query}]
  --tracing-threshold-search=DURATION         Threshold for Search service. [default: {tracing_threshold_search}]
  --tracing-threshold-analytics=DURATION      Threshold for Analytics service. [default: {tracing_threshold_analytics}]
  --tracing-threshold-management=DURATION     Threshold for Management operations. [default: {tracing_threshold_management}]
  --tracing-threshold-eventing=DURATION       Threshold for Eventing service. [default: {tracing_threshold_eventing}]
  --tracing-threshold-view=DURATION           Threshold for View service. [default: {tracing_threshold_view}]

Behavior options:
  --user-agent-extra=STRING          Append extra string SDK identifiers (full user-agent is "{sdk_id};{user_agent_extra}"). [default: {user_agent_extra}].
  --show-queries                     Log queries on INFO level.
  --enable-clustermap-notifications  Allow server to send notifications when cluster configuration changes.
  --disable-mutation-tokens          Do not request Key/Value service to send mutation tokens.
  --disable-unordered-execution      Disable unordered execution for Key/Value service.
  --dump-configuration               Dump every new configuration on TRACE log level.
)",
      fmt::arg("connection_string", connection_string),
      fmt::arg("username", default_options.username),
      fmt::arg("password", default_options.password),
      fmt::arg("available_configuration_profiles", fmt::join(couchbase::configuration_profiles_registry::available_profiles(), ", ")),
      fmt::arg("tls_verify_mode", default_options.security.tls_verify),
      fmt::arg("bootstrap_timeout", default_options.timeouts.bootstrap_timeout),
      fmt::arg("connect_timeout", default_options.timeouts.connect_timeout),
      fmt::arg("resolve_timeout", default_options.timeouts.resolve_timeout),
      fmt::arg("key_value_timeout", default_options.timeouts.key_value_timeout),
      fmt::arg("key_value_durable_timeout", default_options.timeouts.key_value_durable_timeout),
      fmt::arg("query_timeout", default_options.timeouts.query_timeout),
      fmt::arg("search_timeout", default_options.timeouts.search_timeout),
      fmt::arg("eventing_timeout", default_options.timeouts.eventing_timeout),
      fmt::arg("analytics_timeout", default_options.timeouts.analytics_timeout),
      fmt::arg("view_timeout", default_options.timeouts.view_timeout),
      fmt::arg("management_timeout", default_options.timeouts.management_timeout),
      fmt::arg("compression_minimum_size", default_options.compression.min_size),
      fmt::arg("compression_minimum_ratio", default_options.compression.min_ratio),
      fmt::arg("dns_srv_timeout", default_options.dns.timeout),
      fmt::arg("tcp_keep_alive_interval", default_options.network.tcp_keep_alive_interval),
      fmt::arg("config_poll_interval", default_options.network.config_poll_interval),
      fmt::arg("idle_http_connection_timeout", default_options.network.idle_http_connection_timeout),
      fmt::arg("transactions_durability_level", default_options.transactions.level),
      fmt::arg("transactions_expiration_time",
               std::chrono::duration_cast<std::chrono::milliseconds>(default_options.transactions.expiration_time)),
      fmt::arg("transactions_metadata_scope", couchbase::scope::default_name),
      fmt::arg("transactions_metadata_collection", couchbase::scope::default_name),
      fmt::arg("transactions_query_scan_consistency", default_options.transactions.query_config.scan_consistency),
      fmt::arg("transactions_cleanup_window", default_options.transactions.cleanup_config.cleanup_window),
      fmt::arg("metrics_emit_interval", default_options.metrics.emit_interval),
      fmt::arg("tracing_orphaned_emit_interval", default_options.tracing.orphaned_emit_interval),
      fmt::arg("tracing_orphaned_sample_size", default_options.tracing.orphaned_sample_size),
      fmt::arg("tracing_threshold_emit_interval", default_options.tracing.threshold_emit_interval),
      fmt::arg("tracing_threshold_sample_size", default_options.tracing.threshold_sample_size),
      fmt::arg("tracing_threshold_key_value", default_options.tracing.key_value_threshold),
      fmt::arg("tracing_threshold_query", default_options.tracing.query_threshold),
      fmt::arg("tracing_threshold_view", default_options.tracing.view_threshold),
      fmt::arg("tracing_threshold_search", default_options.tracing.search_threshold),
      fmt::arg("tracing_threshold_analytics", default_options.tracing.analytics_threshold),
      fmt::arg("tracing_threshold_management", default_options.tracing.management_threshold),
      fmt::arg("tracing_threshold_eventing", default_options.tracing.eventing_threshold),
      fmt::arg("sdk_id", couchbase::core::meta::sdk_id()),
      fmt::arg("user_agent_extra", default_user_agent_extra));
}

void
fill_cluster_options(const docopt::Options& options, couchbase::cluster_options& cluster_options, std::string& connection_string)
{
    connection_string = options.at("--connection-string").asString();
    if (options.find("--certificate-path") != options.end() && options.at("--certificate-path") &&
        options.find("--key-path") != options.end() && options.at("--key-path")) {
        cluster_options = couchbase::cluster_options(
          couchbase::certificate_authenticator(options.at("--certificate-path").asString(), options.at("--key-path").asString()));
    } else {
        auto username = options.at("--username").asString();
        auto password = options.at("--password").asString();
        if (options.find("--ldap-compatible") != options.end() && options.at("--ldap-compatible") &&
            options.at("--ldap-compatible").asBool()) {
            cluster_options = couchbase::cluster_options(couchbase::password_authenticator::ldap_compatible(username, password));
        } else {
            cluster_options = couchbase::cluster_options(couchbase::password_authenticator(username, password));
        }
    }

    if (options.find("--configuration-profile") != options.end() && options.at("--configuration-profile")) {
        cluster_options.apply_profile(options.at("--configuration-profile").asString());
    }

    parse_disable_option(cluster_options.security().enabled, "--disable-tls");
    parse_string_option(cluster_options.security().trust_certificate, "--trust-certificate-path");
    if (options.find("--tls-verify-mode") != options.end() && options.at("--tls-verify-mode")) {
        if (auto value = options.at("--tls-verify-mode").asString(); value == "none") {
            cluster_options.security().tls_verify(couchbase::tls_verify_mode::none);
        } else if (value == "peer") {
            cluster_options.security().tls_verify(couchbase::tls_verify_mode::peer);
        } else {
            throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --tls-verify-mode", value));
        }
    }

    parse_duration_option(cluster_options.timeouts().bootstrap_timeout, "--bootstrap-timeout");
    parse_duration_option(cluster_options.timeouts().connect_timeout, "--connect-timeout");
    parse_duration_option(cluster_options.timeouts().resolve_timeout, "--resolve-timeout");
    parse_duration_option(cluster_options.timeouts().key_value_timeout, "--key-value-timeout");
    parse_duration_option(cluster_options.timeouts().key_value_durable_timeout, "--key-value-durable-timeout");
    parse_duration_option(cluster_options.timeouts().query_timeout, "--query-timeout");
    parse_duration_option(cluster_options.timeouts().search_timeout, "--search-timeout");
    parse_duration_option(cluster_options.timeouts().eventing_timeout, "--eventing-timeout");
    parse_duration_option(cluster_options.timeouts().analytics_timeout, "--analytics-timeout");
    parse_duration_option(cluster_options.timeouts().view_timeout, "--view-timeout");
    parse_duration_option(cluster_options.timeouts().management_timeout, "--management-timeout");

    parse_disable_option(cluster_options.compression().enabled, "--disable-compression");
    parse_integer_option(cluster_options.compression().min_size, "--compression-minimum-size");
    parse_float_option(cluster_options.compression().min_ratio, "--compression-minimum-ratio");

    parse_duration_option(cluster_options.dns().timeout, "--dns-srv-timeout");
    if (options.find("--dns-srv-nameserver") != options.end() && options.at("--dns-srv-nameserver")) {
        auto hostname = options.at("--dns-srv-nameserver").asString();
        if (options.find("--dns-srv-port") != options.end() && options.at("--dns-srv-port")) {
            auto value = options.at("--dns-srv-port").asString();
            try {
                cluster_options.dns().nameserver(hostname, static_cast<std::uint16_t>(std::stoul(value, nullptr, 10)));
            } catch (const std::invalid_argument&) {
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as integer in --dns-srv-port: not a number", value));
            } catch (const std::out_of_range&) {
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as integer in --dns-srv-port: out of range", value));
            }
        } else {
            cluster_options.dns().nameserver(hostname);
        }
    }

    parse_duration_option(cluster_options.network().tcp_keep_alive_interval, "--tcp-keep-alive-interval");
    parse_duration_option(cluster_options.network().config_poll_interval, "--config-poll-interval");
    parse_duration_option(cluster_options.network().idle_http_connection_timeout, "--idle-http-connection-timeout");

    if (options.find("--transactions-durability-level") != options.end() && options.at("--transactions-durability-level")) {
        if (auto value = options.at("--transactions-durability-level").asString(); value == "none") {
            cluster_options.transactions().durability_level(couchbase::durability_level::none);
        } else if (value == "majority") {
            cluster_options.transactions().durability_level(couchbase::durability_level::majority);
        } else if (value == "majority_and_persist_to_active") {
            cluster_options.transactions().durability_level(couchbase::durability_level::majority_and_persist_to_active);
        } else if (value == "persist_to_majority") {
            cluster_options.transactions().durability_level(couchbase::durability_level::persist_to_majority);
        } else {
            throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --transactions-durability-level", value));
        }
    }
    parse_duration_option(cluster_options.transactions().expiration_time, "--transactions-expiration-time");
    parse_duration_option(cluster_options.transactions().kv_timeout, "--transactions-key-value-timeout");
    if (options.find("--transactions-metadata-bucket") != options.end() && options.at("--transactions-metadata-bucket")) {
        if (auto bucket = options.at("--transactions-metadata-bucket").asString(); !bucket.empty()) {
            cluster_options.transactions().metadata_collection({ bucket,
                                                                 options.at("--transactions-metadata-scope").asString(),
                                                                 options.at("--transactions-metadata-collection").asString() });
        } else {
            throw docopt::DocoptArgumentError("empty value for --transactions-metadata-bucket");
        }
    }
    if (options.find("--transactions-query-scan-consistency") != options.end() && options.at("--transactions-query-scan-consistency")) {
        if (auto value = options.at("--transactions-query-scan-consistency").asString(); value == "not_bounded") {
            cluster_options.transactions().query_config().scan_consistency(couchbase::query_scan_consistency::not_bounded);
        } else if (value == "request_plus") {
            cluster_options.transactions().query_config().scan_consistency(couchbase::query_scan_consistency::request_plus);
        } else {
            throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --transactions-query-scan-consistency", value));
        }
    }
    parse_disable_option(cluster_options.transactions().cleanup_config().cleanup_lost_attempts,
                         "--transactions-cleanup-ignore-lost-attempts");
    parse_disable_option(cluster_options.transactions().cleanup_config().cleanup_client_attempts,
                         "--transactions-cleanup-ignore-client-attempts");
    parse_duration_option(cluster_options.transactions().cleanup_config().cleanup_window, "--transactions-cleanup-window");

    parse_disable_option(cluster_options.metrics().enable, "--disable-metrics");
    parse_duration_option(cluster_options.metrics().emit_interval, "--metrics-emit-interval");

    parse_disable_option(cluster_options.tracing().enable, "--disable-tracing");
    parse_duration_option(cluster_options.tracing().orphaned_emit_interval, "--tracing-orphaned-emit-interval");
    parse_integer_option(cluster_options.tracing().orphaned_sample_size, "--tracing-orphaned-sample-size");
    parse_duration_option(cluster_options.tracing().threshold_emit_interval, "--tracing-threshold-emit-interval");
    parse_integer_option(cluster_options.tracing().threshold_sample_size, "--tracing-threshold-sample-size");
    parse_duration_option(cluster_options.tracing().key_value_threshold, "--tracing-threshold-key-value");
    parse_duration_option(cluster_options.tracing().query_threshold, "--tracing-threshold-query");
    parse_duration_option(cluster_options.tracing().search_threshold, "--tracing-threshold-search");
    parse_duration_option(cluster_options.tracing().analytics_threshold, "--tracing-threshold-analytics");
    parse_duration_option(cluster_options.tracing().management_threshold, "--tracing-threshold-management");
    parse_duration_option(cluster_options.tracing().eventing_threshold, "--tracing-threshold-eventing");
    parse_duration_option(cluster_options.tracing().view_threshold, "--tracing-threshold-view");

    parse_string_option(cluster_options.behavior().append_to_user_agent, "--user-agent-extra");
    parse_enable_option(cluster_options.behavior().show_queries, "--show-queries");
    parse_enable_option(cluster_options.behavior().dump_configuration, "--dump-configuration");
    parse_enable_option(cluster_options.behavior().enable_clustermap_notification, "--enable-clustermap-notification");
    parse_disable_option(cluster_options.behavior().enable_mutation_tokens, "--disable-mutation-tokens");
    parse_disable_option(cluster_options.behavior().enable_unordered_execution, "--disable-disable-unordered-execution");
}

auto
default_log_level() -> const std::string&
{
    static const std::string default_log_level_ = getenv_or_default("CBC_LOG_LEVEL", "off");

    return default_log_level_;
}

auto
usage_block_for_logger() -> std::string
{
    static const std::vector<std::string> allowed_log_levels{
        "trace", "debug", "info", "warning", "error", "critical", "off",
    };

    return fmt::format(R"(
Logger options:
  --log-level=LEVEL  Log level (allowed values are: {allowed_levels}). CBC_LOG_LEVEL. [default: {log_level}]
  --log-output=PATH  File to send logs (when is not set, logs will be written to STDERR).
)",
                       fmt::arg("allowed_levels", fmt::join(allowed_log_levels, ", ")),
                       fmt::arg("log_level", default_log_level()));
}

void
apply_logger_options(const docopt::Options& options)
{
    auto log_level = options.at("--log-level").asString();
    auto level = couchbase::core::logger::level_from_str(log_level);

    if (level != couchbase::core::logger::level::off) {
        couchbase::core::logger::configuration configuration{};

        if (options.find("--log-output") != options.end() && options.at("--log-output")) {
            configuration.filename = options.at("--log-output").asString();
        } else {
            configuration.console = true;
            configuration.unit_test = true;
        }
        configuration.log_level = level;
        couchbase::core::logger::create_file_logger(configuration);
    }

    spdlog::set_level(spdlog::level::from_str(log_level));
    couchbase::core::logger::set_log_levels(level);
}

auto
extract_inlined_keyspace(const std::string& id) -> std::optional<keyspace_with_id>
{
    static const std::regex inlined_keyspace_regex{ R"(^(.*?):(.*?)\.(.*?):(.*)$)" };

    std::smatch match;
    if (std::regex_match(id, match, inlined_keyspace_regex)) {
        keyspace_with_id ks_id{};
        ks_id.bucket_name = match[1];
        ks_id.scope_name = match[2];
        ks_id.collection_name = match[3];
        ks_id.id = match[4];
        return ks_id;
    }

    return {};
}

bool
get_bool_option(const docopt::Options& options, const std::string& name)
{
    return options.find(name) != options.end() && options.at(name) && options.at(name).asBool();
}

double
get_double_option(const docopt::Options& options, const std::string& name)
{
    auto value = options.at(name).asString();
    std::size_t pos;
    const double ret = std::stod(value, &pos);
    if (pos != value.length()) {
        throw std::runtime_error(value + " contains non-numeric characters.");
    }
    return ret;
}
} // namespace cbc
