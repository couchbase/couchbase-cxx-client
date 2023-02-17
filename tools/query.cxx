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

#include "query.hxx"
#include "core/utils/binary.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/query_status.hxx>

#include <asio/io_context.hpp>
#include <fmt/chrono.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <tao/json.hpp>

#include <regex>

namespace cbc
{
namespace
{
static auto
usage() -> std::string
{
    static const std::string default_bucket_name{ "default" };

    static const std::string usage_string = fmt::format(
      R"(Display version information.

Usage:
  cbc query [options] <statement>...
  cbc query [options] --param=NAME=VALUE... <statement>...
  cbc query [options] --raw=NAME=VALUE... <statement>...
  cbc query (-h|--help)

Options:
  -h --help                      Show this screen.
  --param=NAME=VALUE             Parameters for the query. Without '=' sign value will be treated as positional parameter.
  --prepare                      Prepare statement.
  --read-only                    Mark query as read only. Any mutations will fail.
  --preserve-expiry              Ensure that expiry will be preserved after mutations.
  --disable-metrics              Do not request metrics.
  --profile=MODE                 Request the service to profile the query and return report (allowed values: off, phases, timings).
  --bucket-name=STRING           Name of the bucket where the scope is defined (see --scope-name).
  --scope-name=STRING            Name of the scope.
  --client-context-id=STRING     Override client context ID for the query(-ies).
  --flex-index                   Tell query service to utilize flex index (full text search).
  --maximum-parallelism=INTEGER  Parallelism for query execution (0 to disable).
  --scan-cap=INTEGER             Maximum buffer size between indexer and query service.
  --scan-wait=DURATION           How long query engine will wait for indexer to catch up on scan consistency.
  --scan-consistency=MODE        Set consistency guarantees for the query (allowed values: not_bounded, request_plus).
  --pipeline-batch=INTEGER       Number of items execution operators can batch for fetch from the Key/Value service.
  --pipeline-cap=INTEGER         Maximum number of items each execution operator can buffer between various operators.
  --raw=NAME=VALUE               Set any query option for the query. Read the documentation: https://docs.couchbase.com/server/current/n1ql/n1ql-rest-api.
  --json-lines                   Use JSON Lines format (https://jsonlines.org/) to print results.

{logger_options}{cluster_options}

Examples:

1. Query with positional parameters:

  cbc query --param 1 --param 2 'SELECT $1 + $2'

2. Query with named parameters:

  cbc query --param a=1 --param b=2 'SELECT $a + $b'
)",
      fmt::arg("logger_options", usage_block_for_logger()),
      fmt::arg("cluster_options", usage_block_for_cluster_options()));

    return usage_string;
}

struct console_output_options {
    bool json_lines{ false };
};

struct scope_with_bucket {
    std::string bucket_name;
    std::string scope_name;
};

template<typename QueryEndpoint>
static auto
do_query(QueryEndpoint& endpoint, std::string statement, const couchbase::query_options& options)
{
    return endpoint.query(std::move(statement), options);
}

static void
print_result_json_line(const std::optional<scope_with_bucket>& scope_id,
                       const std::string& statement,
                       const couchbase::query_error_context& ctx,
                       const couchbase::query_result& resp,
                       const couchbase::query_options& query_options)
{
    auto built_options = query_options.build();

    tao::json::value line = tao::json::empty_object;

    tao::json::value meta = {
        { "statement", statement },
    };
    if (scope_id) {
        meta["bucket_name"] = scope_id->bucket_name;
        meta["scope_name"] = scope_id->scope_name;
    }
    if (ctx.parameters()) {
        try {
            auto json = tao::json::from_string(ctx.parameters().value());
            json.erase("statement");
            meta["options"] = json;
        } catch (const tao::pegtl::parse_error&) {
            meta["options"] = ctx.parameters().value();
        }
    }

    if (ctx.ec()) {
        tao::json::value error = {
            { "code", ctx.ec().value() },
            { "message", ctx.ec().message() },
        };
        try {
            error["body"] = tao::json::from_string(ctx.http_body());
        } catch (const tao::pegtl::parse_error&) {
            error["text"] = ctx.http_body();
        }
        line["error"] = error;
    } else {
        meta["status"] = fmt::format("{}", resp.meta_data().status());
        meta["client_context_id"] = resp.meta_data().client_context_id();
        meta["request_id"] = resp.meta_data().request_id();
        if (const auto& signature = resp.meta_data().signature()) {
            try {
                meta["signature"] = couchbase::core::utils::json::parse_binary(signature.value());
            } catch (const tao::pegtl::parse_error&) {
                meta["signature"] = signature.value();
            }
        }
        if (const auto& metrics = resp.meta_data().metrics(); metrics.has_value()) {
            meta["metrics"] = {
                { "elapsed_time", fmt::format("{}", metrics->elapsed_time()) },
                { "execution_time", fmt::format("{}", metrics->execution_time()) },
                { "result_count", metrics->result_count() },
                { "result_size", metrics->result_size() },
                { "sort_count", metrics->sort_count() },
                { "mutation_count", metrics->mutation_count() },
                { "error_count", metrics->error_count() },
                { "warning_count", metrics->warning_count() },
            };
        }
        if (const auto& profile = resp.meta_data().profile(); profile) {
            try {
                meta["profile"] = couchbase::core::utils::json::parse_binary(profile.value());
            } catch (const tao::pegtl::parse_error&) {
                meta["profile"] = profile.value();
            }
        }
        if (!resp.meta_data().warnings().empty()) {
            tao::json::value warnings = tao::json::empty_array;
            for (const auto& item : resp.meta_data().warnings()) {
                tao::json::value warning = {
                    { "message", item.message() },
                    { "code", item.code() },
                };
                if (item.reason()) {
                    warning["reason"] = item.reason().value();
                }
                if (item.retry()) {
                    warning["retry"] = item.retry().value();
                }
                warnings.emplace_back(warning);
            }
            meta["warnings"] = warnings;
        }
        tao::json::value rows = tao::json::empty_array;
        for (const auto& row : resp.rows_as_binary()) {
            try {
                rows.emplace_back(couchbase::core::utils::json::parse_binary(row));
            } catch (const tao::pegtl::parse_error&) {
                rows.emplace_back(row);
            }
        }
        line["rows"] = rows;
    }
    line["meta"] = meta;
    fmt::print(stdout, "{}\n", tao::json::to_string<tao::json::events::binary_to_base64>(line));
    fflush(stdout);
}

static void
print_result(const std::optional<scope_with_bucket>& scope_id,
             const std::string& statement,
             const couchbase::query_error_context& ctx,
             const couchbase::query_result& resp,
             const couchbase::query_options& query_options)
{
    auto built_options = query_options.build();

    auto header = fmt::memory_buffer();
    if (scope_id) {
        fmt::format_to(std::back_inserter(header), "bucket_name: {}, scope_name: {}", scope_id->bucket_name, scope_id->scope_name);
    }
    fmt::format_to(
      std::back_inserter(header), "{}statement: \"{}\"", header.size() == 0 ? "" : ", ", tao::json::internal::escape(statement));

    if (ctx.parameters()) {
        try {
            auto json = tao::json::from_string(ctx.parameters().value());
            json.erase("statement");
            fmt::format_to(std::back_inserter(header), ", options: {}", tao::json::to_string(json));
        } catch (const tao::pegtl::parse_error&) {
            fmt::format_to(std::back_inserter(header), ", options: {}", ctx.parameters().value());
        }
    }
    fmt::print(stdout, "--- {}\n", std::string_view{ header.data(), header.size() });

    if (ctx.ec()) {
        fmt::print("ERROR. code: {}, message: {}, server: {} \"{}\")\n",
                   ctx.ec().value(),
                   ctx.ec().message(),
                   ctx.first_error_code(),
                   tao::json::internal::escape(ctx.first_error_message()));
        if (!ctx.http_body().empty()) {
            try {
                fmt::print("{}\n", tao::json::to_string(tao::json::from_string(ctx.http_body())));
            } catch (const tao::pegtl::parse_error&) {
                fmt::print("{}\n", ctx.http_body());
            }
        }
    } else {
        auto meta = fmt::memory_buffer();
        fmt::format_to(std::back_inserter(meta),
                       "status: {}, client_context_id: \"{}\", request_id: \"{}\"",
                       resp.meta_data().status(),
                       resp.meta_data().client_context_id(),
                       resp.meta_data().request_id());
        if (const auto& metrics = resp.meta_data().metrics(); metrics.has_value()) {
            fmt::format_to(std::back_inserter(meta),
                           ", elapsed: {}, execution: {}, result: {}, sort: {}, mutations: {}, errors: {}, warnings: {}",
                           metrics->elapsed_time(),
                           metrics->execution_time(),
                           metrics->result_count(),
                           metrics->sort_count(),
                           metrics->mutation_count(),
                           metrics->error_count(),
                           metrics->warning_count());
        }
        fmt::print(stdout, "{}\n", std::string_view{ meta.data(), meta.size() });
        if (!resp.meta_data().warnings().empty()) {
            for (const auto& item : resp.meta_data().warnings()) {
                auto warning = fmt::memory_buffer();
                fmt::format_to(std::back_inserter(warning), "WARNING. code: {}, message: \"{}\"\n", item.code(), item.message());
                if (item.reason()) {
                    fmt::format_to(std::back_inserter(warning), ", reason: {}\n", item.reason().value());
                }
                if (item.retry()) {
                    fmt::format_to(std::back_inserter(warning), ", retry: {}\n", item.retry().value());
                }
                fmt::print(stdout, "{}\n", std::string_view{ warning.data(), warning.size() });
            }
        }
        if (const auto& profile = resp.meta_data().profile(); profile) {
            try {
                fmt::print("{}\n", tao::json::to_string(couchbase::core::utils::json::parse_binary(profile.value()), 2));
            } catch (const tao::pegtl::parse_error&) {
                fmt::print("{:a}\n", spdlog::to_hex(profile.value()));
            }
        }
        for (const auto& row : resp.rows_as_binary()) {
            try {
                fmt::print("{}\n", tao::json::to_string(couchbase::core::utils::json::parse_binary(row)));
            } catch (const tao::pegtl::parse_error&) {
                fmt::print("{:a}\n", spdlog::to_hex(row));
            }
        }
    }
    fflush(stdout);
}

static void
do_work(const std::string& connection_string,
        couchbase::cluster_options& cluster_options,
        const std::vector<std::string>& statements,
        const std::optional<scope_with_bucket>& scope_id,
        const couchbase::query_options& query_options,
        const console_output_options& output_options)
{
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, cluster_options).get();
    if (ec) {
        throw std::system_error(ec);
    }

    std::optional<couchbase::scope> scope{};
    if (scope_id) {
        scope = cluster.bucket(scope_id->bucket_name).scope(scope_id->scope_name);
    }

    for (const auto& statement : statements) {
        auto [ctx, resp] = (scope ? do_query(scope.value(), statement, query_options) : do_query(cluster, statement, query_options)).get();

        if (output_options.json_lines) {
            print_result_json_line(scope_id, statement, ctx, resp, query_options);
        } else {
            print_result(scope_id, statement, ctx, resp, query_options);
        }
    }

    cluster.close();
    guard.reset();

    io_thread.join();
}
} // namespace

void
cbc::query::execute(const std::vector<std::string>& argv)
{
    try {

        auto options = cbc::parse_options(usage(), argv);
        if (options["--help"].asBool()) {
            fmt::print(stdout, usage());
            return;
        }

        cbc::apply_logger_options(options);

        couchbase::cluster_options cluster_options{ cbc::default_cluster_options() };
        std::string connection_string{ cbc::default_connection_string() };
        cbc::fill_cluster_options(options, cluster_options, connection_string);

        couchbase::query_options query_options{};
        parse_disable_option(query_options.adhoc, "--prepare");
        parse_enable_option(query_options.readonly, "--read-only");
        parse_enable_option(query_options.preserve_expiry, "--preserve-expiry");
        parse_disable_option(query_options.metrics, "--disable-metrics");
        parse_enable_option(query_options.flex_index, "--flex-index");
        parse_integer_option(query_options.max_parallelism, "--maximum-parallelism");
        parse_integer_option(query_options.scan_cap, "--scan-cap");
        parse_integer_option(query_options.pipeline_batch, "--pipeline-batch");
        parse_integer_option(query_options.pipeline_cap, "--pipeline-cap");
        parse_duration_option(query_options.scan_wait, "--scan-wait");
        parse_string_option(query_options.client_context_id, "--client-context-id");

        if (options.find("--scan-consistency") != options.end() && options.at("--scan-consistency")) {
            if (auto value = options.at("--scan-consistency").asString(); value == "not_bounded") {
                query_options.scan_consistency(couchbase::query_scan_consistency::not_bounded);
            } else if (value == "request_plus") {
                query_options.scan_consistency(couchbase::query_scan_consistency::request_plus);
            } else {
                throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --scan-consistency", value));
            }
        }

        if (options.find("--profile") != options.end() && options.at("--profile")) {
            if (auto value = options.at("--profile").asString(); value == "off") {
                query_options.profile(couchbase::query_profile::off);
            } else if (value == "phases") {
                query_options.profile(couchbase::query_profile::phases);
            } else if (value == "timings") {
                query_options.profile(couchbase::query_profile::phases);
            } else {
                throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --profile", value));
            }
        }

        std::optional<scope_with_bucket> scope{};
        if (options.find("--bucket-name") != options.end() && options.at("--bucket-name") &&
            options.find("--scope-name") != options.end() && options.at("--scope-name")) {
            scope = { options.at("--bucket-name").asString(), options.at("--scope-name").asString() };
        }

        static const std::regex named_param_regex{ R"(^([\w\d_]+)=(.*)$)" };
        if (options.find("--param") != options.end() && options.at("--param")) {
            std::vector<couchbase::codec::binary> positional_params{};
            std::map<std::string, couchbase::codec::binary, std::less<>> named_params{};

            for (const auto& param : options.at("--param").asStringList()) {
                if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
                    named_params[match[1].str()] = std::move(couchbase::core::utils::to_binary(match[2].str()));
                } else {
                    positional_params.push_back(couchbase::core::utils::to_binary(param));
                }
            }
            if (!positional_params.empty() && !named_params.empty()) {
                throw docopt::DocoptArgumentError("mixing positional and named parameters is not allowed (parameters must be specified "
                                                  "either as --param=VALUE or --param=NAME=VALUE)");
            }
            if (!positional_params.empty()) {
                query_options.encoded_positional_parameters(std::move(positional_params));
            } else if (!named_params.empty()) {
                query_options.encoded_named_parameters(std::move(named_params));
            }
        }
        if (options.find("--raw") != options.end() && options.at("--raw")) {
            std::map<std::string, couchbase::codec::binary, std::less<>> raw_params{};
            for (const auto& param : options.at("--raw").asStringList()) {
                if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
                    raw_params.emplace(match[1].str(), couchbase::core::utils::to_binary(match[2].str()));
                } else {
                    throw docopt::DocoptArgumentError("raw parameters should be in NAME=VALUE form, (i.e. --raw=NAME=VALUE)");
                }
            }
            if (!raw_params.empty()) {
                query_options.encoded_raw_options(raw_params);
            }
        }

        console_output_options output_options{};
        output_options.json_lines = cbc::get_bool_option(options, "--json-lines");

        auto statements{ options["<statement>"].asStringList() };
        do_work(connection_string, cluster_options, statements, scope, query_options, output_options);
    } catch (const docopt::DocoptArgumentError& e) {
        fmt::print(stderr, "Error: {}\n", e.what());
    } catch (const std::system_error& e) {
        fmt::print(stderr, "Error: {}\n", e.what());
    }
}
} // namespace cbc
