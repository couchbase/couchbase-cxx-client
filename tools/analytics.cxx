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

#include "analytics.hxx"
#include "core/utils/binary.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/analytics_status.hxx>

#include <asio/io_context.hpp>
#include <fmt/chrono.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <tao/json.hpp>

#include <regex>

namespace cbc
{
namespace
{
auto
usage() -> std::string
{
    static const std::string default_bucket_name{ "default" };

    static const std::string usage_string = fmt::format(
      R"(Perform Analytics query.

Usage:
  cbc analytics [options] <statement>...
  cbc analytics [options] --param=NAME=VALUE... <statement>...
  cbc analytics [options] --raw=NAME=VALUE... <statement>...
  cbc analytics (-h|--help)

Options:
  -h --help                      Show this screen.
  --param=NAME=VALUE             Parameters for the query. Without '=' sign value will be treated as positional parameter.
  --boost-priority               Prioritize this query among the others.
  --read-only                    Mark query as read only. Any mutations will fail.
  --bucket-name=STRING           Name of the bucket where the scope is defined (see --scope-name).
  --scope-name=STRING            Name of the scope.
  --client-context-id=STRING     Override client context ID for the query(-ies).
  --scan-wait=DURATION           How long query engine will wait for indexer to catch up on scan consistency.
  --scan-consistency=MODE        Set consistency guarantees for the query (allowed values: not_bounded, request_plus).
  --raw=NAME=VALUE               Set any query option for the query. Read the documentation: https://docs.couchbase.com/server/current/analytics/rest-analytics.html.
  --json-lines                   Use JSON Lines format (https://jsonlines.org) to print results.

{logger_options}{cluster_options}

Examples:

1. Query with positional parameters:

  cbc analytics --param 1 --param 2 'SELECT $1 + $2'

2. Query with named parameters:

  cbc analytics --param a=1 --param b=2 'SELECT $a + $b'
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
auto
do_analytics(QueryEndpoint& endpoint, std::string statement, const couchbase::analytics_options& options)
{
    return endpoint.analytics_query(std::move(statement), options);
}

void
print_result_json_line(const std::optional<scope_with_bucket>& scope_id,
                       const std::string& statement,
                       const couchbase::analytics_error_context& ctx,
                       const couchbase::analytics_result& resp,
                       const couchbase::analytics_options& analytics_options)
{
    auto built_options = analytics_options.build();

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
        meta["metrics"] = {
            { "elapsed_time", fmt::format("{}", resp.meta_data().metrics().elapsed_time()) },
            { "execution_time", fmt::format("{}", resp.meta_data().metrics().execution_time()) },
            { "result_count", resp.meta_data().metrics().result_count() },
            { "result_size", resp.meta_data().metrics().result_size() },
            { "processed_objects", resp.meta_data().metrics().processed_objects() },
            { "error_count", resp.meta_data().metrics().error_count() },
            { "warning_count", resp.meta_data().metrics().warning_count() },
        };
        if (!resp.meta_data().warnings().empty()) {
            tao::json::value warnings = tao::json::empty_array;
            for (const auto& item : resp.meta_data().warnings()) {
                warnings.emplace_back(tao::json::value{
                  { "message", item.message() },
                  { "code", item.code() },
                });
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
    (void)fflush(stdout);
}

void
print_result(const std::optional<scope_with_bucket>& scope_id,
             const std::string& statement,
             const couchbase::analytics_error_context& ctx,
             const couchbase::analytics_result& resp,
             const couchbase::analytics_options& analytics_options)
{
    auto built_options = analytics_options.build();

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
        fmt::print(stdout,
                   "status: {}, client_context_id: \"{}\", request_id: \"{}\", elapsed: {} ({}), execution: {} ({}), result: {}, "
                   "processed_objects: {}, errors: {}, warnings: {}\n",
                   resp.meta_data().status(),
                   resp.meta_data().client_context_id(),
                   resp.meta_data().request_id(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta_data().metrics().elapsed_time()),
                   resp.meta_data().metrics().elapsed_time(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta_data().metrics().execution_time()),
                   resp.meta_data().metrics().execution_time(),
                   resp.meta_data().metrics().result_count(),
                   resp.meta_data().metrics().processed_objects(),
                   resp.meta_data().metrics().error_count(),
                   resp.meta_data().metrics().warning_count());
        if (!resp.meta_data().warnings().empty()) {
            for (const auto& item : resp.meta_data().warnings()) {
                fmt::print(stdout, "WARNING. code: {}, message: \"{}\"\n", item.code(), item.message());
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
    (void)fflush(stdout);
}

void
do_work(const std::string& connection_string,
        const couchbase::cluster_options& cluster_options,
        const std::vector<std::string>& statements,
        const std::optional<scope_with_bucket>& scope_id,
        const couchbase::analytics_options& analytics_options,
        const console_output_options& output_options)
{
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, cluster_options).get();
    if (ec) {
        guard.reset();
        io_thread.join();

        throw std::system_error(ec);
    }

    std::optional<couchbase::scope> scope{};
    if (scope_id) {
        scope = cluster.bucket(scope_id->bucket_name).scope(scope_id->scope_name);
    }

    for (const auto& statement : statements) {
        auto [ctx, resp] =
          (scope ? do_analytics(scope.value(), statement, analytics_options) : do_analytics(cluster, statement, analytics_options)).get();

        if (output_options.json_lines) {
            print_result_json_line(scope_id, statement, ctx, resp, analytics_options);
        } else {
            print_result(scope_id, statement, ctx, resp, analytics_options);
        }
    }

    cluster.close();
    guard.reset();

    io_thread.join();
}
} // namespace

void
cbc::analytics::execute(const std::vector<std::string>& argv)
{
    auto options = cbc::parse_options(usage(), argv);
    if (options["--help"].asBool()) {
        fmt::print(stdout, "{}", usage());
        return;
    }

    cbc::apply_logger_options(options);

    couchbase::cluster_options cluster_options{ cbc::default_cluster_options() };
    std::string connection_string{ cbc::default_connection_string() };
    cbc::fill_cluster_options(options, cluster_options, connection_string);

    couchbase::analytics_options analytics_options{};
    parse_enable_option(analytics_options.priority, "--boost-priority");
    parse_enable_option(analytics_options.readonly, "--read-only");
    parse_duration_option(analytics_options.scan_wait, "--scan-wait");
    parse_string_option(analytics_options.client_context_id, "--client-context-id");

    if (options.find("--scan-consistency") != options.end() && options.at("--scan-consistency")) {
        if (auto value = options.at("--scan-consistency").asString(); value == "not_bounded") {
            analytics_options.scan_consistency(couchbase::analytics_scan_consistency::not_bounded);
        } else if (value == "request_plus") {
            analytics_options.scan_consistency(couchbase::analytics_scan_consistency::request_plus);
        } else {
            throw docopt::DocoptArgumentError(fmt::format("unexpected value '{}' for --scan-consistency", value));
        }
    }

    std::optional<scope_with_bucket> scope{};
    if (options.find("--bucket-name") != options.end() && options.at("--bucket-name") && options.find("--scope-name") != options.end() &&
        options.at("--scope-name")) {
        scope = { options.at("--bucket-name").asString(), options.at("--scope-name").asString() };
    }

    static const std::regex named_param_regex{ R"(^([\w\d_]+)=(.*)$)" };
    if (options.find("--param") != options.end() && options.at("--param")) {
        std::vector<couchbase::codec::binary> positional_params{};
        std::map<std::string, couchbase::codec::binary, std::less<>> named_params{};

        for (const auto& param : options.at("--param").asStringList()) {
            if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
                named_params[match[1].str()] = couchbase::core::utils::to_binary(match[2].str());
            } else {
                positional_params.push_back(couchbase::core::utils::to_binary(param));
            }
        }
        if (!positional_params.empty() && !named_params.empty()) {
            throw docopt::DocoptArgumentError("mixing positional and named parameters is not allowed (parameters must be specified "
                                              "either as --param=VALUE or --param=NAME=VALUE)");
        }
        if (!positional_params.empty()) {
            analytics_options.encoded_positional_parameters(std::move(positional_params));
        } else if (!named_params.empty()) {
            analytics_options.encoded_named_parameters(std::move(named_params));
        }
    }
    if (options.find("--raw") != options.end() && options.at("--raw")) {
        std::map<std::string, couchbase::codec::binary, std::less<>> raw_params{};
        for (const auto& param : options.at("--raw").asStringList()) {
            if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
                raw_params[match[1].str()] = couchbase::core::utils::to_binary(match[2].str());
            } else {
                throw docopt::DocoptArgumentError("raw parameters should be in NAME=VALUE form, (i.e. --raw=NAME=VALUE)");
            }
        }
        if (!raw_params.empty()) {
            analytics_options.encoded_raw_options(raw_params);
        }
    }

    console_output_options output_options{};
    output_options.json_lines = cbc::get_bool_option(options, "--json-lines");

    auto statements{ options["<statement>"].asStringList() };
    do_work(connection_string, cluster_options, statements, scope, analytics_options, output_options);
}
} // namespace cbc
