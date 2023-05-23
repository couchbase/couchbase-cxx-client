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

#include "get.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/fmt/cas.hxx>

#include <asio/io_context.hpp>
#include <fmt/chrono.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <tao/json.hpp>

namespace cbc
{
namespace
{
static auto
usage() -> std::string
{
    static const std::string default_bucket_name{ "default" };

    static const std::string usage_string = fmt::format(
      R"(Retrieve document from the server.

Usage:
  cbc get [options] <id>...
  cbc get [options] --project=STRING... <id>...
  cbc get (-h|--help)

Options:
  -h --help                 Show this screen.
  --verbose                 Include more context and information where it is applicable.
  --bucket-name=STRING      Name of the bucket. [default: {bucket_name}]
  --scope-name=STRING       Name of the scope. [default: {scope_name}]
  --collection-name=STRING  Name of the collection. [default: {collection_name}]
  --inlined-keyspace        Extract bucket, scope, collection and key from the IDs (captures will be done with /^(.*?):(.*?)\.(.*?):(.*)$/).
  --with-expiry             Return document expiry time, if set.
  --project=STRING          Return only part of the document, that corresponds given JSON-pointer (could be used multiple times, up to {max_projections}).
  --hexdump                 Print value using hexdump encoding (safe for binary data on STDOUT).
  --pretty-json             Try to pretty-print as JSON value (prints AS-IS is the document is not a JSON).
  --json-lines              Use JSON Lines format (https://jsonlines.org) to print results.

{logger_options}{cluster_options}
)",
      fmt::arg("bucket_name", default_bucket_name),
      fmt::arg("scope_name", couchbase::scope::default_name),
      fmt::arg("collection_name", couchbase::collection::default_name),
      fmt::arg("logger_options", usage_block_for_logger()),
      fmt::arg("cluster_options", usage_block_for_cluster_options()),
      fmt::arg("max_projections", couchbase::get_options::maximum_number_of_projections));

    return usage_string;
}

struct command_options {
    std::string bucket_name;
    std::string scope_name;
    std::string collection_name;
    std::vector<std::string> projections{};
    bool with_expiry{ false };
    bool inlined_keyspace{ false };
    bool hexdump{ false };
    bool pretty_json{ false };
    bool json_lines{ false };
    bool verbose{ false };
};

static void
print_result_json_line(const std::string& bucket_name,
                       const std::string& scope_name,
                       const std::string& collection_name,
                       const std::string& document_id,
                       const couchbase::key_value_error_context& ctx,
                       const couchbase::get_result& resp)
{
    tao::json::value line = tao::json::empty_object;
    tao::json::value meta = {
        { "bucket_name", bucket_name },
        { "scope_name", scope_name },
        { "collection_name", collection_name },
        { "document_id", document_id },
    };
    if (ctx.ec()) {
        tao::json::value error = {
            { "code", ctx.ec().value() },
            { "message", ctx.ec().message() },
        };
        if (auto ext = ctx.extended_error_info(); ext.has_value()) {
            error["ref"] = ext->reference();
            error["ctx"] = ext->context();
        }
        line["error"] = error;
    } else {
        auto [value, flags] = resp.content_as<cbc::passthrough_transcoder>();
        meta["cas"] = fmt::format("0x{}", resp.cas());
        meta["flags"] = flags;
        if (const auto& expiry = resp.expiry_time(); expiry) {
            meta["expiry_time"] = fmt::format("{}", expiry.value());
        }
        try {
            line["json"] = couchbase::core::utils::json::parse_binary(value);
        } catch (const tao::pegtl::parse_error&) {
            line["base64"] = value;
        }
    }
    line["meta"] = meta;
    fmt::print(stdout, "{}\n", tao::json::to_string<tao::json::events::binary_to_base64>(line));
    (void)fflush(stdout);
}

static void
print_result(const std::string& bucket_name,
             const std::string& scope_name,
             const std::string& collection_name,
             const std::string& document_id,
             const couchbase::key_value_error_context& ctx,
             const couchbase::get_result& resp,
             const command_options& cmd_options)
{
    const std::string prefix = fmt::format("bucket: {}, collection: {}.{}, id: {}", bucket_name, scope_name, collection_name, document_id);
    (void)fflush(stderr);
    if (ctx.ec()) {
        if (auto ext = ctx.extended_error_info(); ext.has_value()) {
            fmt::print(stderr, "{}, error: {} (ref: {}, ctx: {})\n", prefix, ctx.ec().message(), ext->reference(), ext->context());
        } else {
            fmt::print(stderr, "{}, error: {}\n", prefix, ctx.ec().message());
        }
        if (cmd_options.verbose) {
            fmt::print(stderr, "{}\n", ctx.to_json());
        }
    } else {
        auto [value, flags] = resp.content_as<cbc::passthrough_transcoder>();
        if (const auto& exptime = resp.expiry_time(); exptime.has_value()) {
            fmt::print(
              stderr, "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}, expiry: {}\n", prefix, value.size(), resp.cas(), flags, exptime.value());
        } else {
            fmt::print(stderr, "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}\n", prefix, value.size(), resp.cas(), flags);
        }
        (void)fflush(stderr);
        (void)fflush(stdout);
        if (cmd_options.hexdump) {
            auto hex = fmt::format("{:a}", spdlog::to_hex(value));
            fmt::print(stdout, "{}\n", std::string_view(hex.data() + 1, hex.size() - 1));
        } else if (cmd_options.pretty_json) {
            try {
                auto json = couchbase::core::utils::json::parse_binary(value);
                fmt::print(stdout, "{}\n", tao::json::to_string(json, 2));
            } catch (const tao::pegtl::parse_error&) {
                fmt::print(stdout, "{}\n", std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
            }
        } else {
            fmt::print(stdout, "{}\n", std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
        }
        (void)fflush(stdout);
    }
}

static void
do_work(const std::string& connection_string,
        const couchbase::cluster_options& cluster_options,
        const std::vector<std::string>& ids,
        const command_options& cmd_options)
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

    couchbase::get_options get_options{};
    if (cmd_options.with_expiry) {
        get_options.with_expiry(true);
    }
    if (!cmd_options.projections.empty()) {
        get_options.project(cmd_options.projections);
    }

    for (const auto& id : ids) {
        auto bucket_name = cmd_options.bucket_name;
        auto scope_name = cmd_options.scope_name;
        auto collection_name = cmd_options.collection_name;
        auto document_id = id;

        if (cmd_options.inlined_keyspace) {
            if (auto keyspace_with_id = extract_inlined_keyspace(id); keyspace_with_id) {
                bucket_name = keyspace_with_id->bucket_name;
                scope_name = keyspace_with_id->scope_name;
                collection_name = keyspace_with_id->collection_name;
                document_id = keyspace_with_id->id;
            }
        }

        auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

        auto [ctx, resp] = collection.get(document_id, get_options).get();
        if (cmd_options.json_lines) {
            print_result_json_line(bucket_name, scope_name, collection_name, document_id, ctx, resp);
        } else {
            print_result(bucket_name, scope_name, collection_name, document_id, ctx, resp, cmd_options);
        }
    }

    cluster.close();
    guard.reset();

    io_thread.join();
}
} // namespace

void
cbc::get::execute(const std::vector<std::string>& argv)
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

    command_options cmd_options{};
    cmd_options.verbose = options["--verbose"].asBool();
    cmd_options.bucket_name = options["--bucket-name"].asString();
    cmd_options.scope_name = options["--scope-name"].asString();
    cmd_options.collection_name = options["--collection-name"].asString();
    cmd_options.inlined_keyspace = options["--inlined-keyspace"].asBool();
    cmd_options.with_expiry = options["--with-expiry"].asBool();
    cmd_options.hexdump = options["--hexdump"].asBool();
    cmd_options.pretty_json = options["--pretty-json"].asBool();
    cmd_options.projections = options["--project"].asStringList();
    cmd_options.json_lines = options["--json-lines"].asBool();
    auto ids{ options["<id>"].asStringList() };

    do_work(connection_string, cluster_options, ids, cmd_options);
}
} // namespace cbc
