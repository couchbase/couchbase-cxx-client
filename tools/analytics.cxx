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
#include "utils.hxx"

#include "core/error_context/analytics_json.hxx"
#include "core/impl/internal_error_context.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/fmt/analytics_status.hxx>
#include <couchbase/fmt/error.hxx>

#include <asio/io_context.hpp>
#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json.hpp>

#include <regex>

namespace cbc
{
namespace
{
struct scope_with_bucket {
  std::string bucket_name;
  std::string scope_name;
};

class analytics_app : public CLI::App
{
public:
  analytics_app()
    : CLI::App{ R"(Perform Analytics query.)", "analytics" }
  {
    add_option("queries", queries_, "One or many queries to execute.")->required(true);
    add_option(
      "--param",
      params_,
      "Parameters for the query. Without '=' sign value will be treated as positional parameter.")
      ->allow_extra_args(false);
    add_flag("--read-only", read_only_, "Mark query as read only. Any mutations will fail.");
    add_flag("--boost-priority", boost_priority_, "Prioritize this query among the others.");
    add_option("--bucket-name", bucket_name_, "Name of the bucket.");
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);
    add_option(
      "--client-context-id", client_context_id_, "Override client context ID for the query(-ies).");
    add_option("--scan-wait",
               scan_wait_,
               "How long query engine will wait for indexer to catch up on scan consistency.")
      ->type_name("DURATION");
    add_option("--scan-consistency", scan_consistency_, "Set consistency guarantees for the query.")
      ->transform(CLI::IsMember(available_analytics_scan_consistency_modes()));
    add_option("--raw",
               raw_,
               "Set any query option for the query. Read the documentation: "
               "https://docs.couchbase.com/server/current/n1ql/n1ql-rest-api.")
      ->allow_extra_args(false);
    add_flag("--json-lines",
             json_lines_,
             "Use JSON Lines format (https://jsonlines.org) to print results.");

    add_common_options(this, common_options_);

    allow_extras(true);
  }

  [[nodiscard]] int execute() const
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = cbc::build_cluster_options(common_options_);

    couchbase::analytics_options analytics_options{};
    analytics_options.readonly(read_only_);
    if (scan_wait_) {
      analytics_options.scan_wait(scan_wait_.value());
    }
    if (client_context_id_) {
      analytics_options.client_context_id(client_context_id_.value());
    }
    if (boost_priority_) {
      analytics_options.priority(boost_priority_);
    }

    if (scan_consistency_ == "not_bounded") {
      analytics_options.scan_consistency(couchbase::analytics_scan_consistency::not_bounded);
    } else if (scan_consistency_ == "request_plus") {
      analytics_options.scan_consistency(couchbase::analytics_scan_consistency::request_plus);
    } else if (!scan_consistency_.empty()) {
      fail(fmt::format("unexpected value '{}' for --scan-consistency", scan_consistency_));
    }

    std::optional<scope_with_bucket> scope_id{};
    if (!bucket_name_.empty() && !scope_name_.empty()) {
      scope_id = { bucket_name_, scope_name_ };
    }

    static const std::regex named_param_regex{ R"(^([\w\d_]+)=(.*)$)" };
    if (!params_.empty()) {
      std::vector<couchbase::codec::binary> positional_params{};
      std::map<std::string, couchbase::codec::binary, std::less<>> named_params{};

      for (const auto& param : params_) {
        if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
          named_params[match[1].str()] = couchbase::core::utils::to_binary(match[2].str());
        } else {
          positional_params.push_back(couchbase::core::utils::to_binary(param));
        }
      }
      if (!positional_params.empty() && !named_params.empty()) {
        fail("mixing positional and named parameters is not allowed (parameters must be specified "
             "either as --param=VALUE or --param=NAME=VALUE)");
      }
      if (!positional_params.empty()) {
        analytics_options.encoded_positional_parameters(std::move(positional_params));
      } else if (!named_params.empty()) {
        analytics_options.encoded_named_parameters(std::move(named_params));
      }
    }

    if (!raw_.empty()) {
      std::map<std::string, couchbase::codec::binary, std::less<>> raw_params{};
      for (const auto& param : raw_) {
        if (std::smatch match; std::regex_match(param, match, named_param_regex)) {
          raw_params[match[1].str()] = couchbase::core::utils::to_binary(match[2].str());
        } else {
          fail("raw parameters should be in NAME=VALUE form, (i.e. --raw=NAME=VALUE)");
        }
      }
      if (!raw_params.empty()) {
        analytics_options.encoded_raw_options(raw_params);
      }
    }

    const auto connection_string = common_options_.connection.connection_string;

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {
      fail(fmt::format(
        "Failed to connect to the cluster at \"{}\": {}", connection_string, connect_err));
    }

    std::optional<couchbase::scope> scope{};
    if (scope_id) {
      scope = cluster.bucket(scope_id->bucket_name).scope(scope_id->scope_name);
    }

    for (const auto& statement : queries_) {
      auto [error, resp] = (scope ? do_analytics(scope.value(), statement, analytics_options)
                                  : do_analytics(cluster, statement, analytics_options))
                             .get();

      if (json_lines_) {
        print_result_json_line(scope_id,
                               statement,
                               error.ctx().impl()->as<couchbase::core::error_context::analytics>(),
                               resp,
                               analytics_options);
      } else {
        print_result(scope_id,
                     statement,
                     error.ctx().impl()->as<couchbase::core::error_context::analytics>(),
                     resp,
                     analytics_options);
      }
    }

    cluster.close().get();

    return 0;
  }

private:
  template<typename QueryEndpoint>
  auto do_analytics(QueryEndpoint& endpoint,
                    std::string statement,
                    const couchbase::analytics_options& options) const
    -> std::future<std::pair<couchbase::error, couchbase::analytics_result>>
  {
    return endpoint.analytics_query(std::move(statement), options);
  }

  void print_result_json_line(const std::optional<scope_with_bucket>& scope_id,
                              const std::string& statement,
                              const couchbase::core::error_context::analytics& ctx,
                              const couchbase::analytics_result& resp,
                              const couchbase::analytics_options& analytics_options) const
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
    if (ctx.parameters) {
      try {
        auto json = tao::json::from_string(ctx.parameters.value());
        json.erase("statement");
        meta["options"] = json;
      } catch (const tao::pegtl::parse_error&) {
        meta["options"] = ctx.parameters.value();
      }
    }

    if (ctx.ec) {
      tao::json::value error = {
        { "code", ctx.ec.value() },
        { "message", ctx.ec.message() },
      };
      try {
        error["body"] = tao::json::from_string(ctx.http_body);
      } catch (const tao::pegtl::parse_error&) {
        error["text"] = ctx.http_body;
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

  void print_result(const std::optional<scope_with_bucket>& scope_id,
                    const std::string& statement,
                    const couchbase::core::error_context::analytics& ctx,
                    const couchbase::analytics_result& resp,
                    const couchbase::analytics_options& analytics_options) const
  {
    auto built_options = analytics_options.build();

    auto header = fmt::memory_buffer();
    if (scope_id) {
      fmt::format_to(std::back_inserter(header),
                     "bucket_name: {}, scope_name: {}",
                     scope_id->bucket_name,
                     scope_id->scope_name);
    }
    fmt::format_to(std::back_inserter(header),
                   "{}statement: \"{}\"",
                   header.size() == 0 ? "" : ", ",
                   tao::json::internal::escape(statement));

    if (ctx.parameters) {
      try {
        auto json = tao::json::from_string(ctx.parameters.value());
        json.erase("statement");
        fmt::format_to(std::back_inserter(header), ", options: {}", tao::json::to_string(json));
      } catch (const tao::pegtl::parse_error&) {
        fmt::format_to(std::back_inserter(header), ", options: {}", ctx.parameters.value());
      }
    }
    fmt::print(stdout, "--- {}\n", std::string_view{ header.data(), header.size() });

    if (ctx.ec) {
      fmt::print("ERROR. code: {}, message: {}, server: {} \"{}\"\n",
                 ctx.ec.value(),
                 ctx.ec.message(),
                 ctx.first_error_code,
                 tao::json::internal::escape(ctx.first_error_message));
      if (!ctx.http_body.empty()) {
        try {
          fmt::print("{}\n", tao::json::to_string(tao::json::from_string(ctx.http_body)));
        } catch (const tao::pegtl::parse_error&) {
          fmt::print("{}\n", ctx.http_body);
        }
      }
    } else {
      fmt::print(stdout,
                 "status: {}, client_context_id: \"{}\", request_id: \"{}\", elapsed: {} ({}), "
                 "execution: {} ({}), result: {}, "
                 "processed_objects: {}, errors: {}, warnings: {}\n",
                 resp.meta_data().status(),
                 resp.meta_data().client_context_id(),
                 resp.meta_data().request_id(),
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                   resp.meta_data().metrics().elapsed_time()),
                 resp.meta_data().metrics().elapsed_time(),
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                   resp.meta_data().metrics().execution_time()),
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

  cbc::common_options common_options_{};

  std::string bucket_name_{};
  std::string scope_name_{ couchbase::scope::default_name };

  std::vector<std::string> params_{};
  bool boost_priority_{ false };
  bool read_only_{ false };
  std::optional<std::chrono::milliseconds> scan_wait_;
  std::optional<std::string> client_context_id_;
  std::string scan_consistency_{};
  std::vector<std::string> raw_{};
  bool json_lines_{ false };

  std::vector<std::string> queries_{};
};
} // namespace

auto
make_analytics_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<analytics_app>();
}

auto
execute_analytics_command(const CLI::App* app) -> int
{
  if (const auto* query = dynamic_cast<const analytics_app*>(app); query != nullptr) {
    return query->execute();
  }
  return 1;
}
} // namespace cbc
