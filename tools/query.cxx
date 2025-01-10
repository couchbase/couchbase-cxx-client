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
#include "utils.hxx"

#include "core/error_context/query.hxx"
#include "core/error_context/query_json.hxx"
#include "core/impl/internal_error_context.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/query_profile.hxx>
#include <couchbase/fmt/query_status.hxx>

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

auto
default_options()
{
  static auto defaults = couchbase::query_options{}.build();
  return defaults;
}

class query_app : public CLI::App
{
public:
  query_app()
    : CLI::App{ R"(Perform N1QL query.

Examples:

1. Query with positional parameters:

    cbc query --param 1 --param 2 'SELECT $1 + $2'

2. Query with named parameters:

    cbc query --param a=1 --param b=2 'SELECT $a + $b'
)",
                "query" }
  {
    const std::vector<std::string> allowed_profile_modes{ "off", "phases", "timings" };

    add_option("queries", queries_, "One or many queries to execute.")->required(true);
    add_option(
      "--param",
      params_,
      "Parameters for the query. Without '=' sign value will be treated as positional parameter.")
      ->allow_extra_args(false);
    add_flag("--prepare", prepare_, "Prepare statement.");
    add_flag("--read-only", read_only_, "Mark query as read only. Any mutations will fail.");
    add_flag("--preserve-expiry",
             preserve_expiry_,
             "Ensure that expiry will be preserved after mutations.");
    add_flag("--disable-metrics", disable_metrics_, "Do not request metrics.");
    add_flag("--user-replica", use_replica_, "Allow using replica nodes for KV operations.");
    add_option("--profile", profile_, "Request the service to profile the query and return report.")
      ->transform(CLI::IsMember(allowed_profile_modes));
    add_option("--bucket-name", bucket_name_, "Name of the bucket.");
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);
    add_option(
      "--client-context-id", client_context_id_, "Override client context ID for the query(-ies).");
    add_flag(
      "--flex-index", flex_index_, "Tell query service to utilize flex index (full text search).");
    add_option("--maximum-parallelism",
               maximum_parallelism_,
               "Parallelism for query execution (0 to disable).");
    add_option("--scan-cap", scan_cap_, "Maximum buffer size between indexer and query service.");
    add_option("--scan-wait",
               scan_wait_,
               "How long query engine will wait for indexer to catch up on scan consistency.")
      ->type_name("DURATION");
    add_option("--scan-consistency", scan_consistency_, "Set consistency guarantees for the query.")
      ->transform(CLI::IsMember(available_query_scan_consistency_modes()));
    add_option(
      "--pipeline-batch",
      pipeline_batch_,
      "Number of items execution operators can batch for fetch from the Key/Value service.");
    add_option(
      "--pipeline-cap",
      pipeline_cap_,
      "Maximum number of items each execution operator can buffer between various operators.");
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

    couchbase::query_options query_options{};
    query_options.adhoc(!prepare_);
    query_options.readonly(read_only_);
    query_options.preserve_expiry(preserve_expiry_);
    query_options.metrics(!disable_metrics_);
    if (use_replica_) {
      query_options.use_replica(use_replica_.value());
    }
    query_options.flex_index(flex_index_);
    if (maximum_parallelism_) {
      query_options.max_parallelism(maximum_parallelism_.value());
    }
    if (scan_cap_) {
      query_options.scan_cap(scan_cap_.value());
    }
    if (pipeline_batch_) {
      query_options.pipeline_batch(pipeline_batch_.value());
    }
    if (pipeline_cap_) {
      query_options.pipeline_cap(pipeline_cap_.value());
    }
    if (scan_wait_) {
      query_options.scan_wait(scan_wait_.value());
    }
    if (client_context_id_) {
      query_options.client_context_id(client_context_id_.value());
    }

    if (scan_consistency_ == "not_bounded") {
      query_options.scan_consistency(couchbase::query_scan_consistency::not_bounded);
    } else if (scan_consistency_ == "request_plus") {
      query_options.scan_consistency(couchbase::query_scan_consistency::request_plus);
    } else if (!scan_consistency_.empty()) {
      fail(fmt::format("unexpected value '{}' for --scan-consistency", scan_consistency_));
    }

    if (profile_.has_value()) {
      if (profile_.value() == "off") {
        query_options.profile(couchbase::query_profile::off);
      } else if (profile_.value() == "phases") {
        query_options.profile(couchbase::query_profile::phases);
      } else if (profile_.value() == "timings") {
        query_options.profile(couchbase::query_profile::timings);
      } else if (!profile_.value().empty()) {
        fail(fmt::format("unexpected value '{}' for --profile", profile_.value()));
      }
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
        query_options.encoded_positional_parameters(std::move(positional_params));
      } else if (!named_params.empty()) {
        query_options.encoded_named_parameters(std::move(named_params));
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
        query_options.encoded_raw_options(raw_params);
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
      auto [error, resp] = (scope ? do_query(scope.value(), statement, query_options)
                                  : do_query(cluster, statement, query_options))
                             .get();

      if (json_lines_) {
        print_result_json_line(scope_id,
                               statement,
                               error.ctx().impl()->as<couchbase::core::error_context::query>(),
                               resp,
                               query_options);
      } else {
        print_result(scope_id,
                     statement,
                     error.ctx().impl()->as<couchbase::core::error_context::query>(),
                     resp,
                     query_options);
      }
    }

    cluster.close().get();

    return 0;
  }

private:
  template<typename QueryEndpoint>
  auto do_query(QueryEndpoint& endpoint,
                std::string statement,
                const couchbase::query_options& options) const
    -> std::future<std::pair<couchbase::error, couchbase::query_result>>
  {
    return endpoint.query(std::move(statement), options);
  }

  void print_result_json_line(const std::optional<scope_with_bucket>& scope_id,
                              const std::string& statement,
                              const couchbase::core::error_context::query& ctx,
                              const couchbase::query_result& resp,
                              const couchbase::query_options& query_options) const
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
    (void)fflush(stdout);
  }

  void print_result(const std::optional<scope_with_bucket>& scope_id,
                    const std::string& statement,
                    const couchbase::core::error_context::query& ctx,
                    const couchbase::query_result& resp,
                    const couchbase::query_options& query_options) const
  {
    auto built_options = query_options.build();

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
      auto meta = fmt::memory_buffer();
      fmt::format_to(std::back_inserter(meta),
                     R"(status: {}, client_context_id: "{}", request_id: "{}")",
                     resp.meta_data().status(),
                     resp.meta_data().client_context_id(),
                     resp.meta_data().request_id());
      if (const auto& metrics = resp.meta_data().metrics(); metrics.has_value()) {
        fmt::format_to(std::back_inserter(meta),
                       ", elapsed: {}, execution: {}, result: {}, sort: {}, mutations: {}, errors: "
                       "{}, warnings: {}",
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
          fmt::format_to(std::back_inserter(warning),
                         "WARNING. code: {}, message: \"{}\"",
                         item.code(),
                         item.message());
          if (item.reason()) {
            fmt::format_to(std::back_inserter(warning), ", reason: {}", item.reason().value());
          }
          if (item.retry()) {
            fmt::format_to(std::back_inserter(warning), ", retry: {}", item.retry().value());
          }
          fmt::print(stdout, "{}\n", std::string_view{ warning.data(), warning.size() });
        }
      }
      if (const auto& profile = resp.meta_data().profile(); profile) {
        try {
          fmt::print(
            "{}\n",
            tao::json::to_string(couchbase::core::utils::json::parse_binary(profile.value()), 2));
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
    (void)fflush(stdout);
  }

  cbc::common_options common_options_{};

  std::string bucket_name_{};
  std::string scope_name_{ couchbase::scope::default_name };

  std::vector<std::string> params_{};
  bool prepare_{ !default_options().adhoc };
  bool read_only_{ default_options().readonly };
  bool preserve_expiry_{ default_options().preserve_expiry };
  bool disable_metrics_{ !default_options().metrics };
  std::optional<std::string> profile_{};
  std::optional<bool> use_replica_{ default_options().use_replica };
  std::optional<std::uint64_t> maximum_parallelism_{ default_options().max_parallelism };
  std::optional<std::uint64_t> scan_cap_{ default_options().scan_cap };
  std::optional<std::chrono::milliseconds> scan_wait_{ default_options().scan_wait };
  std::optional<std::uint64_t> pipeline_batch_{ default_options().pipeline_batch };
  std::optional<std::uint64_t> pipeline_cap_{ default_options().pipeline_cap };
  std::optional<std::string> client_context_id_;
  bool flex_index_{ default_options().flex_index };
  std::string scan_consistency_{};
  std::vector<std::string> raw_{};
  bool json_lines_{ false };

  std::vector<std::string> queries_{};
};
} // namespace

auto
make_query_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<query_app>();
}

auto
execute_query_command(const CLI::App* app) -> int
{
  if (const auto* query = dynamic_cast<const query_app*>(app); query != nullptr) {
    return query->execute();
  }
  return 1;
}
} // namespace cbc
