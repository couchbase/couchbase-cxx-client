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
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

namespace cbc
{
namespace
{
class get_app : public CLI::App
{
public:
  get_app()
    : CLI::App{ "Retrieve document from the server.", "get" }
  {
    add_option("id", ids_, "IDs of the documents to retrieve.")->required(true);
    add_flag("--verbose", verbose_, "Include more context and information where it is applicable.");
    add_option("--bucket-name", bucket_name_, "Name of the bucket.")
      ->default_val(default_bucket_name);
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);
    add_option("--collection-name", collection_name_, "Name of the collection.")
      ->default_val(couchbase::collection::default_name);
    add_flag("--inlined-keyspace",
             inlined_keyspace_,
             "Extract bucket, scope, collection and key from the IDs (captures will be done with "
             "/^(.*?):(.*?)\\.(.*?):(.*)$/).");
    add_flag("--with-expiry", with_expiry_, "Return document expiry time, if set.");
    add_option("--project",
               projections_,
               fmt::format("Return only part of the document, that corresponds given JSON-pointer "
                           "(could be used multiple times)."))
      ->allow_extra_args(false);
    add_flag("--hexdump",
             hexdump_,
             "Print value using hexdump encoding (safe for binary data on STDOUT).");
    add_flag("--pretty-json",
             pretty_json_,
             "Try to pretty-print as JSON value (prints AS-IS if the document is not a JSON).");
    add_flag("--json-lines",
             json_lines_,
             "Use JSON Lines format (https://jsonlines.org) to print results.");

    add_common_options(this, common_options_);
    allow_extras(true);
  }

  [[nodiscard]] int execute() const
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    couchbase::get_options get_options{};
    if (with_expiry_) {
      get_options.with_expiry(true);
    }
    if (!projections_.empty()) {
      get_options.project(projections_);
    }

    const auto connection_string = common_options_.connection.connection_string;

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {

      fail(fmt::format(
        "Failed to connect to the cluster at \"{}\": {}", connection_string, connect_err));
    }

    for (const auto& id : ids_) {
      auto bucket_name = bucket_name_;
      auto scope_name = scope_name_;
      auto collection_name = collection_name_;
      auto document_id = id;

      if (inlined_keyspace_) {
        if (auto keyspace_with_id = extract_inlined_keyspace(id); keyspace_with_id) {
          bucket_name = keyspace_with_id->bucket_name;
          scope_name = keyspace_with_id->scope_name;
          collection_name = keyspace_with_id->collection_name;
          document_id = keyspace_with_id->id;
        }
      }

      auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

      auto [err, resp] = collection.get(document_id, get_options).get();
      if (json_lines_) {
        print_result_json_line(bucket_name, scope_name, collection_name, document_id, err, resp);
      } else {
        print_result(bucket_name, scope_name, collection_name, document_id, err, resp);
      }
    }

    cluster.close().get();

    return 0;
  }

private:
  void print_result_json_line(const std::string& bucket_name,
                              const std::string& scope_name,
                              const std::string& collection_name,
                              const std::string& document_id,
                              const couchbase::error& err,
                              const couchbase::get_result& resp) const
  {
    tao::json::value line = tao::json::empty_object;
    tao::json::value meta = {
      { "bucket_name", bucket_name },
      { "scope_name", scope_name },
      { "collection_name", collection_name },
      { "document_id", document_id },
    };
    if (err.ec()) {
      line["error"] = fmt::format("{}", err);
    } else {
      auto [value, flags] = resp.content_as<passthrough_transcoder>();
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

  void print_result(const std::string& bucket_name,
                    const std::string& scope_name,
                    const std::string& collection_name,
                    const std::string& document_id,
                    const couchbase::error& err,
                    const couchbase::get_result& resp) const
  {
    const std::string prefix = fmt::format("bucket: {}, collection: {}.{}, id: {}",
                                           bucket_name,
                                           scope_name,
                                           collection_name,
                                           document_id);
    (void)fflush(stderr);
    if (err.ec()) {
      fmt::print(stderr, "{}, error: {}\n", prefix, err.ec().message());
      if (verbose_) {
        fmt::print(stderr, "{}\n", err.ctx().to_json());
      }
    } else {
      auto [value, flags] = resp.content_as<passthrough_transcoder>();
      if (const auto& exptime = resp.expiry_time(); exptime.has_value()) {
        fmt::print(stderr,
                   "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}, expiry: {}\n",
                   prefix,
                   value.size(),
                   resp.cas(),
                   flags,
                   exptime.value());
      } else {
        fmt::print(stderr,
                   "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}\n",
                   prefix,
                   value.size(),
                   resp.cas(),
                   flags);
      }
      (void)fflush(stderr);
      (void)fflush(stdout);
      if (hexdump_) {
        auto hex = fmt::format("{:a}", spdlog::to_hex(value));
        fmt::print(stdout, "{}\n", std::string_view(hex.data() + 1, hex.size() - 1));
      } else if (pretty_json_) {
        try {
          auto json = couchbase::core::utils::json::parse_binary(value);
          fmt::print(stdout, "{}\n", tao::json::to_string(json, 2));
        } catch (const tao::pegtl::parse_error&) {
          fmt::print(stdout,
                     "{}\n",
                     std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
        }
      } else {
        fmt::print(stdout,
                   "{}\n",
                   std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
      }
      (void)fflush(stdout);
    }
  }

  common_options common_options_{};

  std::string bucket_name_{ default_bucket_name };
  std::string scope_name_{ couchbase::scope::default_name };
  std::string collection_name_{ couchbase::collection::default_name };
  std::vector<std::string> projections_{};
  bool with_expiry_{ false };
  bool inlined_keyspace_{ false };
  bool hexdump_{ false };
  bool pretty_json_{ false };
  bool json_lines_{ false };
  bool verbose_{ false };

  std::vector<std::string> ids_{};
};
} // namespace

auto
make_get_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<get_app>();
}

auto
execute_get_command(const CLI::App* app) -> int
{
  if (const auto* get = dynamic_cast<const get_app*>(app); get != nullptr) {
    return get->execute();
  }
  return 1;
}
} // namespace cbc
