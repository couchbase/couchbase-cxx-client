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

#include "remove.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/durability_level.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/mutation_token.hxx>
#include <couchbase/fmt/persist_to.hxx>
#include <couchbase/fmt/replicate_to.hxx>

namespace cbc
{
namespace
{
class remove_app : public CLI::App
{
public:
  remove_app()
    : CLI::App{ "Remove document from the server.", "remove" }
  {
    alias("del");
    alias("delete");
    alias("rm");

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
    add_option(
      "--inlined-cas-separator",
      inlined_cas_separator_,
      "Specify CAS with the key for optimistic locking (use '0x' prefix for base-16 encoding).");
    add_flag("--json-lines",
             json_lines_,
             "Use JSON Lines format (https://jsonlines.org) to print results.");

    auto* durability = add_option_group("Durability", "Extra persistency requirements.");

    const std::vector<std::string> available_durability_levels{
      fmt::format("{}", couchbase::durability_level::none),
      fmt::format("{}", couchbase::durability_level::majority),
      fmt::format("{}", couchbase::durability_level::majority_and_persist_to_active),
      fmt::format("{}", couchbase::durability_level::persist_to_majority),
    };
    durability
      ->add_option("--durability-level", durability_level_, "Durability level for the server.")
      ->transform(CLI::IsMember(available_durability_levels));

    auto* legacy_durability = durability->add_option_group(
      "Legacy Durability", "Client-side poll-based durability requirements.");

    const std::vector<std::string> available_persist_to{
      fmt::format("{}", couchbase::persist_to::none),
      fmt::format("{}", couchbase::persist_to::active),
      fmt::format("{}", couchbase::persist_to::one),
      fmt::format("{}", couchbase::persist_to::two),
      fmt::format("{}", couchbase::persist_to::three),
      fmt::format("{}", couchbase::persist_to::four),
    };
    legacy_durability
      ->add_option(
        "--persist-to", persist_to_, "Number of the nodes that have to have the document persisted")
      ->transform(CLI::IsMember(available_persist_to));

    const std::vector<std::string> available_replicate_to{
      fmt::format("{}", couchbase::replicate_to::none),
      fmt::format("{}", couchbase::replicate_to::one),
      fmt::format("{}", couchbase::replicate_to::two),
      fmt::format("{}", couchbase::replicate_to::three),
    };
    legacy_durability
      ->add_option("--replicate-to",
                   replicate_to_,
                   "Number of the nodes that have to have the document replicated")
      ->transform(CLI::IsMember(available_replicate_to));

    durability->require_option(0, 1);

    add_common_options(this, common_options_);
    allow_extras(true);
  }

  [[nodiscard]] auto execute() const -> int
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    couchbase::remove_options remove_options{};

    if (durability_level_) {
      if (durability_level_ == "none") {
        remove_options.durability(couchbase::durability_level::none);
      } else if (durability_level_ == "majority") {
        remove_options.durability(couchbase::durability_level::majority);
      } else if (durability_level_ == "majority_and_persist_to_active") {
        remove_options.durability(couchbase::durability_level::majority_and_persist_to_active);
      } else if (durability_level_ == "persist_to_majority") {
        remove_options.durability(couchbase::durability_level::persist_to_majority);
      }
    } else {
      couchbase::persist_to persist_to{ couchbase::persist_to::none };
      if (persist_to_ == "none") {
        persist_to = couchbase::persist_to::none;
      } else if (persist_to_ == "active") {
        persist_to = couchbase::persist_to::active;
      } else if (persist_to_ == "one") {
        persist_to = couchbase::persist_to::one;
      } else if (persist_to_ == "two") {
        persist_to = couchbase::persist_to::two;
      } else if (persist_to_ == "three") {
        persist_to = couchbase::persist_to::three;
      } else if (persist_to_ == "four") {
        persist_to = couchbase::persist_to::four;
      }
      couchbase::replicate_to replicate_to{ couchbase::replicate_to::none };
      if (replicate_to_ == "none") {
        replicate_to = couchbase::replicate_to::none;
      } else if (replicate_to_ == "one") {
        replicate_to = couchbase::replicate_to::one;
      } else if (replicate_to_ == "two") {
        replicate_to = couchbase::replicate_to::two;
      } else if (replicate_to_ == "three") {
        replicate_to = couchbase::replicate_to::three;
      }
      remove_options.durability(persist_to, replicate_to);
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

      if (auto document_id_with_cas = extract_inlined_cas(document_id, inlined_cas_separator_);
          document_id_with_cas) {
        document_id = document_id_with_cas->id;
        remove_options.cas(document_id_with_cas->cas);
      }

      auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

      auto [err, resp] = collection.remove(document_id, remove_options).get();
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
                              const couchbase::mutation_result& resp) const
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
      meta["cas"] = fmt::format("0x{}", resp.cas());
      if (const auto& token = resp.mutation_token(); token) {
        meta["token"] = {
          { "partition_id", token->partition_id() },
          { "partition_uuid", token->partition_uuid() },
          { "sequence_number", token->sequence_number() },
        };
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
                    const couchbase::mutation_result& resp) const
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
      std::string verbose_cas = fmt::format(" ({})", cas_to_time_point(resp.cas()));
      std::string token_str("<none>");
      if (const auto& token = resp.mutation_token(); token) {
        token_str = fmt::format(
          "{}:{}:{}", token->partition_id(), token->partition_uuid(), token->sequence_number());
      };
      fmt::print(stderr,
                 "{}, CAS: 0x{}{}, token: {}\n",
                 prefix,
                 resp.cas(),
                 verbose_ ? verbose_cas : "",
                 token_str);
    }
    (void)fflush(stderr);
    (void)fflush(stdout);
  }

  common_options common_options_{};

  std::string bucket_name_{ default_bucket_name };
  std::string scope_name_{ couchbase::scope::default_name };
  std::string collection_name_{ couchbase::collection::default_name };
  std::optional<std::string> inlined_cas_separator_{};
  bool inlined_keyspace_{ false };
  bool json_lines_{ false };
  bool verbose_{ false };
  std::optional<std::string> durability_level_{};
  std::optional<std::string> persist_to_{};
  std::optional<std::string> replicate_to_{};
  std::vector<std::string> ids_{};
};
} // namespace

auto
make_remove_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<remove_app>();
}

auto
execute_remove_command(const CLI::App* app) -> int
{
  if (const auto* remove = dynamic_cast<const remove_app*>(app); remove != nullptr) {
    return remove->execute();
  }
  return 1;
}
} // namespace cbc
