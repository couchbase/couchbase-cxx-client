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

#include "keygen.hxx"

#include "core/cluster.hxx"
#include "core/operations/management/bucket_describe.hxx"
#include "core/topology/configuration_json.hxx"
#include "core/utils/json.hxx"
#include "key_generator.hxx"
#include "utils.hxx"

#include <couchbase/cluster.hxx>

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

#include <couchbase/fmt/error.hxx>

namespace cbc
{

namespace
{
template<typename Key>
void
remove_duplicates(std::vector<Key>& keys)
{
  std::sort(keys.begin(), keys.end());
  auto last = std::unique(keys.begin(), keys.end());
  keys.erase(last, keys.end());
}

class keygen_app : public CLI::App
{
public:
  keygen_app()
    : CLI::App{ "Generates keys with various properties.", "keygen" }
  {
    add_option("--number-of-keys", number_of_keys_, "How many keys to generate.")->default_val(1);
    add_option("--fixed-length", fixed_length_, "The length of the key to generate.")
      ->default_val(6);
    add_flag("--randomize", randomize_, "Generate different keys every time.");
    add_option("--prefix", prefix_, "Prefix for the keys.")->default_val("");
    add_option("--vbucket", vbuckets_, "Pin generated keys to the given vBucket.");
    add_option(
      "--parent-key", parent_keys_, "Pin generated keys to the same vBucket as the given key.");
    add_flag("--all-vbuckets", all_vbuckets_, "Generate key(s) for each available vBucket.");
    add_flag("--vbuckets-for-nodes",
             vbuckets_for_nodes_,
             "Generate key(s) for vBuckets and group them by nodes of given level.")
      ->transform(CLI::IsMember({ "active", "replica_1", "replica_2", "replica_3" }));

    add_option("--number-of-vbuckets",
               number_of_vbuckets_,
               "Override number of vBuckets. Otherwise try to connect to cluster and infer number "
               "of vBuckets from the bucket configuration.");
    add_option("--bucket-name",
               bucket_name_,
               "Name of the bucket (only used when --number-of-vbuckets switch is not specified).")
      ->default_val(default_bucket_name);
    add_flag("--json", json_, "Output generation result as JSON.");
    add_flag("--verbose", verbose_, "Print group name (vBucket or parent key) and indent keys.");
    add_flag("--no-duplicates",
             no_duplicates_,
             "Do not allow duplicates (due you to length restrictions).");

    add_common_options(this, common_options_);
  }

  [[nodiscard]] auto execute() -> int
  {
    apply_logger_options(common_options_.logger);

    std::uint16_t number_of_vbuckets{};
    std::map<std::string, key_value_node> vbuckets_by_node{};

    if (number_of_vbuckets_) {
      if (!vbuckets_for_nodes_.empty()) {
        fail(
          "--vbuckets-for-nodes requires cluster connection to fetch configuration and cannot be "
          "used with --number-of-vbuckets.");
      }
      number_of_vbuckets = number_of_vbuckets_.value();
    } else {
      auto cluster_options = build_cluster_options(common_options_);
      const auto connection_string = common_options_.connection.connection_string;

      auto [connect_err, cluster] =
        couchbase::cluster::connect(connection_string, cluster_options).get();
      if (connect_err) {
        fail(fmt::format("Failed to connect to the cluster at {:?}: {}. Try --number-of-vbuckets "
                         "to specify number of vBuckets directly.",
                         connection_string,
                         connect_err));
      }

      auto core = couchbase::core::get_core_cluster(cluster);

      auto config = [core, bucket = bucket_name_]() {
        couchbase::core::operations::management::bucket_describe_request req{ bucket };
        auto barrier = std::make_shared<
          std::promise<couchbase::core::operations::management::bucket_describe_response>>();
        auto f = barrier->get_future();
        core.execute(req, [barrier](auto resp) {
          barrier->set_value(std::move(resp));
        });
        auto resp = f.get();

        if (resp.ctx.ec) {
          fail(
            fmt::format("Failed to get bucket config for {:?}: {}", bucket, resp.ctx.ec.message()));
        }
        return couchbase::core::utils::json::parse(resp.ctx.http_body)
          .as<couchbase::core::topology::configuration>();
      }();

      if (!config.vbmap) {
        fail(fmt::format("vBucketMap for bucket {:?} is empty", bucket_name_));
      }

      number_of_vbuckets = static_cast<std::uint16_t>(config.vbmap->size());

      if (!vbuckets_for_nodes_.empty()) {
        vbuckets_by_node = extract_vbucket_map(config);

        if (std::all_of(vbuckets_by_node.begin(),
                        vbuckets_by_node.end(),
                        [vbucket_type = vbuckets_for_nodes_](const auto& pair) {
                          return pair.second.vbuckets(vbucket_type).empty();
                        })) {

          fail(fmt::format(
            "--vbucket-for-nodes={} specified, but the none of the nodes have {} vBuckets",
            vbuckets_for_nodes_,
            vbuckets_for_nodes_));
        }
      }
    }

    key_generator generator(key_generator_options{
      prefix_,
      randomize_,
      number_of_vbuckets,
      vbuckets_by_node,
      fixed_length_,
    });

    if (all_vbuckets_) {
      vbuckets_.clear();
      vbuckets_.reserve(number_of_vbuckets);
      for (std::uint16_t vbucket = 0; vbucket < number_of_vbuckets; ++vbucket) {
        vbuckets_.push_back(vbucket);
      }
    }

    tao::json::value result;
    if (!parent_keys_.empty()) {
      // Group keys by parent.
      // Determine vBucket of each parent key and generate group of keys that will be mapped to the
      // same vBucket.
      result = tao::json::empty_object;
      for (const auto& parent_key : parent_keys_) {
        result[parent_key] =
          generator.next_keys_for_parent(number_of_keys_, parent_key, no_duplicates_);
      }
    } else if (!vbuckets_.empty()) {
      // Group keys by vBucket
      // Generate keys for given vBucket directly
      result = tao::json::empty_object;
      for (const auto& vbucket : vbuckets_) {
        result[std::to_string(vbucket)] =
          generator.next_keys_for_vbucket(number_of_keys_, vbucket, no_duplicates_);
      }
    } else if (!vbuckets_for_nodes_.empty()) {
      // Group keys by node.
      // Generate requested number of the keys for each node without specific vBucket selection.
      for (const auto& [endpoint, node] : vbuckets_by_node) {
        result[endpoint] =
          generator.next_keys_for_node(number_of_keys_, node, vbuckets_for_nodes_, no_duplicates_);
      }
    } else {
      // No special rule, just generate group of unique keys.
      result = generator.next_keys(number_of_keys_, no_duplicates_);
    }

    if (json_) {
      fmt::println("{}", tao::json::to_string(result, 2));
    } else {
      if (result.is_object()) {
        for (const auto& [name, keys] : result.get_object()) {
          if (verbose_) {
            fmt::println("{}: ", name);
          }
          for (const auto& key : keys.get_array()) {
            fmt::println("{}{}", verbose_ ? "  " : "", key.as<std::string>());
          }
        }
      } else {
        for (const auto& key : result.get_array()) {
          fmt::println(key.as<std::string>());
        }
      }
    }

    return 0;
  }

private:
  common_options common_options_{};

  std::string prefix_{};
  bool randomize_{ false };
  std::optional<std::uint16_t> number_of_vbuckets_{};
  std::string bucket_name_{ default_bucket_name };

  bool json_{ false };
  bool verbose_{ false };
  bool no_duplicates_{ false };
  bool all_vbuckets_{ false };
  std::string vbuckets_for_nodes_{};
  std::size_t number_of_keys_{};
  std::size_t fixed_length_{};
  std::vector<std::string> parent_keys_{};
  std::vector<std::uint16_t> vbuckets_{};
};
} // namespace

auto
make_keygen_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<keygen_app>();
}

auto
execute_keygen_command(CLI::App* app) -> int
{
  if (auto* keygen = dynamic_cast<keygen_app*>(app); keygen != nullptr) {
    return keygen->execute();
  }
  return 1;
}
} // namespace cbc
