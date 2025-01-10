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

#include "beam.hxx"
#include "core/cluster.hxx"
#include "core/operations/management/bucket_describe.hxx"
#include "core/topology/configuration_json.hxx"
#include "core/utils/json.hxx"
#include "utils.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/ranges.h>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/retry_reason.hxx>

#include <csignal>

namespace cbc
{
namespace
{
std::atomic_flag running{ true };

void
sigint_handler(int signal)
{
  fmt::print(stderr, "\nrequested stop, signal={}\n", signal);
  running.clear();
}

auto
timestamp()
{
  auto currentTime = std::chrono::system_clock::now();
  auto ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()) % 1000;
  return fmt::format("[{:%T}.{:03}] ",
                     fmt::localtime(std::chrono::system_clock::to_time_t(currentTime)),
                     ms.count());
}

class beam_app : public CLI::App
{
public:
  beam_app()
    : CLI::App{ "Send series of get operations focused on vBucketID or node index.", "beam" }
  {
    auto* focus = add_option_group("focus", "Selector for the target");
    focus->add_option("--vbucket-id", vbucket_ids_, "vBucketIDs to send the operations.");
    focus->add_option("--node-index", node_indexes_, "vBucketIDs to send the operations.");
    focus->require_option(1, 0);

    add_flag(
      "--use-upsert", use_upsert_, "Use 'upsert' operation instead of 'get' to generate workload.");
    add_flag("--verbose", verbose_, "Include more context and information where it is applicable.");
    add_option("--bucket-name", bucket_name_, "Name of the bucket.")
      ->default_val(default_bucket_name);
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);
    add_option("--collection-name", collection_name_, "Name of the collection.")
      ->default_val(couchbase::collection::default_name);

    add_common_options(this, common_options_);
  }

  [[nodiscard]] int execute()
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    const auto connection_string = common_options_.connection.connection_string;

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {
      fail(fmt::format(
        "Failed to connect to the cluster at {:?}: {}", connection_string, connect_err));
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
    const auto& vbmap = config.vbmap.value();
    // get all vbuckets for the nodes and add them to the list
    for (std::uint16_t vbucket_id = 0; static_cast<std::size_t>(vbucket_id) < vbmap.size();
         ++vbucket_id) {
      if (std::find(node_indexes_.begin(), node_indexes_.end(), vbmap[vbucket_id][0]) !=
          node_indexes_.end()) {
        vbucket_ids_.insert(vbucket_id);
      }
    }

    std::map<std::size_t, std::vector<std::uint16_t>> vbuckets_by_master_index;
    for (std::uint16_t vbucket_id = 0; static_cast<std::size_t>(vbucket_id) < vbmap.size();
         ++vbucket_id) {
      auto master_index = vbmap[vbucket_id][0];
      if (master_index < 0) {
        fail(fmt::format("negative value for master node of vBucketID {}", vbucket_id));
      }
      vbuckets_by_master_index[static_cast<std::size_t>(master_index)].push_back(vbucket_id);
    }

    if (verbose_) {
      for (const auto& [master_index, vbuckets] : vbuckets_by_master_index) {
        fmt::print("{}. {:?}: {}\n",
                   master_index,
                   config.nodes[master_index].hostname,
                   fmt::join(vbuckets, ", "));
      }
    }

    std::vector<std::string> ids;
    ids.reserve(vbucket_ids_.size());
    for (const auto& vbucket_id : vbucket_ids_) {
      for (std::size_t index = 0;; ++index) {
        std::string key = fmt::format("vb-{:03}_{:05}", vbucket_id, index);
        auto [vbid, _] = config.map_key(key, 0);
        if (vbid == vbucket_id) {
          ids.push_back(key);
          break;
        }
      }
    }

    if (verbose_) {
      fmt::print("{} IDs will be used for the workload:", ids.size());
      for (size_t i = 0; i < ids.size(); ++i) {
        fmt::print("{}{}", i % 16 == 0 ? "\n" : " ", ids[i]);
      }
      fmt::print("\n");
    }
    (void)fflush(stdout);

    auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

    const auto dummy_value{ "{\"value\":42}" };

    if (!use_upsert_) {
      // Populate the keys first
      for (const auto& id : ids) {
        auto [err, resp] = collection.upsert(id, dummy_value).get();
        if (err.ec()) {
          fail(fmt::format("Failed to store value for key {:?}: {}", id, err.ec().message()));
        }
      }
    }

    (void)std::signal(SIGINT, sigint_handler);
    (void)std::signal(SIGTERM, sigint_handler);

    bool has_error{ false };

    while (running.test_and_set()) {
      for (const auto& id : ids) {
        couchbase::error err;
        if (use_upsert_) {
          auto [e, _] = collection.upsert(id, dummy_value).get();
          err = std::move(e);
        } else {
          auto [e, _] = collection.get(id, {}).get();
          err = std::move(e);
        }

        if (err.ec()) {
          fmt::print(stderr,
                     "{} failed to {} value for key {:?}: {}, {}\n",
                     timestamp(),
                     use_upsert_ ? "upsert" : "get",
                     id,
                     err);
          has_error = true;
        } else if (has_error) {
          fmt::print(stderr, "{} success for key {:?}, {}\n", timestamp(), id, err);
          has_error = false;
        }
      }
    }

    cluster.close().get();

    return 0;
  }

private:
  common_options common_options_{};

  std::string bucket_name_{ default_bucket_name };
  std::string scope_name_{ couchbase::scope::default_name };
  std::string collection_name_{ couchbase::collection::default_name };
  bool use_upsert_{ false };
  bool verbose_{ false };

  std::vector<std::size_t> node_indexes_{};
  std::set<std::uint16_t> vbucket_ids_{};
};
} // namespace

auto
make_beam_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<beam_app>();
}

auto
execute_beam_command(CLI::App* app) -> int
{
  if (auto* beam = dynamic_cast<beam_app*>(app); beam != nullptr) {
    return beam->execute();
  }
  return 1;
}
} // namespace cbc
