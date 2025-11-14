/* -*- Mode: C++; tabrr-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "config.hxx"

#include "core/operations/management/bucket_describe.hxx"
#include "core/operations/management/cluster_describe.hxx"
#include "core/utils/json.hxx"
#include "utils.hxx"

#include "core/cluster.hxx"

#include <couchbase/cluster.hxx>

#include <spdlog/fmt/bundled/format.h>
#include <tao/json.hpp>
#include <tao/json/from_string.hpp>

#include <couchbase/fmt/error.hxx>

#include <csignal>
#include <thread>

namespace cbc
{
namespace
{

std::atomic_flag running{ true };

void
sigint_handler(int /* signal */)
{
  running.clear();
}

class config_helper
{
public:
  config_helper(couchbase::core::cluster core, std::string bucket_name)
    : core_{ std::move(core) }
    , bucket_name_{ std::move(bucket_name) }
  {
  }

  [[nodiscard]] auto get_bucket_config() const -> std::string
  {
    if (bucket_name_.empty()) {
      fail("Bucket name cannot be empty for bucket configuration");
    }

    couchbase::core::operations::management::bucket_describe_request req{ bucket_name_ };
    std::promise<couchbase::core::operations::management::bucket_describe_response> barrier;
    auto f = barrier.get_future();
    core_.execute(req, [barrier = std::move(barrier)](auto resp) mutable -> auto {
      barrier.set_value(std::move(resp));
    });
    auto resp = f.get();

    if (resp.ctx.ec) {
      fail(fmt::format(
        "Failed to get bucket config for {:?}: {}", bucket_name_, resp.ctx.ec.message()));
    }
    return resp.ctx.http_body;
  }

  [[nodiscard]] auto get_cluster_config() const -> std::string
  {
    couchbase::core::operations::management::cluster_describe_request req{};
    std::promise<couchbase::core::operations::management::cluster_describe_response> barrier;
    auto f = barrier.get_future();
    core_.execute(req, [barrier = std::move(barrier)](auto resp) mutable -> auto {
      barrier.set_value(std::move(resp));
    });
    auto resp = f.get();

    if (resp.ctx.ec) {
      fail(fmt::format("Failed to get cluster config: {}", resp.ctx.ec.message()));
    }
    return resp.ctx.http_body;
  }

private:
  couchbase::core::cluster core_;
  std::string bucket_name_;
};

class config_app : public CLI::App
{
public:
  config_app()
    : CLI::App("Display cluster configuration.", "config")
  {
    alias("conf");
    alias("cfg");

    add_flag("--pretty-json",
             pretty_json_,
             "Try to pretty-print as JSON value (prints AS-IS if the document is not a JSON).");

    add_option("--level", level_, "Level of the config (--bucket-name is required for \"bucket\").")
      ->transform(CLI::IsMember({ "bucket", "cluster" }))
      ->default_val("bucket");

    add_option("--bucket-name", bucket_name_, "Name of the bucket.")
      ->default_val(default_bucket_name);

    add_option("--watch-interval", watch_interval_, "Request configuration periodically.")
      ->type_name("DURATION");

    add_common_options(this, common_options_);
    allow_extras(true);
  }

  void execute() const
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    const auto connection_string = common_options_.connection.connection_string;

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {
      fail(fmt::format(
        "Failed to connect to the cluster at \"{}\": {}", connection_string, connect_err));
    }

    auto get_config =
      [&, core = couchbase::core::get_core_cluster(cluster)]() -> std::function<std::string()> {
      auto helper = config_helper(core, bucket_name_);
      if (level_ == "bucket") {
        return [helper = std::move(helper)]() -> std::string {
          return helper.get_bucket_config();
        };
      }
      return [helper = std::move(helper)]() -> std::string {
        return helper.get_cluster_config();
      };
    }();

    std::signal(SIGINT, sigint_handler);
    std::signal(SIGTERM, sigint_handler);

    bool poll_config = watch_interval_ != std::chrono::milliseconds::zero();
    do {
      std::string raw_config = get_config();
      if (pretty_json_) {
        tao::json::value config = tao::json::from_string(raw_config);
        fmt::println(stdout, "{}", tao::json::to_string(config, 2));
      } else {
        fmt::println(stdout, "{}", raw_config);
      }
      std::this_thread::sleep_for(watch_interval_);
    } while (running.test_and_set() && poll_config);
  }

private:
  common_options common_options_{};

  bool pretty_json_{ false };
  std::chrono::milliseconds watch_interval_{ 0 };
  std::string level_{ "bucket" };
  std::string bucket_name_{ default_bucket_name };
};

} // namespace

auto
make_config_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<config_app>();
}

auto
execute_config_command(const CLI::App* app) -> int
{
  if (const auto* config = dynamic_cast<const config_app*>(app); config != nullptr) {
    config->execute();
    return 0;
  }
  return 1;
}
} // namespace cbc
