/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/integration_test_guard.hxx"
#include "utils/test_data.hxx"

#include "core/agent_group.hxx"
#include "core/range_scan_orchestrator.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/scope.hxx>

#include <chrono>
#include <future>
#include <memory>
#include <variant>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;

using scan_type = std::variant<std::monostate,
                               couchbase::core::range_scan,
                               couchbase::core::prefix_scan,
                               couchbase::core::sampling_scan>;

auto
get_vbucket_map(const ::test::utils::integration_test_guard& integration)
  -> couchbase::core::topology::configuration::vbucket_map
{
  auto barrier = std::make_shared<std::promise<
    tl::expected<couchbase::core::topology::configuration::vbucket_map, std::error_code>>>();
  auto f = barrier->get_future();
  integration.cluster.with_bucket_configuration(
    integration.ctx.bucket,
    [barrier](std::error_code ec,
              const std::shared_ptr<couchbase::core::topology::configuration>& config) mutable {
      if (ec) {
        return barrier->set_value(tl::unexpected(ec));
      }
      if (!config->vbmap || config->vbmap->empty()) {
        return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
      }
      barrier->set_value(config->vbmap.value());
    });
  auto vbucket_map = f.get();
  assert_success(vbucket_map ? std::error_code{} : vbucket_map.error(),
                 "the bucket configuration carries a vbucket map");
  return vbucket_map.value();
}

void
scan_missing_collection(const scan_type& scan)
{
  ::test::utils::integration_test_guard integration;

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  assert_success(ag.open_bucket(integration.ctx.bucket), "the agent opens the bucket");
  auto agent = ag.get_agent(integration.ctx.bucket);
  assert_true(agent.has_value(), "the agent group has an agent for the bucket");

  // Long enough that a timeout and an immediate failure cannot be confused.
  couchbase::core::range_scan_orchestrator_options options{};
  options.timeout = 10s;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        ::test::utils::uniq_id("missing"),
                                                        scan,
                                                        options);

  const auto start = std::chrono::steady_clock::now();
  auto result = orchestrator.scan();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // Regression: GetCollectionID answered unknown_collection was retried until the scan timeout,
  // and the scan failed with unambiguous_timeout.
  assert_false(result.has_value(), "a scan of a missing collection does not start");
  assert_error(result.error(),
               couchbase::errc::common::collection_not_found,
               "the scan reports the missing collection");
  assert_true(elapsed < 2500ms, "the scan fails without waiting for its timeout");
}

void
range_scan_of_a_missing_collection_fails_with_collection_not_found([[maybe_unused]] context& ctx)
{
  scan_missing_collection(couchbase::core::range_scan{});
}

void
sampling_scan_of_a_missing_collection_fails_with_collection_not_found([[maybe_unused]] context& ctx)
{
  scan_missing_collection(couchbase::core::sampling_scan{ 10 });
}
} // namespace

auto
tests() -> test_suite
{
  const std::vector<requirement_ptr> range_scan{ needs::real_cluster(),
                                                 needs::bucket_capability("range_scan") };
  return {
    suite_name,
    {
      { CASE(range_scan_of_a_missing_collection_fails_with_collection_not_found),
        range_scan,
        timeout::integration },
      { CASE(sampling_scan_of_a_missing_collection_fails_with_collection_not_found),
        range_scan,
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
