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
#include "utils/wait_until.hxx"

#include "core/agent_group.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/range_scan_orchestrator.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/scope.hxx>

#include <chrono>
#include <future>
#include <memory>
#include <variant>
#include <vector>

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

void
range_scan_creates_sharing_a_failed_collection_id_lookup_are_released([[maybe_unused]] context& ctx)
{
  // Smoke test against a server for the unit case
  // every_request_queued_behind_a_collection_not_found_lookup_is_retried in
  // test/unit/core/collections_component.cxx. The callbacks hold the agent, as range_scan_stream
  // does. Whether one create is queued behind the other's lookup when teardown starts depends on
  // timing, so this case does not fail on every run without that fix.
  std::vector<std::weak_ptr<couchbase::core::mcbp::queue_request>> creates;
  {
    ::test::utils::integration_test_guard integration;

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    assert_success(ag.open_bucket(integration.ctx.bucket), "the agent opens the bucket");
    auto agent = ag.get_agent(integration.ctx.bucket);
    assert_true(agent.has_value(), "the agent group has an agent for the bucket");

    couchbase::core::range_scan_create_options options{
      couchbase::scope::default_name,
      ::test::utils::uniq_id("missing"),
      couchbase::core::range_scan{
        couchbase::core::scan_term{ "a" },
        couchbase::core::scan_term{ "b" },
      },
    };
    // Longer than the case, so that both creates are still retrying at teardown.
    options.timeout = 1min;

    std::vector<std::shared_ptr<couchbase::core::mcbp::queue_request>> requests;
    for (int i = 0; i < 2; ++i) {
      auto op = agent->range_scan_create(
        12, options, [holder = agent.value()](auto /* res */, auto /* ec */) {
        });
      assert_true(op.has_value(), "the create is dispatched");
      requests.emplace_back(
        std::dynamic_pointer_cast<couchbase::core::mcbp::queue_request>(op.value()));
      assert_true(requests.back() != nullptr, "the create is a queue_request");
      creates.emplace_back(requests.back());
    }

    // Each lookup answers collection_not_found, and every create it held is handed to a retry.
    assert_true(::test::utils::wait_until(
                  [&requests]() {
                    for (const auto& request : requests) {
                      if (request->retry_attempts() == 0) {
                        return false;
                      }
                    }
                    return true;
                  },
                  10s,
                  10ms),
                "both creates are retried after a collection_not_found lookup");
  }

  for (const auto& create : creates) {
    assert_true(create.expired(), "the create is released after teardown");
  }
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
      { CASE(range_scan_creates_sharing_a_failed_collection_id_lookup_are_released),
        range_scan,
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
