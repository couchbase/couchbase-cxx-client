/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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
#include "utils/topology_fixtures.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/bucket.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/mcbp/queue_response.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/document_get.hxx"
#include "core/origin.hxx"
#include "core/orphan_reporter.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/fail_fast_retry_strategy.hxx>

#include <asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
using ::test::utils::config_with_vbmap;
using ::test::utils::vbucket_map;

constexpr std::uint16_t key_value_port{ 11210 };

[[nodiscard]] auto
lazy_options() -> couchbase::core::cluster_options
{
  couchbase::core::cluster_options options{};
  options.enable_lazy_connections = true;
  return options;
}

/**
 * A bucket that opens no sessions of its own, which enable_lazy_connections
 * buys two ways.
 *
 * Without it, update_config() opens a session per node in the configuration.
 * The nodes here carry a loopback key-value address, so tearing the fixture
 * down would run those bootstraps for real: ten seconds of connect retries
 * against a closed port, or an indefinite wait against an open one.
 *
 * It is also the production precondition for connect_session(), which runs
 * only when no session exists for the index.
 */
class bucket_fixture
{
public:
  bucket_fixture()
  {
    couchbase::core::cluster_credentials auth{};
    auth.username = "Administrator";
    auth.password = "password";
    bucket_ = std::make_shared<couchbase::core::bucket>(
      "client-id",
      ctx_,
      tls_,
      couchbase::core::tracing::tracer_wrapper::create(nullptr, nullptr),
      couchbase::core::metrics::meter_wrapper::create(nullptr, nullptr),
      std::make_shared<couchbase::core::orphan_reporter>(
        ctx_, couchbase::core::orphan_reporter_options{}),
      std::make_shared<couchbase::core::app_telemetry_meter>(),
      "default",
      couchbase::core::origin{ auth, "127.0.0.1", key_value_port, lazy_options() },
      std::vector<couchbase::core::protocol::hello_feature>{},
      nullptr);
  }

  ~bucket_fixture()
  {
    bucket_->close();
    ctx_.run();
  }

  bucket_fixture(const bucket_fixture&) = delete;
  bucket_fixture(bucket_fixture&&) = delete;
  auto operator=(const bucket_fixture&) -> bucket_fixture& = delete;
  auto operator=(bucket_fixture&&) -> bucket_fixture& = delete;

  /**
   * @param rev must increase between calls, since update_config() keeps the
   * newer revision and would otherwise ignore a shrink.
   * @param with_key_value_port when false the nodes carry no address at all,
   * which is the topology a non-key-value node presents to this path.
   */
  void install_config(std::size_t number_of_nodes, std::uint64_t rev, bool with_key_value_port)
  {
    auto config = config_with_vbmap(vbucket_map{ { 0 } }, /* num_replicas = */ 0, number_of_nodes);
    config.rev = rev;
    if (with_key_value_port) {
      for (auto& node : config.nodes) {
        node.hostname = "127.0.0.1";
        node.services_plain.key_value = key_value_port;
      }
    }
    bucket_->update_config(std::move(config));
  }

  [[nodiscard]] auto get() const -> const std::shared_ptr<couchbase::core::bucket>&
  {
    return bucket_;
  }

  void run()
  {
    ctx_.run();
  }

private:
  asio::io_context ctx_{};
  couchbase::core::tls_context_provider tls_{};
  std::shared_ptr<couchbase::core::bucket> bucket_{};
};

// map_and_send() defers a command after asking for a session, and only a bootstrap completion
// drains the deferred queue. A true return from connect_session() therefore promises that the
// deferred command will be dispatched or completed: a bootstrap is pending, or the bucket is
// closed and close() cancels the command instead.

void
a_routable_node_is_connected([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ true);
  assert_true(fixture.get()->connect_session(0), "a bootstrap was started");
}

void
an_index_one_past_the_node_list_names_no_node([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ true);
  assert_false(fixture.get()->connect_session(3), "nothing was started");
}

void
an_index_far_beyond_the_node_list_names_no_node([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ true);
  assert_false(fixture.get()->connect_session(99), "nothing was started");
}

void
an_index_valid_under_an_earlier_configuration_is_stale_after_a_shrink([[maybe_unused]] context& ctx)
{
  // The index is resolved under one lock and consumed under another, so this
  // is the interleaving the bounds check exists for.
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ true);
  fixture.install_config(2, /* rev = */ 2, /* with_key_value_port = */ true);
  assert_false(fixture.get()->connect_session(2), "nothing was started");
}

void
a_node_advertising_no_key_value_port_starts_nothing([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ false);
  assert_false(fixture.get()->connect_session(0), "nothing was started");
}

void
a_bucket_with_no_configuration_starts_nothing([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  assert_false(fixture.get()->connect_session(0), "nothing was started");
}

void
a_closed_bucket_reports_success_because_close_cancels_deferred_commands(
  [[maybe_unused]] context& ctx)
{
  // defer_command() completes a command with request_canceled once the bucket
  // is closed, so the caller must defer rather than retry.
  bucket_fixture fixture{};
  fixture.install_config(3, /* rev = */ 1, /* with_key_value_port = */ true);
  fixture.get()->close();
  assert_true(fixture.get()->connect_session(0), "close() completes the deferred command");
}

void
a_command_is_not_deferred_behind_a_session_that_cannot_start([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  // Node 0 owns the vbucket and is present in the topology, but advertises no
  // key-value port, so no session opens for it and no bootstrap completion will
  // drain the deferred queue. Deferring the command here would hold it until its
  // deadline; the dispatch path has to retry instead, and a strategy that
  // declines to retry turns that into an immediate completion.
  fixture.install_config(1, /* rev = */ 1, /* with_key_value_port = */ false);

  // retry_context holds a const member, so the strategy is placed by aggregate
  // initialization rather than assigned after the fact.
  const couchbase::core::operations::get_request request{
    couchbase::core::document_id{ "default", "_default", "_default", "key" },
    /* partition = */ 0,
    /* opaque = */ 0,
    /* timeout = */ {},
    { std::make_shared<couchbase::fail_fast_retry_strategy>() },
  };

  std::error_code ec{};
  auto completed{ false };
  fixture.get()->execute(request, [&ec, &completed](const auto& response) {
    completed = true;
    ec = response.ctx.ec();
  });
  fixture.run();

  assert_true(completed, "the request was completed rather than deferred");
  assert_error(ec, errc::common::request_canceled, "the declined retry completes the request");
}

// The two cases below drive direct_dispatch() and direct_re_queue(), which carry the
// current key-value operations. bucket::execute() reaches neither: it takes the
// mcbp_command path through map_and_send(), so a regression in either branch is
// invisible to the case above.
[[nodiscard]] auto
fail_fast_queue_request(std::error_code& ec, bool& completed)
  -> std::shared_ptr<couchbase::core::mcbp::queue_request>
{
  auto request = std::make_shared<couchbase::core::mcbp::queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [&ec, &completed](std::shared_ptr<couchbase::core::mcbp::queue_response> /* response */,
                      std::shared_ptr<couchbase::core::mcbp::queue_request> /* request */,
                      std::error_code error) {
      completed = true;
      ec = error;
    });
  request->key_ = { std::byte{ 'k' } };
  request->vbucket_ = 0;
  request->retry_strategy_ = std::make_shared<couchbase::fail_fast_retry_strategy>();
  return request;
}

void
a_queue_request_is_not_deferred_behind_a_session_that_cannot_start([[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(1, /* rev = */ 1, /* with_key_value_port = */ false);

  std::error_code ec{};
  auto completed{ false };
  const auto rc = fixture.get()->direct_dispatch(fail_fast_queue_request(ec, completed));
  fixture.run();

  // The bucket-level path reports the refusal through its return value; completing
  // the request is cluster_impl's job, and there is no cluster here. Deferring
  // would report neither.
  assert_error(rc, errc::common::service_not_available, "the refusal reported to the caller");
  assert_false(completed, "the bucket path does not complete the request itself");
}

void
a_re_queued_request_is_not_deferred_behind_a_session_that_cannot_start(
  [[maybe_unused]] context& ctx)
{
  bucket_fixture fixture{};
  fixture.install_config(1, /* rev = */ 1, /* with_key_value_port = */ false);

  std::error_code ec{};
  auto completed{ false };
  fixture.get()->direct_re_queue(fail_fast_queue_request(ec, completed), /* is_retry = */ true);
  fixture.run();

  // A re-queue is driven by a backoff timer that ignores what it returns, so this
  // path has to complete the request itself.
  assert_true(completed, "the re-queued request was completed rather than deferred");
  assert_error(ec, errc::common::service_not_available, "the code it was completed with");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_routable_node_is_connected), {}, timeout::fast },
      { CASE(an_index_one_past_the_node_list_names_no_node), {}, timeout::fast },
      { CASE(an_index_far_beyond_the_node_list_names_no_node), {}, timeout::fast },
      { CASE(an_index_valid_under_an_earlier_configuration_is_stale_after_a_shrink),
        {},
        timeout::fast },
      { CASE(a_node_advertising_no_key_value_port_starts_nothing), {}, timeout::fast },
      { CASE(a_bucket_with_no_configuration_starts_nothing), {}, timeout::fast },
      { CASE(a_closed_bucket_reports_success_because_close_cancels_deferred_commands),
        {},
        timeout::fast },
      { CASE(a_command_is_not_deferred_behind_a_session_that_cannot_start), {}, timeout::fast },
      { CASE(a_queue_request_is_not_deferred_behind_a_session_that_cannot_start),
        {},
        timeout::fast },
      { CASE(a_re_queued_request_is_not_deferred_behind_a_session_that_cannot_start),
        {},
        timeout::fast },
    },
  };
}

} // namespace couchbase::test
