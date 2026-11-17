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

/*
 * A completed mcbp_command writes nothing more. Collection resolution subscribes GetCollectionID
 * under an opaque the command does not record, so cancel() cannot withdraw it, and the paths that
 * continue from its response have to stop on their own. A mutation that times out before collection
 * resolution completes reports unambiguous_timeout, because the server cannot have received it.
 *
 * The session is never bootstrapped, so a write lands in its pending buffer and no case touches the
 * network. Every write is subscribed under an opaque the session hands out, which is what
 * requests_written_since() counts. Each case declares its completions before the fixture, whose
 * teardown can still run a command's completion.
 */

#include "framework/test_registry.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/bucket.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/impl/bootstrap_state_listener.hxx"
#include "core/impl/with_cancellation.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/io/mcbp_session_unit_test_api.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_replica.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/origin.hxx"
#include "core/orphan_reporter.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <asio/error.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::bucket;
using couchbase::core::io::mcbp_message;
using couchbase::core::io::mcbp_session;

constexpr std::chrono::milliseconds command_timeout{ 10'000 };
constexpr std::uint32_t resolved_collection_uid{ 8 };

class stub_bootstrap_listener : public couchbase::core::impl::bootstrap_state_listener
{
public:
  void report_bootstrap_error(const std::string& /*endpoint*/, std::error_code /*ec*/) override
  {
  }

  void report_bootstrap_success(const std::vector<std::string>& /*endpoints*/) override
  {
  }

  void register_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }

  void unregister_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }
};

class command_fixture
{
public:
  command_fixture()
    : bucket_{ std::make_shared<bucket>(
        "client-id",
        io_,
        tls_,
        couchbase::core::tracing::tracer_wrapper::create(
          std::make_shared<couchbase::core::tracing::noop_tracer>(),
          std::make_shared<couchbase::core::cluster_label_listener>()),
        couchbase::core::metrics::meter_wrapper::create(
          std::make_shared<couchbase::core::metrics::noop_meter>(),
          std::make_shared<couchbase::core::cluster_label_listener>()),
        std::make_shared<couchbase::core::orphan_reporter>(
          io_,
          couchbase::core::orphan_reporter_options{}),
        std::make_shared<couchbase::core::app_telemetry_meter>(),
        "default",
        couchbase::core::origin{},
        std::vector<couchbase::core::protocol::hello_feature>{},
        nullptr) }
    , session_{ "client-id",
                "node-uuid",
                io_,
                couchbase::core::origin(
                  couchbase::core::cluster_credentials{ "user", "pass" },
                  couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1")),
                std::make_shared<stub_bootstrap_listener>(),
                "default",
                { couchbase::core::protocol::hello_feature::collections } }
  {
  }

  ~command_fixture()
  {
    bucket_->close();
    session_.stop(couchbase::retry_reason::do_not_retry);
    io_.restart();
    io_.run();
  }

  command_fixture(const command_fixture&) = delete;
  command_fixture(command_fixture&&) = delete;
  auto operator=(const command_fixture&) -> command_fixture& = delete;
  auto operator=(command_fixture&&) -> command_fixture& = delete;

  template<typename Request>
  [[nodiscard]] auto make_command(Request request)
    -> std::shared_ptr<couchbase::core::operations::mcbp_command<bucket, Request>>
  {
    return std::make_shared<couchbase::core::operations::mcbp_command<bucket, Request>>(
      io_, bucket_, std::move(request), command_timeout);
  }

  void close_bucket()
  {
    bucket_->close();
  }

  [[nodiscard]] auto io() -> asio::io_context&
  {
    return io_;
  }

  [[nodiscard]] auto session() -> mcbp_session&
  {
    return session_;
  }

  // Withdraws every subscription made after `marker` and returns how many there were.
  [[nodiscard]] auto requests_written_since(std::uint32_t marker) -> std::size_t
  {
    const auto last = session_.next_opaque();
    std::size_t written{ 0 };
    for (auto opaque = marker + 1; opaque < last; ++opaque) {
      if (session_.cancel(
            opaque, asio::error::operation_aborted, couchbase::retry_reason::do_not_retry)) {
        ++written;
      }
    }
    return written;
  }

private:
  asio::io_context io_{};
  couchbase::core::tls_context_provider tls_{};
  std::shared_ptr<bucket> bucket_;
  mcbp_session session_;
};

[[nodiscard]] auto
named_collection_key() -> couchbase::core::document_id
{
  return { "default", "inventory", "airline", "key" };
}

void
a_mutation_that_times_out_during_collection_resolution_is_not_sent([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  // No collection id is cached, so the dispatch writes GetCollectionID and waits for it.
  cmd->send_to(fixture.session());
  // The deadline handler's call.
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  assert_eq(completions.size(), std::size_t{ 1 }, "the timeout completes the command");
  const auto completed_at = fixture.session().next_opaque();

  // What the GetCollectionID success branch does once its response arrives.
  fixture.session().update_collection_uid(cmd->request.id.collection_path(),
                                          resolved_collection_uid);
  cmd->request.id.collection_uid(resolved_collection_uid);
  cmd->send();

  // A write here is the upsert itself, applied by the server after the caller was told it timed
  // out.
  assert_eq(fixture.requests_written_since(completed_at),
            std::size_t{ 0 },
            "nothing is written after the completion");
  assert_eq(completions.size(), std::size_t{ 1 }, "the command completes once");
}

void
a_cancelled_replica_read_does_not_resolve_its_collection([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::impl::with_cancellation<couchbase::core::operations::get_replica_request>
    request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });
  cmd->session_ = fixture.session();

  const auto marker = fixture.session().next_opaque();
  // Both calls arrive off the command strand and are queued on it in this order: a sibling's
  // cancellation, then the unknown-collection retry that was already in flight.
  cmd->cancel(couchbase::retry_reason::do_not_retry, /* is_timeout = */ false);
  cmd->request_collection_id();
  fixture.io().run();

  assert_eq(completions.size(), std::size_t{ 1 }, "the cancellation completes the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::request_canceled },
            "and is its answer");
  assert_eq(fixture.requests_written_since(marker),
            std::size_t{ 0 },
            "a cancelled command writes no GetCollectionID");
}

void
an_unknown_collection_response_after_completion_arms_no_retry([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });
  cmd->session_ = fixture.session();
  // A cancellation that leaves the deadline far off, as a closed bucket's does. After the deadline
  // itself no backoff is armed, because less than its 500 ms remain.
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  assert_eq(completions.size(), std::size_t{ 1 }, "the cancellation completes the command");

  // An unknown-collection response that was queued behind the cancellation.
  cmd->handle_unknown_collection();

  // An armed backoff ends in request_collection_id(), and holds the command until it fires.
  assert_eq(cmd->retry_backoff.cancel(), std::size_t{ 0 }, "a completed command arms no retry");
  assert_eq(completions.size(), std::size_t{ 1 }, "the command completes once");
}

void
a_mutation_that_times_out_during_collection_resolution_is_unambiguous([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  // Only GetCollectionID is written; the upsert waits for its response.
  cmd->send_to(fixture.session());
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.size(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "a mutation the server never received");
}

void
a_mutation_that_times_out_after_it_was_written_is_ambiguous([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  fixture.session().update_collection_uid(cmd->request.id.collection_path(),
                                          resolved_collection_uid);
  cmd->send_to(fixture.session());
  // cancel() then finds no subscription to withdraw, as when the session has already taken the
  // response, and classifies the timeout itself.
  cmd->opaque_ = fixture.session().next_opaque(); // an opaque with no subscription
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.size(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "a mutation the server may have applied");
}

void
a_mutation_whose_collection_stays_unknown_until_its_deadline_is_unambiguous(
  [[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  // Less than the 500 ms unknown-collection backoff, so the next unknown-collection answer ends the
  // command instead of retrying.
  request.timeout = std::chrono::milliseconds{ 100 };
  auto cmd = fixture.make_command(std::move(request));

  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });
  cmd->session_ = fixture.session();

  // What GetCollectionID's collection_not_found answer does; the upsert itself was never written.
  cmd->handle_unknown_collection();

  assert_eq(completions.size(), std::size_t{ 1 }, "the remaining time cannot cover a retry");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "a mutation the server never received");
}

void
a_mutation_rejected_for_its_collection_then_timed_out_is_unambiguous([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));
  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  // A stale cached id: the upsert is written under it.
  fixture.session().update_collection_uid(cmd->request.id.collection_path(),
                                          resolved_collection_uid);
  cmd->send_to(fixture.session());
  // What the response handler does when the server answers unknown_collection. The response has
  // taken the attempt's subscription, so cancel() finds nothing to withdraw.
  cmd->handle_unknown_collection();
  cmd->opaque_ = fixture.session().next_opaque(); // an opaque with no subscription
  // The deadline, while the retry resolves the collection again.
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.size(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "the only attempt was rejected by the server");
}

void
a_rejected_retry_keeps_the_ambiguity_of_an_earlier_attempt([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));
  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  fixture.session().update_collection_uid(cmd->request.id.collection_path(),
                                          resolved_collection_uid);
  // An attempt the server may have applied, then a retry that it rejects.
  cmd->send_to(fixture.session());
  cmd->send_to(fixture.session());
  cmd->handle_unknown_collection();
  cmd->opaque_ = fixture.session().next_opaque(); // an opaque with no subscription
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.size(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "the first attempt may still have been applied");
}

void
constructing_mcbp_command_moves_request_value_buffer([[maybe_unused]] context& ctx)
{
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  request.value.assign(65'536, std::byte{ 0x5a });
  const auto* original_data = request.value.data();

  auto cmd = fixture.make_command(std::move(request));

  assert_eq(cmd->request.value.size(), std::size_t{ 65'536 }, "request value size preserved");
  assert_true(cmd->request.value.data() == original_data,
              "mcbp_command moves the request value buffer instead of copying it");
}

// bucket::close() marks the bucket closed, then stops its sessions, which answer every written
// command with request_canceled, and only then stops the deadline queue. A command that retries
// the session's answer finds the bucket closed. Starting the command after close() keeps that
// order: the stopped queue's on_stop() arrives through the io_context, after the session's
// answer, or the retry of it, has completed the command.
template<typename Request>
[[nodiscard]] auto
answer_command_retried_on_a_closed_bucket(Request request) -> std::vector<std::error_code>
{
  std::vector<std::error_code> completions{};
  {
    command_fixture fixture{};
    fixture.close_bucket();
    request.id = { "default", "_default", "_default", "key" };
    auto cmd = fixture.make_command(std::move(request));
    cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
      completions.push_back(ec);
    });
    cmd->send_to(fixture.session());

    fixture.session().stop(couchbase::retry_reason::do_not_retry);
    assert_eq(completions.size(), std::size_t{ 1 }, "answered before on_stop() can run");
    fixture.io().run();
  }
  return completions;
}

void
an_idempotent_command_in_flight_when_its_bucket_closes_is_canceled([[maybe_unused]] context& ctx)
{
  const auto completions =
    answer_command_retried_on_a_closed_bucket(couchbase::core::operations::get_request{});

  assert_eq(completions.size(), std::size_t{ 1 }, "the command completes once");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::request_canceled },
            "the retry finds the bucket closed and is abandoned, not timed out");
}

void
a_non_idempotent_command_in_flight_when_its_bucket_closes_is_canceled([[maybe_unused]] context& ctx)
{
  const auto completions =
    answer_command_retried_on_a_closed_bucket(couchbase::core::operations::upsert_request{});

  assert_eq(completions.size(), std::size_t{ 1 }, "the command completes once");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::request_canceled },
            "the session's answer completes the command");
}
void
each_dispatch_of_a_command_is_encoded_afresh([[maybe_unused]] context& ctx)
{
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{ { "default", "_default", "_default", "key" },
                                                       couchbase::core::utils::to_binary("{}") };
  request.durability_level = couchbase::durability_level::majority;
  request.preserve_expiry = true;
  auto cmd = fixture.make_command(std::move(request));
  cmd->send_to(fixture.session());
  cmd->send(); // as a retry dispatches the same command again

  const auto frames = fixture.session().unit_test_api().pending_frames();
  assert_eq(frames.size(), std::size_t{ 2 }, "both dispatches are written");
  for (const auto& frame : frames) {
    // The framing extras length: a durability frame with a timeout takes 4 bytes, a
    // preserve-expiry frame 1.
    assert_eq(std::to_integer<std::size_t>(frame.at(2)),
              std::size_t{ 5 },
              "one durability frame and one preserve-expiry frame");
  }
}

void
a_command_whose_handler_throws_is_released_before_its_deadline([[maybe_unused]] context& ctx)
{
  command_fixture fixture{};
  couchbase::core::operations::get_request request{};
  request.id = { "default", "_default", "_default", "key" };
  auto cmd = fixture.make_command(std::move(request));
  cmd->start([](std::error_code /* ec */, std::optional<mcbp_message>&& /* msg */) {
    throw std::runtime_error("handler");
  });
  const std::weak_ptr weak{ cmd };

  bool thrown = false;
  try {
    cmd->cancel(couchbase::retry_reason::do_not_retry, /* is_timeout = */ false);
  } catch (const std::runtime_error&) {
    thrown = true;
  }
  cmd.reset();

  assert_true(thrown, "the handler's exception reaches the caller");
  // A linked deadline entry would keep the command alive for the ten-second timeout.
  assert_true(weak.expired(), "the deadline queue released the command");
}

void
a_command_pending_when_its_bucket_closes_is_canceled([[maybe_unused]] context& ctx)
{
  std::vector<std::error_code> completions{};
  command_fixture fixture{};
  couchbase::core::operations::upsert_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request));
  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.push_back(ec);
  });

  fixture.close_bucket();

  // A timeout here would tell the caller the deadline passed, ten seconds early.
  assert_eq(completions.size(), std::size_t{ 1 }, "close() answers the command");
  assert_eq(completions.front(),
            std::error_code{ couchbase::errc::common::request_canceled },
            "the command was abandoned, not timed out");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_mutation_that_times_out_during_collection_resolution_is_not_sent),
        {},
        timeout::fast },
      { CASE(a_cancelled_replica_read_does_not_resolve_its_collection), {}, timeout::fast },
      { CASE(an_unknown_collection_response_after_completion_arms_no_retry), {}, timeout::fast },
      { CASE(a_mutation_that_times_out_during_collection_resolution_is_unambiguous),
        {},
        timeout::fast },
      { CASE(a_mutation_that_times_out_after_it_was_written_is_ambiguous), {}, timeout::fast },
      { CASE(a_mutation_whose_collection_stays_unknown_until_its_deadline_is_unambiguous),
        {},
        timeout::fast },
      { CASE(a_mutation_rejected_for_its_collection_then_timed_out_is_unambiguous),
        {},
        timeout::fast },
      { CASE(a_rejected_retry_keeps_the_ambiguity_of_an_earlier_attempt), {}, timeout::fast },
      { CASE(an_idempotent_command_in_flight_when_its_bucket_closes_is_canceled),
        {},
        timeout::fast },
      { CASE(a_non_idempotent_command_in_flight_when_its_bucket_closes_is_canceled),
        {},
        timeout::fast },
      { CASE(constructing_mcbp_command_moves_request_value_buffer), {}, timeout::fast },
      { CASE(each_dispatch_of_a_command_is_encoded_afresh), {}, timeout::fast },
      { CASE(a_command_whose_handler_throws_is_released_before_its_deadline), {}, timeout::fast },
      { CASE(a_command_pending_when_its_bucket_closes_is_canceled), {}, timeout::fast },
    },
  };
}

} // namespace couchbase::test
