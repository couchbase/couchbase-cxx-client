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
 * A non-cancellable mcbp_command is dispatched on the thread that calls map_and_send(), while its
 * deadline runs cancel() on an I/O thread. A command completes once, and a mutation reported as
 * unambiguous_timeout is never written afterwards.
 *
 * The session is never bootstrapped, so a written request stays in its pending buffer, where
 * upserts_written() counts it whether or not its subscription is later withdrawn. No case touches
 * the network. Under ThreadSanitizer, the concurrent cases check selected interleavings:
 *
 * - a deadline against a dispatch, a retried dispatch, or a dispatch blocked in the tracer;
 * - a response handler and its retry decision against the claim;
 * - a late withdrawal or a session cancellation against the completion.
 *
 * Every completion log outlives the fixture, whose teardown can still run a command's completion.
 */

#include "framework/test_registry.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/bucket.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_label_listener.hxx"
#include "core/error_context/key_value_status_code.hxx"
#include "core/impl/bootstrap_state_listener.hxx"
#include "core/impl/with_cancellation.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/io/mcbp_session_unit_test_api.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/origin.hxx"
#include "core/orphan_reporter.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/fail_fast_retry_strategy.hxx>
#include <couchbase/retry_action.hxx>
#include <couchbase/retry_reason.hxx>
#include <couchbase/retry_strategy.hxx>
#include <couchbase/tracing/request_span.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <asio/error.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::bucket;
using couchbase::core::io::mcbp_message;
using couchbase::core::io::mcbp_session;
using couchbase::core::operations::mcbp_command;
using couchbase::core::operations::upsert_request;

constexpr std::chrono::milliseconds command_timeout{ 10'000 };
constexpr std::uint32_t collection_uid{ 8 };
constexpr int race_rounds{ 500 };

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

[[nodiscard]] auto
named_collection_key() -> couchbase::core::document_id
{
  return { "default", "inventory", "airline", "key" };
}

// Recorded completions of one command, synchronized across threads.
class completion_log
{
public:
  void add(std::error_code ec)
  {
    const std::scoped_lock lock(mutex_);
    codes_.push_back(ec);
  }

  [[nodiscard]] auto count() const -> std::size_t
  {
    const std::scoped_lock lock(mutex_);
    return codes_.size();
  }

  [[nodiscard]] auto first() const -> std::error_code
  {
    const std::scoped_lock lock(mutex_);
    return codes_.empty() ? std::error_code{} : codes_.front();
  }

private:
  mutable std::mutex mutex_{};
  std::vector<std::error_code> codes_{};
};

// Records the thread that ends each dispatch span. Three paths end one: the response handler that
// takes it, complete() for a span no handler took, and send() for a span it created after the
// claim and could not publish.
class span_end_recorder : public couchbase::tracing::request_tracer
{
public:
  class recording_span : public couchbase::tracing::request_span
  {
  public:
    recording_span(std::string name, span_end_recorder& recorder)
      : request_span(std::move(name))
      , recorder_{ recorder }
    {
    }

    void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
    {
    }

    void add_tag(const std::string& /* name */, const std::string& /* value */) override
    {
    }

    void end() override
    {
      if (name() == couchbase::core::tracing::operation::step_dispatch) {
        recorder_.record(std::this_thread::get_id());
        if (recorder_.on_dispatch_end) {
          recorder_.on_dispatch_end();
        }
      }
    }

    [[nodiscard]] auto uses_tags() const -> bool override
    {
      return false;
    }

  private:
    span_end_recorder& recorder_;
  };

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> /* parent */)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    return std::make_shared<recording_span>(std::move(name), *this);
  }

  void record(std::thread::id thread)
  {
    const std::scoped_lock lock(mutex_);
    ended_on_.push_back(thread);
  }

  [[nodiscard]] auto ended_on() const -> std::vector<std::thread::id>
  {
    const std::scoped_lock lock(mutex_);
    return ended_on_;
  }

  // Runs after each dispatch span end is recorded. Set before the command starts.
  std::function<void()> on_dispatch_end{};

private:
  mutable std::mutex mutex_{};
  std::vector<std::thread::id> ended_on_{};
};

class command_fixture
{
public:
  explicit command_fixture(std::shared_ptr<couchbase::tracing::request_tracer> tracer =
                             std::make_shared<couchbase::core::tracing::noop_tracer>())
    : orphans_{ std::make_shared<couchbase::core::orphan_reporter>(
        io_,
        couchbase::core::orphan_reporter_options{}) }
    , telemetry_{ std::make_shared<couchbase::core::app_telemetry_meter>() }
    , bucket_{ std::make_shared<bucket>(
        "client-id",
        io_,
        tls_,
        couchbase::core::tracing::tracer_wrapper::create(
          std::move(tracer),
          std::make_shared<couchbase::core::cluster_label_listener>()),
        couchbase::core::metrics::meter_wrapper::create(
          std::make_shared<couchbase::core::metrics::noop_meter>(),
          std::make_shared<couchbase::core::cluster_label_listener>()),
        orphans_,
        telemetry_,
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
    // A cached id sends the request without collection resolution.
    session_.update_collection_uid(named_collection_key().collection_path(), collection_uid);
  }

  ~command_fixture()
  {
    bucket_->close();
    session_.stop(couchbase::retry_reason::do_not_retry);
    // Ready handlers only: a case that failed before completing its command leaves the deadline
    // armed, and waiting for it would turn the failed assertion into a timeout.
    io_.restart();
    while (io_.poll() > 0) {
    }
  }

  command_fixture(const command_fixture&) = delete;
  command_fixture(command_fixture&&) = delete;
  auto operator=(const command_fixture&) -> command_fixture& = delete;
  auto operator=(command_fixture&&) -> command_fixture& = delete;

  template<typename Request>
  [[nodiscard]] auto make_command(Request request, completion_log& completions)
    -> std::shared_ptr<mcbp_command<bucket, Request>>
  {
    auto cmd = std::make_shared<mcbp_command<bucket, Request>>(
      io_, bucket_, std::move(request), command_timeout);
    cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
      completions.add(ec);
    });
    return cmd;
  }

  [[nodiscard]] auto make_upsert(upsert_request request, completion_log& completions)
    -> std::shared_ptr<mcbp_command<bucket, upsert_request>>
  {
    return make_command(std::move(request), completions);
  }

  // A started upsert, whose completions `completions` records.
  [[nodiscard]] auto start_upsert(completion_log& completions,
                                  couchbase::core::document_id id = named_collection_key())
    -> std::shared_ptr<mcbp_command<bucket, upsert_request>>
  {
    upsert_request request{};
    request.id = std::move(id);
    return make_command(std::move(request), completions);
  }

  // The application telemetry recorded since the last call; empty when nothing was.
  [[nodiscard]] auto telemetry_report() -> std::vector<std::byte>
  {
    std::vector<std::byte> report{};
    telemetry_->generate_report(report);
    return report;
  }

  // The orphan report since the last call; empty when nothing was reported.
  [[nodiscard]] auto orphan_output() -> std::string
  {
    return orphans_->flush_and_create_output().value_or(std::string{});
  }

  // How many orphans were reported since the last call.
  [[nodiscard]] auto orphan_count() -> std::size_t
  {
    const auto output = orphans_->flush_and_create_output();
    if (!output) {
      return 0;
    }
    const std::string key{ R"("total_count":)" };
    const auto at = output->find(key);
    return at == std::string::npos ? 0 : std::stoul(output->substr(at + key.size()));
  }

  // Whether an orphan was reported since the last call.
  [[nodiscard]] auto orphan_reported() -> bool
  {
    return orphans_->flush_and_create_output().has_value();
  }

  [[nodiscard]] auto manager() -> std::shared_ptr<bucket>
  {
    return bucket_;
  }

  [[nodiscard]] auto io() -> asio::io_context&
  {
    return io_;
  }

  [[nodiscard]] auto session() -> mcbp_session&
  {
    return session_;
  }

  // How many upserts the session holds written. A written request stays counted when its
  // subscription is withdrawn.
  [[nodiscard]] auto upserts_written() const -> std::size_t
  {
    return written(couchbase::core::protocol::client_opcode::upsert);
  }

  [[nodiscard]] auto written(couchbase::core::protocol::client_opcode opcode) const -> std::size_t
  {
    const auto opcodes = session_.unit_test_api().pending_opcodes();
    return static_cast<std::size_t>(std::count(opcodes.begin(), opcodes.end(), opcode));
  }

  // Runs what is ready on the io_context: aborted timer waits, and work queued on a cancellable
  // command's strand.
  void drain()
  {
    io_.restart();
    while (io_.poll() > 0) {
    }
  }

  // Withdraws every subscription made after `marker` and returns how many there were.
  [[nodiscard]] auto withdraw_subscriptions_since(std::uint32_t marker) -> std::size_t
  {
    const auto last = session_.next_opaque();
    std::size_t live{ 0 };
    for (auto opaque = marker + 1; opaque < last; ++opaque) {
      if (session_.cancel(
            opaque, asio::error::operation_aborted, couchbase::retry_reason::do_not_retry)) {
        ++live;
      }
    }
    return live;
  }

private:
  asio::io_context io_{};
  couchbase::core::tls_context_provider tls_{};
  std::shared_ptr<couchbase::core::orphan_reporter> orphans_;
  std::shared_ptr<couchbase::core::app_telemetry_meter> telemetry_;
  std::shared_ptr<bucket> bucket_;
  mcbp_session session_;
};

// Runs `first` and `second` on two threads released together.
template<typename First, typename Second>
void
race(First&& first, Second&& second)
{
  std::atomic_bool go{ false };
  std::thread one([&go, &first] {
    while (!go.load()) {
      std::this_thread::yield();
    }
    first();
  });
  std::thread two([&go, &second] {
    while (!go.load()) {
      std::this_thread::yield();
    }
    second();
  });
  go.store(true);
  one.join();
  two.join();
}

void
a_deadline_racing_the_first_dispatch_reports_unambiguous_only_for_an_unwritten_request(
  [[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  command_fixture fixture{};
  std::size_t not_completed_once{ 0 };
  std::size_t unambiguous_and_written{ 0 };
  std::size_t subscribed_after_completion{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = fixture.start_upsert(completions);
    const auto marker = fixture.session().next_opaque();
    const auto written_before = fixture.upserts_written();

    race(
      [&cmd, &fixture] {
        // The caller's thread, as bucket::execute() reaches send_to() through map_and_send().
        cmd->send_to(fixture.session());
      },
      [&cmd] {
        // The I/O thread running the deadline handler.
        cmd->cancel(couchbase::retry_reason::do_not_retry);
      });

    const auto written = fixture.upserts_written() - written_before;
    fixture.drain();
    if (fixture.withdraw_subscriptions_since(marker) != 0) {
      ++subscribed_after_completion;
    }
    if (completions.count() != 1) {
      ++not_completed_once;
    } else if (completions.first() == couchbase::errc::common::unambiguous_timeout &&
               written != 0) {
      ++unambiguous_and_written;
    }
  }

  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
  // An unambiguous_timeout tells the caller a retry cannot apply the mutation twice.
  assert_eq(unambiguous_and_written,
            std::size_t{ 0 },
            "no request reported as unambiguous_timeout is written");
  // A subscription left behind holds the command until the session stops.
  assert_eq(
    subscribed_after_completion, std::size_t{ 0 }, "no subscription outlives the completion");
}

void
a_command_completed_before_its_dispatch_writes_nothing([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);

  cmd->cancel(couchbase::retry_reason::do_not_retry);
  cmd->send_to(fixture.session());

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "a mutation the server never received");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "nothing is written");
}

void
a_dispatch_after_the_completion_reads_its_state_without_a_race([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);

  // Relaxed, so the completion does not happen-before the dispatch, and ThreadSanitizer checks the
  // completed-command guard in bucket::map_and_send() against complete().
  std::atomic_bool completed{ false };
  std::thread dispatcher([&cmd, &fixture, &completed] {
    while (!completed.load(std::memory_order_relaxed)) {
      std::this_thread::yield();
    }
    fixture.manager()->map_and_send(cmd);
  });
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  completed.store(true, std::memory_order_relaxed);
  dispatcher.join();

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "nothing is written");
}

void
a_timeout_after_the_write_is_ambiguous_and_withdraws_the_subscription([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  const auto marker = fixture.session().next_opaque();

  cmd->send_to(fixture.session());
  assert_eq(fixture.upserts_written(), std::size_t{ 1 }, "the dispatch writes the upsert");
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "a mutation the server may have applied");
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "the timeout withdraws the subscription");
}

// Holds a worker until released, signalling once it has entered.
class encode_gate
{
public:
  void enter_and_wait()
  {
    entered_.set_value();
    released_.wait();
  }

  // Bounded: a caller that never reaches the gate fails its case.
  [[nodiscard]] auto wait_until_entered() -> bool
  {
    return entered_future_.wait_for(scaled_budget(std::chrono::milliseconds{ 1'000 })) ==
           std::future_status::ready;
  }

  void release()
  {
    if (!released_once_.exchange(true)) {
      release_.set_value();
    }
  }

private:
  std::promise<void> entered_{};
  std::shared_future<void> entered_future_{ entered_.get_future().share() };
  std::promise<void> release_{};
  std::shared_future<void> released_{ release_.get_future().share() };
  std::atomic_bool released_once_{ false };
};

void
a_subscription_registered_after_the_claim_is_withdrawn([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto gate = std::make_shared<encode_gate>();
  fixture.session().unit_test_api().on_before_subscribe([gate](std::uint32_t /* opaque */) {
    gate->enter_and_wait();
  });
  auto cmd = fixture.start_upsert(completions);
  const auto marker = fixture.session().next_opaque();
  static_cast<void>(fixture.orphan_count());

  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = gate->wait_until_entered();
  if (entered) {
    // The deadline claims after publish_dispatch() and before command_handlers_ registers opaque.
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  gate->release();
  dispatcher.join();
  fixture.session().unit_test_api().on_before_subscribe(nullptr);
  fixture.drain();

  assert_true(entered, "the dispatch reached registration");
  assert_eq(fixture.upserts_written(), std::size_t{ 1 }, "the committed write still goes out");
  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "a mutation committed before the timeout is ambiguous");
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "no subscription outlives the completion");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 1 },
            "the discarded document subscription is reported as an orphan");
}

void
a_collection_lookup_registered_after_the_claim_is_withdrawn([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto gate = std::make_shared<encode_gate>();
  fixture.session().unit_test_api().on_before_subscribe([gate](std::uint32_t /* opaque */) {
    gate->enter_and_wait();
  });
  auto cmd = fixture.start_upsert(completions, { "default", "inventory", "route", "key" });
  const auto marker = fixture.session().next_opaque();
  static_cast<void>(fixture.orphan_count());

  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = gate->wait_until_entered();
  if (entered) {
    // The deadline claims after request_collection_id() publishes opaque_ and before registration.
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  gate->release();
  dispatcher.join();
  fixture.session().unit_test_api().on_before_subscribe(nullptr);
  fixture.drain();

  assert_true(entered, "the lookup reached registration");
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get_collection_id),
            std::size_t{ 1 },
            "the committed lookup goes out");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "the document request is never written");
  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "the mutation was never written");
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "request_collection_id() withdraws the late lookup subscription");
  assert_eq(
    fixture.orphan_count(), std::size_t{ 0 }, "discarding a collection lookup reports no orphan");
}

void
a_response_racing_the_deadline_completes_the_command_once([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  command_fixture fixture{};
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = fixture.start_upsert(completions);
    const auto marker = fixture.session().next_opaque();
    cmd->send_to(fixture.session());

    race(
      [&cmd] {
        // The response handler's completion, on the session's I/O thread.
        cmd->invoke_handler(couchbase::errc::common::request_canceled);
      },
      [&cmd] {
        cmd->cancel(couchbase::retry_reason::do_not_retry);
      });

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1) {
      ++not_completed_once;
    }
  }

  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
}

void
a_deadline_racing_a_retried_dispatch_reports_the_earlier_write([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  command_fixture fixture{};
  std::size_t first_dispatch_not_written{ 0 };
  std::size_t not_ambiguous{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = fixture.start_upsert(completions);
    const auto marker = fixture.session().next_opaque();
    const auto written_before = fixture.upserts_written();
    cmd->send_to(fixture.session());
    if (fixture.upserts_written() - written_before != 1) {
      ++first_dispatch_not_written;
    }

    race(
      [&cmd, &fixture] {
        // A retry reassigns session_ and opaque_, which the deadline must not read meanwhile.
        cmd->send_to(fixture.session());
      },
      [&cmd] {
        cmd->cancel(couchbase::retry_reason::do_not_retry);
      });

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1 ||
        completions.first() != couchbase::errc::common::ambiguous_timeout) {
      ++not_ambiguous;
    }
  }

  assert_eq(first_dispatch_not_written, std::size_t{ 0 }, "every first dispatch writes the upsert");
  assert_eq(not_ambiguous,
            std::size_t{ 0 },
            "a request written by an earlier attempt times out as ambiguous_timeout");
}

void
a_completed_command_leaves_no_collection_lookup_subscribed([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  // No id is cached for this collection, so the dispatch looks it up first.
  auto cmd = fixture.start_upsert(completions, { "default", "inventory", "route", "key" });
  const auto marker = fixture.session().next_opaque();

  cmd->send_to(fixture.session());
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get_collection_id),
            std::size_t{ 1 },
            "the dispatch writes GetCollectionID");
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "a mutation the server never received");
  // A lookup left subscribed holds the command until it is answered or the session stops.
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "no subscription outlives the completion");
}

void
a_late_withdrawal_reads_nothing_a_retry_is_assigning([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  command_fixture fixture{};
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = fixture.start_upsert(completions);
    const auto marker = fixture.session().next_opaque();
    cmd->send_to(fixture.session());

    race(
      [&cmd, &fixture] {
        // A retry, which reassigns session_ and opaque_.
        cmd->send_to(fixture.session());
      },
      [&cmd] {
        // The deadline, then the first attempt's dispatcher returning from write_and_subscribe().
        cmd->cancel(couchbase::retry_reason::do_not_retry);
        cmd->withdraw_if_completed();
      });

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1) {
      ++not_completed_once;
    }
  }

  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
}

void
a_late_withdrawal_runs_no_response_handler_beside_the_completion([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  auto recorder = std::make_shared<span_end_recorder>();
  command_fixture fixture{ recorder };
  constexpr int withdraw_attempts{ 1'000'000 };
  std::vector<std::shared_ptr<mcbp_command<bucket, upsert_request>>> commands{};
  std::size_t not_completed_once{ 0 };
  std::size_t left_subscribed{ 0 };
  std::size_t span_not_ended_once_by_its_completer{ 0 };

  // I/O threads running throughout, as in an application, so that timer and strand work runs beside
  // the racing threads.
  auto work = asio::make_work_guard(fixture.io());
  std::thread io_one([&fixture] {
    fixture.io().run();
  });
  std::thread io_two([&fixture] {
    fixture.io().run();
  });

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = commands.emplace_back(fixture.start_upsert(completions));
    const auto marker = fixture.session().next_opaque();
    cmd->send_to(fixture.session());
    const auto spans_before = recorder->ended_on().size();
    std::thread::id completer{};

    race(
      [&cmd, &completions] {
        // The dispatching thread returning from write_and_subscribe() while a completion claims,
        // repeated until the round completes.
        for (int attempt = 0; attempt < withdraw_attempts && completions.count() == 0; ++attempt) {
          cmd->withdraw_if_completed();
          std::this_thread::yield();
        }
      },
      [&cmd, &completer] {
        // A completion that leaves the subscription in place, as a claim that looked it up before
        // its registration does.
        completer = std::this_thread::get_id();
        cmd->invoke_handler(couchbase::errc::common::request_canceled);
      });
    // A withdrawal after complete() has finished.
    cmd->withdraw_if_completed();

    if (completions.count() != 1) {
      ++not_completed_once;
    }
    if (fixture.withdraw_subscriptions_since(marker) != 0) {
      ++left_subscribed;
    }
    // complete() ends the round's dispatch span on its own thread, before the caller is answered.
    const auto ended_on = recorder->ended_on();
    if (ended_on.size() != spans_before + 1 || ended_on.back() != completer) {
      ++span_not_ended_once_by_its_completer;
    }
  }

  work.reset();
  io_one.join();
  io_two.join();

  // The response handler's first effect is to record the server duration. A withdrawal that runs
  // it records telemetry beside the completion.
  std::size_t handlers_run{ 0 };
  for (const auto& cmd : commands) {
    handlers_run += cmd->server_durations_.size();
  }
  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
  assert_eq(left_subscribed, std::size_t{ 0 }, "the late withdrawal removes every subscription");
  assert_eq(
    handlers_run, std::size_t{ 0 }, "no response handler runs for a withdrawn subscription");
  assert_eq(span_not_ended_once_by_its_completer,
            std::size_t{ 0 },
            "each round's dispatch span ends once, on that round's completing thread");
}

void
a_first_lookup_answering_collection_not_found_at_the_deadline_is_unambiguous(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  upsert_request request{};
  request.id = { "default", "inventory", "route", "key" };
  // Less than the 500 ms unknown-collection backoff, so the answer ends the command.
  request.timeout = std::chrono::milliseconds{ 100 };
  auto cmd = fixture.make_upsert(std::move(request), completions);
  cmd->send_to(fixture.session());
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get_collection_id),
            std::size_t{ 1 },
            "the dispatch writes GetCollectionID");

  assert_true(fixture.session().cancel(cmd->opaque_.value(),
                                       couchbase::errc::common::collection_not_found,
                                       couchbase::retry_reason::do_not_retry),
              "the lookup receives its answer");

  assert_eq(completions.count(), std::size_t{ 1 }, "the answer ends the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "the upsert was never written");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "nothing but the lookup is written");
}

// An upsert whose encoder blocks on a gate. send() encodes after begin_dispatch() and before
// publish_dispatch(), so the gate holds the dispatch before its write is committed.
struct gated_upsert_request : upsert_request {
  std::shared_ptr<encode_gate> gate{};

  [[nodiscard]] auto encode_to(encoded_request_type& encoded,
                               couchbase::core::mcbp_context&& context) const -> std::error_code
  {
    gate->enter_and_wait();
    return upsert_request::encode_to(encoded, std::move(context));
  }
};

void
a_deadline_before_the_write_is_committed_is_unambiguous_and_nothing_is_written(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto gate = std::make_shared<encode_gate>();
  gated_upsert_request request{};
  request.id = named_collection_key();
  request.gate = gate;
  auto cmd = fixture.make_command(std::move(request), completions);

  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = gate->wait_until_entered();
  if (entered) {
    // The deadline, while the dispatch is encoding and has not committed its write.
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  gate->release();
  dispatcher.join();
  assert_true(entered, "the response reached the gate");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "a mutation never committed to a write");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "the encoded upsert is not written");
}

// A tracer whose dispatch span blocks in start_span() until released, as a slow or blocking
// application tracer would.
class blocking_tracer : public span_end_recorder
{
public:
  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    if (name == couchbase::core::tracing::operation::step_dispatch) {
      entered_.set_value();
      released_.wait();
    }
    return span_end_recorder::start_span(std::move(name), std::move(parent));
  }

  // Bounds the wait when a response never reaches the gate.
  [[nodiscard]] auto wait_until_entered() -> bool
  {
    return entered_future_.wait_for(scaled_budget(std::chrono::milliseconds{ 1'000 })) ==
           std::future_status::ready;
  }

  void release()
  {
    if (!released_once_.exchange(true)) {
      release_.set_value();
    }
  }

private:
  std::promise<void> entered_{};
  std::shared_future<void> entered_future_{ entered_.get_future().share() };
  std::promise<void> release_{};
  std::shared_future<void> released_{ release_.get_future().share() };
  std::atomic_bool released_once_{ false };
};

void
a_deadline_completes_while_the_tracer_blocks_the_dispatch([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.start_upsert(completions);
  const auto marker = fixture.session().next_opaque();
  static_cast<void>(fixture.orphan_reported());

  // The tracer holds the dispatch between publish_dispatch() and the registration of its
  // subscription, so the completion claims in that window.
  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = tracer->wait_until_entered();
  // The deadline, while the dispatch is inside the application's tracer.
  auto deadline = std::async(std::launch::async, [&cmd] {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  });
  const auto finished = deadline.wait_for(scaled_budget(std::chrono::milliseconds{ 1'000 }));
  tracer->release();
  dispatcher.join();
  deadline.wait();

  assert_true(entered, "the dispatch reached the tracer");
  assert_true(finished == std::future_status::ready,
              "the timeout completes while the tracer blocks the dispatch");
  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(tracer->ended_on().size(), std::size_t{ 1 }, "the dispatch span still ends once");
  assert_eq(fixture.upserts_written(), std::size_t{ 1 }, "the committed write still goes out");
  // send() discards the subscription it registered after the claim, without running its handler.
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "no subscription outlives the completion");
  assert_true(cmd->server_durations_.empty(), "the late subscription's handler never ran");
  // The write was committed before the claim, so the server may apply it.
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "a mutation committed before the timeout is ambiguous");
  assert_true(fixture.orphan_reported(), "the discarded subscription is reported as an orphan");
}

void
a_response_after_the_completion_ends_its_dispatch_span_once_and_reports_an_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto recorder = std::make_shared<span_end_recorder>();
  command_fixture fixture{ recorder };
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  assert_eq(fixture.upserts_written(), std::size_t{ 1 }, "the dispatch writes the upsert");
  static_cast<void>(fixture.orphan_reported());

  // A completion that left the subscription registered, as a claim before its registration does.
  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  assert_false(fixture.telemetry_report().empty(), "the completion records telemetry");
  // An I/O thread then delivers the server's answer to that subscription.
  assert_true(fixture.session().unit_test_api().deliver_response(
                couchbase::core::protocol::client_opcode::upsert,
                static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
                cmd->request.opaque),
              "the late subscription receives the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the command completes once");
  // complete() ends the span before the caller can end its parent; the response leaves it alone.
  assert_eq(recorder->ended_on().size(), std::size_t{ 1 }, "the dispatch span ends once");
  assert_true(fixture.orphan_reported(), "the late response is reported as an orphan");
  assert_true(cmd->server_durations_.empty(), "the late response records no server duration");
  assert_true(fixture.telemetry_report().empty(), "the late response records no latency");
}

void
a_lookup_response_after_the_completion_starts_no_retry([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions, { "default", "inventory", "route", "key" });
  cmd->send_to(fixture.session());
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get_collection_id),
            std::size_t{ 1 },
            "the dispatch writes GetCollectionID");

  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  // The lookup's answer reaches the subscription registered after the claim.
  assert_true(fixture.session().cancel(cmd->opaque_.value(),
                                       couchbase::errc::common::collection_not_found,
                                       couchbase::retry_reason::do_not_retry),
              "the late lookup subscription receives the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the command completes once");
  assert_eq(cmd->request.retries.retry_reasons().count(
              couchbase::retry_reason::key_value_collection_outdated),
            std::size_t{ 0 },
            "the late lookup response records no retry on the completed request");
}

void
a_discarded_subscription_has_its_dispatch_span_ended_once([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto recorder = std::make_shared<span_end_recorder>();
  command_fixture fixture{ recorder };
  auto cmd = fixture.start_upsert(completions);
  const auto marker = fixture.session().next_opaque();
  cmd->send_to(fixture.session());

  // A completion that left the subscription registered, then the late withdrawal that discards it.
  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  cmd->withdraw_if_completed();

  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "the subscription was discarded");
  assert_true(cmd->server_durations_.empty(), "the discarded subscription's handler never ran");
  // A span left open shows an external tracer an operation that never finished.
  assert_eq(recorder->ended_on().size(), std::size_t{ 1 }, "the dispatch span ends once");
}

void
a_deadline_that_withdraws_the_write_ends_its_dispatch_span_once([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto recorder = std::make_shared<span_end_recorder>();
  command_fixture fixture{ recorder };
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  // The withdrawn subscription's handler ends the span. accept_retry() fails once completed.
  std::optional<bool> accepted_in_handler{};
  recorder->on_dispatch_end = [&cmd, &accepted_in_handler] {
    accepted_in_handler = cmd->accept_retry();
  };

  // cancel() runs the response handler of the subscription it withdraws, then completes.
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  recorder->on_dispatch_end = nullptr;

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(recorder->ended_on().size(), std::size_t{ 1 }, "the dispatch span ends once");
  assert_true(accepted_in_handler.has_value(), "the withdrawn subscription's handler ran");
  assert_true(!accepted_in_handler.value_or(true),
              "cancel() claims before it withdraws the subscription");
  assert_eq(fixture.orphan_count(), std::size_t{ 1 }, "the withdrawn write is one orphan");
}

void
a_read_committed_before_the_deadline_is_unambiguous([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_tracer>();
  command_fixture fixture{ tracer };
  couchbase::core::operations::get_request request{};
  request.id = named_collection_key();
  auto cmd = fixture.make_command(std::move(request), completions);

  // The tracer holds the dispatch after its write is committed, so the claim finds no subscription
  // and classifies the timeout from the state it observed.
  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = tracer->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer->release();
  dispatcher.join();
  assert_true(entered, "the response reached the gate");

  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get),
            std::size_t{ 1 },
            "the committed read goes out");
  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  // A read applies nothing, so retrying it is safe whether or not the server saw it.
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::unambiguous_timeout },
            "an idempotent request times out unambiguously");
}

void
a_session_stopping_before_a_late_write_reports_no_orphan([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.start_upsert(completions);
  static_cast<void>(fixture.orphan_reported());

  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = tracer->wait_until_entered();
  if (entered) {
    // The deadline, then the session stopping, both while the dispatch is held after its commit.
    cmd->cancel(couchbase::retry_reason::do_not_retry);
    fixture.session().stop(couchbase::retry_reason::do_not_retry);
  }
  tracer->release();
  dispatcher.join();
  assert_true(entered, "the response reached the gate");
  fixture.drain();

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(fixture.upserts_written(), std::size_t{ 0 }, "the stopped session writes nothing");
  // write_and_subscribe() on the stopped session runs the handler with request_canceled.
  assert_false(fixture.orphan_reported(),
               "the session's cancellation that lost the claim to the deadline is no orphan");
}

void
a_late_server_answer_mapped_to_request_canceled_is_reported_as_an_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_reported());

  // A completion that left the subscription registered, then the server's not_my_vbucket answer.
  // The session maps that status to request_canceled, the same code a stopping session delivers.
  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  assert_true(fixture.session().unit_test_api().deliver_response(
                couchbase::core::protocol::client_opcode::upsert,
                static_cast<std::uint16_t>(couchbase::core::key_value_status_code::not_my_vbucket),
                cmd->request.opaque),
              "the late subscription receives the server's answer");

  assert_eq(completions.count(), std::size_t{ 1 }, "the command completes once");
  assert_true(cmd->server_durations_.empty(), "the late answer changes nothing on the command");
  assert_true(fixture.orphan_reported(), "an answer from the server is reported as an orphan");
}

void
a_response_queued_behind_a_sibling_cancellation_changes_nothing([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  couchbase::core::impl::with_cancellation<couchbase::core::operations::get_request> request{};
  request.id = named_collection_key();
  auto token = request.cancel_token;
  auto cmd = fixture.make_command(std::move(request), completions);
  const auto marker = fixture.session().next_opaque();
  cmd->send_to(fixture.session());
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get),
            std::size_t{ 1 },
            "the dispatch writes the get");

  // Both arrive off the command strand and are queued on it in this order: a sibling's success
  // cancels this read, and then this read's own response comes back.
  token->cancel();
  assert_true(fixture.session().unit_test_api().deliver_response(
                couchbase::core::protocol::client_opcode::get,
                static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
                cmd->request.opaque),
              "the response is taken from the session");
  fixture.drain();

  assert_eq(completions.count(), std::size_t{ 1 }, "the command completes once");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::request_canceled },
            "the cancellation is the answer");
  assert_true(cmd->server_durations_.empty(), "the queued response changes nothing");
  assert_eq(fixture.written(couchbase::core::protocol::client_opcode::get),
            std::size_t{ 1 },
            "nothing is written after the completion");
  assert_eq(fixture.withdraw_subscriptions_since(marker),
            std::size_t{ 0 },
            "no subscription outlives the completion");
}

void
a_delivered_response_decodes_with_the_request_opaque([[maybe_unused]] context& ctx)
{
  command_fixture fixture{};
  upsert_request request{};
  request.id = named_collection_key();
  auto cmd = std::make_shared<mcbp_command<bucket, upsert_request>>(
    fixture.io(), fixture.manager(), std::move(request), command_timeout);
  std::optional<std::uint32_t> decoded_opaque{};
  cmd->start([&decoded_opaque](std::error_code /* ec */, std::optional<mcbp_message>&& msg) {
    if (msg) {
      const upsert_request::encoded_response_type response(std::move(*msg));
      decoded_opaque = response.opaque();
    }
  });
  cmd->send_to(fixture.session());

  assert_true(fixture.session().unit_test_api().deliver_response(
                couchbase::core::protocol::client_opcode::upsert,
                static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
                cmd->request.opaque),
              "the active subscription receives the response");

  // The bucket's completion handler builds the error context from the decoded response.
  assert_true(decoded_opaque.has_value(), "the completion receives the response");
  assert_eq(decoded_opaque.value(), cmd->request.opaque, "the response decodes as the request's");
}

// A tracer whose dispatch span blocks in end() until released, as a slow application tracer would.
// The response handler ends the span after its completed check and before it claims completion.
class blocking_end_tracer : public couchbase::tracing::request_tracer
{
public:
  class gated_span : public couchbase::tracing::request_span
  {
  public:
    gated_span(std::string name, std::shared_ptr<encode_gate> gate)
      : request_span(std::move(name))
      , gate_{ std::move(gate) }
    {
    }

    void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
    {
    }

    void add_tag(const std::string& /* name */, const std::string& /* value */) override
    {
    }

    void end() override
    {
      if (name() == couchbase::core::tracing::operation::step_dispatch) {
        gate_->enter_and_wait();
      }
    }

    [[nodiscard]] auto uses_tags() const -> bool override
    {
      return false;
    }

  private:
    std::shared_ptr<encode_gate> gate_;
  };

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> /* parent */)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    return std::make_shared<gated_span>(std::move(name), gate);
  }

  std::shared_ptr<encode_gate> gate{ std::make_shared<encode_gate>() };
};

void
a_response_that_loses_the_claim_to_the_deadline_is_reported_as_an_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_reported());

  // The server's answer passes the handler's completed check and blocks in the span's end().
  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
      cmd->request.opaque);
  });
  const bool entered = tracer->gate->wait_until_entered();
  if (entered) {
    // The deadline claims while the answer is held; the session has already taken its subscription.
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "the deadline's answer");
  assert_true(fixture.orphan_reported(), "the answer that lost the claim is reported as an orphan");
}

[[nodiscard]] auto
upsert_with(std::shared_ptr<couchbase::retry_strategy> strategy,
            std::optional<std::chrono::milliseconds> timeout = {}) -> upsert_request
{
  return { named_collection_key(),
           {},
           0,
           0,
           0,
           0,
           couchbase::durability_level::none,
           timeout,
           couchbase::core::io::retry_context<false>{ std::move(strategy) },
           false,
           nullptr };
}

void
a_server_answer_completed_before_its_retry_decision_is_one_orphan([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  // The answer is held in end() while the deadline claims, so the handler finds the command
  // completed before it consults the strategy: completed_before_retry() reports it and nothing
  // retries. The strategy declines, and is never reached here; the declined race itself is
  // a_server_answer_whose_retry_is_declined_while_the_deadline_claims_is_one_orphan.
  auto cmd = fixture.make_command(
    upsert_with(std::make_shared<couchbase::fail_fast_retry_strategy>()), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque);
  });
  const bool entered = tracer->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 1 },
            "the answer completed before its retry decision is one orphan");
}

// Accepts every retry. A case using it checks that a completion before the retry decision schedules
// nothing even when the strategy would retry.
class accept_every_retry : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    return couchbase::retry_action{ std::chrono::milliseconds{ 10 } };
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "accept_every_retry";
  }
};

void
a_server_answer_completed_before_its_retry_decision_schedules_no_retry(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.make_command(upsert_with(std::make_shared<accept_every_retry>()), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque);
  });
  const bool entered = tracer->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->request.retries.retry_attempts(),
            std::size_t{ 0 },
            "a completed command schedules no retry");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 1 },
            "the answer whose retry would be refused is one orphan");
}

// Runs a response to `cmd` on its own thread, holds it in the dispatch span's end() until the
// deadline has claimed, then lets it finish.
void
deliver_beside_the_deadline(command_fixture& fixture,
                            blocking_end_tracer& tracer,
                            const std::shared_ptr<mcbp_command<bucket, upsert_request>>& cmd,
                            couchbase::core::key_value_status_code status)
{
  bool delivered{ false };
  std::thread io([&cmd, &fixture, status, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(status),
      cmd->request.opaque);
  });
  const bool entered = tracer.gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer.gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");
}

void
an_unknown_collection_answer_that_loses_the_claim_near_the_deadline_is_one_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  // Less than the 500 ms unknown-collection backoff, so the answer tries to end the command.
  auto cmd =
    fixture.make_command(upsert_with(nullptr, std::chrono::milliseconds{ 300 }), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  deliver_beside_the_deadline(
    fixture, *tracer, cmd, couchbase::core::key_value_status_code::unknown_collection);

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_true(cmd->request.retries.retry_reasons().empty(),
              "an answer that lost the claim adds no retry reason");
  assert_eq(
    fixture.orphan_count(), std::size_t{ 1 }, "the answer that lost the claim is one orphan");
}

void
an_unknown_collection_answer_that_would_retry_after_the_claim_is_one_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.make_command(upsert_with(nullptr, std::chrono::minutes{ 1 }), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  deliver_beside_the_deadline(
    fixture, *tracer, cmd, couchbase::core::key_value_status_code::unknown_collection);

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->retry_backoff.cancel(), std::size_t{ 0 }, "a completed command arms no retry");
  assert_true(cmd->request.retries.retry_reasons().empty(),
              "an answer whose retry is refused adds no retry reason");
  assert_eq(
    fixture.orphan_count(), std::size_t{ 1 }, "the answer whose retry is refused is one orphan");
}

void
a_claim_after_the_unknown_collection_backoff_is_accepted_reports_no_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.make_command(upsert_with(nullptr), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  const bool delivered = fixture.session().unit_test_api().deliver_response(
    couchbase::core::protocol::client_opcode::upsert,
    static_cast<std::uint16_t>(couchbase::core::key_value_status_code::unknown_collection),
    cmd->request.opaque);
  assert_true(delivered, "the subscription received the response");
  assert_eq(completions.count(), std::size_t{ 0 }, "the answer schedules a retry");

  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->retry_backoff.cancel(), std::size_t{ 0 }, "the completion disarms the backoff");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 0 },
            "a timeout during the backoff leaves no answer outstanding");
}

void
a_session_cancellation_that_loses_the_claim_reports_no_orphan([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  auto tracer = std::make_shared<blocking_end_tracer>();
  command_fixture fixture{ tracer };
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // The session stops; stop() runs the subscription with request_canceled and an empty message on
  // the stopping thread, where it blocks in end() while the deadline claims.
  std::thread io([&fixture] {
    fixture.session().stop(couchbase::retry_reason::do_not_retry);
  });
  const bool entered = tracer->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  tracer->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "the deadline is the completion");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 0 },
            "a cancellation from the session that lost the claim is not an orphan");
}

// Declines every retry, after holding the first decision until released.
class blocking_declining_strategy : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    if (!held_.exchange(true)) {
      gate->enter_and_wait();
    }
    return couchbase::retry_action::do_not_retry();
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "blocking_declining_strategy";
  }

  std::shared_ptr<encode_gate> gate{ std::make_shared<encode_gate>() };

private:
  std::atomic_bool held_{ false };
};

void
a_server_answer_whose_retry_is_declined_while_the_deadline_claims_is_one_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto strategy = std::make_shared<blocking_declining_strategy>();
  auto cmd = fixture.make_command(upsert_with(strategy), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // The answer passes every completed check and is inside the retry decision when the deadline
  // claims; the declined retry then reaches invoke_handler() without the message.
  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque);
  });
  const bool entered = strategy->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  strategy->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(fixture.orphan_count(), std::size_t{ 1 }, "the declined answer is one orphan");
}

// Accepts every retry, after holding the first decision until released.
class blocking_accepting_strategy : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    if (!held_.exchange(true)) {
      gate->enter_and_wait();
    }
    return couchbase::retry_action{ std::chrono::milliseconds{ 10 } };
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "blocking_accepting_strategy";
  }

  std::shared_ptr<encode_gate> gate{ std::make_shared<encode_gate>() };

private:
  std::atomic_bool held_{ false };
};

void
a_server_answer_whose_retry_is_accepted_while_the_deadline_claims_is_one_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto strategy = std::make_shared<blocking_accepting_strategy>();
  auto cmd = fixture.make_command(upsert_with(strategy), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // The answer passes every completed check and is inside the retry decision when the deadline
  // claims. The strategy then accepts the retry, but accept_retry() refuses to schedule it, because
  // the deadline claimed first, so no timer is armed.
  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque);
  });
  const bool entered = strategy->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  strategy->gate->release();
  io.join();
  assert_true(entered, "the response reached the gate");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->retry_backoff.cancel(), std::size_t{ 0 }, "no retry is armed");
  assert_eq(fixture.orphan_count(), std::size_t{ 1 }, "the accepted answer is one orphan");
}

void
a_server_answer_whose_declined_retry_completes_the_command_is_no_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.make_command(
    upsert_with(std::make_shared<couchbase::fail_fast_retry_strategy>()), completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // No deadline: the declined retry's own claim completes the command, with the server's answer.
  assert_true(
    fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque),
    "the subscription receives the answer");

  assert_eq(completions.count(), std::size_t{ 1 }, "the answer completes the command");
  assert_eq(fixture.orphan_count(), std::size_t{ 0 }, "an answer that is the result is no orphan");
}

void
an_answer_whose_retry_began_and_completed_first_is_no_orphan([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // The answer's handler takes its attempt before the retry decision.
  const auto attempt = cmd->dispatch_attempt();
  // The accepted retry runs on another I/O thread before the handler resumes: it begins dispatch
  // and its own response completes the command.
  cmd->send_to(fixture.session());
  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  // The handler resumes.
  cmd->after_retry_decision(attempt, {});

  assert_eq(completions.count(), std::size_t{ 1 }, "the retry completes the command");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 0 },
            "an answer superseded by a later attempt is no orphan");
}

void
a_late_retry_decision_reads_nothing_a_retry_dispatch_is_assigning([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  command_fixture fixture{};
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto cmd = fixture.start_upsert(completions);
    const auto marker = fixture.session().next_opaque();
    cmd->send_to(fixture.session());
    const auto attempt = cmd->dispatch_attempt();

    std::atomic_bool go{ false };
    // The retry's dispatch, which reassigns session_ and opaque_.
    std::thread retry([&go, &cmd, &fixture] {
      while (!go.load()) {
        std::this_thread::yield();
      }
      cmd->send_to(fixture.session());
    });
    std::thread deadline([&go, &cmd] {
      while (!go.load()) {
        std::this_thread::yield();
      }
      cmd->cancel(couchbase::retry_reason::do_not_retry);
    });
    std::thread handler([&go, &cmd, attempt] {
      while (!go.load()) {
        std::this_thread::yield();
      }
      cmd->after_retry_decision(attempt, {});
    });
    go.store(true);
    retry.join();
    deadline.join();
    handler.join();

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1) {
      ++not_completed_once;
    }
  }

  // Under ThreadSanitizer, an orphan built from the fields the retry reassigns is reported against
  // the retry's dispatch.
  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
}

void
a_claim_after_the_retry_was_accepted_is_a_backoff_timeout_not_an_orphan(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  static_cast<void>(fixture.orphan_count());

  // The answer's handler takes its attempt; the retry is accepted; the deadline then claims during
  // the backoff, before the handler checks.
  const auto attempt = cmd->dispatch_attempt();
  assert_true(cmd->accept_retry(), "the retry is accepted before the claim");
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  cmd->after_retry_decision(attempt, {});

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 0 },
            "a timeout during the backoff leaves no answer outstanding");
}

void
an_always_retry_reason_after_the_claim_schedules_nothing_and_reports_once(
  [[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  static_cast<void>(fixture.orphan_count());

  const auto attempt = cmd->dispatch_attempt();
  cmd->cancel(couchbase::retry_reason::do_not_retry);
  // not_my_vbucket retries without consulting the strategy, so only accept_retry() can refuse it.
  couchbase::core::io::retry_orchestrator::maybe_retry(
    fixture.manager(),
    cmd,
    couchbase::retry_reason::key_value_not_my_vbucket,
    couchbase::errc::common::request_canceled);
  cmd->after_retry_decision(attempt, {});

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->request.retries.retry_attempts(),
            std::size_t{ 0 },
            "a completed command records no retry");
  assert_eq(cmd->retry_backoff.cancel(), std::size_t{ 0 }, "and arms none");
  assert_eq(fixture.orphan_count(), std::size_t{ 1 }, "the answer is reported once");
}

void
a_late_frame_is_reported_with_its_own_server_duration([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto cmd = fixture.start_upsert(completions);
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // A completion that left the subscription registered, then the server's answer, which carries a
  // server duration of its own. No earlier response recorded one.
  cmd->invoke_handler(couchbase::errc::common::request_canceled);
  constexpr std::uint16_t encoded_duration{ 100 };
  assert_true(fixture.session().unit_test_api().deliver_response_with_server_duration(
                couchbase::core::protocol::client_opcode::upsert,
                static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
                cmd->request.opaque,
                encoded_duration),
              "the late subscription receives the answer");

  const auto expected =
    std::to_string(static_cast<std::int64_t>(std::pow(encoded_duration, 1.74) / 2));
  const auto output = fixture.orphan_output();
  assert_contains(output,
                  R"("last_server_duration_us":)" + expected,
                  "the orphan carries the late frame's server duration");
  assert_contains(
    output, R"("total_server_duration_us":)" + expected, "and counts it in the total");
  assert_true(cmd->server_durations_.empty(), "the completed command is not changed");
}

// A manager whose schedule_for_retry() holds the first retry until released. It runs after
// accept_retry() and before the response handler's hand-off, so a claim can land in between. Every
// other call forwards to the fixture's bucket.
class gated_retry_manager
{
public:
  explicit gated_retry_manager(std::shared_ptr<bucket> forward)
    : bucket_{ std::move(forward) }
  {
  }

  [[nodiscard]] auto name() const -> const std::string&
  {
    return bucket_->name();
  }

  [[nodiscard]] auto log_prefix() const -> const std::string&
  {
    return bucket_->log_prefix();
  }

  [[nodiscard]] auto tracer() const
  {
    return bucket_->tracer();
  }

  [[nodiscard]] auto meter() const
  {
    return bucket_->meter();
  }

  [[nodiscard]] auto orphan_reporter() const
  {
    return bucket_->orphan_reporter();
  }

  [[nodiscard]] auto app_telemetry_meter() const
  {
    return bucket_->app_telemetry_meter();
  }

  [[nodiscard]] auto default_retry_strategy() const
  {
    return bucket_->default_retry_strategy();
  }

  void fetch_config()
  {
  }

  template<typename Command>
  void map_and_send(std::shared_ptr<Command> /* cmd */)
  {
  }

  template<typename Command>
  void schedule_for_retry(std::shared_ptr<Command> /* cmd */, std::chrono::milliseconds /* d */)
  {
    if (!held_.exchange(true)) {
      gate->enter_and_wait();
    }
  }

  std::shared_ptr<encode_gate> gate{ std::make_shared<encode_gate>() };

private:
  std::shared_ptr<bucket> bucket_;
  std::atomic_bool held_{ false };
};

void
a_claim_after_a_real_handler_accepted_its_retry_reports_no_orphan([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto manager = std::make_shared<gated_retry_manager>(fixture.manager());
  auto cmd = std::make_shared<mcbp_command<gated_retry_manager, upsert_request>>(
    fixture.io(), manager, upsert_with(std::make_shared<accept_every_retry>()), command_timeout);
  cmd->start([&completions](std::error_code ec, std::optional<mcbp_message>&& /* msg */) {
    completions.add(ec);
  });
  cmd->send_to(fixture.session());
  static_cast<void>(fixture.orphan_count());

  // The handler's retry is accepted, and the deadline claims while it is being scheduled, before
  // the handler's hand-off: a timeout during the backoff, with no answer outstanding.
  bool delivered{ false };
  std::thread io([&cmd, &fixture, &delivered] {
    delivered = fixture.session().unit_test_api().deliver_response(
      couchbase::core::protocol::client_opcode::upsert,
      static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
      cmd->request.opaque);
  });
  const bool entered = manager->gate->wait_until_entered();
  if (entered) {
    cmd->cancel(couchbase::retry_reason::do_not_retry);
  }
  manager->gate->release();
  io.join();
  assert_true(entered, "the retry reached scheduling");
  assert_true(delivered, "the subscription received the response");

  assert_eq(completions.count(), std::size_t{ 1 }, "the timeout completes the command");
  assert_eq(cmd->request.retries.retry_attempts(),
            std::size_t{ 1 },
            "the retry was accepted before the claim");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 0 },
            "a timeout after acceptance leaves no answer outstanding");
}

void
a_deadline_discards_a_subscription_registered_as_the_session_stops([[maybe_unused]] context& ctx)
{
  completion_log completions{};
  command_fixture fixture{};
  auto gate = std::make_shared<encode_gate>();
  fixture.session().unit_test_api().on_before_subscribe([gate](std::uint32_t /* opaque */) {
    gate->enter_and_wait();
  });
  auto cmd = fixture.start_upsert(completions);
  static_cast<void>(fixture.orphan_count());

  std::thread dispatcher([&cmd, &fixture] {
    cmd->send_to(fixture.session());
  });
  const bool entered = gate->wait_until_entered();
  if (entered) {
    // stop() sets stopped_ and drains command_handlers_ after write_and_subscribe() checked
    // stopped_ and before it inserts opaque.
    fixture.session().stop(couchbase::retry_reason::do_not_retry);
  }
  gate->release();
  dispatcher.join();
  fixture.session().unit_test_api().on_before_subscribe(nullptr);

  assert_true(entered, "the dispatch reached registration");
  assert_eq(completions.count(), std::size_t{ 0 }, "the command is still awaiting its deadline");

  // session_->cancel() returns false once stopped_; cancel() discards the late entry directly.
  cmd->cancel(couchbase::retry_reason::do_not_retry);

  assert_eq(completions.count(), std::size_t{ 1 }, "the deadline completes the command");
  assert_eq(completions.first(),
            std::error_code{ couchbase::errc::common::ambiguous_timeout },
            "the write was committed before the deadline");
  assert_false(fixture.session().discard(cmd->opaque_.value()),
               "cancel() discarded the subscription from the stopped session");
  assert_eq(fixture.orphan_count(),
            std::size_t{ 1 },
            "the discarded document subscription is reported as an orphan");
}

void
an_unknown_collection_answer_losing_the_claim_records_no_retry_reason([[maybe_unused]] context& ctx)
{
  struct round_outcome {
    completion_log completions{};
    std::mutex mutex{};
    std::set<couchbase::retry_reason> reasons_at_completion{};

    void record(std::error_code ec, std::set<couchbase::retry_reason> reasons)
    {
      {
        const std::scoped_lock lock(mutex);
        reasons_at_completion = std::move(reasons);
      }
      completions.add(ec);
    }

    [[nodiscard]] auto reasons() -> std::set<couchbase::retry_reason>
    {
      const std::scoped_lock lock(mutex);
      return reasons_at_completion;
    }
  };

  std::deque<round_outcome> rounds{};
  auto recorder = std::make_shared<span_end_recorder>();
  command_fixture fixture{ recorder };
  static_cast<void>(fixture.orphan_count());
  std::size_t not_delivered{ 0 };
  std::size_t not_completed_once{ 0 };
  std::size_t canceled_with_retry_reason{ 0 };
  std::size_t timed_out_without_retry_reason{ 0 };
  std::size_t wrong_orphan_count{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    if ((round % 16) == 0) {
      static_cast<void>(fixture.telemetry_report());
    }
    auto& outcome = rounds.emplace_back();
    auto cmd = std::make_shared<mcbp_command<bucket, upsert_request>>(
      fixture.io(),
      fixture.manager(),
      upsert_with(nullptr, std::chrono::milliseconds{ 300 }),
      command_timeout);
    cmd->start([&outcome, weak_cmd = cmd->weak_from_this()](std::error_code ec,
                                                            std::optional<mcbp_message>&& /* m */) {
      std::set<couchbase::retry_reason> reasons{};
      if (auto locked = weak_cmd.lock()) {
        reasons = locked->request.retries.retry_reasons();
      }
      outcome.record(ec, std::move(reasons));
    });
    cmd->send_to(fixture.session());

    // Release cancel() from the dispatch span's end(), after the response handler's initial
    // completion check, so every round delivers unknown_collection to handle_unknown_collection().
    std::atomic_bool release_cancel{ false };
    recorder->on_dispatch_end = [&release_cancel] {
      release_cancel.store(true, std::memory_order_release);
    };
    bool delivered{ false };
    std::thread io([&cmd, &fixture, &delivered, &release_cancel] {
      delivered = fixture.session().unit_test_api().deliver_response(
        couchbase::core::protocol::client_opcode::upsert,
        static_cast<std::uint16_t>(couchbase::core::key_value_status_code::unknown_collection),
        cmd->request.opaque);
      release_cancel.store(true, std::memory_order_release);
    });
    std::thread canceler([&cmd, &release_cancel] {
      while (!release_cancel.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      cmd->cancel(couchbase::retry_reason::do_not_retry, false);
    });
    io.join();
    canceler.join();
    recorder->on_dispatch_end = nullptr;

    const auto round_orphans = fixture.orphan_count();
    const auto reasons_at_completion = outcome.reasons();
    if (!delivered) {
      ++not_delivered;
    }
    if (outcome.completions.count() != 1) {
      ++not_completed_once;
    } else if (outcome.completions.first() == couchbase::errc::common::request_canceled) {
      if (!reasons_at_completion.empty() || !cmd->request.retries.retry_reasons().empty()) {
        ++canceled_with_retry_reason;
      }
      if (round_orphans != 1) {
        ++wrong_orphan_count;
      }
    } else {
      if (reasons_at_completion.count(couchbase::retry_reason::key_value_collection_outdated) !=
          1) {
        ++timed_out_without_retry_reason;
      }
      if (round_orphans != 0) {
        ++wrong_orphan_count;
      }
    }
  }

  assert_eq(not_delivered,
            std::size_t{ 0 },
            "every round delivers its unknown_collection answer to the handler");
  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
  assert_eq(canceled_with_retry_reason,
            std::size_t{ 0 },
            "an unknown_collection answer that loses the claim adds no retry reason");
  assert_eq(timed_out_without_retry_reason,
            std::size_t{ 0 },
            "an unknown_collection answer that wins the claim records its retry reason");
  assert_eq(wrong_orphan_count,
            std::size_t{ 0 },
            "only an unknown_collection answer that loses the claim reports one orphan");
}

[[nodiscard]] auto
make_threshold_tracer(asio::io_context& io)
  -> std::shared_ptr<couchbase::core::tracing::threshold_logging_tracer>
{
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds{ 0 };
  opts.threshold_sample_size = static_cast<std::size_t>(race_rounds) * 2;
  return std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);
}

[[nodiscard]] auto
start_kv_operation_span(couchbase::tracing::request_tracer& tracer)
  -> std::shared_ptr<couchbase::tracing::request_span>
{
  auto span = tracer.start_span("upsert", nullptr);
  span->add_tag(couchbase::core::tracing::attributes::op::service,
                couchbase::core::tracing::service::key_value);
  return span;
}

[[nodiscard]] auto
start_command_parent_span(couchbase::tracing::request_tracer& tracer)
  -> std::shared_ptr<couchbase::tracing::request_span>
{
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
  return tracer.start_span("wrapper", nullptr);
#else
  return start_kv_operation_span(tracer);
#endif
}

void
ensure_over_zero_threshold()
{
  const auto before = std::chrono::system_clock::now();
  while (
    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - before)
      .count() < 2) {
    std::this_thread::yield();
  }
}

// Wraps threshold_logging_tracer so a test can hold start_span() for step_dispatch while still
// creating real threshold_logging_span instances.
class gated_threshold_tracer : public couchbase::tracing::request_tracer
{
public:
  explicit gated_threshold_tracer(
    std::shared_ptr<couchbase::core::tracing::threshold_logging_tracer> inner)
    : inner_{ std::move(inner) }
  {
  }

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    const bool is_dispatch = (name == couchbase::core::tracing::operation::step_dispatch);
    auto span = inner_->start_span(std::move(name), std::move(parent));
    if (is_dispatch && on_dispatch_created) {
      on_dispatch_created();
    }
    return span;
  }

  std::function<void()> on_dispatch_created{};

private:
  std::shared_ptr<couchbase::core::tracing::threshold_logging_tracer> inner_;
};

void
a_late_threshold_dispatch_span_after_parent_end_leaves_the_report_intact(
  [[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto tracer = make_threshold_tracer(io);
  auto op_span = start_kv_operation_span(*tracer);

  auto first_dispatch =
    tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  assert_true(first_dispatch->try_set_dispatch_operation_id(0x2a), "captures first opaque");
  assert_true(first_dispatch->try_set_dispatch_local_id("conn-1"), "captures first local id");
  assert_true(first_dispatch->try_set_dispatch_result(150, "node-a.example", 11210),
              "captures first result");
  first_dispatch->end();

  auto abandoned_dispatch =
    tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  assert_true(abandoned_dispatch->try_set_dispatch_operation_id(0xdead), "captures late opaque");
  assert_true(abandoned_dispatch->try_set_dispatch_local_id("conn-late"), "captures late local id");
  assert_true(abandoned_dispatch->try_set_dispatch_result(900, "node-late.example", 11210),
              "captures late result");

  ensure_over_zero_threshold();
  op_span->end();
  abandoned_dispatch->end();
  op_span->end();

  const auto report = tracer->flush_and_create_output().value_or(std::string{});
  assert_contains(report, R"("count":1)", "the operation span is reported once");
  assert_contains(report, R"("last_operation_id":"0x2a")", "keeps the first attempt's opaque");
  assert_contains(report, R"("last_local_id":"conn-1")", "keeps the first attempt's local id");
  assert_contains(
    report, R"("last_remote_socket":"node-a.example:11210")", "keeps the first attempt's socket");
  assert_contains(
    report, R"("last_server_duration_us":150)", "keeps the first attempt's server duration");
  assert_contains(
    report, R"("total_server_duration_us":150)", "late dispatch adds no server duration");
}

void
a_threshold_span_created_beside_the_completion_races_no_parent_report([[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  asio::io_context tracer_io{};
  auto threshold = make_threshold_tracer(tracer_io);
  auto gated = std::make_shared<gated_threshold_tracer>(threshold);
  command_fixture fixture{ gated };
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < 50; ++round) {
    auto& completions = rounds.emplace_back();
    auto enter_gate = std::make_shared<encode_gate>();
    auto ready = std::make_shared<std::atomic<int>>(0);
    gated->on_dispatch_created = [enter_gate, ready] {
      enter_gate->enter_and_wait();
      ready->fetch_add(1, std::memory_order_acq_rel);
      while (ready->load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
      }
    };

    auto op_span = start_command_parent_span(*threshold);
    upsert_request request{};
    request.id = named_collection_key();
    request.parent_span = op_span;
    const auto marker = fixture.session().next_opaque();
    auto cmd = std::make_shared<mcbp_command<bucket, upsert_request>>(
      fixture.io(), fixture.manager(), std::move(request), command_timeout);
    cmd->start([&completions, op_span, enter_gate, ready](std::error_code ec,
                                                          std::optional<mcbp_message>&& /* m */) {
      enter_gate->release();
      ready->fetch_add(1, std::memory_order_acq_rel);
      while (ready->load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
      }
      op_span->end();
      completions.add(ec);
    });
    ensure_over_zero_threshold();

    std::thread dispatcher([&cmd, &fixture] {
      cmd->send_to(fixture.session());
    });
    const bool entered = enter_gate->wait_until_entered();
    if (!entered) {
      ready->store(2, std::memory_order_release);
      enter_gate->release();
    }
    cmd->cancel(couchbase::retry_reason::do_not_retry);
    dispatcher.join();
    gated->on_dispatch_created = nullptr;
    assert_true(entered, "dispatch created its threshold span");

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1) {
      ++not_completed_once;
    }
  }
  static_cast<void>(fixture.orphan_count());

  const auto report = threshold->flush_and_create_output().value_or(std::string{});
  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
  assert_contains(report, R"("count":50)", "every operation span is reported once");
}

void
a_threshold_dispatch_span_ending_beside_the_completion_races_no_parent_report(
  [[maybe_unused]] context& ctx)
{
  std::deque<completion_log> rounds{};
  asio::io_context tracer_io{};
  auto threshold = make_threshold_tracer(tracer_io);
  command_fixture fixture{ threshold };
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& completions = rounds.emplace_back();
    auto op_span = start_command_parent_span(*threshold);
    auto request = upsert_with(std::make_shared<couchbase::fail_fast_retry_strategy>());
    request.parent_span = op_span;
    const auto marker = fixture.session().next_opaque();
    auto cmd = std::make_shared<mcbp_command<bucket, upsert_request>>(
      fixture.io(), fixture.manager(), std::move(request), command_timeout);
    cmd->start([&completions, op_span](std::error_code ec, std::optional<mcbp_message>&& /* m */) {
      op_span->end();
      completions.add(ec);
    });
    ensure_over_zero_threshold();
    cmd->send_to(fixture.session());

    race(
      [&cmd, &fixture] {
        static_cast<void>(fixture.session().unit_test_api().deliver_response_with_server_duration(
          couchbase::core::protocol::client_opcode::upsert,
          static_cast<std::uint16_t>(couchbase::core::key_value_status_code::temporary_failure),
          cmd->request.opaque,
          100));
      },
      [&cmd, round] {
        if ((round & 1) != 0) {
          std::this_thread::yield();
        }
        cmd->cancel(couchbase::retry_reason::do_not_retry);
      });

    static_cast<void>(fixture.withdraw_subscriptions_since(marker));
    if (completions.count() != 1) {
      ++not_completed_once;
    }
  }
  static_cast<void>(fixture.orphan_count());

  const auto report = threshold->flush_and_create_output().value_or(std::string{});
  assert_eq(not_completed_once, std::size_t{ 0 }, "every round completes its command once");
  assert_contains(
    report, R"("count":)" + std::to_string(race_rounds), "every operation span is reported once");
}

void
concurrent_replica_dispatch_spans_roll_up_to_one_threshold_parent_without_a_race(
  [[maybe_unused]] context& ctx)
{
  std::deque<completion_log> first_logs{};
  std::deque<completion_log> second_logs{};
  asio::io_context tracer_io{};
  auto threshold = make_threshold_tracer(tracer_io);
  command_fixture fixture{ threshold };
  command_fixture replica_fixture{ threshold };

  auto work_one = asio::make_work_guard(fixture.io());
  auto work_two = asio::make_work_guard(replica_fixture.io());
  std::thread io_one([&fixture] {
    fixture.io().run();
  });
  std::thread io_two([&replica_fixture] {
    replica_fixture.io().run();
  });
  std::size_t not_completed_once{ 0 };

  for (int round = 0; round < race_rounds; ++round) {
    auto& first_completions = first_logs.emplace_back();
    auto& second_completions = second_logs.emplace_back();
    auto op_span = start_command_parent_span(*threshold);

    couchbase::core::impl::with_cancellation<couchbase::core::operations::get_request> req_one{};
    req_one.id = named_collection_key();
    req_one.parent_span = op_span;
    auto token_one = req_one.cancel_token;

    couchbase::core::impl::with_cancellation<couchbase::core::operations::get_request> req_two{};
    req_two.id = named_collection_key();
    req_two.parent_span = op_span;
    auto token_two = req_two.cancel_token;

    const auto marker_one = fixture.session().next_opaque();
    const auto marker_two = replica_fixture.session().next_opaque();
    auto ended = std::make_shared<std::atomic_bool>(false);
    auto cmd_one = std::make_shared<mcbp_command<bucket, decltype(req_one)>>(
      fixture.io(), fixture.manager(), std::move(req_one), command_timeout);
    auto cmd_two = std::make_shared<mcbp_command<bucket, decltype(req_two)>>(
      replica_fixture.io(), replica_fixture.manager(), std::move(req_two), command_timeout);

    cmd_one->start([&first_completions, token_two, op_span, ended](
                     std::error_code ec, std::optional<mcbp_message>&& /* m */) {
      if (!ec && !ended->exchange(true)) {
        token_two->cancel();
        op_span->end();
      }
      first_completions.add(ec);
    });
    cmd_two->start([&second_completions, token_one, op_span, ended](
                     std::error_code ec, std::optional<mcbp_message>&& /* m */) {
      if (!ec && !ended->exchange(true)) {
        token_one->cancel();
        op_span->end();
      }
      second_completions.add(ec);
    });
    ensure_over_zero_threshold();

    cmd_one->send_to(fixture.session());
    cmd_two->send_to(replica_fixture.session());

    race(
      [&cmd_one, &fixture] {
        static_cast<void>(fixture.session().unit_test_api().deliver_response_with_server_duration(
          couchbase::core::protocol::client_opcode::get,
          static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
          cmd_one->request.opaque,
          90));
      },
      [&cmd_two, &replica_fixture] {
        static_cast<void>(
          replica_fixture.session().unit_test_api().deliver_response_with_server_duration(
            couchbase::core::protocol::client_opcode::get,
            static_cast<std::uint16_t>(couchbase::core::key_value_status_code::success),
            cmd_two->request.opaque,
            110));
      });

    const auto wait_deadline =
      std::chrono::steady_clock::now() + scaled_budget(std::chrono::milliseconds{ 1'000 });
    while ((first_completions.count() == 0 || second_completions.count() == 0) &&
           std::chrono::steady_clock::now() < wait_deadline) {
      std::this_thread::yield();
    }
    static_cast<void>(fixture.withdraw_subscriptions_since(marker_one));
    static_cast<void>(replica_fixture.withdraw_subscriptions_since(marker_two));
    if (first_completions.count() != 1 || second_completions.count() != 1) {
      ++not_completed_once;
      break;
    }
  }

  work_one.reset();
  work_two.reset();
  if (not_completed_once != 0) {
    fixture.io().stop();
    replica_fixture.io().stop();
  }
  io_one.join();
  io_two.join();
  static_cast<void>(fixture.orphan_count());
  static_cast<void>(replica_fixture.orphan_count());

  assert_eq(
    not_completed_once, std::size_t{ 0 }, "every round completes both replica commands once");
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
  const auto expected_reports = race_rounds * 2;
#else
  const auto expected_reports = race_rounds;
#endif
  const auto report = threshold->flush_and_create_output().value_or(std::string{});
  assert_contains(
    report, R"("count":)" + std::to_string(expected_reports), "every operation span reports once");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(
          a_deadline_racing_the_first_dispatch_reports_unambiguous_only_for_an_unwritten_request),
        {},
        timeout::slow },
      { CASE(a_command_completed_before_its_dispatch_writes_nothing), {}, timeout::fast },
      { CASE(a_dispatch_after_the_completion_reads_its_state_without_a_race), {}, timeout::fast },
      { CASE(a_timeout_after_the_write_is_ambiguous_and_withdraws_the_subscription),
        {},
        timeout::fast },
      { CASE(a_subscription_registered_after_the_claim_is_withdrawn), {}, timeout::fast },
      { CASE(a_collection_lookup_registered_after_the_claim_is_withdrawn), {}, timeout::fast },
      { CASE(a_response_racing_the_deadline_completes_the_command_once), {}, timeout::slow },
      { CASE(a_deadline_racing_a_retried_dispatch_reports_the_earlier_write), {}, timeout::slow },
      { CASE(a_completed_command_leaves_no_collection_lookup_subscribed), {}, timeout::fast },
      { CASE(a_late_withdrawal_reads_nothing_a_retry_is_assigning), {}, timeout::slow },
      { CASE(a_response_after_the_completion_ends_its_dispatch_span_once_and_reports_an_orphan),
        {},
        timeout::fast },
      { CASE(a_lookup_response_after_the_completion_starts_no_retry), {}, timeout::fast },
      { CASE(a_discarded_subscription_has_its_dispatch_span_ended_once), {}, timeout::fast },
      { CASE(a_first_lookup_answering_collection_not_found_at_the_deadline_is_unambiguous),
        {},
        timeout::fast },
      { CASE(a_deadline_completes_while_the_tracer_blocks_the_dispatch), {}, timeout::fast },
      { CASE(a_read_committed_before_the_deadline_is_unambiguous), {}, timeout::fast },
      { CASE(a_session_stopping_before_a_late_write_reports_no_orphan), {}, timeout::fast },
      { CASE(a_late_server_answer_mapped_to_request_canceled_is_reported_as_an_orphan),
        {},
        timeout::fast },
      { CASE(a_response_queued_behind_a_sibling_cancellation_changes_nothing), {}, timeout::fast },
      { CASE(a_delivered_response_decodes_with_the_request_opaque), {}, timeout::fast },
      { CASE(a_response_that_loses_the_claim_to_the_deadline_is_reported_as_an_orphan),
        {},
        timeout::fast },
      { CASE(a_server_answer_completed_before_its_retry_decision_is_one_orphan),
        {},
        timeout::fast },
      { CASE(a_server_answer_completed_before_its_retry_decision_schedules_no_retry),
        {},
        timeout::fast },
      { CASE(an_unknown_collection_answer_that_loses_the_claim_near_the_deadline_is_one_orphan),
        {},
        timeout::fast },
      { CASE(a_claim_after_the_unknown_collection_backoff_is_accepted_reports_no_orphan),
        {},
        timeout::fast },
      { CASE(an_unknown_collection_answer_that_would_retry_after_the_claim_is_one_orphan),
        {},
        timeout::fast },
      { CASE(a_session_cancellation_that_loses_the_claim_reports_no_orphan), {}, timeout::fast },
      { CASE(a_server_answer_whose_retry_is_declined_while_the_deadline_claims_is_one_orphan),
        {},
        timeout::fast },
      { CASE(a_server_answer_whose_retry_is_accepted_while_the_deadline_claims_is_one_orphan),
        {},
        timeout::fast },
      { CASE(a_server_answer_whose_declined_retry_completes_the_command_is_no_orphan),
        {},
        timeout::fast },
      { CASE(an_answer_whose_retry_began_and_completed_first_is_no_orphan), {}, timeout::fast },
      { CASE(a_late_frame_is_reported_with_its_own_server_duration), {}, timeout::fast },
      { CASE(a_claim_after_a_real_handler_accepted_its_retry_reports_no_orphan),
        {},
        timeout::fast },
      { CASE(a_claim_after_the_retry_was_accepted_is_a_backoff_timeout_not_an_orphan),
        {},
        timeout::fast },
      { CASE(an_always_retry_reason_after_the_claim_schedules_nothing_and_reports_once),
        {},
        timeout::fast },
      { CASE(a_late_retry_decision_reads_nothing_a_retry_dispatch_is_assigning),
        {},
        timeout::slow },
      { CASE(a_deadline_before_the_write_is_committed_is_unambiguous_and_nothing_is_written),
        {},
        timeout::fast },
      { CASE(a_deadline_that_withdraws_the_write_ends_its_dispatch_span_once), {}, timeout::fast },
      { CASE(a_late_withdrawal_runs_no_response_handler_beside_the_completion), {}, timeout::slow },
      { CASE(a_deadline_discards_a_subscription_registered_as_the_session_stops),
        {},
        timeout::fast },
      { CASE(an_unknown_collection_answer_losing_the_claim_records_no_retry_reason),
        {},
        timeout::slow },
      { CASE(a_late_threshold_dispatch_span_after_parent_end_leaves_the_report_intact),
        {},
        timeout::fast },
      { CASE(a_threshold_span_created_beside_the_completion_races_no_parent_report),
        {},
        timeout::slow },
      { CASE(a_threshold_dispatch_span_ending_beside_the_completion_races_no_parent_report),
        {},
        timeout::slow },
      { CASE(concurrent_replica_dispatch_spans_roll_up_to_one_threshold_parent_without_a_race),
        {},
        timeout::slow },
    },
  };
}

} // namespace couchbase::test
