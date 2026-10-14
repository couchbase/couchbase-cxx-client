/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Transaction retries continue in completion handlers on the IO thread. A wait that sleeps that
// thread stalls every other operation on it, so each case runs on one IO thread and requires work
// posted to it to be dispatched promptly while the transaction waits.

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/integration_shortcuts.hxx"
#include "utils/integration_test_guard.hxx"
#include "utils/test_context.hxx"
#include "utils/test_data.hxx"

#include "core/operations.hxx"
#include "core/transactions.hxx"
#include "core/transactions/attempt_context_testing_hooks.hxx"
#include "core/transactions/cleanup_testing_hooks.hxx"

#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json/value.hpp>

#include <asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
using namespace couchbase::core::transactions;
using namespace std::chrono_literals;

// Longest acceptable wait for work posted to the IO thread. Each case includes a wait of at least
// 45 ms, so a wait that sleeps the IO thread exceeds this budget.
constexpr auto dispatch_budget = 40ms;

// Posts to io every 5 ms from its own thread and records the longest wait before dispatch, measured
// in the posted handler.
class dispatch_probe
{
public:
  explicit dispatch_probe(asio::io_context& io)
    : io_{ io }
    , thread_{ [this] {
      run();
    } }
  {
  }

  dispatch_probe(const dispatch_probe&) = delete;
  dispatch_probe(dispatch_probe&&) = delete;
  auto operator=(const dispatch_probe&) -> dispatch_probe& = delete;
  auto operator=(dispatch_probe&&) -> dispatch_probe& = delete;

  ~dispatch_probe()
  {
    stop();
  }

  auto stop() -> std::chrono::steady_clock::duration
  {
    stopped_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
    return slowest_;
  }

private:
  void run()
  {
    while (!stopped_) {
      std::promise<std::chrono::steady_clock::duration> dispatched;
      asio::post(io_, [&dispatched, posted_at = std::chrono::steady_clock::now()] {
        dispatched.set_value(std::chrono::steady_clock::now() - posted_at);
      });
      slowest_ = std::max(slowest_, dispatched.get_future().get());
      std::this_thread::sleep_for(5ms);
    }
  }

  asio::io_context& io_;
  std::atomic_bool stopped_{ false };
  std::chrono::steady_clock::duration slowest_{};
  std::thread thread_;
};

auto
single_io_thread_context() -> ::test::utils::test_context
{
  auto test_ctx = ::test::utils::test_context::load_from_environment();
  test_ctx.number_of_io_threads = 1;
  return test_ctx;
}

struct environment {
  ::test::utils::integration_test_guard integration{ single_io_thread_context() };
  couchbase::core::document_id id{ integration.ctx.bucket,
                                   "_default",
                                   "_default",
                                   ::test::utils::uniq_id("txn") };
  couchbase::codec::encoded_value content{ couchbase::codec::default_json_transcoder::encode(
    tao::json::value{ { "some", "thing" } }) };

  environment()
  {
    ::test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }
};

auto
create_transactions(environment& env, const std::shared_ptr<attempt_context_testing_hooks>& hooks)
  -> std::shared_ptr<couchbase::core::transactions::transactions>
{
  couchbase::transactions::transactions_config cfg{};
  cfg.test_factories(hooks, std::make_shared<cleanup_testing_hooks>());
  auto created =
    couchbase::core::transactions::transactions::create(env.integration.cluster, cfg).get();
  assert_success(created.first, "transactions are created");
  return created.second;
}

// Fails the before_staged_insert hook with ec for the first `failures` calls.
auto
fail_staged_insert(std::atomic_int& calls, int failures, error_class ec) -> error_func2
{
  return [&calls, failures, ec](
           std::shared_ptr<attempt_context>,
           const std::string&,
           couchbase::core::utils::movable_function<void(std::optional<error_class>)>&& handler) {
    if (calls++ < failures) {
      return handler(ec);
    }
    handler(std::nullopt);
  };
}

void
assert_document_committed(environment& env)
{
  auto resp = ::test::utils::execute(env.integration.cluster,
                                     couchbase::core::operations::get_request{ env.id });
  assert_success(resp.ctx.ec(), "the inserted document is committed");
}

// A write that finds another transaction's staged write polls that transaction's ATR entry until
// it completes, and the blocking transaction has to commit while the poll waits.
void
a_blocked_insert_keeps_the_io_thread_responsive([[maybe_unused]] context& ctx)
{
  environment env;

  // The first poll runs without a delay, so the third poll follows at least two backoffs.
  constexpr int polls_after_backoff = 3;
  std::promise<void> staged;
  std::promise<void> backed_off;
  std::promise<void> release;
  const auto released = release.get_future().share();
  std::atomic_bool staged_set{ false };
  std::atomic_int polls{ 0 };

  auto hooks = std::make_shared<attempt_context_testing_hooks>();
  hooks->before_check_atr_entry_for_blocking_doc =
    [&](std::shared_ptr<attempt_context>,
        const std::string&,
        couchbase::core::utils::movable_function<void(std::optional<error_class>)>&& handler) {
      if (++polls == polls_after_backoff) {
        backed_off.set_value();
      }
      handler(std::nullopt);
    };
  auto txns = create_transactions(env, hooks);

  std::exception_ptr blocking_error;
  std::thread blocking([&] {
    try {
      txns->run([&](const std::shared_ptr<attempt_context>& attempt) {
        attempt->insert(env.id, env.content);
        if (!staged_set.exchange(true)) {
          staged.set_value();
        }
        released.wait();
      });
    } catch (...) {
      blocking_error = std::current_exception();
    }
  });

  std::optional<external_exception> blocked_cause;
  std::exception_ptr blocked_error;
  std::thread blocked;
  std::chrono::steady_clock::duration slowest_dispatch{};
  if (staged.get_future().wait_for(10s) == std::future_status::ready) {
    dispatch_probe probe(env.integration.io);
    blocked = std::thread([&] {
      try {
        txns->run([&](const std::shared_ptr<attempt_context>& attempt) {
          attempt->insert(env.id, env.content);
        });
      } catch (const transaction_exception& e) {
        blocked_cause = e.cause();
      } catch (...) {
        blocked_error = std::current_exception();
      }
    });
    backed_off.get_future().wait_for(10s);
    slowest_dispatch = probe.stop();
  }
  release.set_value();
  blocking.join();
  if (blocked.joinable()) {
    blocked.join();
  }

  assert_true(blocking_error == nullptr, "the blocking transaction commits");
  assert_true(blocked_error == nullptr, "the blocked transaction raises transaction_exception");
  assert_true(polls.load() >= polls_after_backoff,
              "the blocked insert backs off between polls of the blocking transaction's ATR entry");
  assert_true(slowest_dispatch < dispatch_budget,
              "the IO thread dispatches work while the blocked insert polls");
  assert_true(blocked_cause == DOCUMENT_EXISTS_EXCEPTION,
              "the blocked insert fails, as the document now exists");
}

// An ambiguous staged insert is retried after a backoff.
void
an_ambiguous_staged_insert_retries_without_holding_the_io_thread([[maybe_unused]] context& ctx)
{
  environment env;

  constexpr int failures = 6;
  std::atomic_int calls{ 0 };
  auto hooks = std::make_shared<attempt_context_testing_hooks>();
  hooks->before_staged_insert = fail_staged_insert(calls, failures, FAIL_AMBIGUOUS);
  auto txns = create_transactions(env, hooks);

  dispatch_probe probe(env.integration.io);
  txns->run([&](const std::shared_ptr<attempt_context>& attempt) {
    attempt->insert(env.id, env.content);
  });
  const auto slowest_dispatch = probe.stop();

  assert_eq(calls.load(), failures + 1, "the staged insert is retried until it succeeds");
  assert_true(slowest_dispatch < dispatch_budget,
              "the IO thread dispatches work while the staged insert backs off");
  assert_document_committed(env);
}

// A transaction whose attempt fails with a retryable error starts a new attempt after a backoff.
void
a_retried_attempt_waits_without_holding_the_io_thread([[maybe_unused]] context& ctx)
{
  environment env;

  constexpr int failures = 8;
  std::atomic_int calls{ 0 };
  auto hooks = std::make_shared<attempt_context_testing_hooks>();
  hooks->before_staged_insert = fail_staged_insert(calls, failures, FAIL_TRANSIENT);
  auto txns = create_transactions(env, hooks);

  dispatch_probe probe(env.integration.io);
  txns->run([&](const std::shared_ptr<attempt_context>& attempt) {
    attempt->insert(env.id, env.content);
  });
  const auto slowest_dispatch = probe.stop();

  assert_eq(calls.load(), failures + 1, "each attempt stages the insert once");
  assert_true(slowest_dispatch < dispatch_budget,
              "the IO thread dispatches work while the transaction waits between attempts");
  assert_document_committed(env);
}
} // namespace

auto
tests() -> test_suite
{
  const std::vector<requirement_ptr> cluster{ needs::real_cluster() };
  return {
    suite_name,
    {
      { CASE(a_blocked_insert_keeps_the_io_thread_responsive), cluster, timeout::integration },
      { CASE(an_ambiguous_staged_insert_retries_without_holding_the_io_thread),
        cluster,
        timeout::integration },
      { CASE(a_retried_attempt_waits_without_holding_the_io_thread),
        cluster,
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
