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

// Tests for the gRPC server-streaming primitive (CXXCBC-895). Drives server-streaming RPCs against
// an in-process QueryService whose behaviour each case configures: how many rows, how slowly, what
// terminal status, and whether to park until cancelled. Env-agnostic (in-process server).
//
// The cases fall into four groups: delivery (rows arrive in order, on the io thread), termination
// (server error, empty stream, deadline, cancellation), lifetime (teardown with work still queued,
// an invoker that throws), and concurrency (many streams sharing one dispatcher, which is what
// makes the call tracker's mutex worth having and gives the TSan leg something to observe).

#include "framework/test_runner.hxx"

#include "callback_queue_keepalive.hxx"

#include "core/protostellar/dispatcher.hxx"
#include "core/protostellar/query_proto.hxx"

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/server_builder.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::cng::test
{
namespace
{
namespace v1 = ::couchbase::query::v1;
using ::couchbase::core::protostellar::dispatcher;
using namespace std::chrono_literals;

// A handler that parks must still come back if the client never cancels, or a bug here becomes a
// hung CI job rather than a failed assertion. Well above any deadline a case sets, well below the
// runner's own budget.
constexpr auto park_giveup = 10s;

// What the in-process service should do for the next call. Constructed per case, so nothing is
// shared between them.
struct service_plan {
  int rows{ 0 };                        // one row per message, numbered from 1
  std::chrono::milliseconds delay{ 0 }; // pause before each message
  bool send_metadata{ false };          // trailing metadata-only message, as N1QL sends
  bool park_until_cancelled{ false };   // hold the stream open so the client's deadline/cancel wins
  grpc::Status terminal{ grpc::Status::OK };
  // Signalled after the first message is written, so a case can wait for the stream to be
  // parked between reads rather than guessing with a sleep.
  std::function<void()> on_first_write{};
};

// This tree is C++17, so designated initialisers are out. Named factories rather than positional
// aggregate init, which is the shape that let fields be set unnoticed elsewhere in this codebase.
[[nodiscard]] auto
streams_rows(int rows) -> service_plan
{
  service_plan plan;
  plan.rows = rows;
  return plan;
}

// Rows followed by the trailing metadata-only message, which is the shape N1QL actually streams.
[[nodiscard]] auto
metadata_plan() -> service_plan
{
  auto plan = streams_rows(3);
  plan.send_metadata = true;
  return plan;
}

// Holds the stream open so the client's deadline or cancellation is what ends the call.
[[nodiscard]] auto
parks() -> service_plan
{
  service_plan plan;
  plan.park_until_cancelled = true;
  return plan;
}

[[nodiscard]] auto
fails_with(grpc::Status status, int rows = 0) -> service_plan
{
  service_plan plan;
  plan.rows = rows;
  plan.terminal = std::move(status);
  return plan;
}

class test_query_service final : public v1::QueryService::Service
{
public:
  explicit test_query_service(service_plan plan)
    : plan_{ std::move(plan) }
  {
  }

  auto Query(grpc::ServerContext* context,
             const v1::QueryRequest* /* request */,
             grpc::ServerWriter<v1::QueryResponse>* writer) -> grpc::Status override
  {
    for (int i = 1; i <= plan_.rows; ++i) {
      if (plan_.delay.count() > 0) {
        std::this_thread::sleep_for(plan_.delay);
      }
      if (context->IsCancelled()) {
        return { grpc::StatusCode::CANCELLED, "cancelled by client" };
      }
      v1::QueryResponse batch;
      batch.add_rows("{\"row\":" + std::to_string(i) + "}");
      if (!writer->Write(batch)) {
        return { grpc::StatusCode::CANCELLED, "client went away" };
      }
      if (i == 1 && plan_.on_first_write) {
        plan_.on_first_write();
      }
    }
    if (plan_.send_metadata) {
      v1::QueryResponse tail;
      auto* meta = tail.mutable_meta_data();
      meta->set_request_id("req-1");
      meta->set_status(v1::QueryResponse_MetaData_Status_STATUS_SUCCESS);
      writer->Write(tail);
    }
    if (plan_.park_until_cancelled) {
      const auto giveup = std::chrono::steady_clock::now() + park_giveup;
      while (!context->IsCancelled() && std::chrono::steady_clock::now() < giveup) {
        std::this_thread::sleep_for(5ms);
      }
      return { grpc::StatusCode::CANCELLED, "cancelled by client" };
    }
    return plan_.terminal;
  }

private:
  service_plan plan_;
};

class in_process_query_server
{
public:
  explicit in_process_query_server(service_plan plan = {})
    : service_{ std::move(plan) }
  {
    // Pin gRPC's process-global callback completion queue for the lifetime of this binary.
    // Without it, destroying the last channel between cases drives a teardown that races gRPC's
    // own polling threads and aborts the process (CXXCBC-919). This binary cycles a channel per
    // case, so it is exactly the shape that trips it.
    pin_callback_queue();

    grpc::ServerBuilder builder;
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
  }
  in_process_query_server(const in_process_query_server&) = delete;
  in_process_query_server(in_process_query_server&&) = delete;
  auto operator=(const in_process_query_server&) -> in_process_query_server& = delete;
  auto operator=(in_process_query_server&&) -> in_process_query_server& = delete;
  ~in_process_query_server()
  {
    // Shutdown is asynchronous, so Wait() before the members go: a parked handler is still inside
    // Query() holding a reference to service_ until the cancellation reaches it, and destroying the
    // service under it is a use-after-free rather than a slow teardown. The immediate deadline is
    // what wakes those handlers -- it cancels in-flight calls instead of waiting them out.
    server_->Shutdown(std::chrono::system_clock::now());
    server_->Wait();
  }
  [[nodiscard]] auto channel() -> std::shared_ptr<grpc::Channel>
  {
    return server_->InProcessChannel(grpc::ChannelArguments{});
  }

private:
  test_query_service service_;
  std::unique_ptr<grpc::Server> server_;
};

// Everything a case needs to keep alive for the duration of a stream. The primitive documents that
// the caller owns the request, and the invoker below hands gRPC a raw pointer into it, so a fixture
// must outlive every call launched from it -- declare it in the case's own scope, never inside a
// launcher thread or a loop body that ends while a call is still in flight.
struct stream_fixture {
  std::unique_ptr<v1::QueryService::Stub> stub;
  std::shared_ptr<v1::QueryRequest> request{ std::make_shared<v1::QueryRequest>() };

  explicit stream_fixture(const std::shared_ptr<grpc::Channel>& channel)
    : stub{ v1::QueryService::NewStub(channel) }
  {
    request->set_statement("SELECT 1");
  }

  // The invoker every case passes to server_stream(): binds the reactor to a Query call.
  [[nodiscard]] auto invoker() const
  {
    return [stub = stub.get(), request = request](
             grpc::ClientContext& ctx, grpc::ClientReadReactor<v1::QueryResponse>* reactor) {
      stub->async()->Query(&ctx, request.get(), reactor);
    };
  }
};

// ── Delivery ──────────────────────────────────────────────────────────────────

void
server_stream_delivers_all_rows_on_the_io_thread()
{
  in_process_query_server server{ metadata_plan() };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  std::vector<std::string> rows;
  auto saw_metadata = false;
  auto rows_on_io_thread = true;
  grpc::Status final_status;
  const auto run_thread = std::this_thread::get_id();

  disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [&](v1::QueryResponse message) {
      if (std::this_thread::get_id() != run_thread) {
        rows_on_io_thread = false;
      }
      for (const auto& row : message.rows()) {
        rows.push_back(row);
      }
      if (message.has_meta_data()) {
        saw_metadata = true;
      }
    },
    [&](grpc::Status status) {
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.ok(), "stream completes OK");
  assert_eq(rows.size(), std::size_t{ 3 }, "all rows delivered");
  assert_eq(rows.at(0), std::string{ "{\"row\":1}" }, "row order preserved");
  assert_eq(rows.at(2), std::string{ "{\"row\":3}" }, "last row delivered");
  assert_true(saw_metadata, "terminal metadata message delivered");
  assert_true(rows_on_io_thread, "every message delivered on the io_context thread");
}

// A short stream can be delivered by a single read, which would hide an ordering bug behind a
// sample size of three. Enough messages here that the reactor has to re-issue StartRead many times,
// and every row is checked rather than the first and last.
void
a_long_stream_preserves_order()
{
  constexpr auto expected_rows = 500;
  in_process_query_server server{ streams_rows(expected_rows) };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  std::vector<std::string> rows;
  grpc::Status final_status;

  disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [&](v1::QueryResponse message) {
      for (const auto& row : message.rows()) {
        rows.push_back(row);
      }
    },
    [&](grpc::Status status) {
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.ok(), "stream completes OK");
  assert_eq(rows.size(), std::size_t{ expected_rows }, "every row delivered");
  auto in_order = true;
  for (int i = 1; i <= expected_rows; ++i) {
    if (rows.at(static_cast<std::size_t>(i - 1)) != "{\"row\":" + std::to_string(i) + "}") {
      in_order = false;
      break;
    }
  }
  assert_true(in_order, "rows arrive in the order the server wrote them");
}

// ── Termination ───────────────────────────────────────────────────────────────

// A stream that yields nothing is not an error, and the completion still has to fire. Without a
// row to force a read cycle this is the shortest path through the reactor, so it is the one most
// likely to be broken by a change to the OnReadDone/OnDone handshake.
void
an_empty_stream_completes_without_rows()
{
  in_process_query_server server{ streams_rows(0) };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  auto row_count = 0;
  auto completions = 0;
  grpc::Status final_status;

  disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [&](v1::QueryResponse) {
      ++row_count;
    },
    [&](grpc::Status status) {
      ++completions;
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.ok(), "an empty stream is still a successful one");
  assert_eq(row_count, 0, "no rows delivered");
  assert_eq(completions, 1, "the completion fires exactly once");
}

void
a_server_error_reaches_on_done()
{
  in_process_query_server server{ fails_with(
    grpc::Status{ grpc::StatusCode::INVALID_ARGUMENT, "syntax error" }) };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  auto row_count = 0;
  grpc::Status final_status;

  disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [&](v1::QueryResponse) {
      ++row_count;
    },
    [&](grpc::Status status) {
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.error_code() == grpc::StatusCode::INVALID_ARGUMENT,
              "the server's status code reaches the completion");
  assert_eq(final_status.error_message(),
            std::string{ "syntax error" },
            "the server's message reaches the completion");
  assert_eq(row_count, 0, "a failed stream delivers no rows");
}

// A stream that fails partway is the case the consumers above this primitive have to get right:
// rows already handed over stay handed over, and the failure arrives after them, not instead of
// them.
void
rows_delivered_before_a_mid_stream_error_are_kept()
{
  in_process_query_server server{ fails_with(
    grpc::Status{ grpc::StatusCode::INTERNAL, "gateway gave up" }, 3) };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  std::vector<std::string> rows;
  auto rows_at_completion = std::size_t{ 0 };
  grpc::Status final_status;

  disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [&](v1::QueryResponse message) {
      for (const auto& row : message.rows()) {
        rows.push_back(row);
      }
    },
    [&](grpc::Status status) {
      rows_at_completion = rows.size();
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.error_code() == grpc::StatusCode::INTERNAL,
              "the terminal error reaches the completion");
  assert_eq(rows.size(), std::size_t{ 3 }, "rows written before the error are delivered");
  assert_eq(rows_at_completion,
            std::size_t{ 3 },
            "every row is delivered before the completion runs, not after it");
}

void
a_deadline_expires_a_parked_stream()
{
  in_process_query_server server{ parks() };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  grpc::Status final_status;
  disp.server_stream<v1::QueryResponse>(
    200ms,
    fixture.invoker(),
    [](v1::QueryResponse) {
    },
    [&](grpc::Status status) {
      final_status = std::move(status);
      work.reset();
    });

  io.run();

  assert_true(final_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED,
              "a stream that outlives its deadline fails with DEADLINE_EXCEEDED");
}

// Regression test for the review finding on this PR: the primitive used to set a deadline only when
// the timeout was positive, so a caller whose budget was already spent got a call with *no*
// deadline -- one that runs until the channel breaks and that cluster::close() then blocks on. Both
// of these park on the server, so if the deadline were absent the case would hang until the
// runner's own timeout rather than fail cleanly.
void
a_non_positive_timeout_expires_the_call_rather_than_removing_the_deadline()
{
  in_process_query_server server{ parks() };
  const auto channel = server.channel();
  stream_fixture fixture{ channel };

  for (const auto timeout : { 0ms, -1ms, -30'000ms }) {
    asio::io_context io;
    auto work = asio::make_work_guard(io);
    dispatcher disp{ io, channel };

    grpc::Status final_status;
    disp.server_stream<v1::QueryResponse>(
      timeout,
      fixture.invoker(),
      [](v1::QueryResponse) {
      },
      [&](grpc::Status status) {
        final_status = std::move(status);
        work.reset();
      });

    io.run();

    assert_true(final_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED,
                "a non-positive timeout expires the call instead of leaving it unbounded");
  }
}

void
cancelling_the_pending_call_ends_the_stream()
{
  in_process_query_server server{ parks() };
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  dispatcher disp{ io, server.channel() };
  stream_fixture fixture{ disp.channel() };

  grpc::Status final_status;
  auto completions = 0;
  const auto call = disp.server_stream<v1::QueryResponse>(
    timeout::network,
    fixture.invoker(),
    [](v1::QueryResponse) {
    },
    [&](grpc::Status status) {
      ++completions;
      final_status = std::move(status);
      work.reset();
    });

  call.cancel();
  io.run();

  assert_true(final_status.error_code() == grpc::StatusCode::CANCELLED,
              "a cancelled stream completes as CANCELLED");
  assert_eq(completions, 1, "cancellation still delivers exactly one completion");
}

// ── Lifetime ──────────────────────────────────────────────────────────────────

// Tears the dispatcher down while a stream is still in flight, which is what cancel_and_drain()
// exists for and what the delivery cases never reach -- they run io.run() to completion, so their
// streams have already finished and there is nothing left to cancel. Here the completion posted
// from OnDone never runs at all, because the io_context is destroyed first, so a reactor that
// released itself from inside that task body would never be released.
//
// `probe` is captured by value in on_done, so a copy of it lives inside the reactor; once the
// reactor is destroyed that copy goes with it and use_count() falls back to this scope's single
// reference. A count of 2 means the reactor outlived everything that could have freed it. The count
// is read only after the io_context has been destroyed, because until then the queued completion
// legitimately owns the reactor -- reading it earlier reports 2 whether or not the reactor leaks.
void
tearing_down_with_an_unrun_completion_releases_the_reactor()
{
  in_process_query_server server{ parks() };
  const auto channel = server.channel();
  stream_fixture fixture{ channel };
  const auto probe = std::make_shared<int>(0);

  {
    asio::io_context io;
    dispatcher disp{ io, channel };
    disp.server_stream<v1::QueryResponse>(
      timeout::network,
      fixture.invoker(),
      [](v1::QueryResponse) {
      },
      [probe](grpc::Status) {
      });
    // io.run() is deliberately never called: ~dispatcher() cancels and drains the call, and then
    // the io_context is destroyed with the completion still sitting in its queue.
  }

  assert_eq(
    probe.use_count(), long{ 1 }, "the reactor is released even though its completion never ran");
}

// An invoker that throws must leave nothing registered. If the tracker kept the entry, the
// dispatcher's destructor would wait for a completion that can never arrive and the case would hang
// rather than fail -- so reaching the assertion at all is most of what is being checked here.
void
an_invoker_that_throws_leaves_nothing_registered()
{
  in_process_query_server server;
  asio::io_context io;
  const auto probe = std::make_shared<int>(0);
  auto threw = false;

  {
    dispatcher disp{ io, server.channel() };
    try {
      disp.server_stream<v1::QueryResponse>(
        timeout::network,
        [](grpc::ClientContext&, grpc::ClientReadReactor<v1::QueryResponse>*) {
          throw std::runtime_error{ "cannot bind the call" };
        },
        [](v1::QueryResponse) {
        },
        [probe](grpc::Status) {
        });
    } catch (const std::runtime_error&) {
      threw = true;
    }
    // ~dispatcher() runs here. It must not block: nothing should still be registered.
  }

  assert_true(threw, "the invoker's exception propagates to the caller");
  assert_eq(probe.use_count(), long{ 1 }, "the reactor is destroyed rather than leaked");
}

// ── Concurrency ───────────────────────────────────────────────────────────────

// The call tracker's registry is mutex-protected because concurrent dispatch and shutdown are the
// hazard it exists for, but nothing in this tree drove more than one call at a time, so the mutex
// was never contended and the TSan leg CI pays for on every pull request had nothing to observe.
//
// This starts a batch of streams that will not finish on their own, then destroys the dispatcher
// while they are all in flight. That puts registration (this thread), completion (gRPC's threads
// calling remove()) and cancel_and_drain() (this thread again) on the same mutex at the same time.
// Every completion must still be delivered exactly once: the drain returns only once every call has
// posted its completion and deregistered, so all of them are queued by the time the io_context is
// allowed to finish.
void
concurrent_streams_are_all_drained_by_the_destructor()
{
  constexpr auto concurrency = std::size_t{ 16 };
  in_process_query_server server{ parks() };
  const auto channel = server.channel();
  stream_fixture fixture{ channel };

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread{ [&io]() {
    io.run();
  } };

  std::array<std::atomic<int>, concurrency> completions{};
  {
    dispatcher disp{ io, channel };
    for (std::size_t i = 0; i < concurrency; ++i) {
      disp.server_stream<v1::QueryResponse>(
        timeout::network,
        fixture.invoker(),
        [](v1::QueryResponse) {
        },
        [&completions, i](grpc::Status) {
          ++completions.at(i);
        });
    }
    // ~dispatcher() cancels every call and blocks until each has posted its completion.
  }
  work.reset();
  io_thread.join();

  auto delivered_once = true;
  for (const auto& count : completions) {
    if (count.load() != 1) {
      delivered_once = false;
    }
  }
  assert_true(delivered_once, "every concurrent stream completes exactly once");
}

// The case above registers every call from one thread. This one registers from several at once, so
// call_tracker::add() contends with itself as well as with the remove() calls arriving on gRPC's
// threads -- the interleaving a single-threaded launcher cannot produce.
void
streams_launched_from_many_threads_all_complete()
{
  constexpr auto launchers = std::size_t{ 4 };
  constexpr auto per_launcher = std::size_t{ 4 };
  in_process_query_server server{ streams_rows(2) };
  const auto channel = server.channel();

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread{ [&io]() {
    io.run();
  } };

  std::atomic<int> completions{ 0 };

  // One fixture per launcher, owned here rather than by the thread: a fixture destroyed at the end
  // of its thread would free the QueryRequest that calls still in flight are reading from. These
  // outlive the dispatcher, so every call's request is alive until the drain has finished with it.
  std::vector<std::unique_ptr<stream_fixture>> fixtures;
  fixtures.reserve(launchers);
  for (std::size_t t = 0; t < launchers; ++t) {
    // A stub per launcher: gRPC stubs are not documented as safe to share across threads, and
    // sharing one would put that under test rather than the tracker.
    fixtures.push_back(std::make_unique<stream_fixture>(channel));
  }

  {
    dispatcher disp{ io, channel };
    std::vector<std::thread> threads;
    threads.reserve(launchers);
    for (std::size_t t = 0; t < launchers; ++t) {
      threads.emplace_back([&, t]() {
        for (std::size_t i = 0; i < per_launcher; ++i) {
          disp.server_stream<v1::QueryResponse>(
            timeout::network,
            fixtures.at(t)->invoker(),
            [](v1::QueryResponse) {
            },
            [&completions](grpc::Status) {
              ++completions;
            });
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    // ~dispatcher() drains whatever is still in flight, so every call has posted its completion by
    // the time this scope ends.
  }
  work.reset();
  io_thread.join();

  assert_eq(completions.load(),
            static_cast<int>(launchers * per_launcher),
            "every stream launched concurrently completes exactly once");
}

// A stream parked between reads -- gRPC has delivered a message and is waiting for the io_context
// to issue the next StartRead -- is the steady state of the backpressure design, and the one state
// TryCancel alone cannot finish: no read is outstanding to fail, and the read hold defers OnDone.
// Without a cancel hook that releases that hold, ~dispatcher() waits for an OnDone that cannot
// fire, so this case fails by timing out rather than by assertion.
void
destructor_drains_a_parked_stream()
{
  std::promise<void> first_write_promise;
  auto first_write_future = first_write_promise.get_future();

  auto plan = streams_rows(3);
  plan.on_first_write = [&first_write_promise]() {
    first_write_promise.set_value();
  };
  in_process_query_server server{ std::move(plan) };
  const auto channel = server.channel();
  stream_fixture fixture{ channel };
  const auto probe = std::make_shared<int>(0);

  {
    asio::io_context io;
    dispatcher disp{ io, channel };
    disp.server_stream<v1::QueryResponse>(
      timeout::network,
      fixture.invoker(),
      [](v1::QueryResponse) {
      },
      [probe](grpc::Status) {
      });
    // The future pins the write: the server has delivered its first batch. The park that follows
    // it is not pinned -- the sleep only gives gRPC time to reach it, and a slow machine could
    // still drain before the reactor parks. The deterministic cover for the parked state is
    // a_row_in_flight_when_the_drain_releases_the_hold_does_not_resume_the_read, which holds the
    // consumer inside on_row until after the drain. The io_context is never run here, so nothing
    // issues the next StartRead.
    const auto write_status = first_write_future.wait_for(10s);
    assert_true(write_status == std::future_status::ready,
                "server delivered its first batch to the stream");
    std::this_thread::sleep_for(10ms);
  }

  assert_eq(
    probe.use_count(), long{ 1 }, "the parked reactor is released once the drain completes");
}

// Releasing the read hold is what permits gRPC to destroy the reader, and it destroys it
// synchronously inside RemoveHold. The drain returns only after OnDone, which gRPC delivers after
// that destruction -- so once ~dispatcher() has returned, a queued continuation that still issues
// StartRead dispatches through a destroyed reader. This parks a continuation between its row and
// its next read, drains, and only then lets it resume, which is the interleaving
// streams_launched_from_many_threads_all_complete reaches by chance and this case reaches every
// run.
//
// The consumer runs outside the reactor's lock, which this case also pins: parking inside on_row
// must not block the drain, so a guard that held the lock across the consumer callback would hang
// here rather than fail.
void
a_row_in_flight_when_the_drain_releases_the_hold_does_not_resume_the_read()
{
  std::promise<void> row_delivered_promise;
  auto row_delivered = row_delivered_promise.get_future();
  std::promise<void> resume_promise;
  auto resume = resume_promise.get_future();

  auto plan = streams_rows(1);
  plan.park_until_cancelled = true;
  in_process_query_server server{ std::move(plan) };
  const auto channel = server.channel();
  stream_fixture fixture{ channel };

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread{ [&io]() {
    io.run();
  } };

  auto rows = 0;
  auto completions = 0;

  // Every assertion runs after join(), not inside the scope. assert_true throws, so an assertion
  // here would skip resume_promise.set_value(), work.reset() and io_thread.join(), and destroying
  // a joinable std::thread terminates the process instead of reporting a failed case.
  auto delivered = std::future_status::timeout;

  {
    dispatcher disp{ io, channel };
    disp.server_stream<v1::QueryResponse>(
      timeout::network,
      fixture.invoker(),
      [&](v1::QueryResponse) {
        if (++rows == 1) {
          row_delivered_promise.set_value();
          // Bounded rather than indefinite: a regression that never releases this parks the io
          // thread, and a case that fails is worth more than one that hangs.
          resume.wait_for(park_giveup);
        }
      },
      [&completions](grpc::Status) {
        ++completions;
      });
    delivered = row_delivered.wait_for(park_giveup);
    // ~dispatcher() cancels and drains here, with the continuation still parked before its read.
    // The park is released only after the scope closes, so the drain provably observes the parked
    // state rather than racing the io thread's next StartRead.
  }
  resume_promise.set_value();

  work.reset();
  io_thread.join();

  assert_true(delivered == std::future_status::ready, "the first row reached the consumer");
  assert_eq(rows, 1, "the row delivered before the drain is not lost");
  assert_eq(completions, 1, "the stream completes exactly once after the parked read is abandoned");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_streaming",
    {
      { "server_stream_delivers_all_rows_on_the_io_thread",
        server_stream_delivers_all_rows_on_the_io_thread,
        timeout::network },
      { "a_long_stream_preserves_order", a_long_stream_preserves_order, timeout::network },
      { "an_empty_stream_completes_without_rows",
        an_empty_stream_completes_without_rows,
        timeout::network },
      { "a_server_error_reaches_on_done", a_server_error_reaches_on_done, timeout::network },
      { "rows_delivered_before_a_mid_stream_error_are_kept",
        rows_delivered_before_a_mid_stream_error_are_kept,
        timeout::network },
      { "a_deadline_expires_a_parked_stream",
        a_deadline_expires_a_parked_stream,
        timeout::network },
      { "a_non_positive_timeout_expires_the_call_rather_than_removing_the_deadline",
        a_non_positive_timeout_expires_the_call_rather_than_removing_the_deadline,
        timeout::network },
      { "cancelling_the_pending_call_ends_the_stream",
        cancelling_the_pending_call_ends_the_stream,
        timeout::network },
      { "tearing_down_with_an_unrun_completion_releases_the_reactor",
        tearing_down_with_an_unrun_completion_releases_the_reactor,
        timeout::network },
      { "an_invoker_that_throws_leaves_nothing_registered",
        an_invoker_that_throws_leaves_nothing_registered,
        timeout::network },
      { "destructor_drains_a_parked_stream", destructor_drains_a_parked_stream, timeout::network },
      { "a_row_in_flight_when_the_drain_releases_the_hold_does_not_resume_the_read",
        a_row_in_flight_when_the_drain_releases_the_hold_does_not_resume_the_read,
        timeout::network },
      { "concurrent_streams_are_all_drained_by_the_destructor",
        concurrent_streams_are_all_drained_by_the_destructor,
        timeout::network },
      { "streams_launched_from_many_threads_all_complete",
        streams_launched_from_many_threads_all_complete,
        timeout::network },
    },
  };
}

} // namespace couchbase::cng::test
