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

// The fake MCBP server against the real SDK, through the public couchbase::cluster API.

#include "benchmark/bench_runner.hxx"
#include "benchmark/fake_server.hxx"
#include "benchmark/kv_cases.hxx"

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/cluster.hxx"
#include "core/logger/configuration.hxx"
#include "core/logger/logger.hxx"

#include <spdlog/common.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/callback_sink.h>

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <asio.hpp>
#include <tao/json.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>

namespace couchbase::test
{
namespace
{
auto
connect(const bench::fake_server& server) -> couchbase::cluster
{
  const couchbase::cluster_options options(server.username(), server.password());
  auto [err, cluster] = couchbase::cluster::connect(server.connection_string(), options).get();
  assert_success(err, "connect to the fake server");
  return std::move(cluster);
}

void
sdk_bootstraps_against_fake(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  // The bucket session opens on the first operation, so one is needed to see it bootstrap.
  auto [err, result] = collection.upsert("bootstrap", tao::json::value{ { "a", 1 } }).get();
  assert_success(err, "upsert over the bucket session");
  cluster.close().get();
}

void
upsert_then_get_round_trips(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  const tao::json::value document{ { "a", 1 } };
  auto [upsert_err, upsert_result] = collection.upsert("k", document).get();
  assert_success(upsert_err, "upsert k");
  auto [get_err, get_result] = collection.get("k", {}).get();
  assert_success(get_err, "get k");
  assert_true(get_result.content_as<tao::json::value>() == document, "the content round-trips");
  assert_eq(get_result.cas(), upsert_result.cas(), "get returns the cas of the upsert");
  cluster.close().get();
}

// A 4 KiB repetitive value is compressed by the SDK once HELLO accepts Snappy. The server must
// store it compressed and return it unchanged.
void
compressible_value_round_trips(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  const tao::json::value document{ { "payload", std::string(4096, 'x') } };
  auto [upsert_err, upsert_result] = collection.upsert("big", document).get();
  assert_success(upsert_err, "upsert big");
  auto [get_err, get_result] = collection.get("big", {}).get();
  assert_success(get_err, "get big");
  assert_true(get_result.content_as<tao::json::value>() == document, "the content round-trips");
  assert_eq(server.snappy_documents(), std::size_t{ 1 }, "the value is stored compressed");
  cluster.close().get();
}

void
get_missing_is_not_found(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  auto [err, result] = collection.get("missing", {}).get();
  assert_error(err, errc::key_value::document_not_found, "get of an absent key");
  cluster.close().get();
}

void
remove_deletes_and_reports_missing(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  auto [upsert_err, upsert_result] = collection.upsert("k", tao::json::value{ { "a", 1 } }).get();
  assert_success(upsert_err, "upsert k");
  auto [remove_err, remove_result] = collection.remove("k").get();
  assert_success(remove_err, "remove k");
  auto [get_err, get_result] = collection.get("k", {}).get();
  assert_error(get_err, errc::key_value::document_not_found, "get of a removed key");
  auto [again_err, again_result] = collection.remove("k").get();
  assert_error(again_err, errc::key_value::document_not_found, "remove of an absent key");
  cluster.close().get();
}

void
pipelined_stops_after_its_failure_budget(context& /* ctx */)
{
  // Operations fail with no supplemental message, as most KV failures do, and the first failure is
  // reported by its error code's message.
  const auto run = [](std::size_t max_failures, auto fails) {
    std::size_t started = 0;
    const auto [failure, all_started] = bench::kv::pipelined(
      300,
      [&started, &fails](std::size_t i) {
        ++started;
        std::promise<std::pair<couchbase::error, int>> p;
        p.set_value(
          { fails(i) ? couchbase::error{ errc::common::temporary_failure } : couchbase::error{},
            0 });
        return p.get_future();
      },
      {},
      max_failures);
    return std::make_tuple(started, failure, all_started);
  };
  const auto expected = std::error_code{ errc::common::temporary_failure }.message();
  const auto one = [](std::size_t i) {
    return i == 3;
  };
  const auto every = [](std::size_t) {
    return true;
  };

  // Seeding: one failure stops it.
  const auto [seeded, seed_failure, seed_all] = run(1, one);
  assert_true(seeded < 300 && !seed_all, "seeding starts nothing once a failure is seen");
  assert_eq(seed_failure, expected, "seeding reports the failure");

  // Removal: a scattered failure leaves every operation attempted.
  const auto [removed, remove_failure, remove_all] = run(64, one);
  assert_true(removed == 300 && remove_all, "removal attempts every operation past one failure");
  assert_eq(remove_failure, expected, "removal reports the failure");

  // Removal against a bucket that fails everything stops after its budget, within two windows.
  const auto [stopped, stop_failure, stop_all] = run(64, every);
  assert_true(stopped <= 128 && !stop_all, "removal stops after 64 failures");
  assert_eq(stop_failure, expected, "a stopped removal reports the first failure");
}

void
server_survives_client_close(context& /* ctx */)
{
  const bench::fake_server server;
  for (int round = 0; round < 2; ++round) {
    auto cluster = connect(server);
    auto [err, result] = cluster.bucket("default")
                           .default_collection()
                           .upsert("k", tao::json::value{ { "round", round } })
                           .get();
    assert_success(err, "upsert after reconnecting");
    cluster.close().get();
  }
}
// Sends one 24-byte header and expects the server to close the connection without waiting for the
// body it declares. A server that buffers the body first never answers, and the case times out.
void
expect_closed_on_header(const bench::fake_server& server,
                        std::uint8_t magic,
                        std::uint32_t body_size,
                        const char* what)
{
  const auto connection_string = server.connection_string();
  const auto port = static_cast<std::uint16_t>(
    std::stoi(connection_string.substr(connection_string.rfind(':') + 1)));
  asio::io_context io;
  asio::ip::tcp::socket socket(io);
  socket.connect({ asio::ip::make_address("127.0.0.1"), port });
  std::array<unsigned char, 24> header{};
  header[0] = magic;
  header[8] = static_cast<unsigned char>(body_size >> 24);
  header[9] = static_cast<unsigned char>(body_size >> 16);
  header[10] = static_cast<unsigned char>(body_size >> 8);
  header[11] = static_cast<unsigned char>(body_size);
  asio::write(socket, asio::buffer(header));
  std::array<char, 1> reply{};
  std::error_code ec;
  socket.read_some(asio::buffer(reply), ec);
  assert_true(static_cast<bool>(ec), what);
}

void
bad_magic_closes_before_body(context& /* ctx */)
{
  const bench::fake_server server;
  expect_closed_on_header(server, 0x81, 1024, "a response magic closes the connection");
}

void
oversized_body_closes_before_body(context& /* ctx */)
{
  const bench::fake_server server;
  expect_closed_on_header(server, 0x80, 0xffffffff, "a body over the limit closes the connection");
  auto cluster = connect(server);
  auto [err, result] = cluster.bucket("default")
                         .default_collection()
                         .upsert("k", tao::json::value{ { "a", 1 } })
                         .get();
  assert_success(err, "the server still serves a new connection");
  cluster.close().get();
}
// Regression: a connection string option applied after connect_options() could override either a
// feature compared between sdk_default and bare or their shared compression setting.
void
a_connection_string_that_sets_a_profile_option_is_rejected(context& /* ctx */)
{
  for (const std::string_view name : {
         "enable_metrics",
         "enable_tracing",
         "enable_app_telemetry",
         "enable_compression",
       }) {
    const auto connection_string = "couchbase://127.0.0.1?" + std::string{ name } + "=true";
    assert_throws_with<std::invalid_argument>(
      name,
      [&connection_string]() {
        bench::kv::require_no_profile_option(connection_string);
      },
      connection_string);
  }
  bench::kv::require_no_profile_option(
    "couchbase://127.0.0.1?enable_dns_srv=false&config_poll_interval=1000");
}
// Fails every write and flush while *fail is set.
class switchable_sink final : public spdlog::sinks::base_sink<std::mutex>
{
public:
  explicit switchable_sink(std::shared_ptr<std::atomic<bool>> fail)
    : fail_{ std::move(fail) }
  {
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    if (fail_->load()) {
      throw spdlog::spdlog_ex("the write failed");
    }
  }

  void flush_() override
  {
    if (fail_->load()) {
      throw spdlog::spdlog_ex("the flush failed");
    }
  }

private:
  std::shared_ptr<std::atomic<bool>> fail_;
};

// The waits a trace selftest makes on the logger worker or the cluster's I/O thread, scaled with
// the runner's timeout multiplier.
auto
trace_wait() -> std::chrono::milliseconds
{
  return scaled_budget(std::chrono::seconds{ 2 });
}

// Regression: a trace log that received nothing passed its check and was measured.
void
a_trace_log_that_receives_nothing_is_rejected(context& /* ctx */)
{
  bench::kv::trace_file_log trace{ scaled_budget(std::chrono::milliseconds{ 50 }) };
  assert_throws_with<std::runtime_error>("nothing was written", [&trace]() {
    trace.require_written();
  });
}

// Regression: a trace log whose file failed to take writes passed its check and was measured.
void
a_trace_log_that_cannot_write_is_rejected_before_the_measurement(context& /* ctx */)
{
  bench::kv::trace_file_log trace{
    trace_wait(), std::make_shared<switchable_sink>(std::make_shared<std::atomic<bool>>(true))
  };
  CB_LOG_INFO_RAW("message");
  assert_throws_with<std::runtime_error>("failed", [&trace]() {
    trace.require_written();
  });
}

std::shared_ptr<std::atomic<bool>> fail_trace_writes{};

void
measures_with_a_trace_log_that_fails_afterwards(bench::state& st)
{
  fail_trace_writes = std::make_shared<std::atomic<bool>>(false);
  bench::kv::trace_file_log trace{ trace_wait(),
                                   std::make_shared<switchable_sink>(fail_trace_writes) };
  CB_LOG_INFO_RAW("before");
  trace.require_written();
  st.per_op("t1", 1, [](std::size_t) {
    CB_LOG_INFO_RAW("operation");
    return true;
  });
  fail_trace_writes->store(true);
  CB_LOG_INFO_RAW("after");
  bench::kv::finish_trace(st, trace, "t1");
}

// Regression: a write that failed after the measurement failed only the case, and the trace key
// kept its measurements in the reports.
void
a_trace_write_that_fails_after_the_measurement_fails_its_key(context& ctx)
{
  using namespace std::chrono_literals;
  const bench::bench_suite suite{ "s",
                                  { { CASE(measures_with_a_trace_log_that_fails_afterwards) } } };
  context run_ctx{ ctx.config() };
  std::ostringstream progress;
  const auto results =
    bench::run(suite, {}, run_ctx, bench::options{ 10ms, 50ms, 200ms, 5.0 }, progress);
  assert_eq(results.size(), std::size_t{ 1 });
  const auto& r = results.front();
  assert_eq(r.key, std::string{ "measures_with_a_trace_log_that_fails_afterwards/t1" });
  assert_true(r.status == bench::outcome::failed, "the key failed");
  assert_contains(r.detail, "failed", "with the write failure");
  assert_true(r.operations == 0 && r.latency_ns.median == 0, "with no measurements");
}

// Counts what it writes, a millisecond per message, and records the count at each flush.
class slow_sink final : public spdlog::sinks::base_sink<std::mutex>
{
public:
  explicit slow_sink(std::shared_ptr<std::atomic<int>> written)
    : written_{ std::move(written) }
  {
  }

  // How many messages had been written when the sink was last flushed.
  std::atomic<int> written_at_last_flush{ -1 };

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
    written_->fetch_add(1);
  }

  void flush_() override
  {
    written_at_last_flush = written_->load();
  }

private:
  std::shared_ptr<std::atomic<int>> written_;
};

// Regression: messages queued before a measurement window were written, or flushed, inside it, so
// the window counted their processing. drain() returns once every message queued before it is
// written and flushed.
void
draining_the_trace_log_writes_every_queued_message(context& /* ctx */)
{
  auto written = std::make_shared<std::atomic<int>>(0);
  auto sink = std::make_shared<slow_sink>(written);
  bench::kv::trace_file_log trace{ trace_wait(), sink };
  constexpr int queued = 20;
  for (int i = 0; i < queued; ++i) {
    CB_LOG_INFO_RAW("queued");
  }
  trace.drain();
  // The drain marker passes through the file sink too, after the queued messages.
  assert_eq(written->load(), queued + 1, "every message queued before the drain was written");
  assert_eq(sink->written_at_last_flush.load(), queued + 1, "and flushed after the last of them");
}

struct bounded_gate {
  std::mutex mutex{};
  std::condition_variable cv{};
  bool entered{ false };
  bool released{ false };

  void enter()
  {
    {
      const std::scoped_lock lock(mutex);
      entered = true;
    }
    cv.notify_all();
  }

  auto enter_and_wait(std::chrono::milliseconds budget) -> bool
  {
    std::unique_lock<std::mutex> lock(mutex);
    entered = true;
    cv.notify_all();
    return cv.wait_for(lock, budget, [this]() {
      return released;
    });
  }

  auto wait_entered(std::chrono::milliseconds budget) -> bool
  {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, budget, [this]() {
      return entered;
    });
  }

  void release()
  {
    {
      const std::scoped_lock lock(mutex);
      released = true;
    }
    cv.notify_all();
  }
};

// Regression: a handler on the cluster's I/O thread can log after it has completed an operation's
// future, and a drain queued before that message missed it.
void
a_drain_behind_the_io_barrier_counts_what_the_io_thread_logs(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto trailing = std::make_shared<std::atomic<int>>(0);
  auto gate = std::make_shared<bounded_gate>();
  {
    bench::kv::trace_file_log trace{ trace_wait(),
                                     std::make_shared<spdlog::sinks::callback_sink_mt>(
                                       [trailing](const spdlog::details::log_msg& msg) {
                                         if (std::string_view{ msg.payload.data(),
                                                               msg.payload.size() } == "trailing") {
                                           trailing->fetch_add(1);
                                         }
                                       }) };
    const struct gate_release_guard {
      std::shared_ptr<bounded_gate> gate{};
      ~gate_release_guard()
      {
        gate->release();
      }
    } release_on_exit{ gate };
    asio::post(core::get_core_cluster(cluster).io_context(), [gate]() {
      if (gate->enter_and_wait(trace_wait())) {
        CB_LOG_INFO_RAW("trailing");
      }
    });
    assert_true(gate->wait_entered(trace_wait()), "the I/O handler started");
    assert_throws_with<std::runtime_error>("did not reach the barrier", [&cluster]() {
      bench::kv::finish_io(cluster, scaled_budget(std::chrono::milliseconds{ 20 }));
    });
    assert_eq(trailing->load(), 0, "nothing was logged while the handler stayed gated");
    gate->release();
    bench::kv::finish_io(cluster, trace_wait());
    trace.drain();
    assert_eq(trailing->load(), 1, "the message the I/O thread logged was written by the drain");
  }
  cluster.close().get();
}

// Regression: stream_impl binds socket operations to a strand, and a single callback posted to the
// bare io_context ran ahead of handlers still queued in a strand and continuations they posted.
void
the_io_barrier_waits_for_queued_strand_work_and_its_continuations(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto queued = std::make_shared<std::atomic<int>>(0);
  auto reposted = std::make_shared<std::atomic<int>>(0);
  auto continuation = std::make_shared<std::atomic<int>>(0);
  auto active_gate = std::make_shared<bounded_gate>();
  auto queued_gate = std::make_shared<bounded_gate>();
  auto continuation_gate = std::make_shared<bounded_gate>();
  auto posted_gate = std::make_shared<bounded_gate>();
  {
    bench::kv::trace_file_log trace{
      trace_wait(),
      std::make_shared<spdlog::sinks::callback_sink_mt>(
        [queued, reposted, continuation](const spdlog::details::log_msg& msg) {
          const std::string_view payload{ msg.payload.data(), msg.payload.size() };
          if (payload == "strand_queued") {
            queued->fetch_add(1);
          } else if (payload == "strand_reposted") {
            reposted->fetch_add(1);
          } else if (payload == "strand_continuation") {
            continuation->fetch_add(1);
          }
        })
    };
    std::promise<void> waiter_done;
    auto finished = waiter_done.get_future();
    std::thread waiter;
    const struct cleanup_guard {
      std::shared_ptr<bounded_gate> active_gate{};
      std::shared_ptr<bounded_gate> queued_gate{};
      std::shared_ptr<bounded_gate> continuation_gate{};
      std::thread* waiter{ nullptr };
      ~cleanup_guard()
      {
        active_gate->release();
        queued_gate->release();
        continuation_gate->release();
        if (waiter != nullptr && waiter->joinable()) {
          waiter->join();
        }
      }
    } cleanup{ active_gate, queued_gate, continuation_gate, &waiter };

    auto& io = core::get_core_cluster(cluster).io_context();
    const asio::any_io_executor strand{ asio::make_strand(io) };
    asio::post(strand, [active_gate]() {
      active_gate->enter_and_wait(trace_wait());
    });
    assert_true(active_gate->wait_entered(trace_wait()), "the active strand handler started");
    asio::post(strand, [&io, strand, queued_gate, continuation_gate]() {
      if (!queued_gate->enter_and_wait(trace_wait())) {
        return;
      }
      CB_LOG_INFO_RAW("strand_queued");
      asio::post(strand, []() {
        CB_LOG_INFO_RAW("strand_reposted");
      });
      asio::post(io, [continuation_gate]() {
        if (continuation_gate->enter_and_wait(trace_wait())) {
          CB_LOG_INFO_RAW("strand_continuation");
        }
      });
    });
    waiter = std::thread([&cluster, posted_gate, done = std::move(waiter_done)]() mutable {
      try {
        bench::kv::finish_io(cluster, trace_wait(), [posted_gate]() {
          posted_gate->enter();
        });
        done.set_value();
      } catch (...) {
        done.set_exception(std::current_exception());
      }
    });
    assert_true(posted_gate->wait_entered(trace_wait()),
                "finish_io posted its callback while the strand was busy");
    assert_true(finished.wait_for(scaled_budget(std::chrono::milliseconds{ 20 })) ==
                  std::future_status::timeout,
                "finish_io waits for the active strand handler");

    active_gate->release();
    assert_true(queued_gate->wait_entered(trace_wait()), "the queued strand handler started");
    assert_true(finished.wait_for(scaled_budget(std::chrono::milliseconds{ 20 })) ==
                  std::future_status::timeout,
                "finish_io waits for handlers queued in the strand");
    assert_eq(queued->load(), 0, "the queued strand handler has not logged yet");

    queued_gate->release();
    assert_true(continuation_gate->wait_entered(trace_wait()),
                "the continuation posted by the strand handler started");
    assert_true(finished.wait_for(scaled_budget(std::chrono::milliseconds{ 20 })) ==
                  std::future_status::timeout,
                "finish_io waits for continuations posted by strand handlers");
    assert_eq(continuation->load(), 0, "the continuation has not logged yet");

    continuation_gate->release();
    assert_true(finished.wait_for(trace_wait()) == std::future_status::ready,
                "finish_io completes once the strand and its continuations finish");
    finished.get();
    waiter.join();
    trace.drain();
    assert_eq(queued->load(), 1, "the queued strand message was written by the drain");
    assert_eq(reposted->load(), 1, "the reposted strand message was written by the drain");
    assert_eq(continuation->load(), 1, "the continuation message was written by the drain");
  }
  cluster.close().get();
}

void
fails_to_set_up_its_trace_log(bench::state& st)
{
  bench::kv::with_trace_key(st, "t1", []() {
    bench::kv::trace_file_log trace{ scaled_budget(std::chrono::milliseconds{ 50 }) };
    trace.require_written();
  });
}

// Regression: a trace setup failure failed the whole case, omitting the trace key from reports.
void
a_trace_setup_failure_fails_the_trace_key(context& ctx)
{
  using namespace std::chrono_literals;
  const bench::bench_suite suite{ "s", { { CASE(fails_to_set_up_its_trace_log) } } };
  context run_ctx{ ctx.config() };
  std::ostringstream progress;
  const auto results =
    bench::run(suite, {}, run_ctx, bench::options{ 10ms, 50ms, 200ms, 5.0 }, progress);
  assert_eq(results.size(), std::size_t{ 1 });
  assert_eq(results.front().key, std::string{ "fails_to_set_up_its_trace_log/t1" });
  assert_true(results.front().status == bench::outcome::failed, "the trace key failed");
  assert_contains(results.front().detail, "nothing was written", "with the setup failure");
}

void
upsert_get_with_missing_bucket(bench::state& st)
{
  const bench::fake_server server;
  bench::kv::measure_upsert_get(
    st, "fake", server.connection_string(), server.username(), server.password(), "");
}

class stub_probe_backend final : public probe_backend
{
public:
  [[nodiscard]] auto server_version() -> couchbase::test::server_version override
  {
    return v7_6;
  }
  [[nodiscard]] auto has_service(const std::string& /* name */) -> bool override
  {
    return true;
  }
  [[nodiscard]] auto has_bucket_capability(const std::string& /* capability */) -> bool override
  {
    return true;
  }
  [[nodiscard]] auto number_of_replicas() -> std::size_t override
  {
    return 0;
  }
  [[nodiscard]] auto number_of_nodes() -> std::size_t override
  {
    return 1;
  }
  [[nodiscard]] auto server_groups() -> std::vector<std::string> override
  {
    return {};
  }
  [[nodiscard]] auto storage_backend() -> std::string override
  {
    return "couchstore";
  }
};

// Regression: a failure in measure_upsert_get() before with_trace_key() left a selected trace key
// absent from reports.
void
a_prerequisite_failure_in_upsert_get_fails_a_selected_trace_key(context& ctx)
{
  using namespace std::chrono_literals;
  const bench::bench_suite suite{ "s", { { "kv_upsert_get", &upsert_get_with_missing_bucket } } };
  const bench::options opts{ 10ms, 50ms, 200ms, 5.0 };

  std::ostringstream progress;
  context probe_ctx{ ctx.config(), std::make_unique<stub_probe_backend>() };
  assert_eq(probe_ctx.server_version().to_string(),
            v7_6.to_string(),
            "the version probe is cached before measure_upsert_get()");
  const auto trace_only =
    bench::run(suite, { "kv_upsert_get/fake/t20/mixed/trace" }, probe_ctx, opts, progress);
  assert_eq(trace_only.size(), std::size_t{ 1 });
  assert_eq(trace_only.front().key, std::string{ "kv_upsert_get/fake/t20/mixed/trace" });
  assert_true(trace_only.front().status == bench::outcome::failed, "the selected trace key failed");
  assert_contains(trace_only.front().detail, "seeding failed", "with the seeding failure");
  assert_eq(probe_ctx.server_version().to_string(),
            v7_6.to_string(),
            "a probe cached before measure_upsert_get() remains readable after close_backend()");
  assert_throws<probe_failure>(
    [&]() {
      static_cast<void>(probe_ctx.number_of_nodes());
    },
    "measure_upsert_get() closes the benchmark context's probe backend for uncached probes");

  const auto run_case = [&](const std::set<std::string>& filter) {
    context run_ctx{ ctx.config() };
    return bench::run(suite, filter, run_ctx, opts, progress);
  };

  const auto trace_and_other =
    run_case({ "kv_upsert_get/fake/t20", "kv_upsert_get/fake/t20/mixed/trace" });
  assert_eq(trace_and_other.size(), std::size_t{ 2 });
  assert_eq(trace_and_other[0].key, std::string{ "kv_upsert_get/fake/t20/mixed/trace" });
  assert_true(trace_and_other[0].status == bench::outcome::failed,
              "the trace key is marked failed");
  assert_contains(trace_and_other[0].detail, "seeding failed", "with the seeding failure");
  assert_eq(trace_and_other[1].key, std::string{ "kv_upsert_get" });
  assert_true(trace_and_other[1].status == bench::outcome::failed,
              "the failure for another selected variant is preserved");
  assert_contains(trace_and_other[1].detail, "seeding failed", "with the seeding failure");

  const auto whole_case = run_case({});
  assert_eq(whole_case.size(), std::size_t{ 2 });
  assert_eq(whole_case[0].key, std::string{ "kv_upsert_get/fake/t20/mixed/trace" });
  assert_true(whole_case[0].status == bench::outcome::failed, "the trace key is marked failed");
  assert_contains(whole_case[0].detail, "seeding failed", "with the seeding failure");
  assert_eq(whole_case[1].key, std::string{ "kv_upsert_get" });
  assert_true(whole_case[1].status == bench::outcome::failed,
              "the case failure for the other unreached variants is preserved");
  assert_contains(whole_case[1].detail, "seeding failed", "with the seeding failure");

  const auto non_trace = run_case({ "kv_upsert_get/fake/t20" });
  assert_eq(non_trace.size(), std::size_t{ 1 });
  assert_eq(non_trace.front().key, std::string{ "kv_upsert_get" });
  assert_true(non_trace.front().status == bench::outcome::failed,
              "a filtered-out trace key is left absent while the case still fails");
}

// Regression: TEST_LOG_PROTOCOL logged on both sides of the trace comparison regardless of log
// level.
void
an_active_protocol_logger_is_rejected(context& /* ctx */)
{
  const auto directory = bench::kv::make_temp_directory("cxx-bench-protocol-");
  core::logger::configuration config{};
  config.filename = (directory / "protocol").string();
  config.unit_test = true;
  config.console = false;
  const auto failure = core::logger::create_protocol_logger(config);
  bool rejected = false;
  try {
    bench::kv::require_no_protocol_log();
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  core::logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);
  assert_false(failure.has_value(), "the protocol logger was created");
  assert_true(rejected, "an active protocol logger is rejected");
  bench::kv::require_no_protocol_log();
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(sdk_bootstraps_against_fake), {}, timeout::fast },
      { CASE(upsert_then_get_round_trips), {}, timeout::fast },
      { CASE(compressible_value_round_trips), {}, timeout::fast },
      { CASE(get_missing_is_not_found), {}, timeout::fast },
      { CASE(remove_deletes_and_reports_missing), {}, timeout::fast },
      { CASE(pipelined_stops_after_its_failure_budget), {}, timeout::fast },
      { CASE(server_survives_client_close), {}, timeout::fast },
      { CASE(bad_magic_closes_before_body), {}, timeout::fast },
      { CASE(oversized_body_closes_before_body), {}, timeout::fast },
      { CASE(a_connection_string_that_sets_a_profile_option_is_rejected), {}, timeout::fast },
      { CASE(a_trace_log_that_receives_nothing_is_rejected) },
      { CASE(a_trace_log_that_cannot_write_is_rejected_before_the_measurement) },
      { CASE(a_trace_write_that_fails_after_the_measurement_fails_its_key) },
      { CASE(an_active_protocol_logger_is_rejected) },
      { CASE(a_trace_setup_failure_fails_the_trace_key) },
      { CASE(a_prerequisite_failure_in_upsert_get_fails_a_selected_trace_key) },
      { CASE(draining_the_trace_log_writes_every_queued_message) },
      { CASE(a_drain_behind_the_io_barrier_counts_what_the_io_thread_logs) },
      { CASE(the_io_barrier_waits_for_queued_strand_work_and_its_continuations) },
    },
  };
}
} // namespace couchbase::test
