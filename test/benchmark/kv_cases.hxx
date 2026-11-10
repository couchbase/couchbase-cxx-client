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

#pragma once

// The measured part of the KV cases, shared by the fake-server and cluster binaries so that their
// rows are comparable: the same documents, key sequence and operations.

#include "bench_framework.hxx"

#include "core/cluster.hxx"
#include "core/logger/configuration.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/connection_string.hxx"

#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/callback_sink.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <tao/json.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::test::bench::kv
{
// The bench_case::description of each case, which both binaries register.
constexpr const char* get_description =
  "Gets one of 1,000 small JSON documents per operation, blocking on 1 or 8 threads, or 64 "
  "in flight.";
constexpr const char* upsert_description =
  "Upserts one of 1,000 small JSON documents per operation, blocking on 1 or 8 threads, or "
  "64 in flight.";
constexpr const char* upsert_get_description =
  "Upserts, then gets, one of 100,000 512-byte JSON documents per operation on 20 "
  "threads, sharing one cluster or with one each, with and without trace logging.";

// The text of a failed KV or connect operation: the error code's message, then the supplemental
// message, which is empty for most KV failures.
inline auto
describe(const error& err) -> std::string
{
  return err.message().empty() ? err.ec().message() : err.ec().message() + ": " + err.message();
}

constexpr std::size_t document_count = 1000;
constexpr std::size_t thread_stride = 7919;

// The prefix of every key of this process: bench::, 16 random hex digits, and ::. Runs against one
// bucket never share a key, with each other or with documents already in it.
inline auto
run_prefix() -> const std::string&
{
  static const std::string prefix = [] {
    std::random_device rd;
    const std::uint64_t id = (std::uint64_t{ rd() } << 32U) | std::uint64_t{ rd() };
    std::array<char, 17> hex{};
    std::snprintf(hex.data(), hex.size(), "%016llx", static_cast<unsigned long long>(id));
    return "bench::" + std::string(hex.data()) + "::";
  }();
  return prefix;
}

inline auto
document_key(std::size_t i) -> std::string
{
  return run_prefix() + std::to_string(i);
}

// Four members, one a string of payload_size characters.
inline auto
document_body(std::size_t payload_size = 48) -> tao::json::value
{
  return {
    { "name", "benchmark document" },
    { "kind", "kv" },
    { "payload", std::string(payload_size, 'x') },
    { "flag", true },
  };
}

// Stores every document. Throws when a store fails, which fails the case.
inline void
seed(const collection& c)
{
  const auto body = document_body();
  for (std::size_t i = 0; i < document_count; ++i) {
    if (auto [e, r] = c.upsert(document_key(i), body).get(); e) {
      throw std::runtime_error("seeding failed: " + describe(e));
    }
  }
}

enum class profile : std::uint8_t {
  sdk_default,
  bare,
};

// The options both binaries connect with. Every setting below is set in both profiles, the
// compression thresholds included. The default profile sets the SDK's defaults explicitly, so a
// change of an SDK default does not move these numbers until connect_options() follows it. bare
// turns off metrics, tracing, application telemetry and both transaction cleanups.
inline auto
connect_options(const std::string& username, const std::string& password, profile p)
  -> cluster_options
{
  const bool on = p == profile::sdk_default;
  cluster_options options(username, password);
  options.metrics().enable(on);
  options.tracing().enable(on);
  options.application_telemetry().enable(on);
  options.compression().enabled(true).min_size(32).min_ratio(0.83);
  options.transactions().cleanup_config().cleanup_lost_attempts(on);
  options.transactions().cleanup_config().cleanup_client_attempts(on);
  return options;
}

// The connection string options that set what connect_options() sets. The connection string is
// applied after the options: enable_metrics, enable_tracing and enable_app_telemetry override the
// features compared between sdk_default and bare, while enable_compression overrides the shared
// compression setting both profiles enable.
inline constexpr std::array<std::string_view, 4> profile_options{
  "enable_metrics",
  "enable_tracing",
  "enable_app_telemetry",
  "enable_compression",
};

// Throws when the connection string sets one of profile_options, so it cannot override either the
// features compared between sdk_default and bare or their shared compression setting.
inline void
require_no_profile_option(const std::string& connection_string)
{
  const auto params = core::utils::parse_connection_string(connection_string).params;
  for (const auto name : profile_options) {
    if (params.count(std::string{ name }) > 0) {
      throw std::invalid_argument("the connection string sets " + std::string{ name } +
                                  ", which the benchmark profiles set; remove it");
    }
  }
}

constexpr std::size_t pipeline_depth = 64;

// Variant <prefix>/q64, where prefix is <target> or <target>/bare: one thread keeps pipeline_depth
// operations in flight. An iteration waits for the oldest operation and starts the next. Once round
// trips overlap, the time per iteration is the time per operation. start(n) starts operation n. The
// first iteration fills the window, so a key the filter does not select starts nothing. A failed
// operation throws, which fails the case. After the measurement every operation still in flight is
// consumed. No measured iteration waited for any of them, so the key keeps the samples it recorded,
// and the first failure among them throws once all are done, which fails the case. When the
// measurement throws, they are waited for and the exception is rethrown.
template<typename Start>
void
measure_pipelined(state& st, std::string_view prefix, Start start)
{
  std::deque<decltype(start(std::size_t{}))> window;
  const auto drain = [&window] {
    for (auto& op : window) {
      // The front of the window has been consumed by get() when its operation failed.
      if (op.valid()) {
        op.wait();
      }
    }
  };
  try {
    st.batched(std::string{ prefix } + "/q" + std::to_string(pipeline_depth),
               1,
               [&window, &start, n = std::size_t{ 0 }]() mutable {
                 if (window.empty()) {
                   while (window.size() < pipeline_depth) {
                     window.push_back(start(n++));
                   }
                 }
                 if (auto err = window.front().get().first; err) {
                   throw std::runtime_error("operation failed: " + describe(err));
                 }
                 window.pop_front();
                 window.push_back(start(n++));
               });
  } catch (...) {
    drain();
    throw;
  }
  std::string failure;
  for (auto& op : window) {
    if (auto err = op.get().first; err && failure.empty()) {
      failure = "operation failed: " + describe(err);
    }
  }
  if (!failure.empty()) {
    throw std::runtime_error(failure);
  }
}

// Variants <target>/t1, <target>/t8 and <target>/q64 for the default profile, <target>/bare/t1 and
// <target>/bare/q64 for bare. In t1 and t8, thread t operates on key
// (t * thread_stride + n) % document_count, n being that thread's operation count. In q64,
// operation n uses key n % document_count. In every variant a failed operation throws, which fails
// the case.
inline void
measure(state& st, std::string_view target, profile p, const collection& c, bool write)
{
  const auto body = document_body();
  const auto prefix = std::string{ target } + (p == profile::bare ? "/bare" : "");
  const std::vector<std::size_t> thread_counts =
    p == profile::bare ? std::vector<std::size_t>{ 1 } : std::vector<std::size_t>{ 1, 8 };
  for (const std::size_t threads : thread_counts) {
    // Thread t counts in counters[t][0]. The counters are 64 bytes apart, a cache line, so no two
    // threads write the same line. alignas would pad the type, which MSVC reports as C4324.
    std::vector<std::array<std::size_t, 64 / sizeof(std::size_t)>> counters(threads);
    st.per_op(prefix + "/t" + std::to_string(threads), threads, [&](std::size_t t) {
      const auto key = document_key(((t * thread_stride) + counters[t][0]++) % document_count);
      const auto err = write ? c.upsert(key, body).get().first : c.get(key).get().first;
      if (err) {
        throw std::runtime_error("operation failed: " + describe(err));
      }
      return true;
    });
  }
  if (write) {
    measure_pipelined(st, prefix, [&](std::size_t n) {
      return c.upsert(document_key(n % document_count), body);
    });
  } else {
    measure_pipelined(st, prefix, [&](std::size_t n) {
      return c.get(document_key(n % document_count));
    });
  }
}
// Runs start(0) to start(count - 1), each returning the future of a KV operation, with
// pipeline_depth of them in flight. Failures other than `tolerated` are counted; once there are
// max_failures of them no further operation starts. Returns the message of the first such failure
// once the started ones finish, or an empty string, and whether every operation was started.
template<typename Start>
auto
pipelined(std::size_t count,
          Start start,
          std::error_code tolerated = {},
          std::size_t max_failures = 1) -> std::pair<std::string, bool>
{
  std::deque<decltype(start(std::size_t{}))> window;
  std::string failure;
  std::size_t failures = 0;
  std::size_t next = 0;
  const auto may_start = [&] {
    return next < count && failures < max_failures;
  };
  while (!window.empty() || may_start()) {
    if (may_start() && window.size() < pipeline_depth) {
      window.push_back(start(next++));
      continue;
    }
    if (auto err = window.front().get().first; err && err.ec() != tolerated) {
      if (failures++ == 0) {
        failure = describe(err);
      }
    }
    window.pop_front();
  }
  return { failure, next == count };
}

// Removes the documents at keys 0 to count - 1. A document already gone, such as one a failed
// seeding never stored, is not a failure. Scattered failures leave the rest attempted; after
// pipeline_depth of them, as a bucket that stopped answering gives, no further removal starts, so
// cleanup ends within about one timeout per window rather than one per document. A failure is
// written to stderr and does not fail the case: the measurement is over, and the keys belong to
// this run only.
inline void
remove_documents(const collection& c, std::size_t count)
{
  const auto [failure, all_started] = pipelined(
    count,
    [&c](std::size_t i) {
      return c.remove(document_key(i));
    },
    errc::key_value::document_not_found,
    pipeline_depth);
  if (!failure.empty()) {
    std::cerr << "removing the documents of " << run_prefix() << " failed: " << failure
              << (all_started ? "" : "; removal stopped, so documents with this prefix may remain")
              << '\n';
  }
}

// Connects in profile p, stores the documents and measures the variants of p. The connection is
// closed however the measurement ends. Each profile connects, and seeds, whether or not the filter
// selects one of its keys.
inline void
connect_and_measure(state& st,
                    std::string_view target,
                    profile p,
                    const std::string& connection_string,
                    const std::string& username,
                    const std::string& password,
                    const std::string& bucket,
                    bool write)
{
  require_no_profile_option(connection_string);
  auto [err, c] = cluster::connect(connection_string, connect_options(username, password, p)).get();
  if (err) {
    throw std::runtime_error("connect failed: " + describe(err));
  }
  const auto coll = c.bucket(bucket).default_collection();
  try {
    seed(coll);
    measure(st, target, p, coll, write);
  } catch (...) {
    remove_documents(coll, document_count);
    c.close().get();
    throw;
  }
  remove_documents(coll, document_count);
  c.close().get();
}

constexpr std::size_t upsert_get_document_count = 100000;
constexpr std::size_t upsert_get_threads = 20;
constexpr std::size_t upsert_get_document_size = 512;
// The payload that makes the JSON of document_body() upsert_get_document_size bytes.
constexpr std::size_t upsert_get_payload_size = 446;

// Stores value at keys 0 to upsert_get_document_count - 1 through raw_json_transcoder. The first
// failure throws, which fails the case.
inline void
seed_upsert_get(const collection& c, const std::string& value)
{
  if (auto failure = pipelined(upsert_get_document_count,
                               [&c, &value](std::size_t i) {
                                 return c.upsert<codec::raw_json_transcoder>(document_key(i),
                                                                             value);
                               })
                       .first;
      !failure.empty()) {
    throw std::runtime_error("seeding failed: " + failure);
  }
}

inline void
close_clusters(std::vector<cluster>& clusters)
{
  for (auto& c : clusters) {
    c.close().get();
  }
}

// Connects n clusters concurrently in profile p and passes them to body. They are closed however
// body ends. When a connection fails, the others are closed and it throws.
template<typename Body>
void
with_clusters(std::size_t n,
              const std::string& connection_string,
              const std::string& username,
              const std::string& password,
              profile p,
              Body body)
{
  require_no_profile_option(connection_string);
  const auto options = connect_options(username, password, p);
  std::vector<std::future<std::pair<error, cluster>>> pending;
  for (std::size_t i = 0; i < n; ++i) {
    pending.push_back(cluster::connect(connection_string, options));
  }
  std::vector<cluster> clusters;
  std::string failure;
  for (auto& f : pending) {
    auto [err, c] = f.get();
    if (!err) {
      clusters.push_back(std::move(c));
    } else if (failure.empty()) {
      failure = "connect failed: " + describe(err);
    }
  }
  try {
    if (!failure.empty()) {
      throw std::runtime_error(failure);
    }
    body(clusters);
  } catch (...) {
    close_clusters(clusters);
    throw;
  }
  close_clusters(clusters);
}

// How one upsert_get operation stores and reads its document.
enum class value_path : std::uint8_t {
  // Builds document_body(upsert_get_payload_size) and stores it through the default transcoder;
  // the get decodes it into a tao::json::value.
  json,
  // Stores the document serialised once through raw_json_transcoder; the get reads it back
  // through raw_json_transcoder without parsing.
  raw,
  // Stores as json does; the get copies the value into a std::string through raw_json_transcoder.
  mixed,
};

inline auto
make_temp_directory(std::string_view prefix) -> std::filesystem::path
{
  static std::atomic<std::uint64_t> seq{ 0 };
  auto directory = std::filesystem::temp_directory_path() /
                   (std::string{ prefix } + run_prefix().substr(7, 16) + "-" +
                    std::to_string(seq.fetch_add(1, std::memory_order_relaxed)));
  std::filesystem::create_directories(directory);
  return directory;
}

// Forwards writes and flushes to `file` and records any exception in `failed`. spdlog catches
// sink exceptions inside its error handler, so recording them lets the benchmark fail the key.
class failure_recording_sink final : public spdlog::sinks::base_sink<spdlog::details::null_mutex>
{
public:
  failure_recording_sink(std::shared_ptr<spdlog::sinks::sink> file,
                         std::shared_ptr<std::atomic<bool>> failed)
    : file_{ std::move(file) }
    , failed_{ std::move(failed) }
  {
  }

protected:
  void sink_it_(const spdlog::details::log_msg& msg) override
  {
    try {
      file_->log(msg);
    } catch (const std::exception&) {
      failed_->store(true);
    }
  }

  void flush_() override
  {
    try {
      file_->flush();
    } catch (const std::exception&) {
      failed_->store(true);
    }
  }

  void set_pattern_(const std::string& pattern) override
  {
    file_->set_pattern(pattern);
  }

  void set_formatter_(std::unique_ptr<spdlog::formatter> formatter) override
  {
    file_->set_formatter(std::move(formatter));
  }

private:
  std::shared_ptr<spdlog::sinks::sink> file_;
  std::shared_ptr<std::atomic<bool>> failed_;
};

// Routes the SDK's log, at trace level, to rotating files in a temporary directory and to no other
// sink until the object is destroyed, then removes the directory. `wait` bounds waiting for the
// first write and for a drain marker. `file` replaces the rotating file sink in tests.
class trace_file_log
{
public:
  static constexpr std::size_t trace_file_size = 32 * 1024 * 1024;
  static constexpr std::size_t trace_file_count = 2;

  explicit trace_file_log(std::chrono::milliseconds wait = std::chrono::seconds{ 10 },
                          std::shared_ptr<spdlog::sinks::sink> file = nullptr)
    : directory_{ make_temp_directory("cxx-bench-trace-") }
    , wait_{ wait }
  {
    try {
      if (file == nullptr) {
        // The rotating sink counts the files it keeps besides the one it writes.
        file = std::make_shared<spdlog::sinks::rotating_file_sink_st>(
          (directory_ / "sdk.log").string(), trace_file_size, trace_file_count - 1);
      }
      recording_ = std::make_shared<failure_recording_sink>(std::move(file), failed_);
      sinks_->add_sink(recording_);
      sinks_->add_sink(counter_);
      // Last, so that the marker has been written when this sink sees it. Outside a drain() it
      // costs a measured message one relaxed load.
      sinks_->add_sink(std::make_shared<spdlog::sinks::callback_sink_st>(
        [state = drain_](const spdlog::details::log_msg& msg) {
          if (state->armed.load(std::memory_order_relaxed) &&
              std::string_view{ msg.payload.data(), msg.payload.size() } == drain_marker) {
            const std::scoped_lock lock(state->mutex);
            if (state->pending) {
              state->pending->set_value();
              state->pending.reset();
            }
          }
        }));
      core::logger::configuration config{};
      config.console = false;
      config.log_level = core::logger::level::trace;
      config.sink = sinks_;
      if (auto failure = core::logger::create_file_logger(config); failure.has_value()) {
        throw std::runtime_error("the trace log could not be opened: " + failure.value());
      }
    } catch (...) {
      remove_files();
      throw;
    }
  }

  trace_file_log(const trace_file_log&) = delete;
  trace_file_log(trace_file_log&&) = delete;
  auto operator=(const trace_file_log&) -> trace_file_log& = delete;
  auto operator=(trace_file_log&&) -> trace_file_log& = delete;

  // Waits up to `wait` for at least one write, flushes the sink, and detaches the preflight
  // counter.
  void require_written()
  {
    const auto deadline = std::chrono::steady_clock::now() + wait_;
    while (written_->load(std::memory_order_relaxed) == 0) {
      if (std::chrono::steady_clock::now() >= deadline) {
        throw std::runtime_error("nothing was written to the trace log in " + directory_.string());
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{ 10 });
    }
    // On this thread, so that a file that accepts writes into its buffer but cannot store them
    // fails here.
    sinks_->flush();
    sinks_->remove_sink(counter_);
    require_no_failed_write();
  }

  // Logs a marker through the single-worker async logger, waits up to `wait` for the marker sink
  // to observe it, and flushes `sinks_`.
  void drain()
  {
    auto marker_seen = std::make_shared<std::promise<void>>();
    auto drained = marker_seen->get_future();
    {
      const std::scoped_lock lock(drain_->mutex);
      drain_->pending = marker_seen;
    }
    // The marker is queued after this store, and the queue orders it before the sink's load.
    drain_->armed.store(true, std::memory_order_relaxed);
    core::logger::detail::log(
      __FILE__, __LINE__, "trace_file_log::drain", core::logger::level::critical, drain_marker);
    const bool arrived = drained.wait_for(wait_) == std::future_status::ready;
    drain_->armed.store(false, std::memory_order_relaxed);
    {
      const std::scoped_lock lock(drain_->mutex);
      drain_->pending.reset();
    }
    if (!arrived) {
      throw std::runtime_error("the trace log in " + directory_.string() + " did not drain");
    }
    // The marker shows the messages reached the files' buffers. Flushing them on this thread puts
    // their bytes on the same side of a resource usage sample as the messages.
    sinks_->flush();
  }

  // Drains the logger, detaches the file sink so nothing written later counts, flushes it on this
  // thread, stops the logger, and throws if any write or flush failed.
  void finish()
  {
    drain();
    sinks_->set_sinks({});
    recording_->flush();
    core::logger::reset();
    core::logger::shutdown();
    require_no_failed_write();
  }

  ~trace_file_log()
  {
    core::logger::reset();
    core::logger::shutdown();
    remove_files();
  }

private:
  static constexpr std::string_view drain_marker{ "trace_file_log: drained" };

  // The promise the next drain marker fulfils, if a drain() waits for one. armed is set while it
  // does, so that the marker sink compares no payload otherwise.
  struct drain_state {
    std::atomic<bool> armed{ false };
    std::mutex mutex{};
    std::shared_ptr<std::promise<void>> pending{};
  };

  void require_no_failed_write() const
  {
    if (failed_->load()) {
      throw std::runtime_error("a write to the trace log in " + directory_.string() + " failed");
    }
  }

  void remove_files()
  {
    sinks_->set_sinks({});
    recording_.reset();
    std::error_code ec;
    std::filesystem::remove_all(directory_, ec);
    if (ec) {
      std::cerr << "removing " << directory_.string() << " failed: " << ec.message() << '\n';
    }
  }

  std::filesystem::path directory_;
  std::chrono::milliseconds wait_;
  std::shared_ptr<spdlog::sinks::sink> recording_{};
  std::shared_ptr<drain_state> drain_{ std::make_shared<drain_state>() };
  std::shared_ptr<spdlog::sinks::dist_sink_mt> sinks_{
    std::make_shared<spdlog::sinks::dist_sink_mt>()
  };
  std::shared_ptr<std::atomic<bool>> failed_{ std::make_shared<std::atomic<bool>>(false) };
  std::shared_ptr<std::atomic<std::uint64_t>> written_{
    std::make_shared<std::atomic<std::uint64_t>>(0)
  };
  std::shared_ptr<spdlog::sinks::sink> counter_{ std::make_shared<spdlog::sinks::callback_sink_st>(
    [written = written_](const spdlog::details::log_msg& /* msg */) {
      written->fetch_add(1, std::memory_order_relaxed);
    }) };
};

// Waits, for at most `wait`, until the I/O thread of `c` has finished every ready handler.
// The public cluster runs its io_context on one thread. Posting to `io` waits out the active
// handler, and `io.poll()` drains queued strand handlers and posted continuations without
// waiting on idle sockets or timers. A handler can log after completing an operation's future,
// so this precedes a drain() that counts that message.
inline void
finish_io(const cluster& c,
          std::chrono::milliseconds wait,
          const std::function<void()>& after_post = {})
{
  auto passed = std::make_shared<std::promise<void>>();
  auto barrier = passed->get_future();
  auto& io = core::get_core_cluster(c).io_context();
  asio::post(io, [&io, passed]() {
    io.poll();
    passed->set_value();
  });
  if (after_post) {
    after_post();
  }
  if (barrier.wait_for(wait) != std::future_status::ready) {
    throw std::runtime_error("the cluster's I/O thread did not reach the barrier");
  }
}

// Finishes `trace` after the key of `variant` was measured. A failure fails that key, with no
// measurements, or the case when the filter left the key out.
inline void
finish_trace(state& st, trace_file_log& trace, const std::string& variant)
{
  try {
    trace.finish();
  } catch (const std::exception& e) {
    if (!st.fail_key(variant, e.what())) {
      throw;
    }
  }
}

// Runs body, which sets up a trace log and measures the key of `variant`. Any exception from
// setup, measurement, or drain marks that key failed with no measurements, or rethrows to fail
// the case when the filter left the key out.
template<typename Body>
void
with_trace_key(state& st, const std::string& variant, Body body)
{
  try {
    body();
  } catch (const std::exception& e) {
    if (!st.fail_key(variant, e.what())) {
      throw;
    }
  }
}

// Rejects active SDK protocol logging (TEST_LOG_PROTOCOL), which writes every packet at trace
// regardless of log level.
inline void
require_no_protocol_log()
{
  if (core::logger::should_log_protocol()) {
    throw std::invalid_argument(
      "the SDK protocol logger is active (check TEST_LOG_PROTOCOL); unset it");
  }
}

// Variants of kv_upsert_get. One operation is upsert(key, value) and then get(key), each waited
// for. Thread w, on its n-th operation, uses key (n * upsert_get_threads + w) %
// upsert_get_document_count. Each value_path is described at its enumerator.
//
// - t20: upsert_get_threads threads share one cluster in the default profile, value_path::json.
// - t20/raw: as t20 with value_path::raw.
// - t20/mixed: as t20 with value_path::mixed.
// - t20/mixed/trace: as t20/mixed, with the SDK logging at trace level to a file (trace_file_log).
// - t20/mixed/bare: as t20/mixed in the bare profile.
// - c20: as t20, but each thread has a cluster of its own, connected before and closed after the
//   measurement.
// - c20/mixed: as c20 with value_path::mixed.
//
// t20/mixed/trace runs last under with_trace_key(); a failed upsert or get marks only that key
// failed without measurements. In every other variant, a failed operation throws, failing the
// active key and stopping the case. Any failure before with_trace_key() also marks a selected
// trace key failed, and rethrows if another selected variant was not finished or the trace key was
// filtered out. Seeding, cluster connections, and trace setup run even when the filter selects
// none of these keys.
inline void
measure_upsert_get(state& st,
                   std::string_view target,
                   const std::string& connection_string,
                   const std::string& username,
                   const std::string& password,
                   const std::string& bucket)
{
  const auto key_name = [target](std::string_view variant) {
    return std::string{ target } + "/" + std::string{ variant };
  };
  const auto trace_key = key_name("t20/mixed/trace");
  std::size_t pending_other_keys = 0;
  for (const std::string_view variant : {
         "t20",
         "t20/raw",
         "t20/mixed",
         "t20/mixed/bare",
         "c20",
         "c20/mixed",
       }) {
    if (st.selected(key_name(variant))) {
      ++pending_other_keys;
    }
  }
  std::string raw;
  try {
    require_no_protocol_log();
    // Close the framework's idle probe connection after kv_cluster caches its metadata.
    st.ctx().close_backend();
    // Force the console logger to info so t20/mixed and t20/mixed/trace share one baseline across
    // executables regardless of TEST_LOG_LEVEL.
    if (!core::logger::is_initialized()) {
      core::logger::create_console_logger();
    }
    core::logger::set_log_levels(core::logger::level::info);
    raw = tao::json::to_string(document_body(upsert_get_payload_size));
    if (raw.size() != upsert_get_document_size) {
      throw std::logic_error("the upsert_get document is " + std::to_string(raw.size()) + " bytes");
    }
  } catch (const std::exception& e) {
    if (!st.fail_key(trace_key, e.what()) || pending_other_keys > 0) {
      throw;
    }
    return;
  }
  // settle runs right before each resource usage sample of the window (state::per_op()).
  const auto run = [&](std::string_view variant,
                       const std::vector<collection>& colls,
                       value_path path,
                       const std::function<void()>& settle = {}) {
    // Thread w counts in counters[w][0], a cache line apart from the other threads.
    std::vector<std::array<std::size_t, 64 / sizeof(std::size_t)>> counters(upsert_get_threads);
    const auto variant_key = key_name(variant);
    st.per_op(
      variant_key,
      upsert_get_threads,
      [&](std::size_t w) {
        const auto& c = colls[w % colls.size()];
        const auto key =
          document_key(((counters[w][0]++ * upsert_get_threads) + w) % upsert_get_document_count);
        const auto err = path == value_path::raw
                           ? c.upsert<codec::raw_json_transcoder>(key, raw).get().first
                           : c.upsert(key, document_body(upsert_get_payload_size)).get().first;
        if (err) {
          throw std::runtime_error("upsert failed: " + describe(err));
        }
        auto [get_err, r] = c.get(key).get();
        if (get_err) {
          throw std::runtime_error("get failed: " + describe(get_err));
        }
        if (path == value_path::json) {
          do_not_optimize(r.content_as<tao::json::value>());
        } else {
          do_not_optimize(r.content_as<std::string, codec::raw_json_transcoder>());
        }
        return true;
      },
      settle);
    if (variant != "t20/mixed/trace" && st.selected(variant_key)) {
      --pending_other_keys;
    }
  };
  const auto default_collection = [&bucket](std::vector<cluster>& clusters) {
    std::vector<collection> colls;
    for (auto& c : clusters) {
      colls.push_back(c.bucket(bucket).default_collection());
    }
    return colls;
  };
  // The documents are removed however the variants end. A cleanup that cannot connect is reported
  // and leaves the original failure, if any, to propagate.
  const auto cleanup = [&] {
    try {
      with_clusters(1,
                    connection_string,
                    username,
                    password,
                    profile::sdk_default,
                    [&](std::vector<cluster>& clusters) {
                      remove_documents(clusters.front().bucket(bucket).default_collection(),
                                       upsert_get_document_count);
                    });
    } catch (const std::exception& e) {
      std::cerr << "removing the documents of " << run_prefix() << " failed: " << e.what() << '\n';
    }
  };
  bool trace_reached = false;
  try {
    with_clusters(1,
                  connection_string,
                  username,
                  password,
                  profile::sdk_default,
                  [&](std::vector<cluster>& clusters) {
                    const auto colls = default_collection(clusters);
                    seed_upsert_get(colls.front(), raw);
                    run("t20", colls, value_path::json);
                    run("t20/raw", colls, value_path::raw);
                    run("t20/mixed", colls, value_path::mixed);
                  });
    with_clusters(
      1, connection_string, username, password, profile::bare, [&](std::vector<cluster>& clusters) {
        run("t20/mixed/bare", default_collection(clusters), value_path::mixed);
      });
    with_clusters(upsert_get_threads,
                  connection_string,
                  username,
                  password,
                  profile::sdk_default,
                  [&](std::vector<cluster>& clusters) {
                    const auto colls = default_collection(clusters);
                    run("c20", colls, value_path::json);
                    run("c20/mixed", colls, value_path::mixed);
                  });
    // Last, so that every earlier variant keeps the info console logger set above.
    trace_reached = true;
    with_trace_key(st, trace_key, [&]() {
      trace_file_log trace{};
      with_clusters(1,
                    connection_string,
                    username,
                    password,
                    profile::sdk_default,
                    [&](std::vector<cluster>& clusters) {
                      // Connecting logs at trace level, so the files are checked before the key.
                      trace.require_written();
                      // Drained before and after the window, behind the I/O thread's handlers, so
                      // that it counts the processing of its own messages and none of the
                      // connection's.
                      run(
                        "t20/mixed/trace", default_collection(clusters), value_path::mixed, [&]() {
                          finish_io(clusters.front(), std::chrono::seconds{ 10 });
                          trace.drain();
                        });
                    });
      finish_trace(st, trace, trace_key);
    });
  } catch (const std::exception& e) {
    cleanup();
    if (trace_reached || !st.fail_key(trace_key, e.what()) || pending_other_keys > 0) {
      throw;
    }
    return;
  } catch (...) {
    cleanup();
    throw;
  }
  cleanup();
}
} // namespace couchbase::test::bench::kv
