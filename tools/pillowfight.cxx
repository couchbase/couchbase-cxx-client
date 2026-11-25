/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "pillowfight.hxx"

#include "aimd_controller.hxx"
#include "document_body_generator.hxx"
#include "key_picker.hxx"
#include "overload_detector.hxx"
#include "throttle_sizing.hxx"
#include "token_bucket.hxx"

#include "core/utils/json.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>
#include <core/utils/binary.hxx>
#include <couchbase/best_effort_retry_strategy.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/codec_flags.hxx>

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <gsl/util>
#include <hdr/hdr_histogram.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/std.h>
#include <tao/json.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace cbc
{
namespace
{
enum class operation : std::uint8_t {
  cmd_get,
  cmd_replace,
  cmd_delete,
  cmd_insert,
  cmd_query,
};

struct operation_weights {
  std::size_t gets{ 1 };
  std::size_t replaces{ 1 };
  std::size_t deletes{ 0 };
  std::size_t inserts{ 0 };
  std::size_t queries{ 0 };

  [[nodiscard]] auto to_string() const -> std::string
  {
    return fmt::format("{}:{}:{}:{}:{}", gets, replaces, deletes, inserts, queries);
  }

  [[nodiscard]] auto to_vector() const -> std::vector<std::size_t>
  {
    return { gets, replaces, deletes, inserts, queries };
  }
};

class operation_generator
{
public:
  static auto parse(const std::string& input) -> operation_generator
  {
    operation_weights weights;

    std::stringstream is(input);
    std::string token;

    if (std::getline(is, token, ':')) {
      weights.gets = parse_ratio_term(token, weights.gets);
    }
    if (std::getline(is, token, ':')) {
      weights.replaces = parse_ratio_term(token, weights.replaces);
    }
    if (std::getline(is, token, ':')) {
      weights.deletes = parse_ratio_term(token, weights.inserts);
    }
    if (std::getline(is, token, ':')) {
      weights.inserts = parse_ratio_term(token, weights.inserts);
    }
    if (std::getline(is, token, ':')) {
      weights.queries = parse_ratio_term(token, weights.queries);
    }

    return operation_generator(weights);
  }

  [[nodiscard]] auto next_operation() -> operation
  {
    return operations_[distribution_(generator_)];
  }

  [[nodiscard]] auto to_string() const -> std::string
  {
    return weights_.to_string();
  }

private:
  explicit operation_generator(operation_weights weights)
    : weights_{ weights }
    , weights_vector_{ weights_.to_vector() }
    , distribution_{ weights_vector_.begin(), weights_vector_.end() }
  {
  }

  static auto parse_ratio_term(const std::string& term, std::size_t default_value) -> std::size_t
  {
    try {
      return std::stoull(term);
    } catch (const std::invalid_argument&) {
      /* fall through */
    } catch (const std::out_of_range&) {
      /* fall through */
    }
    return default_value;
  }

  std::random_device random_device_;
  std::mt19937 generator_{ random_device_() };

  std::vector<operation> operations_{ operation::cmd_get,
                                      operation::cmd_replace,
                                      operation::cmd_delete,
                                      operation::cmd_insert,
                                      operation::cmd_query };
  operation_weights weights_;
  std::vector<std::size_t> weights_vector_;
  std::discrete_distribution<std::size_t> distribution_;
};

constexpr const char* default_bucket_name{ "default" };
constexpr std::size_t default_number_of_io_threads{ 1 };
constexpr std::size_t default_number_of_worker_threads{ 1 };
constexpr const char* default_query_statement{
  "SELECT COUNT(*) FROM `{bucket_name}` WHERE type = \"fake_profile\""
};
constexpr operation_weights default_operation_ratio{};
constexpr std::size_t default_document_body_size{ 0 };
constexpr const char* default_document_body_fill{ "constant" };
constexpr const char* default_document_body_format{ "json" };
constexpr std::size_t default_document_body_pool_size{ 0 };

/*
 * A pool smaller than the storage compressor's look-back distance puts the same
 * body into one data block twice, which is the duplication a random fill exists
 * to remove. Measured over a cycled pool of distinct bodies, snappy stops
 * matching once the pool exceeds 16 KiB and lz4 once it exceeds 64 KiB; magma
 * takes its algorithm from magma_data_compression_algo, which is dynamic and
 * accepts zstd, and zstd still matches across roughly a megabyte.
 */
constexpr std::size_t minimum_document_body_pool_size{ 1024 * 1024 };
constexpr std::size_t minimum_pooled_documents{ 2 };

const std::vector<std::string> allowed_document_body_fills{ "constant", "random" };
const std::vector<std::string> allowed_document_body_formats{ "json", "binary" };
constexpr std::size_t default_operations_limit{ 0 };
constexpr std::size_t default_operation_batch_size{ 100 };
constexpr std::chrono::milliseconds default_batch_wait{ 0 };
constexpr std::size_t default_number_of_keys_to_populate{ 1'000 };
constexpr std::size_t minimum_population_batch_size{ 10 };

constexpr const char* default_json_doc = R"({
  "type": "fake_profile",
  "random": 91,
  "random float": 16.439,
  "bool": false,
  "date": "1996-10-23",
  "regEx": "hellooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo to you",
  "enum": "online",
  "firstname": "Anthia",
  "lastname": "Shields",
  "city": "Recife",
  "country": "Malawi",
  "countryCode": "CA",
  "email uses current data": "Anthia.Shields@gmail.com",
  "email from expression": "Anthia.Shields@yopmail.com",
  "array": [
    "Augustine",
    "Pearline",
    "Fredericka",
    "Dorothy",
    "Roz"
  ],
  "array of objects": [
    {
      "index": 0,
      "index start at 5": 5
    },
    {
      "index": 1,
      "index start at 5": 6
    },
    {
      "index": 2,
      "index start at 5": 7
    }
  ],
  "Mildrid": {
    "age": 33
  }
})";

// Set to false by the signal handler, which may touch only a lock-free atomic.
std::atomic<bool> running{ true };
static_assert(std::atomic<bool>::is_always_lock_free);

std::map<std::error_code, std::uint64_t> errors{};
std::mutex errors_mutex{};

void
sigint_handler(int /* signal */)
{
  running.store(false);
}

std::atomic_uint64_t total{ 0 };
// Completions whose error accounting threw: they are in total and the histogram, and may be
// missing from the error stats.
std::atomic_uint64_t failed_records{ 0 };
// Population upserts completed, counted per completion; only the per-second samples read it.
std::atomic_uint64_t populated{ 0 };
// Pipeline operations whose generation, deferral or send threw before their handler ran.
std::atomic_uint64_t failed_sends{ 0 };
// Pipeline operations reserved against --operations-limit, less those whose send failed.
std::atomic_size_t issued_operations{ 0 };
// Set while this thread is inside send_next. A handler the SDK runs inline from
// the send then defers its replacement instead of recursing.
thread_local bool in_send_next{ false };
hdr_histogram* histogram{ nullptr };

/**
 * Admission control of one run, owned by execute() and absent with --no-throttle. The detector and
 * the bucket are thread-safe. The controller is used only by the stats tick, and the next tick's
 * wait is armed only as the handler finishes, so ticks never overlap and the controller needs no
 * lock. decision is written by the tick under stats_tick::mutex; any other reader takes that mutex.
 */
struct throttle {
  throttle(std::size_t bucket_capacity, std::optional<double> ceiling, double floor)
    : bucket{ bucket_capacity,
              ceiling.value_or(token_bucket::unlimited),
              token_bucket::clock::now() }
    , controller{ ceiling, floor }
  {
  }

  std::shared_ptr<overload_detector> detector{ std::make_shared<overload_detector>(
    couchbase::make_best_effort_retry_strategy(),
    default_slow_threshold) };
  token_bucket bucket;
  aimd_controller controller;
  aimd_decision decision{}; // the last tick's; guarded by stats_tick::mutex
  // When the tick last read the bucket's grants; guarded by stats_tick::mutex.
  token_bucket::clock::time_point last_grant_sample{ token_bucket::clock::now() };
};

/**
 * Whether stderr is a terminal. Off a terminal every status line is a full line, and nothing
 * rewrites the current one.
 */
auto
stderr_is_terminal() -> bool
{
#ifdef _WIN32
  static const bool terminal = _isatty(_fileno(stderr)) != 0;
#else
  static const bool terminal = isatty(fileno(stderr)) != 0;
#endif
  return terminal;
}

/// Returns the sequence that rewrites the current terminal line, or nothing off a terminal.
auto
clear_line() -> const char*
{
  return stderr_is_terminal() ? "\r\033[K" : "";
}

/// "unlimited", or the rate in ops/s.
auto
describe_rate(double rate) -> std::string
{
  return std::isinf(rate) ? std::string{ "unlimited" } : fmt::format("{:.0f} ops/s", rate);
}

/**
 * Shared by the stats tick and execute(). Each tick runs entirely under mutex and re-arms only
 * while stopped is false, so once stop() returns no tick runs or re-arms, and the run's totals
 * below can be read without the lock. execute() arms the timer once, before any wait is pending;
 * after that only the tick arms it. Its destructor runs after io stops, and no other thread
 * cancels it.
 */
struct stats_tick {
  std::mutex mutex{};
  bool stopped{ false };
  bool workload_started{ false }; // the status line is printed once set
  // Event lines count their uptime from here; the status line from start_time.
  std::chrono::system_clock::time_point created{ std::chrono::system_clock::now() };
  std::chrono::system_clock::time_point start_time{};

  std::vector<std::uint64_t> per_second{}; // operations of every tick, population included
  std::uint64_t last_operations{ 0 };
  std::uint64_t temporary_failure_retries{ 0 };
  std::uint64_t cuts{ 0 };
  std::uint64_t backing_off_seconds{ 0 };
  overload_window unjudged{}; // mirrors the controller's accumulation up to its next judgement

  /// Population's load says nothing about the workload's, so the workload starts from the
  /// controller's initial rate, and population's completions and grants since the last tick are
  /// dropped rather than judged with the workload's. Their retries still count in the total.
  void start_workload(std::chrono::system_clock::time_point now, throttle* control)
  {
    const std::scoped_lock lock(mutex);
    start_time = now;
    workload_started = true;
    if (control != nullptr) {
      temporary_failure_retries += control->detector->take_window().temporary_failure_retries;
      const auto sampled = token_bucket::clock::now();
      control->bucket.take_granted();
      control->last_grant_sample = sampled;
      control->controller.restart();
      control->bucket.set_rate(control->controller.rate(), sampled);
      unjudged = {};
    }
  }

  void stop()
  {
    const std::scoped_lock lock(mutex);
    stopped = true;
  }

  /// Call after stop(). Without a throttle only the per-second rate is printed.
  void print_summary(throttle* control)
  {
    if (per_second.empty()) {
      fmt::print("Per-second rate: n/a\n");
    } else {
      auto sorted = per_second;
      std::sort(sorted.begin(), sorted.end());
      fmt::print("Per-second rate: min {}, median {}, max {} ops/s\n",
                 sorted.front(),
                 sorted[sorted.size() / 2],
                 sorted.back());
    }
    if (control != nullptr) {
      // The retries since the last tick.
      temporary_failure_retries += control->detector->take_window().temporary_failure_retries;
      fmt::print("Backing off: {}s, cuts: {}\n", backing_off_seconds, cuts);
      fmt::print("Temporary-failure retries: {}\n", temporary_failure_retries);
    }
  }
};

/**
 * Re-arms itself every second until tick.stop(). With a throttle, every tick feeds the detector's
 * window to the controller, applies its rate and prints a line for each cut and recovery, from
 * before population onwards.
 */
void
dump_stats(asio::steady_timer& timer, stats_tick& tick, throttle* control)
{
  struct stats_entry {
    std::chrono::system_clock::time_point timestamp;
    std::uint64_t operations{};
    std::uint64_t errors{};
  };

  static constexpr std::size_t max_window_size{ 3LLU * 60LLU }; // average on last 3 minutes only
  static std::deque<stats_entry> stats_window{};
  static std::uint64_t last_total{ 0 };
  static std::uint64_t last_errors{ 0 };

  timer.expires_after(std::chrono::seconds{ 1 });
  timer.async_wait([&timer, &tick, control](std::error_code ec) {
    if (ec == asio::error::operation_aborted) {
      return;
    }
    const std::scoped_lock lock(tick.mutex);
    if (tick.stopped) {
      return;
    }
    auto now = std::chrono::system_clock::now();
    const std::uint64_t operations = total + populated;
    tick.per_second.push_back(operations - tick.last_operations);
    tick.last_operations = operations;

    if (control != nullptr) {
      const auto event_uptime =
        std::chrono::duration_cast<std::chrono::seconds>(now - tick.created);
      const auto window = control->detector->take_window();
      // Read from the controller: before the first tick, decision holds no state.
      const bool was_unlimited = std::isinf(control->controller.rate());
      tick.temporary_failure_retries += window.temporary_failure_retries;
      // A late tick reads more than one second of grants.
      const auto sampled = token_bucket::clock::now();
      const auto admitted =
        admitted_rate(control->bucket.take_granted(), sampled - control->last_grant_sample);
      control->last_grant_sample = sampled;
      control->decision = control->controller.on_window(window, admitted);
      control->bucket.set_rate(control->decision.rate, sampled);

      tick.unjudged.completions += window.completions;
      tick.unjudged.slow += window.slow;
      tick.unjudged.temporary_failure_retries += window.temporary_failure_retries;
      const auto judged = tick.unjudged;
      if (judged.completions >= aimd_minimum_window_completions) {
        tick.unjudged = {};
      }
      if (control->decision.cut) {
        ++tick.cuts;
        const auto completions = static_cast<double>(judged.completions);
        fmt::print(stderr,
                   "{}[{}] overload: {:.1f}% temporary-failure retries, {:.1f}% slow; admitted "
                   "rate {} -> {:.0f} ops/s\n",
                   clear_line(),
                   event_uptime,
                   100.0 * static_cast<double>(judged.temporary_failure_retries) / completions,
                   100.0 * static_cast<double>(judged.slow) / completions,
                   was_unlimited ? std::string{ "unlimited" }
                                 : fmt::format("{:.0f}", control->decision.previous_rate),
                   control->decision.rate);
      } else if (control->decision.recovered) {
        fmt::print(stderr,
                   "{}[{}] recovered: admitted rate {}\n",
                   clear_line(),
                   event_uptime,
                   describe_rate(control->decision.rate));
      }
      if (control->decision.state == throttle_state::backing_off) {
        ++tick.backing_off_seconds;
      }
    }
    if (!tick.workload_started) {
      fflush(stderr);
      return dump_stats(timer, tick, control);
    }
    const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - tick.start_time);
    const std::uint64_t current_total = total;
    const std::uint64_t current_errors = [] {
      std::scoped_lock lock(errors_mutex);
      return std::accumulate(
        errors.begin(), errors.end(), std::uint64_t{ 0 }, [](auto acc, const auto& pair) {
          return acc + pair.second;
        });
    }();
    stats_window.push_back({
      now,
      current_total - last_total,
      current_errors - last_errors,
    });
    last_total = current_total;
    last_errors = current_errors;
    while (stats_window.size() > max_window_size) {
      stats_window.pop_front();
    }
    std::uint64_t window_ops{ 0 };
    std::uint64_t window_err{ 0 };
    for (const auto& [_, ops, errs] : stats_window) {
      window_ops += ops;
      window_err += errs;
    }
    const auto window_size = static_cast<double>(stats_window.size());
    double ops_rate{ 0 };
    double err_rate{ 0 };
    const auto window_start = stats_window.front().timestamp;
    const auto window_duration =
      std::chrono::duration_cast<std::chrono::seconds>(now - window_start).count();
    if (window_duration > 0) {
      ops_rate = static_cast<double>(window_ops) / window_size;
      err_rate = static_cast<double>(window_err) / window_size;
    }
    std::string admitted_field;
    if (control != nullptr) {
      admitted_field =
        fmt::format(", admitted: {}{}",
                    describe_rate(control->decision.rate),
                    control->decision.state == throttle_state::backing_off ? " (backing off)" : "");
    }
    fmt::print(stderr,
               "{}uptime: {}, rate: {} ops/s (avg {:.0f}){}, {:.2f} err/s, total: {}{}",
               clear_line(),
               uptime,
               stats_window.back().operations,
               ops_rate,
               admitted_field,
               err_rate,
               current_total,
               stderr_is_terminal() ? "" : "\n");

    fflush(stderr);
    return dump_stats(timer, tick, control);
  });
}

auto
uniq_id(const std::string& prefix) -> std::string
{
  return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

/**
 * The body is already encoded. Handing it over as an encoded_value skips the
 * JSON transcoder's extra copy of the body, the encode-function allocation and
 * the request-encoding span.
 */
auto
encode_body(const std::vector<std::byte>& body, bool binary) -> couchbase::codec::encoded_value
{
  return { body,
           binary ? couchbase::codec::codec_flags::binary_common_flags
                  : couchbase::codec::codec_flags::json_common_flags };
}

void
insert_document(const couchbase::collection& collection,
                std::string document_id,
                const std::vector<std::byte>& body,
                bool binary,
                couchbase::insert_handler&& handler)
{
  collection.insert(std::move(document_id), encode_body(body, binary), {}, std::move(handler));
}

void
replace_document(const couchbase::collection& collection,
                 std::string document_id,
                 const std::vector<std::byte>& body,
                 bool binary,
                 couchbase::replace_handler&& handler)
{
  collection.replace(std::move(document_id), encode_body(body, binary), {}, std::move(handler));
}

class pillowfight_app : public CLI::App
{
public:
  pillowfight_app()
    : CLI::App{ "Run workload generator.", "pillowfight" }
  {
    add_flag("--verbose", verbose_, "Include more context and information where it is applicable.");

    add_option("--bucket-name", bucket_name_, "Name of the bucket.")
      ->default_val(default_bucket_name);
    add_option("--document-body-size",
               document_body_size_,
               "Size of the body (if zero, it will use predefined document).")
      ->default_val(default_document_body_size);
    add_option("--document-body-fill",
               document_body_fill_,
               "How to fill a generated document body: \"constant\" repeats a single character, "
               "\"random\" draws every document from a seeded pseudo-random generator. Only "
               "\"random\" gives each document distinct bytes, which is what stops a loaded "
               "bucket from compressing.")
      ->default_val(default_document_body_fill)
      ->transform(CLI::IsMember(allowed_document_body_fills));
    add_option("--document-body-format",
               document_body_format_,
               "Format of a generated document: \"json\" wraps the body in a JSON object and "
               "restricts it to a JSON-safe alphabet, \"binary\" stores the body as opaque bytes "
               "of exactly --document-body-size.")
      ->default_val(default_document_body_format)
      ->transform(CLI::IsMember(allowed_document_body_formats));
    add_option("--document-body-pool-size",
               document_body_pool_size_,
               "Pre-generate this many bytes of document bodies per worker thread and cycle "
               "through them, instead of generating a fresh body for every document (zero). A "
               "pool has to be larger than the storage compressor's look-back distance or its "
               "documents compress again, so it is measured in bytes and not in documents.")
      ->default_val(default_document_body_pool_size);
    add_option("--document-body-seed",
               document_body_seed_,
               "Seed for --document-body-fill=random, so that a dataset can be reproduced. Each "
               "worker thread offsets it by its index. Without it the generator is seeded from "
               "the system entropy source.");
    // Deferred pipeline sends run on this I/O context, so it needs at least one thread.
    add_option("--number-of-io-threads",
               number_of_io_threads_,
               "Number of I/O threads for pillowfight's own timers and deferred sends. The SDK "
               "runs its own I/O thread.")
      ->default_val(default_number_of_io_threads)
      ->check(CLI::Range(std::size_t{ 1 }, std::numeric_limits<std::size_t>::max()));
    add_option("--number-of-keys-to-populate",
               number_of_keys_to_populate_,
               "Preload keys before running workload, so that the worker will not generate new "
               "keys afterwards.")
      ->default_val(default_number_of_keys_to_populate);
    add_option(
      "--number-of-worker-threads", number_of_worker_threads_, "Number of the worker threads.")
      ->default_val(default_number_of_worker_threads);
    add_option(
      "--operation-ratio",
      operation_ratio_string_,
      "The ratio of the operations to generate in form \"G:R:D:I:Q\", where letters represent "
      "ratio of the operations in whole numbers: Get, Replace, Delete, Insert and Query "
      "respectively. (e.g. 5:2:1:1:0 would do on average 5 gets for every insert)")
      ->default_val(default_operation_ratio.to_string());
    add_option("--operations-limit",
               operations_limit_,
               "Stop and exit after the number of the operations reaches this limit. (zero for "
               "running indefinitely). With --pipeline, a send that fails gives its place back and "
               "is replaced only while other operations are still completing, so a run whose last "
               "sends fail ends below the limit.")
      ->default_val(default_operations_limit);
    add_option("--operation-batch-size",
               operation_batch_size_,
               "Number of the operations in single batch operations (1 to wait for completion "
               "after each operation). With --pipeline, the number of operations in flight per "
               "worker thread.")
      ->default_val(default_operation_batch_size);
    auto* batch_wait = add_option("--batch-wait", batch_wait_, "Time to wait after the batch.")
                         ->default_val(default_batch_wait);
    add_flag("--pipeline",
             pipeline_,
             "Keep --operation-batch-size operations in flight per worker thread and send the next "
             "one as soon as any completes, instead of waiting for the whole batch. With "
             "--document-body-fill=random and no --document-body-pool-size, the next one is "
             "generated on pillowfight's I/O threads rather than the SDK's.")
      ->excludes(batch_wait);
    auto* no_throttle = add_flag("--no-throttle",
                                 no_throttle_,
                                 "Disable throttling and overload detection. On a busy server "
                                 "cluster the workload then generates more temporary failures "
                                 "and timeouts.");
    add_option("--rate-limit",
               rate_limit_,
               "Ceiling on total operations per second across all worker threads.")
      ->excludes(no_throttle)
      ->excludes(batch_wait);
    add_option("--query-statement",
               query_statement_,
               "The N1QL query statement to use ({bucket_name}, {scope_name} and {collection_name} "
               "will be substituted).")
      ->default_val(default_query_statement);
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);

    add_common_options(this, common_options_);
    allow_extras(true);

    parse_complete_callback([this]() {
      validate_options();
    });
  }

  [[nodiscard]] auto execute() const -> int
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    // Declared before io so that they outlive every handler: teardown joins the
    // workers, closes the cluster, stops the I/O, joins the I/O threads and
    // destroys io. A handler destroyed without running never dereferences them.
    std::optional<throttle> control;
    if (!no_throttle_) {
      const auto ceiling = has_rate_limit() ? std::optional<double>{ rate_limit_ } : std::nullopt;
      control.emplace(throttle_capacity(operation_batch_size_, minimum_population_batch_size),
                      ceiling,
                      throttle_floor(operation_batch_size_, ceiling));
      cluster_options.default_retry_strategy(control->detector);
    }
    throttle* const throttle_ptr = control ? &*control : nullptr;
    stats_tick tick;
    std::vector<batch_barrier> barriers(number_of_worker_threads_);
    std::vector<std::vector<std::string>> known_keys(number_of_worker_threads_);
    // Not scoped to the pipeline branch: abandoned handlers run at cluster close.
    std::deque<worker_state> workers;
    asio::io_context io;
    auto guard = asio::make_work_guard(io);

    std::vector<std::thread> io_pool{};
    io_pool.reserve(number_of_io_threads_);
    for (std::size_t i = 0; i < number_of_io_threads_; ++i) {
      io_pool.emplace_back([&io]() {
        io.run();
      });
    }

    hdr_init(/* minimum - 1 us*/ 1'000,
             /* maximum - 30 s*/ 30'000'000'000LL,
             /* significant figures */ 2,
             /* output pointer */ &histogram);

    std::signal(SIGINT, sigint_handler);
    std::signal(SIGTERM, sigint_handler);

    const auto connection_string = common_options_.connection.connection_string;

    fmt::print(stderr,
               "Workload Plan\n"
               "| Version: {}\n"
               "| Connection String: {}\n"
               "| Ratio: {} (Get:Replace:Delete:Insert:Query)\n"
               "| Mode: {}\n"
               "| Throttle: {}\n"
               "| Document body: {} {}, {}\n",
               couchbase::core::meta::sdk_semver(),
               connection_string,
               operation_generator::parse(operation_ratio_string_).to_string(),
               pipeline_ ? fmt::format("pipeline, window {} per worker", operation_batch_size_)
                         : fmt::format("batch, {} per batch", operation_batch_size_),
               describe_throttle(),
               document_body_fill_,
               document_body_format_,
               describe_document_body_size());

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {
      guard.reset();
      for (auto& thread : io_pool) {
        thread.join();
      }
      fail(fmt::format(
        "Failed to connect to the cluster at \"{}\": {}", connection_string, connect_err));
    }

    // Started before population, so that population is throttled too.
    asio::steady_timer stats_timer(io);
    dump_stats(stats_timer, tick, throttle_ptr);

    if (number_of_keys_to_populate_ > 0) {
      populate_keys(cluster, known_keys, throttle_ptr);
    }

    const auto start_time = std::chrono::system_clock::now();
    tick.start_workload(start_time, throttle_ptr);

    // One per pipeline worker, for operations waiting on a token. Declared after
    // io, because a timer must be destroyed before its io_context. Not scoped to
    // the pipeline branch: a timer handler may run until io stops.
    std::deque<asio::steady_timer> token_timers; // cppcheck-suppress variableScope
    std::vector<std::thread> worker_pool{};
    worker_pool.reserve(number_of_worker_threads_);
    if (pipeline_) {
      for (std::size_t i = 0; i < number_of_worker_threads_; ++i) {
        workers.emplace_back(
          *this, cluster, known_keys[i], i, throttle_ptr, token_timers.emplace_back(io), io);
      }
      for (auto& state : workers) {
        worker_pool.emplace_back([this, &state]() {
          pipeline_worker(state);
        });
      }
    } else {
      for (std::size_t i = 0; i < number_of_worker_threads_; ++i) {
        worker_pool.emplace_back([this,
                                  cluster = cluster,
                                  &keys = known_keys[i],
                                  &barrier = barriers[i],
                                  i,
                                  throttle_ptr]() {
          worker(cluster, keys, barrier, i, throttle_ptr);
        });
      }
    }
    for (auto& thread : worker_pool) {
      thread.join();
    }

    const auto finish_time = std::chrono::system_clock::now();
    tick.stop();

    fmt::print("\n\nTotal operations: {}\n", total);
    // Pipeline handlers append to known_keys under their worker's mutex until io stops.
    std::size_t keys_used{ 0 };
    for (std::size_t i = 0; i < known_keys.size(); ++i) {
      std::unique_lock<std::mutex> lock{};
      if (pipeline_) {
        lock = std::unique_lock{ workers[i].mutex };
      }
      keys_used += known_keys[i].size();
    }
    fmt::print("Total keys used: {}\n", keys_used);
    const auto total_time = finish_time - start_time;
    fmt::print("Total time: {}s ({}ms)\n",
               std::chrono::duration_cast<std::chrono::seconds>(total_time).count(),
               std::chrono::duration_cast<std::chrono::milliseconds>(total_time).count());
    if (auto diff = std::chrono::duration_cast<std::chrono::seconds>(total_time).count();
        diff > 0) {
      fmt::print("Total rate: {} ops/s\n", total / static_cast<std::uint64_t>(diff));
    }
    tick.print_summary(throttle_ptr);

    cluster.close().get();
    guard.reset();
    io.stop();

    {
      std::scoped_lock lock(errors_mutex);
      if (!errors.empty()) {
        fmt::print("Error stats:\n");
        for (auto [e, count] : errors) {
          fmt::print("    {}: {}\n", e.message(), count);
        }
      }
    }
    if (failed_records > 0) {
      fmt::print("Failed records (exception while recording a completion): {}\n", failed_records);
    }
    if (failed_sends > 0) {
      fmt::print("Failed sends (exception while generating, deferring or sending): {}\n",
                 failed_sends);
    }

    for (auto& thread : io_pool) {
      thread.join();
    }

    if (total > 0) {
      fmt::print("Latency distribution (in ms)\n");
      hdr_percentiles_print(histogram, stdout, 1, 1'000'000.0 /* in ms */, format_type::CLASSIC);
    }

    return 0;
  }

private:
  [[nodiscard]] auto has_rate_limit() const -> bool
  {
    return count("--rate-limit") > 0;
  }

  void validate_options() const
  {
    if (operation_batch_size_ == 0) {
      throw CLI::ValidationError("--operation-batch-size cannot be zero");
    }
    if (has_rate_limit() && !(rate_limit_ > 0 && std::isfinite(rate_limit_))) {
      throw CLI::ValidationError("--rate-limit must be a finite number greater than zero");
    }

    const auto fill = parse_body_fill(document_body_fill_);

    if (fill != body_fill::constant && document_body_size_ == 0) {
      throw CLI::ValidationError("--document-body-fill=" + document_body_fill_ +
                                 " requires a non-zero --document-body-size; without a size the "
                                 "predefined document is used and nothing is generated");
    }
    if (parse_body_format(document_body_format_) == body_format::binary &&
        document_body_size_ == 0) {
      throw CLI::ValidationError("--document-body-format=binary requires a non-zero "
                                 "--document-body-size, because the predefined document is JSON");
    }
    if (document_body_pool_size_ == 0) {
      return;
    }
    if (fill == body_fill::constant) {
      throw CLI::ValidationError("--document-body-pool-size applies only to "
                                 "--document-body-fill=random, which generates a body per "
                                 "document; a constant fill has only one body to pool");
    }
    // Refused rather than warned about: an undersized pool fails silently, by
    // producing exactly the compressible dataset the caller asked to avoid.
    if (document_body_pool_size_ < minimum_document_body_pool_size) {
      throw CLI::ValidationError(
        fmt::format("--document-body-pool-size must be at least {} bytes, or the pooled bodies "
                    "repeat within one storage block and compress; {} was given",
                    minimum_document_body_pool_size,
                    document_body_pool_size_));
    }
    if (document_body_pool_size_ / document_body_size_ < minimum_pooled_documents) {
      throw CLI::ValidationError(
        fmt::format("--document-body-pool-size of {} bytes holds fewer than {} documents of {} "
                    "bytes; raise the pool or lower --document-body-size",
                    document_body_pool_size_,
                    minimum_pooled_documents,
                    document_body_size_));
    }
  }

  [[nodiscard]] auto describe_throttle() const -> std::string
  {
    if (no_throttle_) {
      return "off";
    }
    if (has_rate_limit()) {
      return fmt::format("adaptive, ceiling {} ops/s", rate_limit_);
    }
    return "adaptive (no ceiling)";
  }

  [[nodiscard]] auto describe_document_body_size() const -> std::string
  {
    if (document_body_size_ == 0) {
      return "predefined document";
    }
    if (document_body_pool_size_ == 0) {
      return fmt::format("{} bytes, generated per document", document_body_size_);
    }
    return fmt::format(
      "{} bytes, cycling a {}-byte pool per worker", document_body_size_, document_body_pool_size_);
  }

  [[nodiscard]] auto make_body_generator(std::size_t worker_index) const -> document_body_generator
  {
    return { parse_body_fill(document_body_fill_),
             parse_body_format(document_body_format_),
             document_body_size_,
             document_body_pool_size_,
             seed_for_worker(worker_index),
             couchbase::core::utils::to_binary(default_json_doc) };
  }

  /**
   * Worker threads must not share a seed. Sharing one makes every thread emit
   * the same sequence of bodies, and documents that differ only by which thread
   * wrote them still meet in the same storage block.
   */
  [[nodiscard]] auto seed_for_worker(std::size_t worker_index) const -> std::uint64_t
  {
    if (count("--document-body-seed") > 0) {
      return document_body_seed_ + worker_index;
    }
    return std::random_device{}();
  }

  /**
   * One per worker, reused across its batches. A batch starts only after the
   * previous one drained, and none starts after a stop abandons one, so no
   * handler of an earlier batch can touch pending.
   */
  struct batch_barrier {
    std::atomic<std::size_t> pending{ 0 };
    std::atomic<bool> abandoned{ false };
    std::mutex mutex{};
    std::condition_variable completed{};
  };

  /**
   * Runs on an I/O thread, or inline in the send, for every operation that a
   * stop did not abandon. It waits for nothing but errors_mutex and, with
   * --verbose, stderr. Queries do not feed the detector: they never retry
   * with key_value_temporary_failure, and a healthy query can exceed the
   * slow-completion threshold.
   */
  void on_completion(operation op,
                     std::chrono::nanoseconds latency,
                     const couchbase::error& err,
                     throttle* control) const
  {
    hdr_record_value_atomic(histogram, latency.count());
    ++total;
    if (control != nullptr && op != operation::cmd_query) {
      control->detector->record(latency);
    }
    if (err.ec()) {
      const std::scoped_lock lock(errors_mutex);
      ++errors[err.ec()];
      if (verbose_) {
        fmt::print(stderr, "{}{}\n", clear_line(), err.ctx().to_json());
      }
    }
  }

  void worker(couchbase::cluster connected_cluster,
              std::vector<std::string>& known_keys,
              batch_barrier& barrier,
              std::size_t worker_index,
              throttle* control) const
  {
    auto cluster = std::move(connected_cluster);

    static thread_local std::mt19937_64 gen{ std::random_device()() };
    std::uniform_real_distribution<double> dist(0, 1);

    auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

    auto body = make_body_generator(worker_index);
    auto query_statement{ fmt::format(query_statement_, fmt::arg("bucket_name", bucket_name_)) };

    bool stopping{ false };
    auto operation_generator{ operation_generator::parse(operation_ratio_string_) };

    while (running.load() && !stopping) {
      if (control != nullptr && !acquire_tokens(*control, operation_batch_size_)) {
        break;
      }
      barrier.pending.store(operation_batch_size_, std::memory_order_release);
      auto make_handler = [this, &barrier, control](operation op) {
        return [this, barrier = &barrier, control, op, start = std::chrono::steady_clock::now()](
                 const couchbase::error& err, const auto& /* result */) {
          // Operations of an abandoned batch complete with request_canceled at
          // cluster close and are not recorded.
          if (!barrier->abandoned.load(std::memory_order_acquire)) {
            try {
              on_completion(op, std::chrono::steady_clock::now() - start, err, control);
            } catch (...) {
              // May run on an SDK I/O thread or inline in the send, and must still
              // drain the batch, so the exception is caught and the dropped record
              // counted in failed_records.
              ++failed_records;
            }
          }
          // Recorded before the decrement, so total covers the batch once it
          // drains. Holding the mutex keeps the notify from landing between the
          // worker's predicate check and its wait.
          if (barrier->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            const std::scoped_lock lock(barrier->mutex);
            barrier->completed.notify_one();
          }
        };
      };

      // Keys inserted by this batch may not be stored yet, so picks draw only from keys known
      // when the batch started.
      const auto keys_at_batch_start = known_keys.size();
      for (std::size_t i = 0; i < operation_batch_size_; ++i) {
        auto operation = operation_generator.next_operation();
        const auto known = operation != operation::cmd_insert
                             ? pick_known_key(keys_at_batch_start, gen)
                             : std::nullopt;
        std::string document_id = known ? known_keys[*known] : uniq_id("id");
        switch (operation) {
          case operation::cmd_get: {
            auto handler = make_handler(operation);
            collection.get(std::move(document_id), {}, std::move(handler));
          } break;
          case operation::cmd_replace: {
            const auto& document = body.next();
            auto handler = make_handler(operation);
            replace_document(
              collection, std::move(document_id), document, body.binary(), std::move(handler));
          } break;
          case operation::cmd_delete: {
            auto handler = make_handler(operation);
            collection.remove(std::move(document_id), {}, std::move(handler));
          } break;
          case operation::cmd_insert: {
            known_keys.push_back(document_id);
            const auto& document = body.next();
            auto handler = make_handler(operation);
            insert_document(
              collection, std::move(document_id), document, body.binary(), std::move(handler));
          } break;
          case operation::cmd_query: {
            auto handler = make_handler(operation);
            cluster.query(query_statement, {}, std::move(handler));
          } break;
        }
      }

      {
        std::unique_lock lock(barrier.mutex);
        while (!barrier.completed.wait_for(lock, acquire_wait_slice, [&barrier] {
          return barrier.pending.load(std::memory_order_acquire) == 0;
        })) {
          if (!running.load()) {
            stopping = true;
            barrier.abandoned.store(true, std::memory_order_release);
            break;
          }
        }
      }

      if (stopping || (operations_limit_ > 0 && total >= operations_limit_)) {
        running.store(false);
      } else {
        if (batch_wait_ != std::chrono::milliseconds::zero()) {
          std::this_thread::sleep_for(batch_wait_);
        }
      }
    }
  }

  /**
   * One per pipeline worker, owned by execute() with the same lifetime as a
   * batch_barrier. Completion handlers of this worker generate the next
   * operation on SDK threads and on io, so the generator state below mutex is
   * read and written only while mutex is held, and never during a send.
   */
  struct worker_state {
    worker_state(const pillowfight_app& app,
                 couchbase::cluster connected_cluster,
                 std::vector<std::string>& keys,
                 std::size_t worker_index,
                 throttle* throttle_control,
                 asio::steady_timer& timer,
                 asio::io_context& io_context)
      : control{ throttle_control }
      , token_timer{ timer }
      , cluster{ std::move(connected_cluster) }
      , collection{ cluster.bucket(app.bucket_name_)
                      .scope(app.scope_name_)
                      .collection(app.collection_name_) }
      , query_statement{ fmt::format(app.query_statement_,
                                     fmt::arg("bucket_name", app.bucket_name_)) }
      , io{ io_context }
      , operations{ operation_generator::parse(app.operation_ratio_string_) }
      , body{ app.make_body_generator(worker_index) }
      , known_keys{ keys }
    {
    }

    // pending counts the operations in flight or waiting for a token.
    batch_barrier window{};
    throttle* control;

    // Operations waiting for a token; token_timer is armed while deferred > 0.
    std::mutex token_mutex{};
    asio::steady_timer& token_timer;
    std::size_t deferred{ 0 };
    couchbase::cluster cluster;
    couchbase::collection collection;
    std::string query_statement;
    asio::io_context& io;

    std::mutex mutex{};
    operation_generator operations;
    std::mt19937_64 gen{ std::random_device{}() };
    document_body_generator body;
    std::vector<std::string>& known_keys;
  };

  [[nodiscard]] static auto still_running() -> bool
  {
    return running.load();
  }

  /// Reserves one operation against --operations-limit. False once the limit is reached. A failed
  /// send gives its reservation back, and only a later completion can reserve it again, so a run
  /// whose last sends fail ends below the limit.
  [[nodiscard]] auto reserve_operation() const -> bool
  {
    if (const auto reserved = issued_operations.fetch_add(1, std::memory_order_relaxed);
        operations_limit_ == 0 || reserved < operations_limit_) {
      return true;
    }
    issued_operations.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }

  /// Takes n tokens, which may exceed the bucket's capacity. False when a stop came first.
  static auto acquire_tokens(throttle& control, std::size_t n) -> bool
  {
    return acquire_in_chunks(control.bucket, n, [] {
      return !still_running();
    });
  }

  static void release_slot(worker_state& state)
  {
    // Holding the mutex keeps the notify from landing between the worker's
    // predicate check and its wait.
    if (state.window.pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      const std::scoped_lock lock(state.window.mutex);
      state.window.completed.notify_one();
    }
  }

  /**
   * Reserves a slot for one operation and sends it, or, when the bucket has no
   * token, leaves it to the worker's token timer. Runs on the worker thread for
   * the first window and on SDK threads after that, so it never waits.
   * It also runs on io, from the inline handler of a posted or token-timer send.
   * Called from a handler that the SDK runs inline inside send_next, it posts
   * the send to io instead, so that the stack stays bounded. A replacement
   * whose body is generated per document is posted as well, so that the
   * generation does not run on the SDK's I/O thread.
   */
  void issue_next(worker_state& state, bool replacement) const
  {
    state.window.pending.fetch_add(1, std::memory_order_acq_rel);
    if (state.control != nullptr) {
      if (const auto wait = state.control->bucket.try_take(1, token_bucket::clock::now());
          wait != token_bucket::clock::duration::zero()) {
        try {
          const std::scoped_lock lock(state.token_mutex);
          if (state.deferred == 0) {
            arm_token_timer(state, wait);
          }
          ++state.deferred;
        } catch (...) {
          fail_reserved(state, 1);
        }
        return;
      }
    }
    if (in_send_next || (replacement && state.body.generates_per_document())) {
      post_send(state);
      return;
    }
    send_next(state);
  }

  /// Gives back n reserved slots whose operations were never sent, counting them in failed_sends.
  static void fail_reserved(worker_state& state, std::size_t n)
  {
    failed_sends += n;
    issued_operations.fetch_sub(n, std::memory_order_relaxed);
    for (; n > 0; --n) {
      release_slot(state);
    }
  }

  /// Sends into a reserved slot from io. After a stop, gives the slot back instead.
  void post_send(worker_state& state) const
  {
    try {
      asio::post(state.io, [this, &state] {
        if (state.window.abandoned.load(std::memory_order_acquire) || !still_running()) {
          release_slot(state);
          return;
        }
        send_next(state);
      });
    } catch (...) {
      fail_reserved(state, 1);
    }
  }

  /// Requires state.token_mutex. A wait of one slice at most picks up a raised rate and a stop.
  void arm_token_timer(worker_state& state, token_bucket::clock::duration wait) const
  {
    state.token_timer.expires_after(
      std::min<token_bucket::clock::duration>(wait, acquire_wait_slice));
    state.token_timer.async_wait([this, &state](std::error_code ec) {
      if (!ec) { // aborted only at teardown, after the window was abandoned
        on_token_timer(state);
      }
    });
  }

  /**
   * Runs on pillowfight's io_context. Sends as many deferred operations as the
   * bucket grants and re-arms for the rest. After a stop, gives their slots
   * back instead.
   */
  void on_token_timer(worker_state& state) const
  {
    std::size_t ready{ 0 };
    std::size_t dropped{ 0 };
    std::size_t failed{ 0 };
    {
      const std::scoped_lock lock(state.token_mutex);
      if (state.window.abandoned.load(std::memory_order_acquire) || !still_running()) {
        dropped = std::exchange(state.deferred, 0);
      } else {
        while (state.deferred > 0) {
          const auto wait = state.control->bucket.try_take(1, token_bucket::clock::now());
          if (wait != token_bucket::clock::duration::zero()) {
            try {
              arm_token_timer(state, wait);
            } catch (...) {
              failed = std::exchange(state.deferred, 0);
            }
            break;
          }
          --state.deferred;
          ++ready;
        }
      }
    }
    for (; ready > 0; --ready) {
      send_next(state);
    }
    for (; dropped > 0; --dropped) {
      release_slot(state);
    }
    fail_reserved(state, failed);
  }

  /**
   * Generates one operation under state.mutex and sends it outside the lock,
   * into a slot issue_next reserved. An exception while generating or sending
   * gives the slot back and counts in failed_sends, unless the handler ran
   * first: settled lets exactly one of the two settle the operation.
   */
  void send_next(worker_state& state) const
  {
    in_send_next = true;
    const auto leave = gsl::finally([] {
      in_send_next = false;
    });
    std::shared_ptr<std::atomic<bool>> settled;
    try {
      settled = std::make_shared<std::atomic<bool>>(false);
      operation op{};
      std::string document_id;
      std::optional<couchbase::codec::encoded_value> value;
      {
        const std::scoped_lock lock(state.mutex);
        op = state.operations.next_operation();
        auto& keys = state.known_keys;
        const auto known =
          op != operation::cmd_insert ? pick_known_key(keys.size(), state.gen) : std::nullopt;
        document_id = known ? keys[*known] : uniq_id("id");
        if (op == operation::cmd_insert) {
          keys.push_back(document_id);
        }
        if (op == operation::cmd_insert || op == operation::cmd_replace) {
          value = encode_body(state.body.next(), state.body.binary());
        }
      }

      auto handler = [this, state = &state, settled, op, start = std::chrono::steady_clock::now()](
                       const couchbase::error& err, const auto& /* result */) {
        if (settled->exchange(true, std::memory_order_acq_rel)) {
          return;
        }
        // Operations abandoned by a stop complete with request_canceled at
        // cluster close and are neither recorded nor replaced.
        if (!state->window.abandoned.load(std::memory_order_acquire)) {
          try {
            on_completion(op, std::chrono::steady_clock::now() - start, err, state->control);
          } catch (...) {
            // May run on an SDK I/O thread or inline in the send, and must still
            // settle the slot, so the exception is caught and the dropped record
            // counted in failed_records.
            ++failed_records;
          }
          if (still_running() && reserve_operation()) {
            issue_next(*state, true);
          }
        }
        // After the replacement is in flight, so the window drains only when
        // nothing replaces this operation.
        release_slot(*state);
      };
      switch (op) {
        case operation::cmd_get:
          state.collection.get(std::move(document_id), {}, std::move(handler));
          break;
        case operation::cmd_replace:
          state.collection.replace(
            std::move(document_id), std::move(*value), {}, std::move(handler));
          break;
        case operation::cmd_delete:
          state.collection.remove(std::move(document_id), {}, std::move(handler));
          break;
        case operation::cmd_insert:
          state.collection.insert(
            std::move(document_id), std::move(*value), {}, std::move(handler));
          break;
        case operation::cmd_query:
          state.cluster.query(state.query_statement, {}, std::move(handler));
          break;
      }
    } catch (...) {
      if (!settled || !settled->exchange(true, std::memory_order_acq_rel)) {
        fail_reserved(state, 1);
      }
    }
  }

  /**
   * Starts the window, then waits until it drains (--operations-limit
   * reached, or every send failed) or a stop abandons it.
   */
  void pipeline_worker(worker_state& state) const
  {
    for (std::size_t i = 0; i < operation_batch_size_ && still_running() && reserve_operation();
         ++i) {
      issue_next(state, false);
    }
    std::unique_lock lock(state.window.mutex);
    while (!state.window.completed.wait_for(lock, acquire_wait_slice, [&state] {
      return state.window.pending.load(std::memory_order_acquire) == 0;
    })) {
      if (!running.load()) {
        state.window.abandoned.store(true, std::memory_order_release);
        break;
      }
    }
  }

  /**
   * Upserts --number-of-keys-to-populate new keys per worker, or until a stop
   * is requested, and appends the stored ones to the worker's known_keys. With
   * a throttle, each batch takes its tokens first, and each upsert feeds the
   * detector with its own latency, measured from send to callback.
   */
  void populate_keys(const couchbase::cluster& cluster,
                     std::vector<std::vector<std::string>>& known_keys,
                     throttle* control) const
  {
    const std::size_t total_keys{ number_of_worker_threads_ * number_of_keys_to_populate_ };

    auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

    const auto start_time = std::chrono::system_clock::now();

    std::size_t stored_keys{ 0 };
    std::size_t retried_keys{ 0 };
    for (std::size_t i = 0; i < number_of_worker_threads_; ++i) {
      auto keys_left = number_of_keys_to_populate_;
      auto body = make_body_generator(i);

      while (keys_left > 0 && still_running()) {
        if (stderr_is_terminal()) {
          fmt::print(stderr,
                     "\r\033[K{:02.2f}% {} of {}, {}\r",
                     static_cast<double>(stored_keys) / gsl::narrow_cast<double>(total_keys) * 100,
                     stored_keys,
                     total_keys,
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now() - start_time));
        }

        auto batch_size =
          std::min(keys_left, std::max(operation_batch_size_, minimum_population_batch_size));
        if (control != nullptr && !acquire_tokens(*control, batch_size)) {
          break;
        }

        std::vector<std::pair<std::string, std::future<couchbase::error>>> futures;
        futures.reserve(batch_size);
        for (std::size_t k = 0; k < batch_size; ++k) {
          std::string document_id = uniq_id("id");
          // Every result is read below, so each handler has run, and finished with
          // control, before populate_keys returns.
          auto result = std::make_shared<std::promise<couchbase::error>>();
          futures.emplace_back(document_id, result->get_future());
          collection.upsert(
            std::move(document_id),
            encode_body(body.next(), body.binary()),
            {},
            [result, control, start = std::chrono::steady_clock::now()](
              const couchbase::error& err, const couchbase::mutation_result& /* result */) {
              if (control != nullptr) {
                control->detector->record(std::chrono::steady_clock::now() - start);
              }
              ++populated;
              result->set_value(err);
            });
        }

        for (auto&& [document_id, future] : futures) {
          const auto ctx = future.get();
          if (ctx.ec()) {
            ++retried_keys;
          } else {
            known_keys[i].emplace_back(document_id);
            ++stored_keys;
            --keys_left;
          }
        }
      }
    }
    const auto finish_time = std::chrono::system_clock::now();
    const auto total_time = finish_time - start_time;

    fmt::print(stderr,
               "{}{} keys populated in {}s ({}ms) with {} retries\n",
               clear_line(),
               stored_keys,
               std::chrono::duration_cast<std::chrono::seconds>(total_time).count(),
               std::chrono::duration_cast<std::chrono::milliseconds>(total_time).count(),
               retried_keys);
  }

  common_options common_options_{};

  std::string bucket_name_{ default_bucket_name };
  std::string scope_name_{ couchbase::scope::default_name };
  std::string collection_name_{ couchbase::collection::default_name };
  bool verbose_{ false };
  bool pipeline_{ false };
  bool no_throttle_{ false };
  double rate_limit_{ 0 };
  std::size_t operation_batch_size_{};
  std::chrono::milliseconds batch_wait_{};
  std::size_t number_of_io_threads_{};
  std::size_t number_of_worker_threads_{};
  std::size_t number_of_keys_to_populate_{};
  std::string operation_ratio_string_{ default_operation_ratio.to_string() };
  std::string query_statement_;
  std::string document_body_fill_{ default_document_body_fill };
  std::string document_body_format_{ default_document_body_format };
  std::size_t document_body_pool_size_{ default_document_body_pool_size };
  std::uint64_t document_body_seed_{ 0 };
  std::size_t document_body_size_{};
  std::size_t operations_limit_{};
};
} // namespace

auto
make_pillowfight_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<pillowfight_app>();
}

auto
execute_pillowfight_command(const CLI::App* app) -> int
{
  if (const auto* pillowfight = dynamic_cast<const pillowfight_app*>(app); pillowfight != nullptr) {
    return pillowfight->execute();
  }
  return 1;
}
} // namespace cbc
