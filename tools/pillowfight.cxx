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

#include "core/utils/json.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>
#include <core/utils/binary.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/binary_noop_serializer.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <gsl/util>
#include <hdr/hdr_histogram.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/std.h>
#include <tao/json.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

#include <csignal>
#include <deque>
#include <numeric>
#include <random>
#include <thread>

namespace cbc
{
namespace
{
enum class operation {
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
      return default_value;
    } catch (const std::out_of_range&) {
      return default_value;
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
  std::discrete_distribution<> distribution_;
};

constexpr const char* default_bucket_name{ "default" };
constexpr std::size_t default_number_of_io_threads{ 1 };
constexpr std::size_t default_number_of_worker_threads{ 1 };
constexpr const char* default_query_statement{
  "SELECT COUNT(*) FROM `{bucket_name}` WHERE type = \"fake_profile\""
};
constexpr operation_weights default_operation_ratio{};
constexpr std::size_t default_document_body_size{ 0 };
constexpr std::size_t default_operations_limit{ 0 };
constexpr std::size_t default_operation_batch_size{ 100 };
constexpr std::chrono::milliseconds default_batch_wait{ 0 };
constexpr std::size_t default_number_of_keys_to_populate{ 1'000 };

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

using raw_json_transcoder =
  couchbase::codec::json_transcoder<couchbase::codec::binary_noop_serializer>;

std::atomic_flag running{ true };

std::map<std::error_code, std::uint64_t> errors{};
std::mutex errors_mutex{};

void
sigint_handler(int signal)
{
  fmt::print(stderr, "\nrequested stop, signal={}\n", signal);
  running.clear();
}

std::atomic_uint64_t total{ 0 };
hdr_histogram* histogram{ nullptr };

void
dump_stats(asio::steady_timer& timer, std::chrono::system_clock::time_point start_time)
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
  timer.async_wait([&timer, start_time](std::error_code ec) {
    if (ec == asio::error::operation_aborted) {
      return;
    }
    auto now = std::chrono::system_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
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
    for (const auto& [_, operations, errors] : stats_window) {
      window_ops += operations;
      window_err += errors;
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
    fmt::print(stderr,
               "\r\033[Kuptime: {}, rate: {:.2f} ops/s, {:.2f} err/s, total: {}",
               uptime,
               ops_rate,
               err_rate,
               current_total);

    fflush(stderr);
    return dump_stats(timer, start_time);
  });
}

auto
uniq_id(const std::string& prefix) -> std::string
{
  return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

auto
random_text(std::size_t length) -> std::string
{
  std::string alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static thread_local std::mt19937_64 gen{ std::random_device()() };
  std::uniform_int_distribution<std::size_t> dis(0, alphabet.size() - 1);
  std::string text(length, '-');
  for (std::size_t i = 0; i < length; ++i) {
    text[i] = alphabet[dis(gen)];
  }
  return text;
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
    add_flag("--incompressible-body",
             incompressible_body_,
             "Use random characters to fill generated document value (by default uses 'x' to fill "
             "the body).");
    add_option("--number-of-io-threads", number_of_io_threads_, "Number of the IO threads.")
      ->default_val(default_number_of_io_threads);
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
               "running indefinitely)")
      ->default_val(default_operations_limit);
    add_option("--operation-batch-size",
               operation_batch_size_,
               "Number of the operations in single batch operations (1 to wait for completion "
               "after each operation).")
      ->default_val(default_operation_batch_size);
    add_option("--batch-wait", batch_wait_, "Time to wait after the batch.")
      ->default_val(default_batch_wait);
    add_option("--query-statement",
               query_statement_,
               "The N1QL query statement to use ({bucket_name}, {scope_name} and {collection_name} "
               "will be substituted).")
      ->default_val(default_query_statement);
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);

    add_common_options(this, common_options_);
    allow_extras(true);
  }

  [[nodiscard]] auto execute() const -> int
  {
    if (operation_batch_size_ == 0) {
      throw CLI::ValidationError("--operation-batch-size cannot be zero");
    }
    apply_logger_options(common_options_.logger);

    const auto cluster_options = build_cluster_options(common_options_);

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
               "| Batch size: {}\n",
               couchbase::core::meta::sdk_semver(),
               connection_string,
               operation_generator::parse(operation_ratio_string_).to_string(),
               operation_batch_size_);

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

    std::vector<std::vector<std::string>> known_keys(number_of_worker_threads_);
    if (number_of_keys_to_populate_ > 0) {
      populate_keys(cluster, known_keys);
    }

    const auto start_time = std::chrono::system_clock::now();

    asio::steady_timer stats_timer(io);
    dump_stats(stats_timer, start_time);

    std::vector<std::thread> worker_pool{};
    worker_pool.reserve(number_of_worker_threads_);
    for (std::size_t i = 0; i < number_of_worker_threads_; ++i) {
      worker_pool.emplace_back([this, cluster = cluster, &keys = known_keys[i]]() {
        worker(cluster, keys);
      });
    }
    for (auto& thread : worker_pool) {
      thread.join();
    }

    const auto finish_time = std::chrono::system_clock::now();
    stats_timer.cancel();

    fmt::print("\n\nTotal operations: {}\n", total);
    fmt::print(
      "Total keys used: {}\n",
      std::accumulate(known_keys.begin(), known_keys.end(), 0, [](auto count, const auto& keys) {
        return count + keys.size();
      }));
    const auto total_time = finish_time - start_time;
    fmt::print("Total time: {}s ({}ms)\n",
               std::chrono::duration_cast<std::chrono::seconds>(total_time).count(),
               std::chrono::duration_cast<std::chrono::milliseconds>(total_time).count());
    if (auto diff = std::chrono::duration_cast<std::chrono::seconds>(total_time).count();
        diff > 0) {
      fmt::print("Total rate: {} ops/s\n", total / static_cast<std::uint64_t>(diff));
    }

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

    for (auto& thread : io_pool) {
      thread.join();
    }

    if (total > 0) {
      fmt::print("Latency distribution (in ms)\n");
      hdr_percentiles_print(histogram, stdout, 1, 1'000'000.0 /* in ms */, format_type::CLASSIC);
    }

    return 0;
  }

  [[nodiscard]] auto generate_document_body() const -> std::vector<std::byte>
  {
    if (document_body_size_ > 0) {
      return couchbase::core::utils::json::generate_binary({
        { "size", document_body_size_ },
        { "text",
          incompressible_body_ ? random_text(document_body_size_)
                               : std::string(document_body_size_, 'x') },
      });
    }
    return couchbase::core::utils::to_binary(default_json_doc);
  }

private:
  void worker(couchbase::cluster connected_cluster, std::vector<std::string>& known_keys) const
  {
    auto cluster = std::move(connected_cluster);

    static thread_local std::mt19937_64 gen{ std::random_device()() };
    std::uniform_real_distribution<double> dist(0, 1);

    auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

    std::vector<std::byte> json_doc = generate_document_body();
    auto query_statement{ fmt::format(query_statement_, fmt::arg("bucket_name", bucket_name_)) };

    bool stopping{ false };
    auto operation_generator{ operation_generator::parse(operation_ratio_string_) };

    while (running.test_and_set() && !stopping) {
      std::list<
        std::pair<std::chrono::system_clock::time_point,
                  std::variant<std::future<std::pair<couchbase::error, couchbase::mutation_result>>,
                               std::future<std::pair<couchbase::error, couchbase::get_result>>,
                               std::future<std::pair<couchbase::error, couchbase::query_result>>>>>
        futures;

      auto known_keys_distribution =
        std::uniform_int_distribution<std::size_t>(0, known_keys.size() - 1);
      for (std::size_t i = 0; i < operation_batch_size_; ++i) {
        auto operation = operation_generator.next_operation();
        std::string document_id = (operation != operation::cmd_insert && !known_keys.empty())
                                    ? known_keys[known_keys_distribution(gen)]
                                    : uniq_id("id");
        switch (operation) {
          case operation::cmd_get:
            futures.emplace_back(std::chrono::system_clock::now(), collection.get(document_id));
            break;
          case operation::cmd_replace:
            futures.emplace_back(std::chrono::system_clock::now(),
                                 collection.replace<raw_json_transcoder>(document_id, json_doc));
            break;
          case operation::cmd_delete:
            futures.emplace_back(std::chrono::system_clock::now(), collection.remove(document_id));
            break;
          case operation::cmd_insert:
            known_keys.push_back(document_id);
            futures.emplace_back(std::chrono::system_clock::now(),
                                 collection.replace<raw_json_transcoder>(document_id, json_doc));
            break;
          case operation::cmd_query:
            futures.emplace_back(std::chrono::system_clock::now(),
                                 cluster.query(query_statement, couchbase::query_options{}));
            break;
        }
      }

      for (auto&& [start, future] : futures) {
        std::visit(
          [&stopping, start = start, &known_keys, verbose = verbose_](auto f) mutable {
            using T = std::decay_t<decltype(f)>;

            while (f.wait_for(std::chrono::milliseconds{ 200 }) != std::future_status::ready) {
              if (!running.test_and_set()) {
                stopping = true;
                running.clear();
                return;
              }
            }
            auto [err, resp] = f.get();
            hdr_record_value_atomic(histogram, (std::chrono::system_clock::now() - start).count());
            ++total;
            if (err.ec()) {
              const std::scoped_lock lock(errors_mutex);
              ++errors[err.ec()];
              if (verbose) {
                fmt::print(stderr, "\r\033[K{}\n", err.ctx().to_json());
              }
            }
          },
          std::move(future));
      }

      if (stopping || (operations_limit_ > 0 && total >= operations_limit_)) {
        running.clear();
      } else {
        if (batch_wait_ != std::chrono::milliseconds::zero()) {
          std::this_thread::sleep_for(batch_wait_);
        }
      }
    }
    running.clear();
  }

  void populate_keys(const couchbase::cluster& cluster,
                     std::vector<std::vector<std::string>>& known_keys) const
  {
    const std::size_t total_keys{ number_of_worker_threads_ * number_of_keys_to_populate_ };

    auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

    const auto json_doc = generate_document_body();
    const auto start_time = std::chrono::system_clock::now();

    constexpr std::size_t minimum_batch_size{ 10 };
    std::size_t stored_keys{ 0 };
    std::size_t retried_keys{ 0 };
    for (std::size_t i = 0; i < number_of_worker_threads_; ++i) {
      auto keys_left = number_of_keys_to_populate_;

      while (keys_left > 0) {
        fmt::print(stderr,
                   "\r\033[K{:02.2f}% {} of {}, {}\r",
                   static_cast<double>(stored_keys) / gsl::narrow_cast<double>(total_keys) * 100,
                   stored_keys,
                   total_keys,
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now() - start_time));

        auto batch_size = std::min(keys_left, std::max(operation_batch_size_, minimum_batch_size));

        std::vector<std::pair<std::string,
                              std::future<std::pair<couchbase::error, couchbase::mutation_result>>>>
          futures;
        futures.reserve(batch_size);
        for (std::size_t k = 0; k < batch_size; ++k) {
          const std::string document_id = uniq_id("id");
          futures.emplace_back(document_id,
                               collection.upsert<raw_json_transcoder>(document_id, json_doc));
        }

        for (auto&& [document_id, future] : futures) {
          auto [ctx, res] = future.get();
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
               "\r\033[K{} keys populated in {}s ({}ms) with {} retries\n",
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
  std::size_t operation_batch_size_{};
  std::chrono::milliseconds batch_wait_{};
  std::size_t number_of_io_threads_{};
  std::size_t number_of_worker_threads_{};
  std::size_t number_of_keys_to_populate_{};
  std::string operation_ratio_string_{ default_operation_ratio.to_string() };
  std::string query_statement_;
  bool incompressible_body_{};
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
