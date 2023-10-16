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

#include "core/operations/document_upsert.hxx"
#include "couchbase/codec/binary_noop_serializer.hxx"
#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/fmt/cas.hxx>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <fmt/chrono.h>
#include <fmt/std.h>

#include <csignal>
#include <numeric>
#include <random>
#include <thread>

namespace cbc
{
namespace
{
const std::string default_bucket_name{ "default" };
const std::size_t default_number_of_io_threads{ 1 };
const std::size_t default_number_of_worker_threads{ 1 };
const double default_chance_of_get{ 0.6 };
const double default_hit_chance_for_get{ 1.0 };
const double default_hit_chance_for_upsert{ 1 };
const double default_chance_of_query{ 0.0 };
const std::string default_query_statement{ "SELECT COUNT(*) FROM `{bucket_name}` WHERE type = \"fake_profile\"" };
const std::size_t default_document_body_size{ 0 };
const std::size_t default_operations_limit{ 0 };
const std::size_t default_batch_size{ 100 };
const std::chrono::milliseconds default_batch_wait{ 0 };
const std::size_t default_number_of_keys_to_populate{ 1'000 };

const char* default_json_doc = R"({
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

enum class operation {
    get,
    upsert,
};

using raw_json_transcoder = couchbase::codec::json_transcoder<couchbase::codec::binary_noop_serializer>;

std::atomic_flag running{ true };
std::atomic_uint64_t total{ 0 };

std::map<std::error_code, std::size_t> errors{};
std::mutex errors_mutex{};

void
sigint_handler(int signal)
{
    fmt::print(stderr, "\nrequested stop, signal={}\n", signal);
    running.clear();
}

void
dump_stats(asio::steady_timer& timer, std::chrono::system_clock::time_point start_time)
{
    timer.expires_after(std::chrono::seconds{ 1 });
    timer.async_wait([&timer, start_time](std::error_code ec) {
        if (ec == asio::error::operation_aborted) {
            return;
        }
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start_time);
        auto diff = uptime.count();
        const std::uint64_t ops = total;
        fmt::print(
          stderr, "\r\033[Kuptime: {}, rate: {} ops/s, total: {}\r", uptime, diff == 0 ? ops : ops / static_cast<std::uint64_t>(diff), ops);
        return dump_stats(timer, start_time);
    });
}

std::string
uniq_id(const std::string& prefix)
{
    return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string
random_text(std::size_t length)
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
        add_option("--bucket-name", bucket_name_, "Name of the bucket.")->default_val(default_bucket_name);
        add_option("--scope-name", scope_name_, "Name of the scope.")->default_val(couchbase::scope::default_name);
        add_option("--batch-size", batch_size_, "Number of the operations in single batch.")->default_val(default_batch_size);
        add_option("--batch-wait", batch_wait_, "Time to wait after the batch.")->default_val(default_batch_wait);
        add_option("--number-of-io-threads", number_of_io_threads_, "Number of the IO threads.")->default_val(default_number_of_io_threads);
        add_option("--number-of-worker-threads", number_of_worker_threads_, "Number of the worker threads.")
          ->default_val(default_number_of_worker_threads);
        add_option("--chance-of-get", chance_of_get_, "The probability of get operation (where 1 means only get, and 0 - only upsert).")
          ->default_val(default_chance_of_get);
        add_option("--hit-chance-for-get", hit_chance_for_get_, "The probability of using existing ID for get operation.")
          ->default_val(default_hit_chance_for_get);
        add_option("--hit-chance-for-upsert", hit_chance_for_upsert_, "The probability of using existing ID for upsert operation.")
          ->default_val(default_hit_chance_for_upsert);
        add_option("--chance-of-query", chance_of_query_, "The probability of N1QL query will be send on after get/upsert.")
          ->default_val(default_chance_of_query);
        add_option("--query-statement",
                   query_statement_,
                   "The N1QL query statement to use ({bucket_name}, {scope_name} and {collection_name} will be substituted).")
          ->default_val(default_query_statement);
        add_option("--document-body-size", document_body_size_, "Size of the body (if zero, it will use predefined document).")
          ->default_val(default_document_body_size);
        add_option("--number-of-keys-to-populate",
                   number_of_keys_to_populate_,
                   "Preload keys before running workload, so that the worker will not generate new keys afterwards.")
          ->default_val(default_number_of_keys_to_populate);
        add_option("--operations-limit",
                   operations_limit_,
                   "Stop and exit after the number of the operations reaches this limit. (zero for running indefinitely)")
          ->default_val(default_operations_limit);
        add_flag("--incompressible-body",
                 incompressible_body_,
                 "Use random characters to fill generated document value (by default uses 'x' to fill the body).");

        add_common_options(this, common_options_);
        allow_extras(true);
    }

    [[nodiscard]] int execute() const
    {
        apply_logger_options(common_options_.logger);

        auto cluster_options = build_cluster_options(common_options_);

        asio::io_context io;
        auto guard = asio::make_work_guard(io);

        std::vector<std::thread> io_pool{};
        io_pool.reserve(number_of_io_threads_);
        for (std::size_t i = 0; i < number_of_io_threads_; ++i) {
            io_pool.emplace_back([&io]() { io.run(); });
        }

        std::signal(SIGINT, sigint_handler);
        std::signal(SIGTERM, sigint_handler);

        const auto connection_string = common_options_.connection.connection_string;

        auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, cluster_options).get();
        if (ec) {
            guard.reset();
            for (auto& thread : io_pool) {
                thread.join();
            }
            fail(fmt::format("Failed to connect to the cluster at \"{}\": {}", connection_string, ec.message()));
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
            worker_pool.emplace_back([this, cluster = cluster, &keys = known_keys[i]]() { worker(cluster, keys); });
        }
        for (auto& thread : worker_pool) {
            thread.join();
        }

        const auto finish_time = std::chrono::system_clock::now();
        stats_timer.cancel();

        fmt::print("\n\ntotal operations: {}\n", total);
        fmt::print("total keys used: {}\n", std::accumulate(known_keys.begin(), known_keys.end(), 0, [](auto count, const auto& keys) {
                       return count + keys.size();
                   }));
        const auto total_time = finish_time - start_time;
        fmt::print("total time: {}s ({}ms)\n",
                   std::chrono::duration_cast<std::chrono::seconds>(total_time).count(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(total_time).count());
        if (auto diff = std::chrono::duration_cast<std::chrono::seconds>(total_time).count(); diff > 0) {
            fmt::print("total rate: {} ops/s\n", total / static_cast<std::uint64_t>(diff));
        }
        {
            std::scoped_lock lock(errors_mutex);
            if (!errors.empty()) {
                fmt::print("error stats:\n");
                for (auto [e, count] : errors) {
                    fmt::print("    {}: {}\n", e.message(), count);
                }
            }
        }

        cluster.close();
        guard.reset();

        for (auto& thread : io_pool) {
            thread.join();
        }

        return 0;
    }

    [[nodiscard]] auto generate_document_body() const -> std::vector<std::byte>
    {
        if (document_body_size_ > 0) {
            return couchbase::core::utils::json::generate_binary({
              { "size", document_body_size_ },
              { "text", incompressible_body_ ? random_text(document_body_size_) : std::string(document_body_size_, 'x') },
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

        while (running.test_and_set()) {
            std::list<std::variant<std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>,
                                   std::future<std::pair<couchbase::key_value_error_context, couchbase::get_result>>,
                                   std::future<std::pair<couchbase::query_error_context, couchbase::query_result>>>>
              futures;
            for (std::size_t i = 0; i < batch_size_; ++i) {
                auto opcode = (dist(gen) <= chance_of_get_) ? operation::get : operation::upsert;
                if (opcode == operation::get && known_keys.empty()) {
                    opcode = operation::upsert;
                }
                bool should_check_known_keys{ false };
                switch (opcode) {
                    case operation::get:
                        should_check_known_keys = hit_chance_for_get_ > dist(gen);
                        break;
                    case operation::upsert:
                        should_check_known_keys = hit_chance_for_upsert_ > dist(gen);
                        break;
                }
                std::string document_id = uniq_id("id");
                if (should_check_known_keys && !known_keys.empty()) {
                    auto key_index = std::uniform_int_distribution<std::size_t>(0, known_keys.size() - 1)(gen);
                    document_id = known_keys[key_index];
                }

                switch (opcode) {
                    case operation::upsert:
                        futures.emplace_back(collection.upsert<raw_json_transcoder>(document_id, json_doc));
                        break;
                    case operation::get:
                        futures.emplace_back(collection.get(document_id));
                        break;
                }
                if (chance_of_query_ > 0 && dist(gen) <= chance_of_query_) {
                    futures.emplace_back(cluster.query(query_statement_, couchbase::query_options{}));
                }
                if (operations_limit_ > 0 && total >= operations_limit_) {
                    running.clear();
                }
            }

            for (auto&& future : futures) {
                std::visit(
                  [&known_keys, verbose = verbose_](auto f) mutable {
                      using T = std::decay_t<decltype(f)>;

                      auto [ctx, resp] = f.get();
                      ++total;
                      if (ctx.ec()) {
                          const std::scoped_lock lock(errors_mutex);
                          ++errors[ctx.ec()];
                          if (verbose) {
                              fmt::print(stderr, "\r\033[K{}\n", ctx.to_json());
                          }
                      } else if constexpr (std::is_same_v<
                                             T,
                                             std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>>) {
                          known_keys.emplace_back(ctx.id());
                      }
                  },
                  std::move(future));
            }
            if (batch_wait_ != std::chrono::milliseconds::zero()) {
                std::this_thread::sleep_for(batch_wait_);
            }
        }
    }

    void populate_keys(const couchbase::cluster& cluster, std::vector<std::vector<std::string>>& known_keys) const
    {
        const std::size_t total_keys{ number_of_worker_threads_ * number_of_keys_to_populate_ };

        auto collection = cluster.bucket(bucket_name_).scope(scope_name_).collection(collection_name_);

        const auto json_doc = generate_document_body();
        const auto start_time = std::chrono::system_clock::now();

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
                           std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start_time));

                auto batch_size = std::min(keys_left, batch_size_);

                std::vector<std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>> futures;
                futures.reserve(batch_size);
                for (std::size_t k = 0; k < batch_size; ++k) {
                    const std::string document_id = uniq_id("id");
                    futures.emplace_back(collection.upsert<raw_json_transcoder>(document_id, json_doc));
                }

                for (auto&& future : futures) {
                    auto [ctx, res] = future.get();
                    if (ctx.ec()) {
                        ++retried_keys;
                    } else {
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
    std::size_t batch_size_;
    std::chrono::milliseconds batch_wait_;
    std::size_t number_of_io_threads_;
    std::size_t number_of_worker_threads_;
    std::size_t number_of_keys_to_populate_;
    double chance_of_get_;
    double hit_chance_for_get_;
    double hit_chance_for_upsert_;
    double chance_of_query_;
    std::string query_statement_;
    bool incompressible_body_;
    std::size_t document_body_size_;
    std::size_t operations_limit_;
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
