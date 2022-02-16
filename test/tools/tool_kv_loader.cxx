/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-Present Couchbase, Inc.
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

#include "../utils/integration_shortcuts.hxx"
#include "../utils/test_context.hxx"
#include "../utils/uniq_id.hxx"

#include <couchbase/logger/logger.hxx>
#include <couchbase/utils/json.hxx>

#include <spdlog/details/os.h>

#include <csignal>
#include <thread>

enum class operation {
    get,
    upsert,
};

namespace
{
volatile std::sig_atomic_t running{ 1 };
} // namespace

void
sigint_handler(int /* signal */)
{
    running = 0;
}

static void
dump_stats(asio::steady_timer& timer, std::chrono::system_clock::time_point start_time, std::atomic_uint64_t& total)
{
    timer.expires_after(std::chrono::seconds{ 1 });
    timer.async_wait([&timer, start_time, &total](std::error_code ec) {
        if (ec == asio::error::operation_aborted) {
            return;
        }
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start_time).count();
        std::uint64_t ops = total;
        fmt::print(stderr, "\rrate: {} ops/s\r", diff == 0 ? ops : ops / static_cast<std::uint64_t>(diff));
        return dump_stats(timer, start_time, total);
    });
}

int
main()
{
    couchbase::logger::create_console_logger();
    if (auto val = spdlog::details::os::getenv("TEST_LOG_LEVEL"); !val.empty()) {
        couchbase::logger::set_log_levels(couchbase::logger::level_from_str(val));
    } else {
        couchbase::logger::set_log_levels(couchbase::logger::level::info);
    }

    auto ctx = test::utils::test_context::load_from_environment();

    std::size_t number_of_io_threads = 4;
    if (auto val = spdlog::details::os::getenv("TEST_NUMBER_OF_IO_THREADS"); !val.empty()) {
        number_of_io_threads = std::stoul(val, nullptr, 10);
    }
    LOG_INFO("number_of_threads: {}, username: {}, connection_string: {}", number_of_io_threads, ctx.username, ctx.connection_string);

    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    if (!ctx.certificate_path.empty()) {
        auth.certificate_path = ctx.certificate_path;
        auth.key_path = ctx.key_path;
    } else {
        auth.username = ctx.username;
        auth.password = ctx.password;
    }

    asio::io_context io(static_cast<int>(number_of_io_threads));

    auto origin = couchbase::origin(auth, connstr);
    auto cluster = couchbase::cluster::create(io);

    std::vector<std::thread> io_pool{};
    io_pool.reserve(number_of_io_threads);
    for (std::size_t i = 0; i < number_of_io_threads; ++i) {
        io_pool.emplace_back(std::thread([&io]() { io.run(); }));
    }

    test::utils::open_cluster(cluster, origin);
    test::utils::open_bucket(cluster, ctx.bucket);

    std::vector<std::string> known_keys{};

    double chance_of_get = 0.6;
    double hit_chance_for_upsert = 0.7;
    double hit_chance_for_get = 1.0;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dist(0, 1);

    std::atomic_uint64_t total{};
    std::map<std::error_code, std::size_t> errors{};
    std::mutex errors_mutex{};

    std::signal(SIGINT, sigint_handler);
    std::signal(SIGTERM, sigint_handler);

    LOG_INFO("start workload, chance_of_get: {}, hit_chance_for_upsert: {}, hit_chance_for_get: {}",
             chance_of_get,
             hit_chance_for_upsert,
             hit_chance_for_get);

    const std::string json_doc = R"({
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

    const auto start_time = std::chrono::system_clock::now();

    asio::steady_timer stats_timer(io);
    dump_stats(stats_timer, start_time, total);
    while (running != 0) {
        auto opcode = chance_of_get >= dist(gen) ? operation::get : operation::upsert;
        if (opcode == operation::get && known_keys.empty()) {
            opcode = operation::upsert;
        }
        bool should_check_known_keys{ false };
        switch (opcode) {
            case operation::get:
                should_check_known_keys = hit_chance_for_get > dist(gen);
                break;
            case operation::upsert:
                should_check_known_keys = hit_chance_for_upsert > dist(gen);
                break;
        }
        std::string current_key = test::utils::uniq_id("id");
        if (should_check_known_keys && !known_keys.empty()) {
            auto key_index = std::uniform_int_distribution<std::size_t>(0, known_keys.size() - 1)(gen);
            current_key = known_keys[key_index];
        } else {
            known_keys.emplace_back(current_key);
        }

        couchbase::document_id id{ ctx.bucket, "_default", "_default", current_key };
        switch (opcode) {
            case operation::get: {
                couchbase::operations::upsert_request req{ id, json_doc };
                cluster->execute(req, [&total, &errors_mutex, &errors](const couchbase::operations::upsert_response& resp) {
                    ++total;
                    if (resp.ctx.ec) {
                        std::scoped_lock lock(errors_mutex);
                        ++errors[resp.ctx.ec];
                    }
                });
            } break;
            case operation::upsert: {
                couchbase::operations::get_request req{ id };
                cluster->execute(req, [&total, &errors_mutex, &errors](const couchbase::operations::get_response&& resp) {
                    ++total;
                    if (resp.ctx.ec) {
                        std::scoped_lock lock(errors_mutex);
                        ++errors[resp.ctx.ec];
                    }
                });
            } break;
        }
    }
    const auto finish_time = std::chrono::system_clock::now();
    stats_timer.cancel();

    fmt::print("total operations: {}\n", total);
    fmt::print("total keys used: {}\n", known_keys.size());
    const auto total_time = finish_time - start_time;
    fmt::print("total time: {}s ({}ms)\n",
               std::chrono::duration_cast<std::chrono::seconds>(total_time).count(),
               std::chrono::duration_cast<std::chrono::milliseconds>(total_time).count());
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(total_time).count();
    fmt::print("total rate: {} ops/s\n", total / static_cast<std::uint64_t>(diff));
    if (!errors.empty()) {
        fmt::print("error stats:\n");
        for (auto [ec, count] : errors) {
            fmt::print("    {} ({}): {}\n", ec.message(), ec.value(), count);
        }
    }

    test::utils::close_cluster(cluster);

    for (auto& thread : io_pool) {
        thread.join();
    }

    return 0;
}