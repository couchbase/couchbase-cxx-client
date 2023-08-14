/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include <couchbase/cluster.hxx>

#include <asio.hpp>
#include <fmt/format.h>
#include <tao/json/to_string.hpp>

#include <chrono>
#include <future>
#include <system_error>

static constexpr auto connection_string{ "couchbase://127.0.0.1" };
static constexpr auto username{ "Administrator" };
static constexpr auto password{ "password" };
static constexpr auto bucket_name{ "default" };
static constexpr auto scope_name{ couchbase::scope::default_name };
static constexpr auto collection_name{ couchbase::collection::default_name };

void
run_workload_sequential(couchbase::collection collection)
{
    constexpr std::size_t number_of_operations{ 1'000 };

    fmt::print("\n===== SEQUENTIAL\n");

    const std::string document_id_prefix{ "sequential" };
    std::vector<std::string> document_ids;
    document_ids.reserve(number_of_operations);
    for (std::size_t i = 0; i < number_of_operations; ++i) {
        document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
    }
    fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n", document_ids.size(), document_ids[0], document_ids[document_ids.size() - 1]);

    const tao::json::value basic_doc{
        { "a", 1.0 },
        { "b", 2.0 },
    };
    fmt::print("Using JSON body for the document:\n{}\n", tao::json::to_string(basic_doc));

    auto start = std::chrono::system_clock::now();

    {
        std::map<std::string, std::size_t> errors;

        auto exec_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < number_of_operations; ++i) {
            auto [ctx, result] = collection.upsert(document_ids[i], basic_doc, {}).get();
            errors[ctx.ec().message()]++;
        }
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("Executed {} upsert operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count());

        for (auto [error, hits] : errors) {
            fmt::print("\t{}: {}\n", error, hits);
        }
    }
    {
        std::map<std::string, std::size_t> errors;

        auto exec_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < number_of_operations; ++i) {
            auto [ctx, result] = collection.get(document_ids[i], {}).get();
            errors[ctx.ec().message()]++;
        }
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("Executed {} get operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count());

        for (auto [error, hits] : errors) {
            fmt::print("\t{}: {}\n", error, hits);
        }
    }

    auto end = std::chrono::system_clock::now();

    fmt::print("Total time for sequential execution {}ms ({}us)\n",
               std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / number_of_operations);
}

void
run_workload_bulk(couchbase::collection collection)
{
    constexpr std::size_t number_of_operations{ 1'000 };

    fmt::print("\n===== BULK\n");
    const std::string document_id_prefix{ "bulk" };
    std::vector<std::string> document_ids;
    document_ids.reserve(number_of_operations);
    for (std::size_t i = 0; i < number_of_operations; ++i) {
        document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
    }
    fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n", document_ids.size(), document_ids[0], document_ids[document_ids.size() - 1]);

    const tao::json::value basic_doc{
        { "a", 1.0 },
        { "b", 2.0 },
    };
    fmt::print("Using JSON body for the document:\n{}\n", tao::json::to_string(basic_doc));

    auto start = std::chrono::system_clock::now();

    {
        using upsert_result = std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>;

        std::map<std::string, std::size_t> errors;
        std::vector<upsert_result> results;
        results.reserve(number_of_operations);

        auto schedule_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < number_of_operations; ++i) {
            results.emplace_back(collection.upsert(document_ids[i], basic_doc, {}));
        }
        auto schedule_end = std::chrono::system_clock::now();
        fmt::print("Scheduled {} upsert operations in {}ms ({}us)\n",
                   results.size(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count());

        auto completion_start = std::chrono::system_clock::now();
        for (auto& future : results) {
            auto [ctx, result] = future.get();
            errors[ctx.ec().message()]++;
        }
        auto completion_end = std::chrono::system_clock::now();
        fmt::print("Completed {} upsert operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - completion_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(completion_end - completion_start).count());

        fmt::print("Executed {} upsert operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(completion_end - schedule_start).count());

        for (auto [error, hits] : errors) {
            fmt::print("\t{}: {}\n", error, hits);
        }
    }
    {
        using get_result = std::future<std::pair<couchbase::key_value_error_context, couchbase::get_result>>;

        std::map<std::string, std::size_t> errors;
        std::vector<get_result> results;
        results.reserve(number_of_operations);

        auto schedule_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < number_of_operations; ++i) {
            results.emplace_back(collection.get(document_ids[i], {}));
        }
        auto schedule_end = std::chrono::system_clock::now();
        fmt::print("Scheduled {} get operations in {}ms ({}us)\n",
                   results.size(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count());

        auto completion_start = std::chrono::system_clock::now();
        for (auto& future : results) {
            auto [ctx, result] = future.get();
            errors[ctx.ec().message()]++;
        }
        auto completion_end = std::chrono::system_clock::now();
        fmt::print("Completed {} get operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - completion_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(completion_end - completion_start).count());

        fmt::print("Executed {} get operations in {}ms ({}us)\n",
                   number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(completion_end - schedule_start).count());

        for (auto [error, hits] : errors) {
            fmt::print("\t{}: {}\n", error, hits);
        }
    }

    auto end = std::chrono::system_clock::now();

    fmt::print("Total time for bulk execution {}ms ({}us), average latency: {}us\n",
               std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / number_of_operations);
}

int
main()
{
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    options.apply_profile("wan_development");
    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    if (ec) {
        fmt::print("Unable to connect to cluster at \"{}\", error: {}\n", connection_string, ec.message());
    } else {
        run_workload_sequential(cluster.bucket(bucket_name).scope(scope_name).collection(collection_name));
        run_workload_bulk(cluster.bucket(bucket_name).scope(scope_name).collection(collection_name));
    }

    cluster.close();
    guard.reset();

    io_thread.join();

    return 0;
}
