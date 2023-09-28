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
#include <couchbase/transactions.hxx>

#include <asio.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <tao/json/to_string.hpp>

#include <chrono>
#include <future>
#include <random>
#include <system_error>

struct program_arguments {
    std::string connection_string{ "couchbase://127.0.0.1" };
    std::string username{ "Administrator" };
    std::string password{ "password" };
    std::string bucket_name{ "default" };
    std::string scope_name{ couchbase::scope::default_name };
    std::string collection_name{ couchbase::collection::default_name };
    std::size_t number_of_operations{ 1'000 };
    std::size_t document_body_size{ 1'024 };
    std::chrono::seconds transaction_expiration_time{ 120 };

    static auto load_from_environment() -> program_arguments
    {
        program_arguments arguments;
        if (const auto* val = getenv("CB_CONNECTION_STRING"); val != nullptr && val[0] != '\0') {
            arguments.connection_string = val;
        }
        if (const auto* val = getenv("CB_USERNAME"); val != nullptr && val[0] != '\0') {
            arguments.username = val;
        }
        if (const auto* val = getenv("CB_PASSWORD"); val != nullptr && val[0] != '\0') {
            arguments.password = val;
        }
        if (const auto* val = getenv("CB_BUCKET_NAME"); val != nullptr && val[0] != '\0') {
            arguments.bucket_name = val;
        }
        if (const auto* val = getenv("CB_SCOPE_NAME"); val != nullptr && val[0] != '\0') {
            arguments.scope_name = val;
        }
        if (const auto* val = getenv("CB_COLLECTION_NAME"); val != nullptr && val[0] != '\0') {
            arguments.collection_name = val;
        }
        if (const auto* val = getenv("CB_NUMBER_OF_OPERATIONS"); val != nullptr && val[0] != '\0') {
            char* end = nullptr;
            auto int_val = std::strtoul(val, &end, 10);
            if (end != val) {
                arguments.number_of_operations = int_val;
            }
        }
        if (const auto* val = getenv("CB_DOCUMENT_BODY_SIZE"); val != nullptr && val[0] != '\0') {
            char* end = nullptr;
            auto int_val = std::strtoul(val, &end, 10);
            if (end != val) {
                arguments.document_body_size = int_val;
            }
        }
        if (const auto* val = getenv("CB_TRANSACTION_EXPIRATION_TIME"); val != nullptr && val[0] != '\0') {
            char* end = nullptr;
            auto int_val = std::strtoul(val, &end, 10);
            if (end != val) {
                arguments.transaction_expiration_time = std::chrono::seconds{ int_val };
            }
        }
        return arguments;
    }
};

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

auto
generate_document(std::size_t document_body_size) -> tao::json::value
{
    return {
        { "size", document_body_size },
        { "text", random_text(document_body_size) },
    };
}

void
run_workload_sequential(const std::shared_ptr<couchbase::transactions::transactions>& transactions,
                        const couchbase::collection& collection,
                        const program_arguments& arguments)
{
    if (arguments.number_of_operations <= 0) {
        return;
    }

    fmt::print("\n===== SEQUENTIAL\n");

    const std::string document_id_prefix{ "tx_sequential" };
    std::vector<std::string> document_ids;
    document_ids.reserve(arguments.number_of_operations);
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
        document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
    }
    fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n", document_ids.size(), document_ids[0], document_ids[document_ids.size() - 1]);

    // Transactions do not have upsert operation, so we need to ensure that the documents do not exist in the collection.
    fmt::print("Removing {} IDs in collection \"{}.{}\"\n", document_ids.size(), arguments.scope_name, arguments.collection_name);
    {
        using remove_result = std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>;

        std::map<std::string, std::size_t> errors;
        std::vector<remove_result> results;
        results.reserve(arguments.number_of_operations);

        auto cleanup_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
            results.emplace_back(collection.remove(document_ids[i], {}));
        }
        for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
            auto [ctx, result] = results[i].get();
            if (ctx.ec()) {
                errors[ctx.ec().message()]++;
            }
        }
        auto cleanup_end = std::chrono::system_clock::now();

        fmt::print("Removed {} keys in {}ms ({}us, {}s)\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(cleanup_end - cleanup_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(cleanup_end - cleanup_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(cleanup_end - cleanup_start).count());

        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }

    const auto document = generate_document(arguments.document_body_size);

    auto start = std::chrono::system_clock::now();

    {
        std::map<std::string, std::size_t> errors;

        auto exec_start = std::chrono::system_clock::now();
        auto [err, result] = transactions->run(
          [&collection, &document_ids, &document, &arguments, &errors](couchbase::transactions::attempt_context& attempt) {
              for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
                  auto [ctx, res] = attempt.insert(collection, document_ids[i], document);
                  if (ctx.ec()) {
                      errors[ctx.ec().message()]++;
                  }
                  fmt::print("\rexecute insert: {}", i);
                  fflush(stdout);
              }
          });
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("\rExecuted transaction with {} INSERT operations in {}ms ({}us, {}s), average latency: {}ms\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() / arguments.number_of_operations);
        if (err.ec()) {
            fmt::print("\tTransaction completed with error {}, cause={}\n", err.ec().message(), err.cause().message());
            if (err.ec() == couchbase::errc::transaction::expired) {
                fmt::print("\tINFO: Try to increase CB_TRANSACTION_EXPIRATION_TIME, current value is {} seconds\n",
                           arguments.transaction_expiration_time);
            }
        } else {
            fmt::print("\tTransaction completed successfully\n");
        }
        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }

    {
        std::map<std::string, std::size_t> errors;

        auto exec_start = std::chrono::system_clock::now();
        auto [err, result] =
          transactions->run([&collection, &document_ids, &arguments, &errors](couchbase::transactions::attempt_context& attempt) {
              for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
                  auto [ctx, res] = attempt.get(collection, document_ids[i]);
                  if (ctx.ec()) {
                      errors[ctx.ec().message()]++;
                  }
                  fmt::print("\rexecute get: {}", i);
                  fflush(stdout);
              }
          });
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("\rExecuted transaction with {} GET operations in {}ms ({}us, {}s), average latency: {}ms\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() / arguments.number_of_operations);
        if (err.ec()) {
            fmt::print("\tTransaction completed with error {}, cause={}\n", err.ec().message(), err.cause().message());
            if (err.ec() == couchbase::errc::transaction::expired) {
                fmt::print("\tINFO: Try to increase CB_TRANSACTION_EXPIRATION_TIME, current value is {} seconds\n",
                           arguments.transaction_expiration_time);
            }
        } else {
            fmt::print("\tTransaction completed successfully\n");
        }
        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }

    auto end = std::chrono::system_clock::now();

    fmt::print("Total time for sequential execution {}ms ({}us, {}s)\n",
               std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::seconds>(end - start).count());
}

void
run_workload_bulk(const std::shared_ptr<couchbase::transactions::transactions>& transactions,
                  const couchbase::collection& collection,
                  const program_arguments& arguments)
{
    if (arguments.number_of_operations <= 0) {
        return;
    }

    fmt::print("\n===== BULK\n");

    const std::string document_id_prefix{ "tx_bulk" };
    std::vector<std::string> document_ids;
    document_ids.reserve(arguments.number_of_operations);
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
        document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
    }
    fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n", document_ids.size(), document_ids[0], document_ids[document_ids.size() - 1]);

    // Transactions do not have upsert operation, so we need to ensure that the documents do not exist in the collection.
    fmt::print("Removing {} IDs in collection \"{}.{}\"\n", document_ids.size(), arguments.scope_name, arguments.collection_name);
    {
        using remove_result = std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>;

        std::map<std::string, std::size_t> errors;
        std::vector<remove_result> results;
        results.reserve(arguments.number_of_operations);

        auto cleanup_start = std::chrono::system_clock::now();
        for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
            results.emplace_back(collection.remove(document_ids[i], {}));
        }
        for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
            auto [ctx, result] = results[i].get();
            if (ctx.ec()) {
                errors[ctx.ec().message()]++;
            }
        }
        auto cleanup_end = std::chrono::system_clock::now();

        fmt::print("Removed {} keys in {}ms ({}us, {}s)\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(cleanup_end - cleanup_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(cleanup_end - cleanup_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(cleanup_end - cleanup_start).count());

        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }

    const auto document = generate_document(arguments.document_body_size);

    auto start = std::chrono::system_clock::now();

    {
        std::map<std::string, std::size_t> errors;

        auto tx_promise =
          std::make_shared<std::promise<std::pair<couchbase::transaction_error_context, couchbase::transactions::transaction_result>>>();
        auto tx_future = tx_promise->get_future();

        auto schedule_start = std::chrono::system_clock::now();
        transactions->run(
          [&collection, &document_ids, &document, &arguments, &errors](couchbase::transactions::async_attempt_context& attempt) {
              for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
                  attempt.insert(collection, document_ids[i], document, [&errors](auto ctx, auto) {
                      if (ctx.ec()) {
                          errors[ctx.ec().message()]++;
                      }
                  });
              }
          },
          [tx_promise](auto err, auto result) {
              tx_promise->set_value({ err, result });
          });

        auto schedule_end = std::chrono::system_clock::now();
        fmt::print("\rScheduled transaction with {} INSERT operations in {}ms ({}us, {}s)\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(schedule_end - schedule_start).count());

        auto exec_start = std::chrono::system_clock::now();
        auto [err, result] = tx_future.get();
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("\rExecuted transaction with {} INSERT operations in {}ms ({}us, {}s), average latency: {}ms\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() / arguments.number_of_operations);
        if (err.ec()) {
            fmt::print("\tTransaction completed with error {}, cause={}\n", err.ec().message(), err.cause().message());
            if (err.ec() == couchbase::errc::transaction::expired) {
                fmt::print("\tINFO: Try to increase CB_TRANSACTION_EXPIRATION_TIME, current value is {} seconds\n",
                           arguments.transaction_expiration_time);
            }
        } else {
            fmt::print("\tTransaction completed successfully\n");
        }
        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }

    {
        std::map<std::string, std::size_t> errors;

        auto tx_promise =
          std::make_shared<std::promise<std::pair<couchbase::transaction_error_context, couchbase::transactions::transaction_result>>>();
        auto tx_future = tx_promise->get_future();

        auto schedule_start = std::chrono::system_clock::now();
        transactions->run(
          [&collection, &document_ids, &arguments, &errors](couchbase::transactions::async_attempt_context& attempt) {
              for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
                  attempt.get(collection, document_ids[i], [&errors](auto ctx, auto) {
                      if (ctx.ec()) {
                          errors[ctx.ec().message()]++;
                      }
                  });
              }
          },
          [tx_promise](auto err, auto result) {
              tx_promise->set_value({ err, result });
          });

        auto schedule_end = std::chrono::system_clock::now();
        fmt::print("\rScheduled transaction with {} GET operations in {}ms ({}us, {}s)\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(schedule_end - schedule_start).count());

        auto exec_start = std::chrono::system_clock::now();
        auto [err, result] = tx_future.get();
        auto exec_end = std::chrono::system_clock::now();

        fmt::print("\rExecuted transaction with {} GET operations in {}ms ({}us, {}s), average latency: {}ms\n",
                   arguments.number_of_operations,
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() / arguments.number_of_operations);
        if (err.ec()) {
            fmt::print("\tTransaction completed with error {}, cause={}\n", err.ec().message(), err.cause().message());
            if (err.ec() == couchbase::errc::transaction::expired) {
                fmt::print("\tINFO: Try to increase CB_TRANSACTION_EXPIRATION_TIME, current value is {} seconds\n",
                           arguments.transaction_expiration_time);
            }
        } else {
            fmt::print("\tTransaction completed successfully\n");
        }
        if (err.ec() == couchbase::errc::transaction::expired) {
            fmt::print("\tINFO: Try to increase CB_TRANSACTION_EXPIRATION_TIME, current value is {} seconds\n",
                       arguments.transaction_expiration_time);
        }
        if (errors.empty()) {
            fmt::print("\tAll operations completed successfully\n");
        } else {
            fmt::print("\tSome operations completed with errors:\n");
            for (auto [error, hits] : errors) {
                fmt::print("\t{}: {}\n", error, hits);
            }
        }
    }
    auto end = std::chrono::system_clock::now();

    fmt::print("Total time for bulk execution {}ms ({}us, {}s)\n",
               std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(),
               std::chrono::duration_cast<std::chrono::seconds>(end - start).count());
}

int
main()
{
    auto arguments = program_arguments::load_from_environment();

    fmt::print("CB_CONNECTION_STRING={}\n", arguments.connection_string);
    fmt::print("CB_USERNAME={}\n", arguments.username);
    fmt::print("CB_PASSWORD={}\n", arguments.password);
    fmt::print("CB_BUCKET_NAME={}\n", arguments.bucket_name);
    fmt::print("CB_SCOPE_NAME={}\n", arguments.scope_name);
    fmt::print("CB_COLLECTION_NAME={}\n", arguments.collection_name);
    fmt::print("CB_NUMBER_OF_OPERATIONS={}\n", arguments.number_of_operations);
    fmt::print("CB_DOCUMENT_BODY_SIZE={}\n", arguments.document_body_size);
    fmt::print("CB_TRANSACTION_EXPIRATION_TIME={}\n", arguments.transaction_expiration_time.count());

    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(arguments.username, arguments.password);
    options.apply_profile("wan_development");
    options.transactions().expiration_time(arguments.transaction_expiration_time);
    auto [cluster, ec] = couchbase::cluster::connect(io, arguments.connection_string, options).get();
    if (ec) {
        fmt::print("Unable to connect to cluster at \"{}\", error: {}\n", arguments.connection_string, ec.message());
    } else {
        auto transactions = cluster.transactions();
        auto collection = cluster.bucket(arguments.bucket_name).scope(arguments.scope_name).collection(arguments.collection_name);

        run_workload_sequential(transactions, collection, arguments);
        run_workload_bulk(transactions, collection, arguments);
    }

    cluster.close();
    guard.reset();

    io_thread.join();

    return 0;
}
