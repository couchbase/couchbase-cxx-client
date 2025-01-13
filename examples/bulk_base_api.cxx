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
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/format.h>
#include <tao/json/to_string.hpp>

#include <couchbase/fmt/error.hxx>

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
run_workload_sequential(const couchbase::collection& collection, const program_arguments& arguments)
{
  if (arguments.number_of_operations == 0) {
    return;
  }

  fmt::print("\n===== SEQUENTIAL\n");

  const std::string document_id_prefix{ "sequential" };
  std::vector<std::string> document_ids;
  document_ids.reserve(arguments.number_of_operations);
  for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
    document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
  }
  fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n",
             document_ids.size(),
             document_ids[0],
             document_ids[document_ids.size() - 1]);

  const auto document = generate_document(arguments.document_body_size);

  auto start = std::chrono::system_clock::now();

  {
    std::map<std::string, std::size_t> errors;

    auto exec_start = std::chrono::system_clock::now();
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      auto [err, result] = collection.upsert(document_ids[i], document, {}).get();
      if (err.ec()) {
        errors[err.ec().message()]++;
      }
      fmt::print("\rexecute upsert: {}", i);
      fflush(stdout);
    }
    auto exec_end = std::chrono::system_clock::now();

    fmt::print(
      "\rExecuted {} upsert operations in {}ms ({}us, {}s), average latency: {}ms\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() /
        arguments.number_of_operations);

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
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      auto [err, result] = collection.get(document_ids[i], {}).get();
      if (err.ec()) {
        errors[err.ec().message()]++;
      }
      fmt::print("\rexecute get: {}", i);
      fflush(stdout);
    }
    auto exec_end = std::chrono::system_clock::now();

    fmt::print(
      "\rExecuted {} get operations in {}ms ({}us, {}s), average latency: {}ms\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() /
        arguments.number_of_operations);

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
run_workload_bulk(const couchbase::collection& collection, const program_arguments& arguments)
{
  if (arguments.number_of_operations == 0) {
    return;
  }

  fmt::print("\n===== BULK\n");
  const std::string document_id_prefix{ "bulk" };
  std::vector<std::string> document_ids;
  document_ids.reserve(arguments.number_of_operations);
  for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
    document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
  }
  fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n",
             document_ids.size(),
             document_ids[0],
             document_ids[document_ids.size() - 1]);

  const auto document = generate_document(arguments.document_body_size);

  auto start = std::chrono::system_clock::now();

  {
    using upsert_result = std::future<std::pair<couchbase::error, couchbase::mutation_result>>;

    std::map<std::string, std::size_t> errors;
    std::vector<upsert_result> results;
    results.reserve(arguments.number_of_operations);

    auto schedule_start = std::chrono::system_clock::now();
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      results.emplace_back(collection.upsert(document_ids[i], document, {}));
      fmt::print("\rscheduled upsert: {}", i);
      fflush(stdout);
    }
    auto schedule_end = std::chrono::system_clock::now();
    fmt::print(
      "\rScheduled {} upsert operations in {}ms ({}us, {}s)\n",
      results.size(),
      std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(schedule_end - schedule_start).count());

    auto completion_start = std::chrono::system_clock::now();
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      auto [err, result] = results[i].get();
      if (err.ec()) {
        errors[err.ec().message()]++;
      }
      fmt::print("\rcomplete upsert: {}", i);
    }
    auto completion_end = std::chrono::system_clock::now();
    fmt::print(
      "\rCompleted {} upsert operations in {}ms ({}us, {}s)\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - completion_start)
        .count(),
      std::chrono::duration_cast<std::chrono::microseconds>(completion_end - completion_start)
        .count(),
      std::chrono::duration_cast<std::chrono::seconds>(completion_end - completion_start).count());

    fmt::print(
      "Executed {} upsert operations in {}ms ({}us, {}s), average latency: {}ms\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start)
        .count(),
      std::chrono::duration_cast<std::chrono::microseconds>(completion_end - schedule_start)
        .count(),
      std::chrono::duration_cast<std::chrono::seconds>(completion_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start)
          .count() /
        arguments.number_of_operations);

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
    using get_result = std::future<std::pair<couchbase::error, couchbase::get_result>>;

    std::map<std::string, std::size_t> errors;
    std::vector<get_result> results;
    results.reserve(arguments.number_of_operations);

    auto schedule_start = std::chrono::system_clock::now();
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      results.emplace_back(collection.get(document_ids[i], {}));
      fmt::print("\rscheduled get: {}", i);
    }
    auto schedule_end = std::chrono::system_clock::now();
    fmt::print(
      "\rScheduled {} get operations in {}ms ({}us, {}s)\n",
      results.size(),
      std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(schedule_end - schedule_start).count());

    auto completion_start = std::chrono::system_clock::now();
    for (std::size_t i = 0; i < arguments.number_of_operations; ++i) {
      auto [err, result] = results[i].get();
      if (err.ec()) {
        errors[err.ec().message()]++;
      }
      fmt::print("\rcompleted get: {}", i);
      fflush(stdout);
    }
    auto completion_end = std::chrono::system_clock::now();
    fmt::print(
      "\rCompleted {} get operations in {}ms ({}us, {}s)\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - completion_start)
        .count(),
      std::chrono::duration_cast<std::chrono::microseconds>(completion_end - completion_start)
        .count(),
      std::chrono::duration_cast<std::chrono::seconds>(completion_end - completion_start).count());

    fmt::print(
      "Executed {} get operations in {}ms ({}us, {}s), average latency: {}ms\n",
      arguments.number_of_operations,
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start)
        .count(),
      std::chrono::duration_cast<std::chrono::microseconds>(completion_end - schedule_start)
        .count(),
      std::chrono::duration_cast<std::chrono::seconds>(completion_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(completion_end - schedule_start)
          .count() /
        arguments.number_of_operations);

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
             std::chrono::duration_cast<std::chrono::seconds>(end - start).count(),
             std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() /
               arguments.number_of_operations);
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

  auto options = couchbase::cluster_options(arguments.username, arguments.password);
  options.apply_profile("wan_development");
  auto [connect_err, cluster] =
    couchbase::cluster::connect(arguments.connection_string, options).get();
  if (connect_err) {
    fmt::print("Unable to connect to cluster at \"{}\", error: {}\n",
               arguments.connection_string,
               connect_err);
  } else {
    auto collection = cluster.bucket(arguments.bucket_name)
                        .scope(arguments.scope_name)
                        .collection(arguments.collection_name);

    /**
     * Sequential workload consists of two parts:
     * - first it writes {number_of_operations} operations to the collections, waiting for each
     * operation to complete
     * - and then it reads the same documents back, again waiting for each operation to complete
     */
    run_workload_sequential(collection, arguments);

    /**
     * Bulk workload is similar to sequential, but it does not wait for the operation to complete
     * before starting the next one. Instead it starts {number_of_operations} operations, and stores
     * result futures in the collection, deferring the waiting process until whole bulk will be
     * started, and then it waits to fulfill the futures and get the results.
     *
     * This style is applicable when there is not dependencies between operations, and the
     * application can afford to run them asynchronously.
     */
    run_workload_bulk(collection, arguments);
  }

  cluster.close().get();

  return 0;
}
