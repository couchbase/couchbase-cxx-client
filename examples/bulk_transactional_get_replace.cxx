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
#include <couchbase/transactions.hxx>

#include <spdlog/fmt/bundled/chrono.h>
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
  std::size_t number_of_keys{ 1'000 };
  std::size_t number_of_transactions{ 5 };
  std::size_t number_of_keys_per_transaction{ 10 };
  std::size_t document_body_size{ 1'024 };
  std::chrono::seconds transaction_timeout{ 120 };

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
    if (const auto* val = getenv("CB_NUMBER_OF_KEYS"); val != nullptr && val[0] != '\0') {
      char* end = nullptr;
      auto int_val = std::strtoul(val, &end, 10);
      if (end != val) {
        arguments.number_of_keys = int_val;
      }
    }
    if (const auto* val = getenv("CB_NUMBER_OF_TRANSACTIONS"); val != nullptr && val[0] != '\0') {
      char* end = nullptr;
      auto int_val = std::strtoul(val, &end, 10);
      if (end != val) {
        arguments.number_of_transactions = int_val;
      }
    }
    if (const auto* val = getenv("CB_NUMBER_OF_KEYS_PER_TRANSACTION");
        val != nullptr && val[0] != '\0') {
      char* end = nullptr;
      auto int_val = std::strtoul(val, &end, 10);
      if (end != val) {
        arguments.number_of_keys_per_transaction = int_val;
      }
    }
    if (const auto* val = getenv("CB_DOCUMENT_BODY_SIZE"); val != nullptr && val[0] != '\0') {
      char* end = nullptr;
      auto int_val = std::strtoul(val, &end, 10);
      if (end != val) {
        arguments.document_body_size = int_val;
      }
    }
    if (const auto* val = getenv("CB_TRANSACTION_TIMEOUT"); val != nullptr && val[0] != '\0') {
      char* end = nullptr;
      auto int_val = std::strtoul(val, &end, 10);
      if (end != val) {
        arguments.transaction_timeout = std::chrono::seconds{ int_val };
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
run_workload(const std::shared_ptr<couchbase::transactions::transactions>& transactions,
             const couchbase::collection& collection,
             const program_arguments& arguments)
{
  if (arguments.number_of_keys == 0 || arguments.number_of_keys_per_transaction == 0) {
    return;
  }

  const std::string document_id_prefix{ "tx_mix" };
  std::vector<std::string> document_ids;
  document_ids.reserve(arguments.number_of_keys);
  for (std::size_t i = 0; i < arguments.number_of_keys; ++i) {
    document_ids.emplace_back(fmt::format("{}_{:06d}", document_id_prefix, i));
  }
  fmt::print("Using {} IDs in interval [\"{}\"...\"{}\"]\n",
             document_ids.size(),
             document_ids[0],
             document_ids[document_ids.size() - 1]);

  auto start = std::chrono::system_clock::now();

  {
    std::map<std::string, std::size_t> errors;

    using transaction_promise =
      std::promise<std::pair<couchbase::error, couchbase::transactions::transaction_result>>;
    std::vector<transaction_promise> results;
    results.resize(arguments.number_of_transactions);

    auto schedule_start = std::chrono::system_clock::now();

    for (std::size_t i = 0; i < arguments.number_of_transactions; ++i) {
      transactions->run(
        [&collection, &document_ids, &arguments, &errors](
          std::shared_ptr<couchbase::transactions::async_attempt_context> attempt)
          -> couchbase::error {
          std::vector<std::string> selected_keys;
          std::sample(document_ids.begin(),
                      document_ids.end(),
                      std::back_insert_iterator(selected_keys),
                      arguments.number_of_keys_per_transaction,
                      std::mt19937_64{ std::random_device()() });

          for (const auto& id : selected_keys) {
            attempt->get(
              collection, id, [attempt, &collection, id, &arguments, &errors](auto ctx, auto res) {
                if (ctx.ec() == couchbase::errc::transaction_op::document_not_found) {
                  attempt->insert(collection,
                                  id,
                                  generate_document(arguments.document_body_size),
                                  [&errors](auto ctx, auto) {
                                    if (ctx.ec()) {
                                      errors[ctx.ec().message()]++;
                                    }
                                  });
                } else if (ctx.ec()) {
                  errors[ctx.ec().message()]++;
                } else {
                  attempt->replace(res,
                                   generate_document(arguments.document_body_size),
                                   [&errors](auto ctx, auto) {
                                     if (ctx.ec()) {
                                       errors[ctx.ec().message()]++;
                                     }
                                   });
                }
              });
          }
          return {};
        },
        [&promise = results[i]](auto err, auto result) {
          promise.set_value({ err, result });
        });
    }

    auto schedule_end = std::chrono::system_clock::now();
    fmt::print(
      "\rScheduled {} transactions with {} GET+[INSERT|REPLACE] operations in {}ms ({}us, {}s)\n",
      arguments.number_of_transactions,
      arguments.number_of_keys_per_transaction,
      std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(schedule_end - schedule_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(schedule_end - schedule_start).count());

    std::map<std::string, std::size_t> transactions_errors;
    auto exec_start = std::chrono::system_clock::now();
    for (auto& promise : results) {
      auto [err, result] = promise.get_future().get();
      if (err.ec()) {
        transactions_errors[fmt::format("error={}, cause={}",
                                        err.ec().message(),
                                        err.cause().has_value() ? err.cause().value().ec().message()
                                                                : "")]++;
      }
    }
    auto exec_end = std::chrono::system_clock::now();

    fmt::print(
      "\rExecuted {} transactions with {} GET+[INSERT|REPLACE] operations in {}ms ({}us, {}s), "
      "average latency: {}ms\n",
      arguments.number_of_transactions,
      arguments.number_of_keys_per_transaction,
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::seconds>(exec_end - exec_start).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(exec_end - exec_start).count() /
        arguments.number_of_keys_per_transaction);
    if (transactions_errors.empty()) {
      fmt::print("\tAll transactions completed successfully\n");
    } else {
      fmt::print("\tSome transactions completed with errors:\n");
      for (auto [error, hits] : transactions_errors) {
        fmt::print("\t\t{}: {}\n", error, hits);
      }
    }
    if (errors.empty()) {
      fmt::print("\tAll operations completed successfully\n");
    } else {
      fmt::print("\tSome operations completed with errors:\n");
      for (auto [error, hits] : errors) {
        fmt::print("\t\t{}: {}\n", error, hits);
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
  fmt::print("CB_NUMBER_OF_KEYS={}\n", arguments.number_of_keys);
  fmt::print("CB_NUMBER_OF_TRANSACTIONS={}\n", arguments.number_of_transactions);
  fmt::print("CB_NUMBER_OF_KEYS_PER_TRANSACTION={}\n", arguments.number_of_keys_per_transaction);
  fmt::print("CB_DOCUMENT_BODY_SIZE={}\n", arguments.document_body_size);
  fmt::print("CB_TRANSACTION_TIMEOUT={}\n", arguments.transaction_timeout.count());

  auto options = couchbase::cluster_options(arguments.username, arguments.password);
  options.apply_profile("wan_development");
  options.transactions().timeout(arguments.transaction_timeout);
  auto [connect_err, cluster] =
    couchbase::cluster::connect(arguments.connection_string, options).get();
  if (connect_err) {
    fmt::print("Unable to connect to cluster at \"{}\", error: {}\n",
               arguments.connection_string,
               connect_err);
  } else {
    auto transactions = cluster.transactions();
    auto collection = cluster.bucket(arguments.bucket_name)
                        .scope(arguments.scope_name)
                        .collection(arguments.collection_name);

    run_workload(transactions, collection, arguments);
  }

  cluster.close().get();

  return 0;
}
