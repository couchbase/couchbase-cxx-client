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

#include "test_helper_integration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/core.h>
#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

namespace blocking_txn
{
//! [blocking-txn]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/fmt/cas.hxx>

#include <tao/json.hpp>

auto
main(int argc, const char* argv[]) -> int
{
  if (argc != 4) {
    fmt::print("USAGE: ./blocking-txn couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  int retval = 0;

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto options = couchbase::cluster_options(username, password);
  // customize through the 'options'.
  // For example, optimize timeouts for WAN
  options.apply_profile("wan_development");

  // [1] connect to cluster using the given connection string and the options
  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // [2] persist three documents to the default collection of bucket "default"
  auto collection = cluster.bucket("default").default_collection();
  constexpr auto id_1 = "my-doc_1";
  constexpr auto id_2 = "my_doc_2";
  constexpr auto id_3 = "my_doc_3";
  const tao::json::value content = { { "some", "content" } };

  for (const auto& id : { id_1, id_2, id_3 }) {
    if (auto [err, res] = collection.upsert(id, content).get(); err.ec()) {
      fmt::print(
        stderr, "upsert \"{}\" failed before starting transaction: {}\n", id, err.ec().message());
      return 1;
    }
  }

  { // [3] blocking transaction
    //! [simple-blocking-txn]
    auto [tx_err, tx_res] = cluster.transactions()->run(
      // [3.1] closure argument to run() method encapsulates logic, that has to be run in
      // transaction
      [=](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        // [3.2] get document
        auto [err_ctx, doc] = ctx->get(collection, id_1);
        if (err_ctx.ec()) {
          fmt::print(stderr, "failed to get document \"{}\": {}\n", id_1, err_ctx.ec().message());
          // [3.3] don't continue the transaction logic
          return {};
        }
        // [3.4] replace document's content
        ctx->replace(doc, tao::json::value{ { "some", "other content" } });
        return {};
      });
    // [3.5] check the overall status of the transaction
    if (tx_err.ec()) {
      fmt::print(stderr,
                 "error in transaction {}, cause: {}\n",
                 tx_err.ec().message(),
                 tx_err.cause().has_value() ? tx_err.cause().value().ec().message() : "");
      retval = 1;
    } else {
      fmt::print("transaction {} completed successfully\n", tx_res.transaction_id);
    }
    //! [simple-blocking-txn]
  }

  { // [4] asynchronous transaction
    //! [simple-async-txn]
    // [4.1] create promise to retrieve result from the transaction
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.transactions()->run(
      // [4.2] closure argument to run() method encapsulates logic, that has to be run in
      // transaction
      [=](std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
        // [4.3] get document
        ctx->get(collection, id_1, [=](auto err_ctx_1, auto doc) {
          if (err_ctx_1.ec()) {
            fmt::print(
              stderr, "failed to get document \"{}\": {}\n", id_1, err_ctx_1.ec().message());
            return;
          }
          // [4.4] replace document's content
          ctx->replace(doc,
                       tao::json::value{ { "some", "other async content" } },
                       [=](auto err_ctx_2, auto /*res*/) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_1,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}\n", id_1);
                         }
                       });
        });
        ctx->get(collection, id_2, [=](auto err_ctx_1, auto doc) {
          if (err_ctx_1.ec()) {
            fmt::print("error getting doc {}: {}", id_2, err_ctx_1.ec().message());
            return;
          }
          ctx->replace(doc,
                       tao::json::value{ { "some", "other async content" } },
                       [=](auto err_ctx_2, auto /*res*/) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_2,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}\n", id_2);
                         }
                       });
        });
        ctx->get(collection, id_3, [=](auto err_ctx_1, auto doc) {
          if (err_ctx_1.ec()) {
            fmt::print(stderr, "error getting doc {}: {}\n", id_3, err_ctx_1.ec().message());
            return;
          }
          ctx->replace(doc,
                       tao::json::value{ { "some", "other async content" } },
                       [=](auto err_ctx_2, auto /*res*/) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_3,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}\n", id_3);
                         }
                       });
        });
        return {};
      },
      // [4.5], second closure represents transaction completion logic
      [barrier](auto tx_err, auto tx_res) {
        if (tx_err.ec()) {
          fmt::print(stderr,
                     "error in async transaction {}, {}\n",
                     tx_res.transaction_id,
                     tx_err.ec().message());
        }
        barrier->set_value(tx_err.ec());
      });
    if (auto async_err = f.get()) {
      fmt::print(stderr, "received async error from future: message - {}\n", async_err.message());
      retval = 1;
    }
    //! [simple-async-txn]
  }

  // [5], close cluster connection
  cluster.close().get();
  return retval;
}

//! [blocking-txn]
} // namespace blocking_txn

TEST_CASE("example: basic transaction", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  const auto env = test::utils::test_context::load_from_environment();
  const char* argv[] = {
    "blocking-txn", // name of the "executable"
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };

  REQUIRE(blocking_txn::main(4, argv) == 0);
}

namespace read_local_txn
{
//! [read-local-txn]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/transactions/attempt_context.hxx>

#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

auto
main(int argc, const char* argv[]) -> int
{
  if (argc != 4) {
    fmt::println("USAGE: ./read-local-txn couchbase://127.0.0.1 Administrator password");
    return 1;
  }

  int retval = 0;

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto options = couchbase::cluster_options(username, password);
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::println("unable to connect to the cluster: {}", connect_err);
    return 1;
  }

  auto collection = cluster.bucket("default").default_collection();
  constexpr auto id = "my-doc_1";
  const tao::json::value content = {
    { "some", "content" },
  };

  if (auto [upsert_err, res] = collection.upsert(id, content).get(); upsert_err.ec()) {
    fmt::println(stderr,
                 "upsert \"{}\" failed before starting transaction: {}",
                 id,
                 upsert_err.ec().message());
    return 1;
  }

  {
    //! [get_replica_from_preferred_server_group-sync]
    auto [tx_err, tx_res] = cluster.transactions()->run(
      [=](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [err, doc] = ctx->get_replica_from_preferred_server_group(collection, id);
        if (err) {
          fmt::println(stderr, "failed to get document \"{}\": {}", id, err.ec().message());
          return {};
        }
        fmt::println("document content: {}",
                     tao::json::to_string(doc.template content_as<tao::json::value>()));
        return {};
      });

    if (tx_err.ec()) {
      fmt::println(stderr,
                   "error in transaction {}, cause: {}",
                   tx_err.ec().message(),
                   tx_err.cause().has_value() ? tx_err.cause().value().ec().message() : "");
      retval = 1;
    } else {
      fmt::println("transaction {} completed successfully", tx_res.transaction_id);
    }
    //! [get_replica_from_preferred_server_group-sync]
  }

  {
    //! [get_replica_from_preferred_server_group-async]
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.transactions()->run(
      // transaction logic
      [=](std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
        ctx->get_replica_from_preferred_server_group(collection, id, [=](auto err_ctx, auto doc) {
          if (err_ctx.ec()) {
            fmt::print(stderr, "failed to get document \"{}\": {}\n", id, err_ctx.ec().message());
            return;
          }
          fmt::println("document content: {}",
                       tao::json::to_string(doc.template content_as<tao::json::value>()));
        });
        return {};
      },
      // completion logic
      [barrier](auto tx_err, auto tx_res) {
        if (tx_err.ec()) {
          fmt::print(stderr,
                     "error in async transaction {}, {}\n",
                     tx_res.transaction_id,
                     tx_err.ec().message());
        }
        barrier->set_value(tx_err.ec());
      });
    if (auto async_err = f.get()) {
      fmt::print(stderr, "received async error from future: message - {}\n", async_err.message());
      retval = 1;
    }
    //! [get_replica_from_preferred_server_group-async]
  }

  cluster.close().get();
  return retval;
}

//! [read-local-txn]
} // namespace read_local_txn

TEST_CASE("example: read from local server group in transaction", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (integration.cluster_version().is_mock()) {
    SKIP("GOCAVES does not support server groups");
  }
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }
  const auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  const auto server_groups = integration.server_groups();
  if (server_groups.size() != 2) {
    SKIP(fmt::format("This test expects exactly 2 server groups and at least one replica, "
                     "but found {} server groups",
                     integration.server_groups().size()));
  }

  const auto env = test::utils::test_context::load_from_environment();
  const char* argv[] = {
    "read-local-txn", // name of the "executable"
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };

  REQUIRE(read_local_txn::main(4, argv) == 0);
}

class byte_appender
{
public:
  using iterator_category = std::output_iterator_tag;
  using value_type = void;

  explicit byte_appender(std::vector<std::byte>& output)
    : buffer_{ output }
  {
  }

  auto operator=(char ch) -> byte_appender&
  {
    buffer_.push_back(static_cast<std::byte>(ch));
    return *this;
  }

  auto operator*() -> byte_appender&
  {
    return *this;
  }

  auto operator++() const -> byte_appender
  {
    return *this;
  }

  auto operator++(int) const -> byte_appender
  {
    return *this;
  }

private:
  std::vector<std::byte>& buffer_;
};

template<>
struct fmt::detail::is_output_iterator<byte_appender, char> : std::true_type {
};

//! [binary_object_in_transactions-ledger]
struct ledger_entry {
  std::string date{};
  std::string description{};
  std::string account{};
  std::uint64_t debit{};
  std::uint64_t credit{};
};

class ledger
{
public:
  void add_record(const std::string& date,
                  const std::string& from_account,
                  const std::string& to_account,
                  std::uint64_t amount,
                  const std::string& description)
  {
    entries_.push_back({
      date,
      description,
      to_account,
      /* debit  */ amount,
      /* credit */ 0,
    });
    entries_.push_back({
      date,
      description,
      from_account,
      /* debit  */ 0,
      /* credit */ amount,
    });
  }

  [[nodiscard]] auto entries() const -> const std::vector<ledger_entry>&
  {
    return entries_;
  }

  [[nodiscard]] auto to_csv() const -> std::vector<std::byte>
  {
    if (entries_.empty()) {
      return {
        std::byte{ '\n' },
      };
    }

    std::vector<std::byte> buffer;
    byte_appender output{ buffer };

    fmt::format_to(output, "Date,Description,Account,Debit,Credit\n");
    for (const auto& entry : entries_) {
      fmt::format_to(output,
                     "{},{},{},{},{}\n",
                     entry.date,
                     entry.description,
                     entry.account,
                     entry.debit,
                     entry.credit);
    }
    return buffer;
  }

  static auto from_csv(const std::vector<std::byte>& blob) -> ledger
  {
    ledger ret;

    std::istringstream input(std::string{
      reinterpret_cast<const char*>(blob.data()),
      blob.size(),
    });
    std::string line;

    bool header_line{ true };
    while (std::getline(input, line)) {
      if (header_line) {
        header_line = false;
        continue;
      }
      std::istringstream line_stream(line);

      ledger_entry entry;

      std::getline(line_stream, entry.date, ',');
      std::getline(line_stream, entry.description, ',');
      std::getline(line_stream, entry.account, ',');

      std::string field;
      std::getline(line_stream, field, ',');
      if (!field.empty()) {
        entry.debit = std::stoul(field);
      }
      std::getline(line_stream, field, ',');
      if (!field.empty()) {
        entry.credit = std::stoul(field);
      }

      ret.entries_.push_back(entry);
    }
    return ret;
  }

private:
  std::vector<ledger_entry> entries_{};
};
//! [binary_object_in_transactions-ledger]

//! [binary_object_in_transactions-ledger_transcoder]
struct csv_transcoder {
  using document_type = ledger;

  template<typename Document = document_type>
  static auto encode(const Document& document) -> couchbase::codec::encoded_value
  {
    return {
      document.to_csv(),
      couchbase::codec::codec_flags::binary_common_flags,
    };
  }

  template<typename Document = document_type>
  static auto decode(const couchbase::codec::encoded_value& encoded) -> Document
  {
    if (encoded.flags == 0 &&
        !couchbase::codec::codec_flags::has_common_flags(
          encoded.flags, couchbase::codec::codec_flags::binary_common_flags)) {
      throw std::system_error(
        couchbase::errc::common::decoding_failure,
        "csv_transcoder expects document to have binary common flags, flags=" +
          std::to_string(encoded.flags));
    }

    return Document::from_csv(encoded.data);
  }
};

template<>
struct couchbase::codec::is_transcoder<csv_transcoder> : public std::true_type {
};
//! [binary_object_in_transactions-ledger_transcoder]

namespace binary_objects_in_transactions
{
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>

#include <tao/json.hpp>

auto
main(int argc, const char* argv[]) -> int
{
  if (argc != 4) {
    fmt::print("USAGE: ./blocking-txn couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  int retval = 0;

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto options = couchbase::cluster_options(username, password);
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  auto collection = cluster.bucket("default").default_collection();

  //! [binary_object_in_transactions-initial_state]
  // Lets represent a ledger, which keeps the moving of funds between accounts
  // in the system. Lets also assume that the system cannot use JSON representation
  // for some reason, and we will be storing the ledger formatted as CSV (comma-
  // separated values).
  //
  // This is how the ledger might look like at some point in time:
  //
  // Date,Description,Account,Debit,Credit
  // 2024-08-30,Payment received,Cash,1500,0
  // 2024-08-30,Payment received,Accounts Receivable,0,1500
  // 2024-08-31,Rent payment,Expenses,1000,0
  // 2024-08-31,Rent payment,Cash,0,1000
  // 2024-09-01,Office Supplies,Expenses,200,0
  // 2024-09-01,Office Supplies,Cash,0,200
  // 2024-09-02,Client Invoice,Accounts Receivable,1200,0
  // 2024-09-02,Client Invoice,Revenue,0,1200
  //
  // The application must inform the SDK that this is a "binary" (as a opposed
  // to "JSON") data, and provide custom transcoder to ensure that the SDK will
  // handle everything correctly.
  ledger initial_state;
  initial_state.add_record("2024-08-30", "Accounts Receivable", "Cash", 1500, "Payment received");
  auto [err, res] = collection.upsert<csv_transcoder, ledger>("the_ledger", initial_state).get();
  if (err.ec()) {
    fmt::print(
      stderr,
      "Create initial state of \"the_ledger\" has failed before starting transaction: {}\n",
      err.ec().message());
    return 1;
  }
  //! [binary_object_in_transactions-initial_state]

  {
    //! [binary_object_in_transactions-sync]
    auto [tx_err, tx_res] = cluster.transactions()->run(
      [=](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        //! [binary_object_in_transactions-sync_replace]
        //! [binary_object_in_transactions-sync_get]
        auto [err_ctx, doc] = ctx->get(collection, "the_ledger");
        if (err_ctx.ec()) {
          fmt::print(stderr, "Failed to retrieve \"the_ledger\": {}\n", err_ctx.ec().message());
          return {};
        }

        // decode binary object into an application struct
        auto the_ledger = doc.content_as<ledger, csv_transcoder>();
        //! [binary_object_in_transactions-sync_get]
        the_ledger.add_record("2024-09-01", "Cash", "Expenses", 1000, "Rent payment");
        // replace the document contents, that will be treated by Couchbase as a binary object
        ctx->replace<csv_transcoder, ledger>(doc, the_ledger);
        //! [binary_object_in_transactions-sync_replace]
        return {};
      });

    if (tx_err.ec()) {
      fmt::print(stderr,
                 "error in transaction {}, cause: {}\n",
                 tx_err.ec().message(),
                 tx_err.cause().has_value() ? tx_err.cause().value().ec().message() : "");
      retval = 1;
    } else {
      fmt::print("transaction {} completed successfully\n", tx_res.transaction_id);
    }
    //! [binary_object_in_transactions-sync]
  }

  {
    //! [binary_object_in_transactions-async]
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.transactions()->run(
      [=](std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
        ctx->get(collection, "the_ledger", [=](auto err_ctx_1, auto doc) {
          if (err_ctx_1.ec()) {
            fmt::print(
              stderr, "failed to get document \"the_ledger\": {}\n", err_ctx_1.ec().message());
            return;
          }

          // decode binary object into an application struct
          auto the_ledger = doc.template content_as<ledger, csv_transcoder>();
          the_ledger.add_record("2024-09-01", "Cash", "Expenses", 200, "Office Supplies");

          ctx->replace<csv_transcoder, ledger>(
            doc, std::move(the_ledger), [=](auto err_ctx_2, auto /*res*/) {
              if (err_ctx_2.ec()) {
                fmt::print(stderr,
                           "error replacing content in doc \"the_ledger\": {}\n",
                           err_ctx_2.ec().message());
              } else {
                fmt::print("successfully replaced: \"the_ledger\"\n");
              }
            });
        });
        return {};
      },

      [barrier](auto tx_err, auto tx_res) {
        if (tx_err.ec()) {
          fmt::print(stderr,
                     "error in async transaction {}, {}\n",
                     tx_res.transaction_id,
                     tx_err.ec().message());
        }
        barrier->set_value(tx_err.ec());
      });
    if (auto async_err = f.get()) {
      fmt::print(stderr, "received async error from future: message - {}\n", async_err.message());
      retval = 1;
    }
    //! [binary_object_in_transactions-async]
  }

  cluster.close().get();
  return retval;
}

} // namespace binary_objects_in_transactions
TEST_CASE("example: binary objects in transactions", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }
  if (!integration.cluster_version().supports_binary_objects_in_transactions()) {
    SKIP("cluster does not support binary objects in transactions");
  }

  const auto env = test::utils::test_context::load_from_environment();
  const char* argv[] = {
    "binary-objects-in-transactions", // name of the "executable"
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };

  REQUIRE(binary_objects_in_transactions::main(4, argv) == 0);
}
