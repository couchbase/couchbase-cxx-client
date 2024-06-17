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
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

namespace blocking_txn
{
//! [blocking-txn]
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>

#include <tao/json.hpp>

int
main(int argc, const char* argv[])
{
  if (argc != 4) {
    fmt::print("USAGE: ./blocking-txn couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  int retval = 0;

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  // run IO context on separate thread
  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });

  auto options = couchbase::cluster_options(username, password);
  // customize through the 'options'.
  // For example, optimize timeouts for WAN
  options.apply_profile("wan_development");

  // [1] connect to cluster using the given connection string and the options
  auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
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
                       [=](auto err_ctx_2, auto res) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_1,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}, cas={}\n", id_1, res.cas());
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
                       [=](auto err_ctx_2, auto res) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_2,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}, cas={}\n", id_2, res.cas());
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
                       [=](auto err_ctx_2, auto res) {
                         if (err_ctx_2.ec()) {
                           fmt::print(stderr,
                                      "error replacing content in doc {}: {}\n",
                                      id_3,
                                      err_ctx_2.ec().message());
                         } else {
                           fmt::print("successfully replaced: {}, cas={}\n", id_3, res.cas());
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
  cluster.close();
  guard.reset();

  io_thread.join();
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
#include <couchbase/fmt/cas.hxx>
#include <couchbase/transactions/attempt_context.hxx>

#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

int
main(int argc, const char* argv[])
{
  if (argc != 4) {
    fmt::println("USAGE: ./read-local-txn couchbase://127.0.0.1 Administrator password");
    return 1;
  }

  int retval = 0;

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });

  auto options = couchbase::cluster_options(username, password);
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
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
    auto [tx_err, tx_res] = cluster.transactions()->run(
      [=](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [err, doc] = ctx->get_replica_from_preferred_server_group(collection, id);
        if (err) {
          fmt::println(stderr, "failed to get document \"{}\": {}", id, err.ec().message());
          return {};
        }
        fmt::println("document content: {}",
                     tao::json::to_string(doc.template content<tao::json::value>()));
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
  }

  {
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
                       tao::json::to_string(doc.template content<tao::json::value>()));
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
  }

  cluster.close();
  guard.reset();

  io_thread.join();
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
