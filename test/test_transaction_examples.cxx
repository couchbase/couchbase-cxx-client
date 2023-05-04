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
#include <couchbase/transactions/attempt_context.hxx>
#include <tao/json.hpp>

namespace blocking_txn
{
//! [blocking-txn]
#include <couchbase/cluster.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <tao/json.hpp>

int
main(int argc, const char* argv[])
{
    if (argc != 4) {
        fmt::print("USAGE: ./blocking-txn couchbase://127.0.0.1 Administrator password\n");
        return 1;
    }

    int retval = 0;

    std::string connection_string{ argv[1] };
    std::string username{ argv[2] };
    std::string password{ argv[3] };
    // run IO context on separate thread
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    if (ec) {
        fmt::print("unable to connect to the cluster: {}\n", ec.message());
        return 1;
    }

    auto coll = cluster.bucket("default").default_collection();
    auto id = "my-doc";
    auto id2 = "my_doc_2";
    auto id3 = "my_doc_3";
    ::tao::json::value content = { { "some", "content" } };

    // upsert all 3...
    auto [upsert_err, upsert_res] = coll.upsert(id, content).get();
    if (upsert_err.ec()) {
        fmt::print("upsert failed before starting transaction");
        return 1;
    }
    auto [upsert_err2, upsert_res2] = coll.upsert(id2, content).get();
    if (upsert_err2.ec()) {
        fmt::print("upsert failed before starting transaction");
        return 1;
    }
    auto [upsert_err3, upsert_res3] = coll.upsert(id3, content).get();
    if (upsert_err3.ec()) {
        fmt::print("upsert failed before starting transaction");
        return 1;
    }

    //! [simple-blocking-txn]
    auto [e, txn_res] = cluster.transactions()->run([&](couchbase::transactions::attempt_context& ctx) {
        // get document
        auto [get_err, doc] = ctx.get(coll, id);
        if (get_err.ec()) {
            fmt::print("error getting doc {}: {}", id, get_err.ec().message());
            // don't continue the txn logic.
            return;
        }
        // replace document content
        ctx.replace(doc, ::tao::json::value{ { "some", "other content" } });
    });
    if (e.ec()) {
        fmt::print("error in transaction {}, {}", e.ec().message(), e.cause().message());
        retval = 1;
    } else {
        fmt::print("transaction {} completed successfully", txn_res.transaction_id);
    }
    //! [simple-blocking-txn]

    //! [simple-async-txn]
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.transactions()->run(
      [&](couchbase::transactions::async_attempt_context& ctx) {
          ctx.get(coll, id, [&](auto get_err, auto doc) {
              if (get_err.ec()) {
                  fmt::print("error getting doc {}: {}", id, get_err.ec().message());
              } else {
                  ctx.replace(doc, ::tao::json::value{ { "some", "other async content" } }, [&](auto replace_err, auto) {
                      if (replace_err.ec()) {
                          fmt::print("error replacing content in doc {}: {}", id, replace_err.ec().message());
                      }
                  });
              }
          });
          ctx.get(coll, id2, [&](auto get_err, auto doc) {
              if (get_err.ec()) {
                  fmt::print("error getting doc {}: {}", id2, get_err.ec().message());
              } else {
                  ctx.replace(doc, ::tao::json::value{ { "some", "other async content" } }, [&](auto replace_err, auto) {
                      if (replace_err.ec()) {
                          fmt::print("error replacing content in doc {}: {}", id, replace_err.ec().message());
                      }
                  });
              }
          });
          ctx.get(coll, id3, [&](auto get_err, auto doc) {
              if (get_err.ec()) {
                  fmt::print("error getting doc {}: {}", id, get_err.ec().message());
              } else {
                  ctx.replace(doc, ::tao::json::value{ { "some", "other async content" } }, [&](auto replace_err, auto) {
                      if (replace_err.ec()) {
                          fmt::print("error replacing content in doc {}: {}", id3, replace_err.ec().message());
                      }
                  });
              }
          });
      },
      [barrier](auto tx_err, auto tx_result) {
          if (tx_err.ec()) {
              fmt::print("error in async transaction {}, {}", tx_result.transaction_id, tx_err.ec().message());
          }
          barrier->set_value(tx_err.ec());
      });
    if (auto async_err = f.get()) {
        retval = 1;
    }
    //! [simple-async-txn]

    // close cluster connection
    cluster.close();
    guard.reset();

    io_thread.join();
    return retval;
}

//! [blocking-txn]
} // namespace blocking_txn

TEST_CASE("example: start using", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_collections()) {
        return;
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
