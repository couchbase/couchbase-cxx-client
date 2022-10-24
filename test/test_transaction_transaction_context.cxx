/*
 *     Copyright 2022 Couchbase, Inc.
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

#include "test_helper.hxx"
#include "utils/transactions_env.h"

#include "core/transactions/attempt_context_impl.hxx"

#include "core/transactions.hxx"
#include "core/transactions/internal/transaction_context.hxx"

#include <spdlog/spdlog.h>

#include <future>
#include <stdexcept>

using namespace couchbase::core::transactions;
static const tao::json::value tx_content{
    { "some", "thing" },
};

void
txn_completed(std::exception_ptr err, std::shared_ptr<std::promise<void>> barrier)
{
    if (err) {
        barrier->set_exception(err);
    } else {
        barrier->set_value();
    }
}

// blocking txn logic wrapper
template<typename Handler>
couchbase::transactions::transaction_result
simple_txn_wrapper(transaction_context& tx, Handler&& handler)
{
    size_t attempts{ 0 };
    while (attempts++ < 1000) {
        auto barrier = std::make_shared<std::promise<std::optional<couchbase::transactions::transaction_result>>>();
        auto f = barrier->get_future();
        tx.new_attempt_context();
        // in transactions.run, we currently handle exceptions that may come back from the
        // txn logic as well (using tx::handle_error).
        handler();
        tx.finalize([barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
            if (err) {
                return barrier->set_exception(std::make_exception_ptr(*err));
            }
            return barrier->set_value(result);
        });
        if (auto res = f.get()) {
            return *res;
        }
    }
    throw std::runtime_error("exceeded max attempts and didn't timeout!");
}

TEST_CASE("transactions: can do simple transaction with transaction wrapper", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    tao::json::value new_content{
        { "some", "thing else" },
    };

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    auto txn_logic = [&id, &tx, &new_content]() {
        tx.get(id, [&tx, &new_content](std::exception_ptr err, std::optional<transaction_get_result> res) {
            CHECK(res);
            CHECK_FALSE(err);
            tx.replace(*res,
                       couchbase::core::utils::json::generate_binary(new_content),
                       [](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
                           CHECK(replaced);
                           CHECK_FALSE(err);
                       });
        });
    };
    auto result = simple_txn_wrapper(tx, txn_logic);
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == new_content);
}

TEST_CASE("transactions: can do simple transaction with finalize", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&tx, &new_content](std::exception_ptr err, std::optional<transaction_get_result> res) {
        CHECK(res);
        CHECK_FALSE(err);
        tx.replace(*res,
                   couchbase::core::utils::json::generate_binary(new_content),
                   [](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
                       CHECK(replaced);
                       CHECK_FALSE(err);
                   });
    });
    tx.finalize(
      [&barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> /* result */) {
          if (err) {
              return barrier->set_exception(std::make_exception_ptr(*err));
          }
          return barrier->set_value();
      });
    f.get();
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == new_content);
}

TEST_CASE("transactions: can do simple transaction explicit commit", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&tx, &new_content, &barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        CHECK(res);
        CHECK_FALSE(err);
        tx.replace(*res,
                   couchbase::core::utils::json::generate_binary(new_content),
                   [&tx, &barrier](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
                       CHECK(replaced);
                       CHECK_FALSE(err);
                       tx.commit([&barrier](std::exception_ptr err) {
                           CHECK_FALSE(err);
                           txn_completed(err, barrier);
                       });
                   });
    });
    f.get();
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == new_content);
}

TEST_CASE("transactions: can do rollback simple transaction", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&tx, &new_content, &barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        CHECK(res);
        CHECK_FALSE(err);
        tx.replace(*res,
                   couchbase::core::utils::json::generate_binary(new_content),
                   [&tx, &barrier](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
                       CHECK(replaced);
                       CHECK_FALSE(err);
                       // now rollback
                       tx.rollback([&barrier](std::exception_ptr err) {
                           CHECK_FALSE(err); // no error rolling back
                           barrier->set_value();
                       });
                   });
    });
    f.get();
    // this should not throw, as errors should be empty.
    REQUIRE_NOTHROW(tx.existing_error());
}

TEST_CASE("transactions: can get insert errors", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.insert(id,
              couchbase::core::utils::json::generate_binary(tx_content),
              [&barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
                  // this should result in a transaction_operation_failed exception since it already exists, so lets check it
                  CHECK(err);
                  CHECK_FALSE(result);
                  if (err) {
                      barrier->set_exception(err);
                  } else {
                      barrier->set_value();
                  }
              });
    CHECK_THROWS_AS(f.get(), transaction_operation_failed);
    CHECK_THROWS_AS(tx.existing_error(), transaction_operation_failed);
}

TEST_CASE("transactions: can get remove errors", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&tx, &barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        CHECK_FALSE(err);
        CHECK(result);
        // make a cas mismatch error
        result->cas(100);
        tx.remove(*result, [&barrier](std::exception_ptr err) {
            CHECK(err);
            if (err) {
                barrier->set_exception(err);
            } else {
                barrier->set_value();
            }
        });
    });
    CHECK_THROWS_AS(f.get(), transaction_operation_failed);
    CHECK_THROWS_AS(tx.existing_error(), transaction_operation_failed);
}

TEST_CASE("transactions: can get replace errors", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&tx, &barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        CHECK_FALSE(err);
        CHECK(result);
        // make a cas mismatch error
        result->cas(100);
        tx.replace(*result,
                   couchbase::core::utils::json::generate_binary(tx_content),
                   [&barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
                       CHECK(err);
                       CHECK_FALSE(result);
                       if (err) {
                           barrier->set_exception(err);
                       } else {
                           barrier->set_value();
                       }
                   });
    });
    CHECK_THROWS_AS(f.get(), transaction_operation_failed);
    CHECK_THROWS_AS(tx.existing_error(), transaction_operation_failed);
}

TEST_CASE("transactions: RYOW get after insert", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto logic = [&tx, &id]() {
        tx.insert(id,
                  couchbase::core::utils::json::generate_binary(tx_content),
                  [&tx, &id](std::exception_ptr err, std::optional<transaction_get_result> res) {
                      CHECK_FALSE(err);
                      CHECK(res);
                      tx.get(id, [](std::exception_ptr err, std::optional<transaction_get_result> res) {
                          CHECK_FALSE(err);
                          CHECK(res->content<tao::json::value>() == tx_content);
                      });
                  });
    };
    REQUIRE_NOTHROW(simple_txn_wrapper(tx, logic));
    REQUIRE_NOTHROW(tx.existing_error());
}

TEST_CASE("transactions: can get get errors", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        CHECK(err);
        CHECK_FALSE(result);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    CHECK_THROWS_AS(f.get(), transaction_operation_failed);
    CHECK_THROWS_AS(tx.existing_error(), transaction_operation_failed);
}

TEST_CASE("transactions: can do query", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, tx_content));
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    couchbase::transactions::transaction_query_options opts;
    tx.query(query, opts, [&barrier](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> payload) {
        // this should result in a transaction_operation_failed exception since the doc isn't there
        CHECK(payload);
        CHECK_FALSE(err);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    REQUIRE_NOTHROW(f.get());
    REQUIRE_NOTHROW(tx.existing_error());
}

TEST_CASE("transactions: can see some query errors but no transactions failed", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    couchbase::transactions::transaction_query_options opts;
    tx.query("jkjkjl;kjlk;  jfjjffjfj",
             opts,
             [&barrier](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> payload) {
                 // this should result in a query_exception since the query isn't parseable.
                 CHECK(err);
                 CHECK_FALSE(payload);
                 if (err) {
                     barrier->set_exception(err);
                 } else {
                     barrier->set_value();
                 }
             });
    try {
        f.get();
        FAIL("expected future to throw exception");
    } catch (const query_exception&) {

    } catch (...) {
        auto e = std::current_exception();
        std::cout << "got " << typeid(e).name() << std::endl;
        FAIL("expected query_exception to be thrown from the future");
    }
    REQUIRE_NOTHROW(tx.existing_error());
}

TEST_CASE("transactions: can set per transaction config", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    couchbase::transactions::transaction_options per_txn_cfg;
    per_txn_cfg.scan_consistency(couchbase::query_scan_consistency::not_bounded)
      .expiration_time(std::chrono::milliseconds(1))
      .kv_timeout(std::chrono::milliseconds(2))
      .durability_level(couchbase::durability_level::none);
    transaction_context tx(txns, per_txn_cfg);
    REQUIRE(tx.config().level == per_txn_cfg.durability_level());
    REQUIRE(tx.config().kv_timeout == per_txn_cfg.kv_timeout());
    REQUIRE(tx.config().expiration_time == per_txn_cfg.expiration_time());
    REQUIRE(tx.config().query_config.scan_consistency == per_txn_cfg.scan_consistency());
}

TEST_CASE("transactions: can not per transactions config", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    transaction_context tx(txns);
    REQUIRE(tx.config().level == txns.config().level);
    REQUIRE(tx.config().kv_timeout == txns.config().kv_timeout);
    REQUIRE(tx.config().expiration_time == txns.config().expiration_time);
    REQUIRE(tx.config().query_config.scan_consistency == txns.config().query_config.scan_consistency);
}
