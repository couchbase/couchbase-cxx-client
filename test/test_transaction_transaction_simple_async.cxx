/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "simple_object.hxx"
#include "test_helper.hxx"

#include "core/transactions/attempt_context_impl.hxx"

#include "utils/transactions_env.h"

#include <spdlog/spdlog.h>

#include <future>
#include <list>
#include <stdexcept>

using namespace couchbase::core::transactions;

static const tao::json::value async_content{
    { "some", "thing" },
};

void
txn_completed(std::optional<transaction_exception> err,
              std::optional<couchbase::transactions::transaction_result> /* result */,
              std::shared_ptr<std::promise<void>> barrier)
{
    if (err) {
        barrier->set_exception(std::make_exception_ptr(*err));
    } else {
        barrier->set_value();
    }
}

TEST_CASE("transactions: async get", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [id, cb_called](async_attempt_context& ctx) {
          ctx.get(id, [cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  cb_called->store(true);
                  CHECK(res);
                  CHECK(res->content<tao::json::value>() == async_content);
              }
          });
      },
      [cb_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(cb_called->load());
          txn_completed(std::move(err), std::move(res), barrier);
      });
    f.get();
}

TEST_CASE("transactions: can't get from unknown bucket", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    couchbase::core::document_id bad_id{ "secBucket", "_default", "default", uid_generator::next() };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [&bad_id, cb_called, barrier](async_attempt_context& ctx) {
          ctx.get(bad_id, [cb_called, barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
              cb_called->store(true);
              CHECK(err);
              CHECK_FALSE(result);
          });
      },
      [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(cb_called->load());
          txn_completed(std::move(err), std::move(res), barrier);
      });
    REQUIRE_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(cb_called->load());
}

TEST_CASE("transactions: async get fail", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    try {
        txns.run(
          [cb_called, id](async_attempt_context& ctx) {
              ctx.get(id, [cb_called](std::exception_ptr err, std::optional<transaction_get_result> /* res */) {
                  // should be an error
                  CHECK(err);
                  cb_called->store(true);
              });
          },
          [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
              CHECK(cb_called->load());
              txn_completed(std::move(err), std::move(res), barrier);
          });
        f.get();
        FAIL("expected transaction_exception!");
    } catch (const transaction_exception& e) {
        // nothing to do here, but make sure
        REQUIRE(cb_called->load());
        REQUIRE(e.type() == failure_type::FAIL);
    } catch (const std::exception&) {
        FAIL("expected a transaction_failed exception, but got something else");
    }
}

TEST_CASE("transactions: async remove fail", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    try {
        txns.run(
          [cb_called, id](async_attempt_context& ctx) {
              ctx.get(id, [&ctx, cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  // let's just change the cas to make it fail, which it should
                  // do until timeout
                  if (!err) {
                      res->cas(100);
                      ctx.remove(*res, [cb_called](std::exception_ptr err) {
                          CHECK(err);
                          cb_called->store(true);
                      });
                  }
              });
          },
          [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
              CHECK(cb_called->load());
              txn_completed(std::move(err), std::move(res), barrier);
          });
        f.get();
        FAIL("expected txn to fail until timeout, or error out during rollback");
    } catch (const transaction_exception&) {
        REQUIRE(cb_called->load());
    }
}

TEST_CASE("transactions: RYOW on insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [cb_called, id](async_attempt_context& ctx) {
          ctx.insert(id, async_content, [&ctx, cb_called, id](std::exception_ptr err, std::optional<transaction_get_result> res) {
              CHECK_FALSE(err);
              CHECK(res);
              ctx.get(id, [cb_called, id](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  CHECK_FALSE(err);
                  CHECK(res);
                  CHECK(res->content<tao::json::value>() == async_content);
                  cb_called->store(res.has_value());
              });
          });
      },
      [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK_FALSE(err);
          CHECK(res);
          CHECK(cb_called->load());
          txn_completed(err, std::move(res), barrier);
      });
    f.get();
    REQUIRE(cb_called->load());
}

TEST_CASE("transactions: async remove", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [cb_called, id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  ctx.remove(*res, [cb_called](std::exception_ptr err) {
                      CHECK_FALSE(err);
                      cb_called->store(true);
                  });
              }
          });
      },
      [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(cb_called->load());
          txn_completed(std::move(err), res, barrier);
      });
    f.get();
    REQUIRE(cb_called->load());
    try {
        TransactionsTestEnvironment::get_doc(id);
        FAIL("expected get_doc to raise client exception");
    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: async replace", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value new_content{
        { "shiny", "and new" },
    };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [cb_called, &new_content, id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, &new_content, cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  ctx.replace(*res,
                              new_content,
                              [old_cas = res->cas(), cb_called](std::exception_ptr err, std::optional<transaction_get_result> result) {
                                  // replace doesn't actually put the new content in the
                                  // result, but it does change the cas, so...
                                  CHECK_FALSE(err);
                                  CHECK(result->cas() != old_cas);
                                  cb_called->store(true);
                              });
              }
          });
      },
      [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(cb_called->load());
          txn_completed(std::move(err), res, barrier);
      });
    f.get();
    REQUIRE(cb_called->load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>();
    REQUIRE(content == new_content);
}

TEST_CASE("transactions: async replace fail", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value new_content{
        { "shiny", "and new" },
    };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    try {
        txns.run(
          [cb_called, &new_content, id](async_attempt_context& ctx) {
              ctx.get(id, [&ctx, &new_content, cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  if (!err) {
                      ctx.replace(
                        *res, new_content, [cb_called](std::exception_ptr err, std::optional<transaction_get_result> /* result */) {
                            if (!err) {
                                cb_called->store(true);
                                throw std::runtime_error("I wanna roll back");
                            }
                        });
                  }
              });
          },
          [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
              CHECK(cb_called->load());
              txn_completed(std::move(err), res, barrier);
          });
        f.get();
        FAIL("expected exception");
    } catch (const transaction_exception& e) {
        REQUIRE(cb_called->load());
        auto content = TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>();
        REQUIRE(content == async_content);
        REQUIRE(e.type() == failure_type::FAIL);
    };
}

TEST_CASE("transactions: async insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [cb_called, id](async_attempt_context& ctx) {
          ctx.insert(id, async_content, [cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  CHECK_FALSE(res->cas().empty());
                  cb_called->store(true);
              }
          });
      },
      [barrier, cb_called](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(cb_called->load());
          txn_completed(std::move(err), res, barrier);
      });
    f.get();
    REQUIRE(cb_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == async_content);
}

TEST_CASE("transactions: async insert fail", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    try {
        txns.run(
          [cb_called, id, barrier](async_attempt_context& ctx) {
              ctx.insert(id, async_content, [cb_called](std::exception_ptr err, std::optional<transaction_get_result> /* res */) {
                  if (!err) {
                      cb_called->store(true);
                      throw std::runtime_error("I wanna rollback");
                  }
              });
          },
          [barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
              CHECK(err);
              CHECK(err->type() == failure_type::FAIL);
              txn_completed(std::move(err), std::move(result), barrier);
          });
        f.get();
        FAIL("Expected exception");
    } catch (const transaction_exception& e) {
        REQUIRE(cb_called->load());
        REQUIRE(e.type() == failure_type::FAIL);
        try {
            TransactionsTestEnvironment::get_doc(id);
            FAIL("expected get_doc to raise client exception");
        } catch (const client_error& e) {
            REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
        }
    }
}

TEST_CASE("transactions: async query", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    auto f = barrier->get_future();
    auto query_called = std::make_shared<std::atomic<bool>>(false);
    txns.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query,
                    [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                        if (!err) {
                            query_called->store(true);
                        }
                    });
      },
      [query_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(query_called->load());
          CHECK_FALSE(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    f.get();
    REQUIRE(query_called->load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>();
    REQUIRE(content["some"].as<std::string>() == std::string("thing else"));
}

TEST_CASE("transactions: multiple racing queries", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    auto f = barrier->get_future();
    auto query_called = std::make_shared<std::atomic<int>>(0);
    txns.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query,
                    [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                        if (!err) {
                            ++(*query_called);
                        }
                    });
          ctx.query(query,
                    [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                        if (!err) {
                            ++(*query_called);
                        }
                    });
          ctx.query(query,
                    [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                        if (!err) {
                            ++(*query_called);
                        }
                    });
      },
      [query_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(3 == query_called->load());
          CHECK_FALSE(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    f.get();
    REQUIRE(3 == query_called->load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>();
    REQUIRE(content["some"].as<std::string>() == std::string("thing else"));
}

TEST_CASE("transactions: rollback async query", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    auto f = barrier->get_future();
    auto query_called = std::make_shared<std::atomic<bool>>(false);
    txns.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query,
                    [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                        if (!err) {
                            query_called->store(true);
                            // now rollback by throwing arbitrary exception
                            throw 3;
                        }
                    });
      },
      [query_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(query_called->load());
          CHECK(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    CHECK_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(query_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == async_content);
}

TEST_CASE("transactions: async KV get", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto get_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [get_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [get_called, &id, &ctx](std::exception_ptr /* err */, std::optional<transaction_get_result> /* result */) {
              auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
              ctx.query(
                query,
                [get_called, &id, &ctx](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                    if (!err) {
                        ctx.get(id, [get_called](std::exception_ptr err, std::optional<transaction_get_result>) {
                            if (!err) {
                                get_called->store(true);
                            }
                        });
                    }
                });
          });
      },
      [get_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(get_called->load());
          CHECK_FALSE(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    f.get();
    REQUIRE(get_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>()["some"].as<std::string>() == "thing else");
}

TEST_CASE("transactions: rollback async KV get", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto get_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [get_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, get_called, &id](std::exception_ptr /* err */, std::optional<transaction_get_result> /* result */) {
              auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
              ctx.query(
                query,
                [&ctx, get_called, &id](std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                    if (!err) {
                        ctx.get(id, [get_called](std::exception_ptr err, std::optional<transaction_get_result>) {
                            if (!err) {
                                get_called->store(true);
                                throw 3;
                            }
                        });
                    }
                });
          });
      },
      [&get_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(get_called->load());
          CHECK(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    REQUIRE_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(get_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>()["some"].as<std::string>() == "thing");
}

TEST_CASE("transactions: async KV insert", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto insert_called = std::make_shared<std::atomic<bool>>(false);
    txns.run(
      [insert_called, id, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [&ctx, insert_called, id, barrier](std::exception_ptr err,
                                                       std::optional<couchbase::core::operations::query_response> /* resp */) {
                        if (!err) {
                            ctx.insert(
                              id, async_content, [insert_called](std::exception_ptr err, std::optional<transaction_get_result> /* res */) {
                                  insert_called->store(!err);
                              });
                        }
                    });
      },
      [insert_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK_FALSE(err);
          CHECK(insert_called->load());
          txn_completed(std::move(err), std::move(res), barrier);
      });
    f.get();
    REQUIRE(insert_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == async_content);
}

TEST_CASE("transactions: rollback async KV insert", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto insert_called = std::make_shared<std::atomic<bool>>(false);
    txns.run(
      [insert_called, id, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [insert_called, &ctx, id, barrier](std::exception_ptr err,
                                                       std::optional<couchbase::core::operations::query_response> /* resp */) {
                        if (!err) {
                            ctx.insert(
                              id, async_content, [insert_called](std::exception_ptr err, std::optional<transaction_get_result> /* res */) {
                                  insert_called->store(!err);
                                  // now roll it back
                                  throw 3;
                              });
                        }
                    });
      },
      [insert_called, barrier](std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> res) {
          CHECK(err);
          CHECK(insert_called->load());
          txn_completed(std::move(err), std::move(res), barrier);
      });
    REQUIRE_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(insert_called->load());
    try {
        TransactionsTestEnvironment::get_doc(id);
        FAIL("expected get_doc to raise client exception");
    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: async KV replace", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto replace_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [replace_called, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [replace_called, id, &ctx, &new_content](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [replace_called, &ctx, &new_content, doc = *result](
                              std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                                if (!err) {
                                    ctx.replace(
                                      doc,
                                      new_content,
                                      [replace_called](std::exception_ptr err, std::optional<transaction_get_result> /* result */) {
                                          if (!err) {
                                              replace_called->store(true);
                                          }
                                      });
                                }
                            });
              }
          });
      },
      [&replace_called, barrier](std::optional<transaction_exception> err,
                                 std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(replace_called->load());
          CHECK_FALSE(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    f.get();
    REQUIRE(replace_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == new_content);
}

TEST_CASE("transactions: rollback async KV replace", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto replace_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [replace_called, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [replace_called, &ctx, &id, &new_content](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [replace_called, &ctx, &new_content, doc = *result](
                              std::exception_ptr err, std::optional<couchbase::core::operations::query_response> /* payload */) {
                                if (!err) {
                                    ctx.replace(
                                      doc,
                                      new_content,
                                      [replace_called](std::exception_ptr err, std::optional<transaction_get_result> /* result */) {
                                          if (!err) {
                                              replace_called->store(true);
                                              throw 3;
                                          }
                                      });
                                }
                            });
              }
          });
      },
      [&replace_called, barrier](std::optional<transaction_exception> err,
                                 std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(replace_called->load());
          CHECK(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    REQUIRE_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(replace_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == async_content);
}

TEST_CASE("transactions: async KV remove", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto remove_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [remove_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [remove_called, &ctx, &id](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [remove_called, &ctx, doc = *result](std::exception_ptr err,
                                                                 std::optional<couchbase::core::operations::query_response> /* payload */) {
                                if (!err) {
                                    ctx.remove(doc, [remove_called](std::exception_ptr err) {
                                        if (!err) {
                                            remove_called->store(true);
                                        }
                                    });
                                }
                            });
              }
          });
      },
      [remove_called, barrier](std::optional<transaction_exception> err,
                               std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(remove_called->load());
          CHECK_FALSE(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    f.get();
    REQUIRE(remove_called->load());
    try {
        TransactionsTestEnvironment::get_doc(id);
        FAIL("expected get_doc to raise client exception");
    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: rollback async KV remove", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto remove_called = std::make_shared<std::atomic<bool>>(false);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, async_content));
    txns.run(
      [remove_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [remove_called, &id, &ctx](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [remove_called, &ctx, doc = *result](std::exception_ptr err,
                                                                 std::optional<couchbase::core::operations::query_response> /* payload */) {
                                if (!err) {
                                    ctx.remove(doc, [remove_called](std::exception_ptr err) {
                                        REQUIRE_FALSE(err);
                                        remove_called->store(true);
                                        throw 3;
                                    });
                                }
                            });
              }
          });
      },
      [&remove_called, barrier](std::optional<transaction_exception> err,
                                std::optional<couchbase::transactions::transaction_result> result) {
          CHECK(remove_called->load());
          CHECK(err);
          txn_completed(std::move(err), std::move(result), barrier);
      });
    REQUIRE_THROWS_AS(f.get(), transaction_exception);
    REQUIRE(remove_called->load());
    REQUIRE(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == async_content);
}

TEST_CASE("transactions: async get replace", "[transactions]")
{
    static constexpr std::size_t NUM_TXNS{ 2 };
    tao::json::value doc1_content{
        { "number", 0 },
    };
    tao::json::value doc2_content{
        { "number", 200 },
    };
    auto id1 = TransactionsTestEnvironment::get_document_id();
    auto id2 = TransactionsTestEnvironment::get_document_id();
    TransactionsTestEnvironment::upsert_doc(id1, doc1_content);
    TransactionsTestEnvironment::upsert_doc(id2, doc2_content);
    auto txn = TransactionsTestEnvironment::get_transactions();
    auto attempts = std::make_shared<std::atomic<uint32_t>>(0);
    auto errors = std::make_shared<std::atomic<uint32_t>>(0);
    auto txns = std::make_shared<std::atomic<uint32_t>>(0);
    auto done = std::make_shared<std::atomic<bool>>(false);

    uint32_t in_flight{ 0 };
    std::condition_variable cv_in_flight;
    std::condition_variable cv_txns_complete;
    std::mutex mut;
    while (!done->load()) {
        std::unique_lock<std::mutex> lock(mut);
        cv_in_flight.wait(lock, [&in_flight] { return in_flight < NUM_TXNS; });
        in_flight++;
        lock.unlock();
        txn.run(
          [attempts, done, id1, id2](async_attempt_context& ctx) {
              ++(*attempts);
              ctx.get(id1, [done, &ctx](std::exception_ptr err, std::optional<transaction_get_result> doc1) {
                  if (!doc1 || err) {
                      return;
                  }
                  auto content = doc1->content<tao::json::value>();
                  auto count = content["number"].as<uint32_t>();
                  if (count >= 200) {
                      done->store(true);
                      return;
                  }
                  content["number"] = ++count;
                  ctx.replace(*doc1, content, [doc1](std::exception_ptr err, std::optional<transaction_get_result>) {
                      if (!err) {
                          // CHECK(doc1->cas() != doc1_updated->cas());
                      }
                  });
              });
              ctx.get(id2, [&done, &ctx](std::exception_ptr err, std::optional<transaction_get_result> doc2) {
                  if (!doc2 || err) {
                      return;
                  }
                  auto content = doc2->content<tao::json::value>();
                  auto count = content["number"].as<uint32_t>();
                  if (count <= 0) {
                      done->store(true);
                      return;
                  }
                  content["number"] = --count;
                  ctx.replace(*doc2, content, [doc2](std::exception_ptr err, std::optional<transaction_get_result>) {
                      if (!err) {
                          // CHECK(doc2->cas() != doc2_updated->cas());
                      }
                  });
              });
          },
          [txns, done, errors, &in_flight, &cv_in_flight, &cv_txns_complete, &mut](
            std::optional<transaction_exception> err, std::optional<couchbase::transactions::transaction_result> /* result */) {
              ++(*txns);
              std::unique_lock<std::mutex> lock(mut);
              in_flight--;
              if (in_flight < NUM_TXNS) {
                  cv_in_flight.notify_all();
              }
              if (in_flight == 0 && done->load()) {
                  cv_txns_complete.notify_all();
              }
              lock.unlock();
              if (err) {
                  ++(*errors);
              }
          });
    }

    // wait till it is really done and committed that last one...
    std::unique_lock<std::mutex> lock(mut);
    cv_txns_complete.wait(lock, [&in_flight, done] { return (in_flight == 0 && done->load()); });
    lock.unlock();
    // now lets look at the final state of the docs:
    auto doc1 = TransactionsTestEnvironment::get_doc(id1);
    auto doc2 = TransactionsTestEnvironment::get_doc(id2);
    REQUIRE(0 == doc2.content_as<tao::json::value>()["number"].as<uint32_t>());
    REQUIRE(200 == doc1.content_as<tao::json::value>()["number"].as<uint32_t>());
    // could be we have some txns that are successful, but did nothing as they noticed the count
    // is at limits.  So at least 200 txns.
    REQUIRE(txns->load() - errors->load() != 200);
    // No way we don't have at least one conflict, so attempts should be much larger than txns.
    REQUIRE(attempts->load() > 200);
    std::cout << "attempts: " << attempts->load() << ", txns: " << txns->load() << ", errors: " << errors->load() << std::endl;
}
