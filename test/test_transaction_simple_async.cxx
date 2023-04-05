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

#include "core/transactions/attempt_context_impl.hxx"
#include "simple_object.hxx"
#include "test_helper_integration.hxx"

#include <spdlog/spdlog.h>

#include <future>
#include <list>
#include <stdexcept>

using namespace couchbase::core::transactions;

static const tao::json::value async_content{
    { "some", "thing" },
};

couchbase::transactions::transactions_config
get_conf()
{
    couchbase::transactions::transactions_config cfg{};
    cfg.expiration_time(std::chrono::seconds(1));
    return cfg;
}

static const auto async_content_json = couchbase::core::utils::json::generate_binary(async_content);

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
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto cb_called = std::make_shared<std::atomic<bool>>(false);

    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txn.run(
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

TEST_CASE("transactions: can't get from unopened bucket", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());
    couchbase::core::document_id bad_id{ "secBucket", "_default", "default", test::utils::uniq_id("txns") };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txn.run(
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
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    try {
        txn.run(
          [cb_called, id](async_attempt_context& ctx) {
              ctx.get(id, [cb_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
        // nothing to do here, but make sure cb was called, and the txn failed.
        REQUIRE(cb_called->load());
        REQUIRE(e.type() == failure_type::FAIL);
    } catch (const std::exception&) {
        FAIL("expected a transaction_failed exception, but got something else");
    }
}

TEST_CASE("transactions: async remove fail", "[transactions]")
{

    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);

    try {
        txn.run(
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
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txn.run(
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
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: async replace", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    const tao::json::value new_content{
        { "shiny", "and new" },
    };
    txn.run(
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(new_content));
    }
}

TEST_CASE("transactions: async replace fail", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    const tao::json::value new_content{
        { "shiny", "and new" },
    };

    try {
        txn.run(
          [cb_called, &new_content, id](async_attempt_context& ctx) {
              ctx.get(id, [&ctx, &new_content, cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  if (!err) {
                      ctx.replace(*res, new_content, [cb_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    } catch (const transaction_exception&) {
        REQUIRE(cb_called->load());
        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.value == async_content_json);
        }
    };
}

TEST_CASE("transactions: async insert", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);

    txn.run(
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}

TEST_CASE("transactions: async insert can be rolled back", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);

    try {
        txn.run(
          [cb_called, id, barrier](async_attempt_context& ctx) {
              ctx.insert(id, async_content, [cb_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    } catch (const transaction_exception&) {
        REQUIRE(cb_called->load());
        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
        }
    }
}
TEST_CASE("transactions: async query", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto query_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query, [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(couchbase::core::utils::json::parse_binary(resp.value)["some"].as<std::string>() == std::string("thing else"));
    }
}

TEST_CASE("transactions: multiple racing queries", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto query_called = std::make_shared<std::atomic<int>>(0);
    txn.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query, [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
              if (!err) {
                  ++(*query_called);
              }
          });
          ctx.query(query, [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
              if (!err) {
                  ++(*query_called);
              }
          });
          ctx.query(query, [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(couchbase::core::utils::json::parse_binary(resp.value)["some"].as<std::string>() == std::string("thing else"));
    }
}

TEST_CASE("transactions: rollback async query", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto cb_called = std::make_shared<std::atomic<bool>>(false);
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto query_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [query_called, id](async_attempt_context& ctx) {
          auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
          ctx.query(query, [query_called](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}

TEST_CASE("transactions: async KV get", "[transactions]")
{

    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto get_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [get_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [get_called, &id, &ctx](std::exception_ptr, std::optional<transaction_get_result>) {
              auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
              ctx.query(query, [get_called, &id, &ctx](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(couchbase::core::utils::json::parse_binary(resp.value)["some"].as<std::string>() == std::string("thing else"));
    }
}

TEST_CASE("transactions: rollback async KV get", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto get_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [get_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, get_called, &id](std::exception_ptr, std::optional<transaction_get_result>) {
              auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
              ctx.query(query, [&ctx, get_called, &id](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}

TEST_CASE("transactions: async KV insert", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto insert_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [insert_called, id, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [&ctx, insert_called, id, barrier](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
                        if (!err) {
                            ctx.insert(id, async_content, [insert_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}

TEST_CASE("transactions: rollback async KV insert", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    auto insert_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [insert_called, id, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [insert_called, &ctx, id, barrier](std::exception_ptr err, std::optional<couchbase::core::operations::query_response>) {
                        if (!err) {
                            ctx.insert(id, async_content, [insert_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: async KV replace", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto replace_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [replace_called, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [replace_called, id, &ctx, &new_content](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(
                    query,
                    [replace_called, &ctx, &new_content, doc = *result](std::exception_ptr err,
                                                                        std::optional<couchbase::core::operations::query_response>) {
                        if (!err) {
                            ctx.replace(doc, new_content, [replace_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(new_content));
    }
}

TEST_CASE("transactions: rollback async KV replace", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    tao::json::value new_content{
        { "some", "thing else" },
    };
    auto replace_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [replace_called, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [replace_called, &ctx, &id, &new_content](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(
                    query,
                    [replace_called, &ctx, &new_content, doc = *result](std::exception_ptr err,
                                                                        std::optional<couchbase::core::operations::query_response>) {
                        if (!err) {
                            ctx.replace(doc, new_content, [replace_called](std::exception_ptr err, std::optional<transaction_get_result>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}

TEST_CASE("transactions: async KV remove", "[transactions]")
{

    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto remove_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [remove_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [remove_called, &ctx, &id](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [remove_called, &ctx, doc = *result](std::exception_ptr err,
                                                                 std::optional<couchbase::core::operations::query_response>) {
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
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: rollback async KV remove", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    {
        couchbase::core::operations::upsert_request req{ id, async_content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto remove_called = std::make_shared<std::atomic<bool>>(false);
    txn.run(
      [remove_called, &id](async_attempt_context& ctx) {
          ctx.get(id, [remove_called, &id, &ctx](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  CHECK(result);
                  auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
                  ctx.query(query,
                            [remove_called, &ctx, doc = *result](std::exception_ptr err,
                                                                 std::optional<couchbase::core::operations::query_response>) {
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
    REQUIRE(remove_called->load());
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == async_content_json);
    }
}
