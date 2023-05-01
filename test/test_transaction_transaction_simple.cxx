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

#include "test_helper_integration.hxx"

#include "simple_object.hxx"

#include "core/transactions.hxx"
#include "core/transactions/atr_ids.hxx"

#include <spdlog/spdlog.h>
#include <tao/json.hpp>

#include <stdexcept>

#if defined(__GNUC__)
#if __GNUC__ <= 10
#pragma GCC diagnostic ignored "-Wparentheses"
#endif
#if __GNUC__ < 9
#pragma GCC diagnostic ignored "-Wuseless-cast"
#endif
#endif

using namespace couchbase::core::transactions;
static const tao::json::value content{
    { "some_number", 0 },
};
static const std::vector<std::byte> content_json = couchbase::core::utils::json::generate_binary(content);

couchbase::transactions::transactions_config
get_conf()
{
    couchbase::transactions::transactions_config cfg{};
    cfg.expiration_time(std::chrono::seconds(2));
    return cfg;
}

TEST_CASE("transactions: arbitrary runtime error", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    REQUIRE_THROWS_AS(
      [](auto& txn, auto& id) {
          try {
              txn.run([&id](attempt_context& ctx) {
                  ctx.get(id);
                  throw std::runtime_error("Yo");
              });
          } catch (const transaction_exception& e) {
              REQUIRE(e.cause() == external_exception::UNKNOWN);
              REQUIRE(e.type() == failure_type::FAIL);
              REQUIRE(e.what() == std::string{ "Yo" });
              throw;
          }
      }(txn, id),
      transaction_exception);
}

TEST_CASE("transactions: arbitrary exception", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };

    REQUIRE_THROWS_AS(
      [](auto& txn, auto& id) {
          try {
              txn.run([&id](attempt_context& ctx) {
                  ctx.insert(id, content);
                  throw 3;
              });
          } catch (const transaction_exception& e) {
              REQUIRE(e.cause() == external_exception::UNKNOWN);
              REQUIRE(e.type() == failure_type::FAIL);
              REQUIRE(e.what() == std::string("Unexpected error"));
              throw;
          }
      }(txn, id),
      transaction_exception);
}

TEST_CASE("transactions: can get replica", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    txn.run([id](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto new_content = doc.content<tao::json::value>();
        new_content["another one"] = 1;
        ctx.replace(doc, new_content);
    });
    // now add to the initial content, and compare
    const tao::json::value expected{
        { "some_number", 0 },
        { "another one", 1 },
    };
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(expected));
    }
}

TEST_CASE("transactions: can use custom metadata collections per transactions", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::transactions::transactions txn(integration.cluster, get_conf());

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    couchbase::transactions::transaction_options cfg;
    cfg.metadata_collection(couchbase::transactions::transaction_keyspace("secBucket"));
    txn.run(cfg, [id](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto new_content = doc.content<tao::json::value>();
        new_content["another one"] = 1;
        ctx.replace(doc, new_content);
    });

    const tao::json::value expected{
        { "some_number", 0 },
        { "another one", 1 },
    };
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(expected));
    }
}

TEST_CASE("transactions: can use custom metadata collections", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    auto cfg = get_conf().metadata_collection(couchbase::transactions::transaction_keyspace("secBucket"));
    transactions txn(cluster, cfg);

    // upsert initial doc
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto new_content = doc.content<tao::json::value>();
        new_content["another one"] = 1;
        ctx.replace(doc, new_content);
    });
    // now add to the original content, and compare
    const tao::json::value expected{
        { "some_number", 0 },
        { "another one", 1 },
    };
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(expected));
    }
}

TEST_CASE("transactions: non existent bucket in custom metadata collections", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    auto cfg = get_conf().metadata_collection(couchbase::transactions::transaction_keyspace{
      "i_dont_exist",
      couchbase::scope::default_name,
      couchbase::collection::default_name,
    });
    // can't open bucket upon creation of txns, so throws here.
    REQUIRE_THROWS_AS([](auto cluster, auto cfg) { transactions txn(cluster, cfg); }(cluster, cfg), std::runtime_error);
}

TEST_CASE("transactions: non existent scope in custom metadata collections", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    auto cfg = get_conf().metadata_collection(couchbase::transactions::transaction_keyspace{
      integration.ctx.bucket,
      "i_dont_exist",
      couchbase::collection::default_name,
    });
    cfg.expiration_time(std::chrono::seconds(2));
    transactions txn(cluster, cfg);

    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    try {
        txn.run([&](attempt_context& ctx) {
            auto doc = ctx.get(id);
            auto new_content = doc.content<tao::json::value>();
            new_content["another one"] = 1;
            ctx.replace(doc, new_content);
        });
        FAIL("expected txn to timeout");
    } catch (const transaction_exception& e) {
        // type could be expiry or fail, it seems.  The reason is a bit unclear.
        REQUIRE((e.type() == failure_type::EXPIRY || e.type() == failure_type::FAIL));
        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.value == content_json);
        }
    }
}

TEST_CASE("transactions: non existent collection in custom metadata collections", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    auto cfg =
      get_conf()
        .metadata_collection(couchbase::transactions::transaction_keyspace{ "secBucket", couchbase::scope::default_name, "i_dont_exist" })
        .cleanup_config(couchbase::transactions::transactions_cleanup_config().cleanup_lost_attempts(true));
    cfg.expiration_time(std::chrono::seconds(2));
    transactions txn(cluster, cfg);

    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    try {
        txn.run([&](attempt_context& ctx) {
            auto doc = ctx.get(id);
            auto new_content = doc.content<tao::json::value>();
            new_content["another one"] = 1;
            ctx.replace(doc, new_content);
        });
        FAIL("expected txn to timeout");
    } catch (const transaction_exception& e) {
        REQUIRE((e.type() == failure_type::EXPIRY || e.type() == failure_type::FAIL));
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: raw std::strings become json strings", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    std::string new_content("I am an unquoted string");
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    txn.run([id, new_content](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, new_content);
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto parsed_as_json = couchbase::core::utils::json::parse_binary(resp.value);
        REQUIRE(parsed_as_json.get_string() == new_content);
    }
}

TEST_CASE("transactions: quoted std::strings end up with 2 quotes (that's bad)", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    std::string quoted_json_string = "\"imaquotedjsonstring\"";
    // insert the doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    txn.run([id, quoted_json_string](attempt_context& ctx) {
        ctx.insert(id, quoted_json_string);
        auto doc = ctx.get(id);
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto parsed_as_json = couchbase::core::utils::json::parse_binary(resp.value);
        // here the _parsed_ json string will still have quotes in it.
        REQUIRE(parsed_as_json.get_string() == quoted_json_string);
    }
}

TEST_CASE("transactions: query error can be handled", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    txn.run([](attempt_context& ctx) {
        // the EXPECT_THROW will eat the exception, as long as there is one of the correct type.
        REQUIRE_THROWS_AS(ctx.query("wont parse"), query_parsing_failure);
        auto res = ctx.query("Select 'Yo' as greeting");
        REQUIRE(1 == res.rows.size());
    });
}

TEST_CASE("transactions: unhandled query error fails transaction", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    REQUIRE_THROWS_AS(
      [&](auto& txn) {
          txn.run([&](attempt_context& ctx) {
              ctx.query("wont parse");
              ctx.query("Select * from `" + integration.ctx.bucket + "` limit 1");
          });
      }(txn),
      transaction_exception);
}

TEST_CASE("transactions: query mode get optional", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    txn.run([&](attempt_context& ctx) {
        ctx.query(query);
        auto doc = ctx.get_optional(id);
        REQUIRE(doc);
    });
}

TEST_CASE("transactions: can get replace objects", "[transactions]")
{
    test::utils::integration_test_guard integration;
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    txn.run([id, o2](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, o2);
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto final_val = couchbase::core::utils::json::parse_binary(resp.value);
        REQUIRE(final_val.as<SimpleObject>() == o2);
    }
}

TEST_CASE("transactions: can get replace mixed object strings", "[transactions]")
{
    test::utils::integration_test_guard integration;
    SimpleObject o{ "someone", 100 };
    tao::json::value v2 = { { "name", "someone else" }, { "number", 200 } };
    auto o2 = v2.as<SimpleObject>();
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    txn.run([id, v2](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, couchbase::core::utils::json::generate_binary(v2));
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto final_val = couchbase::core::utils::json::parse_binary(resp.value);
        REQUIRE(final_val.as<SimpleObject>() == o2);
    }
}

TEST_CASE("transactions: can rollback insert", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };

    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, get_conf());

    REQUIRE_THROWS_AS(
      [](auto& txn, auto id) {
          txn.run([id](attempt_context& ctx) {
              SimpleObject o{ "someone", 100 };
              ctx.insert(id, o);
              throw 3; // some arbitrary exception...
          });
      }(txn, id),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can rollback remove", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    REQUIRE_THROWS_AS(
      [](auto& txn, auto id) {
          txn.run([id](attempt_context& ctx) {
              auto res = ctx.get(id);
              ctx.remove(res);
              throw 3; // just throw some arbitrary exception to get rollback
          });
      }(txn, id),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
}

TEST_CASE("transactions: can rollback replace", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    REQUIRE_THROWS_AS(
      [](auto& txn, auto id) {
          txn.run([id](attempt_context& ctx) {
              auto res = ctx.get(id);
              tao::json::value new_content{
                  { "some number", 100 },
              };
              ctx.replace(res, new_content);
              throw 3; // just throw some arbitrary exception to get rollback
          });
      }(txn, id),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: can have trivial query in transaction", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    txn.run([statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        REQUIRE(1 == payload.rows.size());
        REQUIRE(content == tao::json::from_string(payload.rows.front())["default"]);
    });
}

TEST_CASE("transactions: can modify doc in query", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    txn.run([statement = stream.str()](attempt_context& ctx) { ctx.query(statement); });

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto value = couchbase::core::utils::json::parse_binary(resp.value);
        REQUIRE(10 == value["some_number"].as<int>());
    }
}

TEST_CASE("transactions: can rollback", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    REQUIRE_THROWS_AS(
      [](auto& txn, std::string statement) {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              throw 3;
          });
      }(txn, stream.str()),
      transaction_exception);

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: query updates insert", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        ctx.insert(id, content);
        ctx.query(statement);
    });

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(10 == couchbase::core::utils::json::parse_binary(resp.value)["some_number"].as<int>());
    }
}

TEST_CASE("transactions: can KV get", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        ctx.insert(id, content);
        auto payload = ctx.query(statement);
        CHECK(payload.rows.empty());
        auto doc = ctx.get(id);
        CHECK(10 == doc.content<tao::json::value>()["some_number"].as<uint32_t>());
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(10 == couchbase::core::utils::json::parse_binary(resp.value)["some_number"].as<int>());
    }
}

TEST_CASE("transactions: can KV insert", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        ctx.insert(id, content);
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: can rollback KV insert", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto statement) {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              ctx.insert(id, content);
              throw 3;
          });
      }(txn, id, stream.str()),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can KV replace", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        auto doc = ctx.get(id);
        auto new_content = doc.content<tao::json::value>();
        new_content["some_number"] = 10;
        auto replaced_doc = ctx.replace(doc, new_content);
        CHECK(replaced_doc.cas() != doc.cas());
        CHECK_FALSE(replaced_doc.cas().empty());
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(10 == couchbase::core::utils::json::parse_binary(resp.value)["some_number"].as<int>());
    }
}

TEST_CASE("transactions: can rollback KV replace", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto statement) {
          txn.run([id, statement](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              auto doc = ctx.get(id);
              auto new_content = doc.template content<tao::json::value>();
              new_content["some_number"] = 10;
              auto replaced_doc = ctx.replace(doc, new_content);
              REQUIRE(replaced_doc.cas() != doc.cas());
              REQUIRE_FALSE(replaced_doc.cas().empty());
              throw 3;
          });
      }(txn, id, stream.str()),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: can KV remove", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        auto doc = ctx.get(id);
        ctx.remove(doc);
    });
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can rollback KV remove", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto statement) {
          txn.run([id, statement](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              auto doc = ctx.get(id);
              ctx.remove(doc);
              throw 3;
          });
      }(txn, id, stream.str()),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: can rollback retry bad KV replace", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    // upsert initial doc
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some_number` = 10", id.bucket(), id.key());
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto query) {
          txn.run([id, query](attempt_context& ctx) {
              auto doc = ctx.get(id);
              auto payload = ctx.query(query);
              auto new_doc = ctx.replace(doc, "{\"some_number\": 20}");
          });
      }(txn, id, query),
      transaction_exception);
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == content_json);
    }
}

TEST_CASE("transactions: atr and client_record are binary documents", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;

    transactions txn(cluster, get_conf());
    std::vector<std::byte> binary_null{ std::byte(0) };
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };

    txn.run([id](attempt_context& ctx) { ctx.insert(id, content); });
    {
        couchbase::core::document_id client_record_id{ integration.ctx.bucket, "_default", "_default", "_txn:client-record" };
        couchbase::core::operations::get_request req{ client_record_id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == binary_null);
    }
    {
        auto atr_key = atr_ids::atr_id_for_vbucket(atr_ids::vbucket_for_key(id.key()));
        couchbase::core::document_id atr_id{ integration.ctx.bucket, "_default", "_default", atr_key };
        couchbase::core::operations::get_request req{ atr_id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == binary_null);
    }
}
TEST_CASE("transactions: get non-existent doc fails txn", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    REQUIRE_THROWS_AS(txn.run([id](attempt_context& ctx) { ctx.get(id); }), transaction_exception);
}

TEST_CASE("transactions: get_optional on non-existent doc doesn't fail txn", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    REQUIRE_NOTHROW(txn.run([id](attempt_context& ctx) { ctx.get_optional(id); }));
}
TEST_CASE("transactions: get after query behaves same as before a query", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    REQUIRE_THROWS_AS(txn.run([id](attempt_context& ctx) {
        ctx.query("select * from `default` limit 1");
        ctx.get(id);
    }),
                      transaction_exception);
}

TEST_CASE("transactions: get_optional after query behaves same as before a query", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    REQUIRE_NOTHROW(txn.run([id](attempt_context& ctx) {
        ctx.query("select * from `default` limit 1");
        ctx.get_optional(id);
    }));
}
TEST_CASE("transactions: sergey example", "[transactions]")
{
    test::utils::integration_test_guard integration;
    auto cluster = integration.cluster;
    transactions txn(cluster, get_conf());
    couchbase::core::document_id id_to_remove{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    couchbase::core::document_id id_to_replace{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    couchbase::core::document_id id_to_insert{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("txn") };
    {
        couchbase::core::operations::upsert_request req{ id_to_remove, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    {
        couchbase::core::operations::upsert_request req{ id_to_replace, content_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    REQUIRE_NOTHROW(txn.run([&](attempt_context& ctx) {
        ctx.query(fmt::format(
          "INSERT INTO `default` (KEY, VALUE) VALUES ('{}', {})", id_to_insert.key(), couchbase::core::utils::json::generate(content)));
        ctx.query(fmt::format("UPDATE `default` USE KEYS '{}' SET `some_number` = 10 ", id_to_replace.key()));
        ctx.query(fmt::format("DELETE FROM `default` WHERE META().id = '{}'", id_to_remove.key()));
        auto insert_res = ctx.get(id_to_insert);
        CHECK(insert_res.content<tao::json::value>() == content);
        auto replace_res = ctx.get(id_to_replace);
        CHECK(replace_res.content<tao::json::value>()["some_number"] == 10);
        auto remove_res = ctx.get_optional(id_to_remove);
        CHECK_FALSE(remove_res.has_value());
    }));
}
