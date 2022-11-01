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
#include "utils/transactions_env.h"

#include <spdlog/spdlog.h>
#include <stdexcept>

using namespace couchbase::core::transactions;

TEST_CASE("transactions: arbitrary runtime error", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value content{
        { "some", "thing" },
    };

    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
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
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);
    auto id = TransactionsTestEnvironment::get_document_id();

    REQUIRE_THROWS_AS(
      [](auto& txn, auto& id) {
          try {
              txn.run([&id](attempt_context& ctx) {
                  const tao::json::value content{
                      { "some", "thing" },
                  };

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
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);
    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value initial{
        { "some number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    txn.run([id](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto content = doc.content<tao::json::value>();
        content["another one"] = 1;
        ctx.replace(doc, content);
    });
    // now add to the initial content, and compare
    const tao::json::value expected{
        { "some number", 0 },
        { "another one", 1 },
    };
    REQUIRE(expected == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can use custom metadata collections per transactions", "[transactions]")
{
    const tao::json::value initial{
        { "some number", 0 },
    };
    auto txn = TransactionsTestEnvironment::get_transactions();

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    couchbase::cluster c(TransactionsTestEnvironment::get_cluster());
    couchbase::transactions::transaction_options cfg;
    cfg.metadata_collection(c.bucket(TransactionsTestEnvironment::get_conf().extra_bucket).default_collection());
    txn.run(cfg, [id](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto content = doc.content<tao::json::value>();
        content["another one"] = 1;
        ctx.replace(doc, content);
    });

    const tao::json::value expected{
        { "some number", 0 },
        { "another one", 1 },
    };
    REQUIRE(expected == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can use custom metadata collections", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    cfg.metadata_collection(couchbase::transactions::transaction_keyspace{
      TransactionsTestEnvironment::get_conf().extra_bucket,
      couchbase::scope::default_name,
      couchbase::collection::default_name,
    });
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value initial{
        { "some number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto content = doc.content<tao::json::value>();
        content["another one"] = 1;
        ctx.replace(doc, content);
    });
    // now add to the original content, and compare
    const tao::json::value expected{
        { "some number", 0 },
        { "another one", 1 },
    };
    REQUIRE(expected == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: non existent bucket in custom metadata collections", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    cfg.metadata_collection(couchbase::transactions::transaction_keyspace{
      "i_dont_exist",
      couchbase::scope::default_name,
      couchbase::collection::default_name,
    });
    REQUIRE_THROWS_AS([](auto cluster, auto cfg) { transactions txn(cluster, cfg); }(cluster, cfg), std::runtime_error);
}

TEST_CASE("transactions: non existent scope in custom metadata collections", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    cfg.metadata_collection(couchbase::transactions::transaction_keyspace{
      TransactionsTestEnvironment::get_conf().extra_bucket,
      "i_dont_exist",
      couchbase::collection::default_name,
    });
    cfg.expiration_time(std::chrono::seconds(2));
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value initial{
        { "some number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    try {
        txn.run([&](attempt_context& ctx) {
            auto doc = ctx.get(id);
            auto content = doc.content<tao::json::value>();
            content["another one"] = 1;
            ctx.replace(doc, content);
        });
        FAIL("expected txn to timeout");
    } catch (const transaction_exception& e) {
        // type could be expiry or fail, it seems.  The reason is a bit unclear.
        REQUIRE((e.type() == failure_type::EXPIRY || e.type() == failure_type::FAIL));
        REQUIRE(initial == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
    }
}

TEST_CASE("transactions: non existent collection in custom metadata collections", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    cfg
      .metadata_collection(couchbase::transactions::transaction_keyspace{
        TransactionsTestEnvironment::get_conf().extra_bucket, couchbase::scope::default_name, "i_dont_exist" })
      .cleanup_config(couchbase::transactions::transactions_cleanup_config().cleanup_lost_attempts(true));
    cfg.expiration_time(std::chrono::seconds(2));
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value initial{
        { "some number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    try {
        txn.run([&](attempt_context& ctx) {
            auto doc = ctx.get(id);
            auto content = doc.content<tao::json::value>();
            content["another one"] = 1;
            ctx.replace(doc, content);
        });
        FAIL("expected txn to timeout");
    } catch (const transaction_exception& e) {
        REQUIRE((e.type() == failure_type::EXPIRY || e.type() == failure_type::FAIL));
        REQUIRE(initial == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
    }
}

TEST_CASE("transactions: can get replace raw strings", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value initial{
        { "some number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, initial));
    txn.run([id](attempt_context& ctx) {
        auto doc = ctx.get(id);
        std::string new_content("{\"aaa\":\"bbb\"}");
        ctx.replace(doc, new_content);
    });
    std::string expected("{\"aaa\":\"bbb\"}");
    REQUIRE(expected == TransactionsTestEnvironment::get_doc(id).content_as<std::string>());
}

TEST_CASE("transactions: can json string as content", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);
    std::string content = "\"imaquotedjsonstring\"";
    // insert the doc
    auto id = TransactionsTestEnvironment::get_document_id();
    txn.run([id, content](attempt_context& ctx) {
        ctx.insert(id, content);
        auto doc = ctx.get(id);
    });
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<std::string>());
}

TEST_CASE("transactions: query error can be handled", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    txns.run([](attempt_context& ctx) {
        // the EXPECT_THROW will eat the exception, as long as there is one of the correct type.
        REQUIRE_THROWS_AS(ctx.query("wont parse"), query_parsing_failure);
        auto res = ctx.query("Select 'Yo' as greeting");
        REQUIRE(1 == res.rows.size());
    });
}

TEST_CASE("transactions: unhandled query error fails transaction", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    REQUIRE_THROWS_AS(
      [](auto& txns) {
          txns.run([&](attempt_context& ctx) {
              ctx.query("wont parse");
              ctx.query("Select * from `" + TransactionsTestEnvironment::get_conf().bucket + "` limit 1");
          });
      }(txns),
      transaction_exception);
}

TEST_CASE("transactions: query mode get optional", "[transactions]")
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    const tao::json::value content{
        { "some", "thing" },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    txns.run([&](attempt_context& ctx) {
        ctx.query(query);
        auto doc = ctx.get_optional(id);
        REQUIRE(doc);
    });
}

TEST_CASE("transactions: can get replace objects", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, o));
    txn.run([id, o2](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, o2);
    });
    REQUIRE(o2 == TransactionsTestEnvironment::get_doc(id).content_as<SimpleObject>());
}

TEST_CASE("transactions: can get replace mixed object strings", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, o));
    txn.run([id, o2](attempt_context& ctx) {
        auto doc = ctx.get(id);
        tao::json::value j2 = o2;
        auto string_content = tao::json::to_string(j2);
        std::vector<std::byte> binary_content;
        binary_content.reserve(string_content.size());
        std::transform(string_content.begin(), string_content.end(), std::back_insert_iterator(binary_content), [](auto e) {
            return static_cast<std::byte>(e);
        });
        ctx.replace(doc, binary_content);
    });
    REQUIRE(o2 == TransactionsTestEnvironment::get_doc(id).content_as<SimpleObject>());
}

TEST_CASE("transactions: can rollback insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id) {
          txn.run([id](attempt_context& ctx) {
              SimpleObject o{ "someone", 100 };
              ctx.insert(id, o);
              throw 3; // some arbitrary exception...
          });
      }(txn, id),
      transaction_exception);
    try {
        auto res = TransactionsTestEnvironment::get_doc(id);
        FAIL("expect a client_error with document_not_found, got result instead");

    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can rollback remove", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id) {
          txn.run([id](attempt_context& ctx) {
              auto res = ctx.get(id);
              ctx.remove(res);
              throw 3; // just throw some arbitrary exception to get rollback
          });
      }(txn, id),
      transaction_exception);
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can rollback replace", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
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
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can have trivial query in transaction", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
    txn.run([content, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        REQUIRE(1 == payload.rows.size());
        REQUIRE(content == tao::json::from_string(payload.rows.front())["default"]);
    });
}

TEST_CASE("transactions: can modify doc in query", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    transactions txn(cluster, cfg);
    txn.run([statement = stream.str()](attempt_context& ctx) { ctx.query(statement); });

    auto res = TransactionsTestEnvironment::get_doc(id);
    auto j = res.content_as<tao::json::value>();
    REQUIRE(10 == j["some_number"].as<int>());
}

TEST_CASE("transactions: can rollback", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    transactions txn(cluster, cfg);
    REQUIRE_THROWS_AS(
      [](auto& txn, std::string statement) {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              throw 3;
          });
      }(txn, stream.str()),
      transaction_exception);

    auto res = TransactionsTestEnvironment::get_doc(id);
    REQUIRE(res.content_as<tao::json::value>() == content);
}

TEST_CASE("transactions: query updates insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    transactions txn(cluster, cfg);
    txn.run([id, content, statement = stream.str()](attempt_context& ctx) {
        ctx.insert(id, content);
        ctx.query(statement);
    });

    auto res = TransactionsTestEnvironment::get_doc(id);
    REQUIRE(10 == res.content_as<tao::json::value>()["some_number"].as<int>());
}

TEST_CASE("transactions: can KV get", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "UPDATE `" << id.bucket() << "` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    transactions txn(cluster, cfg);
    txn.run([id, content, statement = stream.str()](attempt_context& ctx) {
        ctx.insert(id, content);
        auto payload = ctx.query(statement);
        REQUIRE(payload.rows.empty());
        auto doc = ctx.get(id);
        REQUIRE(10 == doc.content<tao::json::value>()["some_number"].as<uint32_t>());
    });
    auto res = TransactionsTestEnvironment::get_doc(id);
    REQUIRE(10 == res.content_as<tao::json::value>()["some_number"]);
}

TEST_CASE("transactions: can KV insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
    txn.run([id, content, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        ctx.insert(id, content);
    });
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can rollback KV insert", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto statement) {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(statement);
              tao::json::value content{ { "some_number", 0 } };
              ctx.insert(id, content);
              throw 3;
          });
      }(txn, id, stream.str()),
      transaction_exception);
    try {
        auto doc = TransactionsTestEnvironment::get_doc(id);
        FAIL("expected doc to not exist");
    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can KV replace", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        auto doc = ctx.get(id);
        auto new_content = doc.content<tao::json::value>();
        new_content["some_number"] = 10;
        auto replaced_doc = ctx.replace(doc, new_content);
        REQUIRE(replaced_doc.cas() != doc.cas());
        REQUIRE_FALSE(replaced_doc.cas().empty());
    });
    REQUIRE(10 == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>()["some_number"].as<int>());
}

TEST_CASE("transactions: can rollback KV replace", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
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
    REQUIRE(0 == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>()["some_number"].as<int>());
}

TEST_CASE("transactions: can KV remove", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
    txn.run([id, statement = stream.str()](attempt_context& ctx) {
        auto payload = ctx.query(statement);
        auto doc = ctx.get(id);
        ctx.remove(doc);
    });
    try {
        auto doc = TransactionsTestEnvironment::get_doc(id);
        FAIL("expected doc to not exist");
    } catch (const client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("transactions: can rollback KV remove", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    cfg.expiration_time(std::chrono::seconds(1));

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    std::ostringstream stream;
    stream << "SELECT * FROM `" << id.bucket() << "` USE KEYS '" << id.key() << "'";
    transactions txn(cluster, cfg);
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
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: can rollback retry bad KV replace", "[transactions]")
{
    auto cluster = TransactionsTestEnvironment::get_cluster();
    tao::json::value content{
        { "some_number", 0 },
    };
    couchbase::transactions::transactions_config cfg;
    cfg.expiration_time(std::chrono::seconds(1));

    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some_number` = 10", id.bucket(), id.key());
    transactions txn(cluster, cfg);
    REQUIRE_THROWS_AS(
      [](auto& txn, auto id, auto query) {
          txn.run([id, query](attempt_context& ctx) {
              auto doc = ctx.get(id);
              auto payload = ctx.query(query);
              auto new_doc = ctx.replace(doc, "{\"some_number\": 20}");
          });
      }(txn, id, query),
      transaction_exception);
    REQUIRE(content == TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>());
}

TEST_CASE("transactions: query can set any durability", "[transactions]")
{
    // Just be sure that the query service understood the durability
    std::list<couchbase::durability_level> levels{ couchbase::durability_level::none,
                                                   couchbase::durability_level::majority,
                                                   couchbase::durability_level::majority_and_persist_to_active,
                                                   couchbase::durability_level::persist_to_majority };
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    tao::json::value content{
        { "some_number", 0 },
    };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    // doesn't matter if the query is read-only or not, just check that there
    // is no error making the query.
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    for (auto durability : levels) {
        txns.run([id, query, durability](attempt_context& ctx) {
            // FIXME: actually pass durability to the query
            (void)durability;
            ctx.query(query);
            auto doc = ctx.get_optional(id);
            REQUIRE(doc);
        });
    }
}

TEST_CASE("get_and_open_buckets: can get buckets", "[transactions]")
{
    auto c = TransactionsTestEnvironment::get_cluster();
    std::list<std::string> buckets = get_and_open_buckets(c);
    REQUIRE(buckets.end() != std::find(buckets.begin(), buckets.end(), std::string("default")));
    REQUIRE(buckets.end() != std::find(buckets.begin(), buckets.end(), std::string("secBucket")));
}

TEST_CASE("get_and_open_buckets: can race to get and open buckets", "[transactions]")
{
    std::list<std::future<std::list<std::string>>> futures;
    std::size_t num_futures = 20;
    auto c = TransactionsTestEnvironment::get_cluster();
    for (std::size_t i = 0; i < num_futures; i++) {
        futures.push_back(std::async(std::launch::async, [&c] { return get_and_open_buckets(c); }));
    }
    for (auto& f : futures) {
        CHECK_NOTHROW(f.get());
    }
}

TEST_CASE("get_and_open_buckets: can race to get and open buckets in multiple threads", "[transactions]")
{
    std::list<std::future<std::list<std::string>>> futures;
    std::size_t num_futures = 20;
    for (std::size_t i = 0; i < num_futures; i++) {
        futures.push_back(std::async(std::launch::async, [] {
            auto c = TransactionsTestEnvironment::get_cluster();
            return get_and_open_buckets(c);
        }));
    }
    for (auto& f : futures) {
        CHECK_NOTHROW(f.get());
    }
}
