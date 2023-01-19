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

#include <memory>
#include <variant>

static const tao::json::value content{ { "some_number", 0 } };

TEST_CASE("can get", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        CHECK(doc->key() == id.key());
        CHECK_FALSE(doc->cas().empty());
        CHECK(doc->content<tao::json::value>() == content);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.ctx.ec());
}

TEST_CASE("get returns error if doc doesn't exist", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        CHECK(doc->ctx().ec());
        CHECK(doc->ctx().ec() == couchbase::errc::transaction_op::document_not_found_exception);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK_FALSE(result.ctx.ec());
}

TEST_CASE("can insert", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.insert(coll, id.key(), content);
        CHECK(doc->key() == id.key());
        CHECK_FALSE(doc->cas().empty());
        CHECK(doc->content<tao::json::value>() == content);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.ctx.ec());
    // check that it is really there now
    auto final_doc = TransactionsTestEnvironment::get_doc(id);
    CHECK(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("insert has error as expected when doc already exists", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    tao::json::value new_content{ { "something", "else" } };
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.insert(coll, id.key(), new_content);
        CHECK(doc->ctx().ec());
        CHECK(doc->ctx().ec() == couchbase::errc::transaction_op::document_exists_exception);
    });
    CHECK_FALSE(result.transaction_id.empty());
    // but the txn is successful
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.ctx.ec());
    // check that it is really unchanged too.
    auto final_doc = TransactionsTestEnvironment::get_doc(id);
    CHECK(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("can replace", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    tao::json::value new_content = { { "some_other_number", 3 } };
    auto result = c.transactions()->run([id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        auto replaced_doc = ctx.replace(doc, new_content);
        CHECK(doc->key() == replaced_doc->key());
        CHECK(doc->cas() != replaced_doc->cas());
        CHECK(doc->content<tao::json::value>() == content);
        CHECK(replaced_doc->content<tao::json::value>() == new_content);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.ctx.ec());
    // check that it is really replaced
    auto final_doc = TransactionsTestEnvironment::get_doc(id);
    CHECK(final_doc.content_as<tao::json::value>() == new_content);
}

TEST_CASE("replace fails as expected with bad cas", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    tao::json::value new_content = { { "some_other_number", 3 } };
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        reinterpret_cast<couchbase::core::transactions::transaction_get_result&>(*doc).cas(100);
        auto replaced_doc = ctx.replace(doc, new_content);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(result.ctx.ec());
    CHECK(result.ctx.ec());
    // check that it is unchanged
    auto doc = TransactionsTestEnvironment::get_doc(id);
    REQUIRE(doc.content_as<tao::json::value>() == content);
}

TEST_CASE("can remove", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        auto removed_doc = ctx.remove(doc);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    // make sure it is really gone...
    try {
        auto doc = TransactionsTestEnvironment::get_doc(id);
        FAIL("expected doc to not exist");
    } catch (const couchbase::core::transactions::client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    }
}
TEST_CASE("remove fails as expected with bad cas", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        // change cas, so remove will fail and retry
        reinterpret_cast<couchbase::core::transactions::transaction_get_result&>(*doc).cas(100);
        auto err = ctx.remove(doc);
        CHECK(err.ec());
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(result.ctx.ec());
    CHECK(result.ctx.ec());
}

TEST_CASE("remove fails as expected with missing doc", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        CHECK(doc->ctx().ec() == couchbase::errc::transaction_op::document_not_found_exception);
        // the doc is 'blank', so trying to use it results in failure
        auto err = ctx.remove(doc);
        CHECK(err.ec());
        CHECK(err.ec() == couchbase::errc::transaction_op::unknown);
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(result.ctx.ec());
    CHECK(result.ctx.ec() == couchbase::errc::transaction::failed);
    CHECK(result.ctx.cause() == couchbase::errc::transaction_op::unknown);
}

TEST_CASE("uncaught exception in lambda will rollback without retry", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([id, coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.insert(coll, id.key(), content);
        CHECK_FALSE(doc->ctx().ec());
        throw std::runtime_error("some exception");
    });
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(result.ctx.ec());
    CHECK(result.ctx.ec() == couchbase::errc::transaction::failed);
    CHECK(result.ctx.cause() == couchbase::errc::transaction_op::unknown);
}

TEST_CASE("can pass per-transaction configs", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto cfg = couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(2));
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto begin = std::chrono::steady_clock::now();
    auto result = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto doc = ctx.get(coll, id.key());
          reinterpret_cast<couchbase::core::transactions::transaction_get_result&>(*doc).cas(100);
          auto err = ctx.remove(doc);
          CHECK(err.ec());
      },
      cfg);
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    // should be greater than the expiration time
    CHECK(elapsed > *cfg.expiration_time());
    // but not by too much (default is 15 seconds, we wanted 2, 2x that is plenty)
    CHECK(elapsed < (2 * *cfg.expiration_time()));
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    // could have failed in rollback, which returns fail rather than expired
    CHECK(result.ctx.ec());
}

TEST_CASE("can do simple query", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto res = ctx.query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key()));
          CHECK_FALSE(res->ctx().ec());
          CHECK(content == res->rows_as_json().front()["default"]);
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(result.ctx.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
}

TEST_CASE("can do simple mutating query", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto res = ctx.query(fmt::format("UPDATE `{}` USE KEYS '{}' SET `some_number` = 10", id.bucket(), id.key()));
          CHECK_FALSE(res->ctx().ec());
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(result.ctx.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
    auto final_doc = TransactionsTestEnvironment::get_doc(id);
    CHECK(final_doc.content_as<tao::json::value>().at("some_number") == 10);
}

TEST_CASE("some query errors don't force rollback", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto get_res = ctx.query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key()));
          CHECK_FALSE(get_res->ctx().ec());
          CHECK(get_res->rows_as_json().size() == 0);
          auto insert_res = ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", id.bucket(), id.key(), content));
          CHECK_FALSE(insert_res->ctx().ec());
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(result.ctx.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
    auto final_doc = TransactionsTestEnvironment::get_doc(id);
    CHECK(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("some query errors do rollback", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    auto id2 = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run(
      [id, id2, coll](couchbase::transactions::attempt_context& ctx) {
          // this one works.
          ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", id2.bucket(), id2.key(), content));
          // but not this one. But the query server doesn't notice until commit, so this _appears_ to succeed
          ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", id.bucket(), id.key(), content));
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK(result.ctx.ec() == couchbase::errc::transaction::failed);

    // id2 should not exist, since the txn should have rolled back.
    auto [err, doc2] = coll.get(id2.key(), {}).get();
    CHECK(err.ec() == couchbase::errc::key_value::document_not_found);
    CHECK(doc2.cas().empty());
}

TEST_CASE("some query errors are seen immediately", "[transactions]")
{
    auto core_cluster = TransactionsTestEnvironment::get_cluster();
    couchbase::cluster c(core_cluster);
    auto result = c.transactions()->run(
      [](couchbase::transactions::attempt_context& ctx) {
          auto res = ctx.query("I am not a valid n1ql query");
          CHECK(res->ctx().ec());
          CHECK(std::holds_alternative<couchbase::query_error_context>(res->ctx().cause()));
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
}

TEST_CASE("can query from a scope", "[transactions]")
{
    const std::string new_scope_name("newscope");
    const std::string new_coll_name("newcoll");
    couchbase::cluster c(TransactionsTestEnvironment::get_cluster());

    TransactionsTestEnvironment::upsert_collection(new_scope_name, new_coll_name);
    auto id = TransactionsTestEnvironment::get_document_id("scopequery", new_scope_name, new_coll_name);
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));

    auto new_scope = c.bucket(TransactionsTestEnvironment::get_conf().bucket).scope(new_scope_name);
    auto statement = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", new_coll_name, id.key());
    auto result = c.transactions()->run([&](couchbase::transactions::attempt_context& ctx) {
        auto res = ctx.query(new_scope, statement);
        CHECK_FALSE(res->ctx().ec());
        CHECK(res->rows_as_json().size() > 0);
        CHECK(res->rows_as_json().front()[new_coll_name] == content);
    });
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
}

TEST_CASE("can get doc from bucket not yet opened", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    // now make new virginal public cluster
    auto connection = conn(TransactionsTestEnvironment::get_conf(), false);
    couchbase::cluster c(connection.c);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([&id, &coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.get(coll, id.key());
        CHECK_FALSE(doc->ctx().ec());
        CHECK(doc->content<tao::json::value>() == content);
    });
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete); //  no mutations = no unstaging
}

TEST_CASE("can insert doc into bucket not yet opened", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    // now make new virginal public cluster
    auto connection = conn(TransactionsTestEnvironment::get_conf(), false);
    couchbase::cluster c(connection.c);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    auto result = c.transactions()->run([&id, &coll](couchbase::transactions::attempt_context& ctx) {
        auto doc = ctx.insert(coll, id.key(), content);
        CHECK_FALSE(doc->ctx().ec());
        CHECK_FALSE(doc->cas().empty());
    });
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK(TransactionsTestEnvironment::get_doc(id).cas != 0);
}

TEST_CASE("can replace doc in bucket not yet opened", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    // now make new virginal public cluster
    conn connection(TransactionsTestEnvironment::get_conf(), false);
    couchbase::cluster c(connection.c);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    tao::json::value new_content = { { "some", "new content" } };
    auto result = c.transactions()->run([&id, &coll, new_content](couchbase::transactions::attempt_context& ctx) {
        auto get_doc = ctx.get(coll, id.key());
        CHECK_FALSE(get_doc->ctx().ec());
        auto doc = ctx.replace(get_doc, new_content);
        CHECK_FALSE(doc->ctx().ec());
        CHECK_FALSE(doc->cas().empty());
    });
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK(TransactionsTestEnvironment::get_doc(id).content_as<tao::json::value>() == new_content);
}

TEST_CASE("can remove doc in bucket not yet opened", "[transactions]")
{
    auto id = TransactionsTestEnvironment::get_document_id();
    REQUIRE(TransactionsTestEnvironment::upsert_doc(id, content));
    // now make new virginal public cluster
    auto connection = conn(TransactionsTestEnvironment::get_conf(), false);
    couchbase::cluster c(connection.c);
    auto coll = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
    tao::json::value new_content = { { "some", "new content" } };
    auto result = c.transactions()->run([&id, &coll, new_content](couchbase::transactions::attempt_context& ctx) {
        auto get_doc = ctx.get(coll, id.key());
        CHECK_FALSE(get_doc->ctx().ec());
        auto res = ctx.remove(get_doc);
        CHECK_FALSE(res.ec());
    });
    CHECK_FALSE(result.ctx.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    try {
        auto removed_doc = TransactionsTestEnvironment::get_doc(id);
        FAIL("expected removed doc to not be present");

    } catch (const couchbase::core::transactions::client_error& e) {
        REQUIRE(e.res()->ec == couchbase::errc::key_value::document_not_found);
    } catch (...) {
        FAIL("Unexpected error");
    }
}
