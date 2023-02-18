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

#include "test_helper_integration.hxx"

#include <core/transactions/transaction_get_result.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/transactions.hxx>
#include <memory>
#include <variant>

static const tao::json::value content{ { "some_number", 0 } };
static const std::string content_json = couchbase::core::utils::json::generate(content);

couchbase::transactions::transaction_options
txn_opts()
{
    couchbase::transactions::transaction_options opts{};
    opts.expiration_time(std::chrono::seconds(2));
    return opts;
}
void
with_new_guard(std::function<void(test::utils::integration_test_guard&)> fn)
{
    test::utils::integration_test_guard integration;
    try {
        fn(integration);
    } catch (...) {
        // noop
    }
}
void
with_new_cluster(test::utils::integration_test_guard& integration, std::function<void(couchbase::cluster&)> fn)
{
    // make new virginal public cluster

    asio::io_context io;
    std::thread io_thread([&io]() { io.run(); });
    auto options = couchbase::cluster_options(integration.ctx.username, integration.ctx.password);
    auto [cluster, ec] = couchbase::cluster::connect(io, integration.ctx.connection_string, options).get();
    CHECK_FALSE(ec);
    try {
        if (!ec) {
            fn(cluster);
        }
    } catch (...) {
        // noop, just eat it.
    }
    cluster.close();
    io.stop();
    if (io_thread.joinable()) {
        io_thread.join();
    }
}

void
upsert_scope_and_collection(std::shared_ptr<couchbase::core::cluster> cluster,
                            const std::string& bucket_name,
                            const std::string& scope_name,
                            const std::string& coll_name)
{
    {
        couchbase::core::operations::management::scope_create_request req{ bucket_name, scope_name };
        auto resp = test::utils::execute(cluster, req);
        if (resp.ctx.ec) {
            REQUIRE(resp.ctx.ec == couchbase::errc::management::scope_exists);
        }
        auto created = test::utils::wait_until_collection_manifest_propagated(cluster, bucket_name, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::core::operations::management::collection_create_request req{ bucket_name, scope_name, coll_name };
        auto resp = test::utils::execute(cluster, req);
        if (resp.ctx.ec) {
            REQUIRE(resp.ctx.ec == couchbase::errc::management::collection_exists);
        }
        auto created = test::utils::wait_until_collection_manifest_propagated(cluster, bucket_name, resp.uid);
        REQUIRE(created);
    }
}

TEST_CASE("can get", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE_FALSE(upsert_res.cas().empty());

    auto [tx_err, result] = c.transactions()->run(
      [id, &coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.get(coll, id);
          CHECK_FALSE(e.ec());
          CHECK(doc.key() == id);
          CHECK_FALSE(doc.cas().empty());
          CHECK(doc.content<tao::json::value>() == content);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(tx_err.ec());
}

TEST_CASE("get returns error if doc doesn't exist", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.get(coll, id);
          CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found_exception);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK_FALSE(tx_err.ec());
}

TEST_CASE("can insert", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.insert(coll, id, content);
          CHECK_FALSE(e.ec());
          CHECK(doc.key() == id);
          CHECK_FALSE(doc.cas().empty());
          auto [e2, inserted_doc] = ctx.get(coll, id);
          CHECK_FALSE(e2.ec());
          CHECK(inserted_doc.content<tao::json::value>() == content);
      },
      txn_opts());
    REQUIRE_FALSE(result.transaction_id.empty());
    REQUIRE(result.unstaging_complete);
    REQUIRE_FALSE(tx_err.ec());
    // check that it is really there now
    auto [err, final_doc] = coll.get(id, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("insert has error as expected when doc already exists", "[transactions]")
{

    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    tao::json::value new_content{ { "something", "else" } };
    auto [tx_err, result] = c.transactions()->run(
      [id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.insert(coll, id, new_content);
          CHECK(e.ec() == couchbase::errc::transaction_op::document_exists_exception);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    // but the txn is successful
    CHECK(result.unstaging_complete);
    CHECK_FALSE(tx_err.ec());
    // check that it is really unchanged too.
    auto [final_err, final_doc] = coll.get(id, {}).get();
    REQUIRE_SUCCESS(final_err.ec());
    REQUIRE(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("can replace", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    tao::json::value new_content = { { "some_other_number", 3 } };
    auto [tx_err, result] = c.transactions()->run(
      [id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
          auto [_, doc] = ctx.get(coll, id);
          auto [e, replaced_doc] = ctx.replace(doc, new_content);
          CHECK_FALSE(e.ec());
          CHECK(doc.key() == replaced_doc.key());
          CHECK(doc.cas() != replaced_doc.cas());
          CHECK(doc.content<tao::json::value>() == content);
          CHECK(replaced_doc.content<tao::json::value>() == new_content);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(tx_err.ec());
    // check that it is really replaced
    auto [final_err, final_doc] = coll.get(id, {}).get();
    REQUIRE_SUCCESS(final_err.ec());
    REQUIRE(couchbase::core::utils::json::generate_binary(final_doc.content_as<tao::json::value>()) ==
            couchbase::core::utils::json::generate_binary(new_content));
}

TEST_CASE("replace fails as expected with bad cas", "[transactions]")
{

    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    tao::json::value new_content = { { "some_other_number", 3 } };
    auto [tx_err, result] = c.transactions()->run(
      [id, coll, new_content](couchbase::transactions::attempt_context& ctx) {
          auto [_, doc] = ctx.get(coll, id);
          // all this to change the cas...
          couchbase::core::transactions::transaction_get_result temp_doc(doc);
          temp_doc.cas(100);
          auto replaced_doc = ctx.replace(temp_doc.to_public_result(), new_content);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(tx_err.ec());
    // check that it is unchanged
    auto [final_err, final_doc] = coll.get(id, {}).get();
    REQUIRE_SUCCESS(final_err.ec());
    REQUIRE(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("can remove", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [_, doc] = ctx.get(coll, id);
          auto removed_doc = ctx.remove(doc);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(tx_err.ec());
    // make sure it is really gone...

    auto [final_err, final_doc] = coll.get(id, {}).get();
    REQUIRE(final_err.ec() == couchbase::errc::key_value::document_not_found);
}

TEST_CASE("remove fails as expected with bad cas", "[transactions]")
{

    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.get(coll, id);
          // change cas, so remove will fail and retry
          // all this to change the cas...
          couchbase::core::transactions::transaction_get_result temp_doc(doc);
          temp_doc.cas(100);
          auto remove_err = ctx.remove(temp_doc.to_public_result());
          CHECK(remove_err.ec());
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(tx_err.ec());
}

TEST_CASE("remove fails as expected with missing doc", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.get(coll, id);
          CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found_exception);
          // the doc is 'blank', so trying to use it results in failure
          auto err = ctx.remove(doc);
          CHECK(err.ec());
          CHECK(err.ec() == couchbase::errc::transaction_op::unknown);
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(tx_err.ec() == couchbase::errc::transaction::failed);
    CHECK(tx_err.cause() == couchbase::errc::transaction_op::unknown);
}

TEST_CASE("uncaught exception in lambda will rollback without retry", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.insert(coll, id, content);
          CHECK_FALSE(e.ec());
          throw std::runtime_error("some exception");
      },
      txn_opts());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    CHECK(tx_err.ec() == couchbase::errc::transaction::failed);
    CHECK(tx_err.cause() == couchbase::errc::transaction_op::unknown);
}

TEST_CASE("can pass per-transaction configs", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto opts = couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(2));
    auto begin = std::chrono::steady_clock::now();
    auto [tx_err, result] = c.transactions()->run(
      [id, coll](couchbase::transactions::attempt_context& ctx) {
          auto [e, doc] = ctx.get(coll, id);
          // all this to change the cas...
          couchbase::core::transactions::transaction_get_result temp_doc(doc);
          temp_doc.cas(100);
          auto remove_err = ctx.remove(temp_doc.to_public_result());
          CHECK(remove_err.ec());
      },
      opts);
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    // should be greater than the expiration time
    CHECK(elapsed > *opts.expiration_time());
    // but not by too much (default is 15 seconds, we wanted 2, 2x that is plenty)
    CHECK(elapsed < (2 * *opts.expiration_time()));
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete);
    // could have failed in rollback, which returns fail rather than expired
    CHECK(tx_err.ec());
}

TEST_CASE("can do simple query", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());
    auto [tx_err, result] = c.transactions()->run(
      [id, coll, test_ctx = integration.ctx](couchbase::transactions::attempt_context& ctx) {
          auto [e, res] = ctx.query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", test_ctx.bucket, id));
          CHECK_FALSE(e.ec());
          CHECK(content == res.rows_as_json().front()["default"]);
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(tx_err.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
}

TEST_CASE("can do simple mutating query", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto [tx_err, result] = c.transactions()->run(
      [id, coll, test_ctx = integration.ctx](couchbase::transactions::attempt_context& ctx) {
          auto [e, res] = ctx.query(fmt::format("UPDATE `{}` USE KEYS '{}' SET `some_number` = 10", test_ctx.bucket, id));
          CHECK_FALSE(e.ec());
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(tx_err.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
    auto [final_err, final_doc] = coll.get(id, {}).get();
    CHECK(final_doc.content_as<tao::json::value>().at("some_number") == 10);
}

TEST_CASE("some query errors don't force rollback", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [id, coll, test_ctx = integration.ctx](couchbase::transactions::attempt_context& ctx) {
          auto [get_err, get_res] = ctx.query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", test_ctx.bucket, id));
          CHECK_FALSE(get_err.ec());
          CHECK(get_res.rows_as_json().size() == 0);
          auto [insert_err, _] =
            ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id, content_json));
          CHECK_FALSE(insert_err.ec());
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(tx_err.ec());
    CHECK(result.unstaging_complete);
    CHECK_FALSE(result.transaction_id.empty());
    auto [final_err, final_doc] = coll.get(id, {}).get();
    CHECK(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("some query errors do rollback", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    auto id2 = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto [tx_err, result] = c.transactions()->run(
      [id, id2, coll, test_ctx = integration.ctx](couchbase::transactions::attempt_context& ctx) {
          // this one works.
          auto [e, _] = ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id2, content_json));
          CHECK_FALSE(e.ec());
          // but not this one. But the query server doesn't notice until commit, so this _appears_ to succeed
          auto [e2, __] = ctx.query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id, content_json));
          CHECK_FALSE(e2.ec());
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK(tx_err.ec() == couchbase::errc::transaction::failed);

    // id2 should not exist, since the txn should have rolled back.
    auto [doc2_err, doc2] = coll.get(id2, {}).get();
    CHECK(doc2_err.ec() == couchbase::errc::key_value::document_not_found);
    CHECK(doc2.cas().empty());
}

TEST_CASE("some query errors are seen immediately", "[transactions]")
{
    test::utils::integration_test_guard integration;

    couchbase::cluster c(integration.cluster);
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [](couchbase::transactions::attempt_context& ctx) {
          auto [e, res] = ctx.query("I am not a valid n1ql query");
          CHECK(e.ec());
          CHECK(std::holds_alternative<couchbase::query_error_context>(e.cause()));
      },
      couchbase::transactions::transaction_options().expiration_time(std::chrono::seconds(10)));
    CHECK_FALSE(tx_err.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
}

TEST_CASE("can query from a scope", "[transactions]")
{
    const std::string new_scope_name("newscope");
    const std::string new_coll_name("newcoll");
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");
    couchbase::cluster c(integration.cluster);

    upsert_scope_and_collection(integration.cluster, integration.ctx.bucket, new_scope_name, new_coll_name);
    auto new_scope = c.bucket(integration.ctx.bucket).scope(new_scope_name);
    auto new_coll = c.bucket(integration.ctx.bucket).scope(new_scope_name).collection(new_coll_name);
    auto [err, upsert_res] = new_coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());

    auto statement = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", new_coll_name, id);
    auto [tx_err, result] = c.transactions()->run(
      [&](couchbase::transactions::attempt_context& ctx) {
          auto [e, res] = ctx.query(new_scope, statement);
          CHECK_FALSE(e.ec());
          CHECK(res.rows_as_json().size() > 0);
          CHECK(res.rows_as_json().front()[new_coll_name] == content);
      },
      txn_opts());
    CHECK_FALSE(tx_err.ec());
    CHECK_FALSE(result.transaction_id.empty());
}

TEST_CASE("can get doc from bucket not yet opened", "[transactions]")
{

    auto id = test::utils::uniq_id("txn");
    {
        test::utils::integration_test_guard integration;
        couchbase::cluster c(integration.cluster);
        auto coll = c.bucket(integration.ctx.bucket).default_collection();
        auto [err, upsert_res] = coll.upsert(id, content, {}).get();
        REQUIRE_SUCCESS(err.ec());
    }

    with_new_guard([&](test::utils::integration_test_guard& integration) {
        couchbase::cluster c(integration.cluster);
        auto coll = c.bucket(integration.ctx.bucket).default_collection();
        auto [tx_err, result] = c.transactions()->run(
          [&id, &coll](couchbase::transactions::attempt_context& ctx) {
              auto [e, doc] = ctx.get(coll, id);
              CHECK_FALSE(e.ec());
              CHECK(doc.content<tao::json::value>() == content);
          },
          txn_opts());
        CHECK_FALSE(tx_err.ec());
        CHECK_FALSE(result.transaction_id.empty());
        CHECK_FALSE(result.unstaging_complete); //  no mutations = no unstaging
    });
}

TEST_CASE("can insert doc into bucket not yet opened", "[transactions]")
{
    test::utils::integration_test_guard integration;

    auto id = test::utils::uniq_id("txn");

    with_new_guard([&](test::utils::integration_test_guard& guard) {
        couchbase::cluster c(guard.cluster);
        auto coll = c.bucket(integration.ctx.bucket).default_collection();

        auto [tx_err, result] = c.transactions()->run(
          [&id, &coll](couchbase::transactions::attempt_context& ctx) {
              auto [e, doc] = ctx.insert(coll, id, content);
              CHECK_FALSE(e.ec());
              CHECK_FALSE(doc.cas().empty());
          },
          txn_opts());
        CHECK_FALSE(tx_err.ec());
        CHECK_FALSE(result.transaction_id.empty());
        CHECK(result.unstaging_complete);
        auto [err, get_res] = coll.get(id, {}).get();
        REQUIRE_SUCCESS(err.ec());
        REQUIRE_FALSE(get_res.cas().empty());
    });
}

TEST_CASE("can replace doc in bucket not yet opened", "[transactions]")
{

    auto id = test::utils::uniq_id("txn");
    {
        test::utils::integration_test_guard integration;
        couchbase::cluster c(integration.cluster);
        auto coll = c.bucket(integration.ctx.bucket).default_collection();
        auto [err, upsert_res] = coll.upsert(id, content, {}).get();
        REQUIRE_SUCCESS(err.ec());
    }

    with_new_guard([&](test::utils::integration_test_guard& guard) {
        couchbase::cluster c(guard.cluster);
        auto coll = c.bucket(guard.ctx.bucket).default_collection();
        tao::json::value new_content = { { "some", "new content" } };

        auto [tx_err, result] = c.transactions()->run(
          [&id, &coll, new_content](couchbase::transactions::attempt_context& ctx) {
              auto [get_err, get_doc] = ctx.get(coll, id);
              CHECK_FALSE(get_err.ec());
              auto [e, doc] = ctx.replace(get_doc, new_content);
              CHECK_FALSE(e.ec());
              CHECK_FALSE(doc.cas().empty());
          },
          txn_opts());
        CHECK_FALSE(tx_err.ec());
        CHECK_FALSE(result.transaction_id.empty());
        CHECK(result.unstaging_complete);
        auto [err, get_res] = coll.get(id, {}).get();
        REQUIRE_SUCCESS(err.ec());
        REQUIRE(get_res.content_as<tao::json::value>() == new_content);
    });
}

TEST_CASE("can remove doc in bucket not yet opened", "[transactions]")
{

    auto id = test::utils::uniq_id("txn");
    {
        test::utils::integration_test_guard integration;
        couchbase::cluster c(integration.cluster);
        auto coll = c.bucket(integration.ctx.bucket).default_collection();
        auto [err, upsert_res] = coll.upsert(id, content, {}).get();
        REQUIRE_SUCCESS(err.ec());
    }

    with_new_guard([&](test::utils::integration_test_guard& guard) {
        couchbase::cluster c(guard.cluster);
        auto coll = c.bucket(guard.ctx.bucket).default_collection();
        tao::json::value new_content = { { "some", "new content" } };
        auto [tx_err, result] = c.transactions()->run(
          [&id, &coll, new_content](couchbase::transactions::attempt_context& ctx) {
              auto [e, get_doc] = ctx.get(coll, id);
              CHECK_FALSE(e.ec());
              auto res = ctx.remove(get_doc);
              CHECK_FALSE(res.ec());
          },
          txn_opts());
        CHECK_FALSE(tx_err.ec());
        CHECK_FALSE(result.transaction_id.empty());
        CHECK(result.unstaging_complete);
        auto [get_err, get_res] = coll.get(id, {}).get();
        CHECK(get_err.ec() == couchbase::errc::key_value::document_not_found);
    });
}
