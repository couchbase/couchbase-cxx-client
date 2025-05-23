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

#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/transactions/transaction_get_result.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/transactions.hxx>

#include <memory>
#include <variant>

static const tao::json::value content{ { "some_number", 0 } };
static const std::string content_json = couchbase::core::utils::json::generate(content);

couchbase::transactions::transaction_options
txn_opts()
{
  couchbase::transactions::transaction_options opts{};
  opts.timeout(std::chrono::seconds(2));
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
with_new_cluster(test::utils::integration_test_guard& integration,
                 std::function<void(couchbase::cluster&)> fn)
{
  // make new virginal public cluster
  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  try {
    if (!err) {
      fn(cluster);
    }
  } catch (...) {
    // noop, just eat it.
  }
}

void
upsert_scope_and_collection(const couchbase::core::cluster& cluster,
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
    auto created =
      test::utils::wait_until_collection_manifest_propagated(cluster, bucket_name, resp.uid);
    REQUIRE(created);
  }

  {
    couchbase::core::operations::management::collection_create_request req{ bucket_name,
                                                                            scope_name,
                                                                            coll_name };
    auto resp = test::utils::execute(cluster, req);
    if (resp.ctx.ec) {
      REQUIRE(resp.ctx.ec == couchbase::errc::management::collection_exists);
    }
    auto created =
      test::utils::wait_until_collection_manifest_propagated(cluster, bucket_name, resp.uid);
    REQUIRE(created);
  }
}

TEST_CASE("transactions public blocking API: can get", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE_FALSE(upsert_res.cas().empty());

  auto [tx_err, result] = c.transactions()->run(
    [id, &coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      CHECK_FALSE(e.ec());
      CHECK(doc.id() == id);
      CHECK(doc.content_as<tao::json::value>() == content);
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(tx_err.ec());
}

TEST_CASE("transactions public blocking API: get returns error if doc doesn't exist",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found);
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  CHECK_FALSE(tx_err.ec());
}

TEST_CASE("transactions public blocking API: can insert", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->insert(coll, id, content);
      CHECK_FALSE(e.ec());
      CHECK(doc.id() == id);
      CHECK(doc.content_as<tao::json::value>() == content);
      auto [e2, inserted_doc] = ctx->get(coll, id);
      CHECK_FALSE(e2.ec());
      CHECK(inserted_doc.content_as<tao::json::value>() == content);
      return {};
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

TEST_CASE("transactions public blocking API: insert has error when doc already exists",
          "[transactions]")
{

  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  tao::json::value new_content{ { "something", "else" } };
  auto [tx_err, result] = c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->insert(coll, id, new_content);
      CHECK(e.ec() == couchbase::errc::transaction_op::document_exists);
      return {};
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

TEST_CASE("transactions public blocking API: can replace", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  tao::json::value new_content = {
    { "some_other_number", 3 },
  };
  auto [tx_err, result] = c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [_, doc] = ctx->get(coll, id);
      auto [e, replaced_doc] = ctx->replace(doc, new_content);
      CHECK_FALSE(e.ec());
      CHECK(doc.id() == replaced_doc.id());
      CHECK(doc.content_as<tao::json::value>() == content);
      CHECK(replaced_doc.content_as<tao::json::value>() == new_content);
      return {};
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

TEST_CASE("transactions public blocking API: replace fails as expected with bad cas",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  tao::json::value new_content = { { "some_other_number", 3 } };
  auto [tx_err, result] = c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [_, doc] = ctx->get(coll, id);
      // all this to change the cas...
      couchbase::core::transactions::transaction_get_result temp_doc(doc);
      temp_doc.cas(100);
      auto replaced_doc = ctx->replace(temp_doc.to_public_result(), new_content);
      return {};
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

TEST_CASE("transactions public blocking API: can remove", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [_, doc] = ctx->get(coll, id);
      auto removed_doc = ctx->remove(doc);
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK(result.unstaging_complete);
  CHECK_FALSE(tx_err.ec());
  // make sure it is really gone...

  auto [final_err, final_doc] = coll.get(id, {}).get();
  REQUIRE(final_err.ec() == couchbase::errc::key_value::document_not_found);
}

TEST_CASE("transactions public blocking API: remove fails as expected with bad cas",
          "[transactions]")
{

  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      // change cas, so remove will fail and retry
      // all this to change the cas...
      couchbase::core::transactions::transaction_get_result temp_doc(doc);
      temp_doc.cas(100);
      auto remove_err = ctx->remove(temp_doc.to_public_result());
      CHECK(remove_err.ec());
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  CHECK(tx_err.ec());
}

TEST_CASE("transactions public blocking API: remove fails as expected with missing doc",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found);
      // the doc is 'blank', so trying to use it results in failure
      auto err = ctx->remove(doc);
      CHECK(err.cause().has_value());
      CHECK(err.cause().value().ec() == couchbase::errc::transaction_op::generic);
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  CHECK(tx_err.ec() == couchbase::errc::transaction::failed);
  CHECK(tx_err.cause().has_value());
  CHECK(tx_err.cause().value().ec() == couchbase::errc::transaction_op::generic);
}

TEST_CASE("transactions public blocking API: get doc not found and propagating error",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found);
      if (e) {
        return e;
      }
      return {};
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  CHECK(tx_err.ec() == couchbase::errc::transaction::failed);
  CHECK(tx_err.cause().has_value());
  CHECK(tx_err.cause().value().ec() == couchbase::errc::transaction_op::document_not_found);
}

TEST_CASE(
  "transactions public blocking API: uncaught exception in lambda will rollback without retry",
  "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->insert(coll, id, content);
      CHECK_FALSE(e.ec());
      throw std::runtime_error("some exception");
    },
    txn_opts());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  CHECK(tx_err.ec() == couchbase::errc::transaction::failed);
  CHECK(tx_err.cause().has_value());
  CHECK(tx_err.cause().value().ec() == couchbase::errc::transaction_op::generic);
}

TEST_CASE("transactions public blocking API: can pass per-transaction configs", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto opts = couchbase::transactions::transaction_options().timeout(std::chrono::seconds(2));
  auto begin = std::chrono::steady_clock::now();
  auto [tx_err, result] = c.transactions()->run(
    [id, coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, doc] = ctx->get(coll, id);
      // all this to change the cas...
      couchbase::core::transactions::transaction_get_result temp_doc(doc);
      temp_doc.cas(100);
      auto remove_err = ctx->remove(temp_doc.to_public_result());
      CHECK(remove_err.ec());
      return {};
    },
    opts);
  auto end = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
  // should be greater than the timeout
  CHECK(elapsed > *opts.timeout());
  // but not by too much (default is 15 seconds, we wanted 2, 2x that is plenty)
  CHECK(elapsed < (2 * *opts.timeout()));
  CHECK_FALSE(result.transaction_id.empty());
  CHECK_FALSE(result.unstaging_complete);
  // could have failed in rollback, which returns fail rather than expired
  CHECK(tx_err.ec());
}

TEST_CASE("transactions public blocking API: can do simple query", "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());
  auto [tx_err, result] = c.transactions()->run(
    [id, coll, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, res] =
        ctx->query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", test_ctx.bucket, id));
      CHECK_FALSE(e.ec());
      CHECK(content == res.rows_as().front()["default"]);
      return {};
    },
    couchbase::transactions::transaction_options().timeout(std::chrono::seconds(10)));
  CHECK_FALSE(tx_err.ec());
  CHECK(result.unstaging_complete);
  CHECK_FALSE(result.transaction_id.empty());
}

TEST_CASE("transactions public blocking API: can do simple mutating query", "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto [tx_err, result] = c.transactions()->run(
    [id, coll, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, res] = ctx->query(
        fmt::format("UPDATE `{}` USE KEYS '{}' SET `some_number` = 10", test_ctx.bucket, id));
      CHECK_FALSE(e.ec());
      return {};
    },
    couchbase::transactions::transaction_options().timeout(std::chrono::seconds(10)));
  CHECK_FALSE(tx_err.ec());
  CHECK(result.unstaging_complete);
  CHECK_FALSE(result.transaction_id.empty());
  auto [final_err, final_doc] = coll.get(id, {}).get();
  CHECK(final_doc.content_as<tao::json::value>().at("some_number") == 10);
}

TEST_CASE("transactions public blocking API: some query errors don't force rollback",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [id, coll, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [get_err, get_res] =
        ctx->query(fmt::format("SELECT * FROM `{}` USE KEYS '{}'", test_ctx.bucket, id));
      CHECK_FALSE(get_err.ec());
      CHECK(get_res.rows_as().size() == 0);
      auto [insert_err, _] = ctx->query(fmt::format(
        R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id, content_json));
      CHECK_FALSE(insert_err.ec());
      return {};
    },
    couchbase::transactions::transaction_options().timeout(std::chrono::seconds(10)));
  CHECK_FALSE(tx_err.ec());
  CHECK(result.unstaging_complete);
  CHECK_FALSE(result.transaction_id.empty());
  auto [final_err, final_doc] = coll.get(id, {}).get();
  CHECK(final_doc.content_as<tao::json::value>() == content);
}

TEST_CASE("transactions public blocking API: some query errors do rollback", "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto id2 = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto [tx_err, result] = c.transactions()->run(
    [id, id2, coll, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      // this one works.
      auto [e, _] = ctx->query(fmt::format(
        R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id2, content_json));
      CHECK_FALSE(e.ec());
      // but not this one. But the query server doesn't notice until commit, so this _appears_ to
      // succeed
      auto [e2, __] = ctx->query(fmt::format(
        R"(INSERT INTO `{}` (KEY, VALUE) VALUES ("{}", {}))", test_ctx.bucket, id, content_json));
      CHECK_FALSE(e2.ec());
      return {};
    },
    couchbase::transactions::transaction_options().timeout(std::chrono::seconds(10)));
  CHECK(tx_err.ec() == couchbase::errc::transaction::failed);

  // id2 should not exist, since the txn should have rolled back.
  auto [doc2_err, doc2] = coll.get(id2, {}).get();
  CHECK(doc2_err.ec() == couchbase::errc::key_value::document_not_found);
  CHECK(doc2.cas().empty());
}

TEST_CASE("transactions public blocking API: some query errors are seen immediately",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = c.transactions()->run(
    [](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, res] = ctx->query("I am not a valid n1ql query");
      CHECK(e.ec());
      CHECK(e.cause().has_value());
      CHECK(e.cause().value().ec() == couchbase::errc::common::parsing_failure);
      return {};
    },
    couchbase::transactions::transaction_options().timeout(std::chrono::seconds(10)));
  CHECK_FALSE(tx_err.ec());
  CHECK_FALSE(result.transaction_id.empty());
  CHECK(result.unstaging_complete);
}

TEST_CASE("transactions public blocking API: can query from a scope", "[transactions]")
{
  const std::string new_scope_name("newscope");
  const std::string new_coll_name("newcoll");
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();

  upsert_scope_and_collection(
    integration.cluster, integration.ctx.bucket, new_scope_name, new_coll_name);
  auto new_scope = c.bucket(integration.ctx.bucket).scope(new_scope_name);
  auto new_coll = c.bucket(integration.ctx.bucket).scope(new_scope_name).collection(new_coll_name);
  auto [err, upsert_res] = new_coll.upsert(id, content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto statement = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", new_coll_name, id);
  auto [tx_err, result] = c.transactions()->run(
    [&](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      auto [e, res] = ctx->query(new_scope, statement);
      CHECK_FALSE(e.ec());
      CHECK(res.rows_as().size() > 0);
      CHECK(res.rows_as().front()[new_coll_name] == content);
      return {};
    },
    txn_opts());
  CHECK_FALSE(tx_err.ec());
  CHECK_FALSE(result.transaction_id.empty());

  {
    couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket,
                                                                     new_scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }
}

TEST_CASE("transactions public blocking API: can get doc from bucket not yet opened",
          "[transactions]")
{

  auto id = test::utils::uniq_id("txn");
  {
    test::utils::integration_test_guard integration;
    auto c = integration.public_cluster();
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());
  }

  with_new_guard([&](test::utils::integration_test_guard& integration) {
    auto c = integration.public_cluster();
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [tx_err, result] = c.transactions()->run(
      [&id,
       &coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [e, doc] = ctx->get(coll, id);
        CHECK_FALSE(e.ec());
        CHECK(doc.content_as<tao::json::value>() == content);
        return {};
      },
      txn_opts());
    CHECK_FALSE(tx_err.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK_FALSE(result.unstaging_complete); //  no mutations = no unstaging
  });
}

TEST_CASE("transactions public blocking API: can insert doc into bucket not yet opened",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");

  with_new_guard([&](test::utils::integration_test_guard& guard) {
    auto c = guard.public_cluster();
    auto coll = c.bucket(integration.ctx.bucket).default_collection();

    auto [tx_err, result] = c.transactions()->run(
      [&id,
       &coll](std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [e, doc] = ctx->insert(coll, id, content);
        CHECK_FALSE(e.ec());
        CHECK(doc.id() == id);
        return {};
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

TEST_CASE("transactions public blocking API: can replace doc in bucket not yet opened",
          "[transactions]")
{

  auto id = test::utils::uniq_id("txn");
  {
    test::utils::integration_test_guard integration;
    auto c = integration.public_cluster();
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());
  }

  with_new_guard([&](test::utils::integration_test_guard& guard) {
    auto c = guard.public_cluster();
    auto coll = c.bucket(guard.ctx.bucket).default_collection();
    tao::json::value new_content = { { "some", "new content" } };

    auto [tx_err, result] = c.transactions()->run(
      [&id, &coll, new_content](
        std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [get_err, get_doc] = ctx->get(coll, id);
        CHECK_FALSE(get_err.ec());
        auto [e, doc] = ctx->replace(get_doc, new_content);
        CHECK_FALSE(e.ec());
        CHECK(doc.id() == id);
        return {};
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

TEST_CASE("transactions public blocking API: can remove doc in bucket not yet opened",
          "[transactions]")
{

  auto id = test::utils::uniq_id("txn");
  {
    test::utils::integration_test_guard integration;
    auto c = integration.public_cluster();
    auto coll = c.bucket(integration.ctx.bucket).default_collection();
    auto [err, upsert_res] = coll.upsert(id, content, {}).get();
    REQUIRE_SUCCESS(err.ec());
  }

  with_new_guard([&](test::utils::integration_test_guard& guard) {
    auto c = guard.public_cluster();
    auto coll = c.bucket(guard.ctx.bucket).default_collection();
    tao::json::value new_content = { { "some", "new content" } };
    auto [tx_err, result] = c.transactions()->run(
      [&id, &coll, new_content](
        std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [e, get_doc] = ctx->get(coll, id);
        CHECK_FALSE(e.ec());
        auto res = ctx->remove(get_doc);
        CHECK_FALSE(res.ec());
        return {};
      },
      txn_opts());
    CHECK_FALSE(tx_err.ec());
    CHECK_FALSE(result.transaction_id.empty());
    CHECK(result.unstaging_complete);
    auto [get_err, get_res] = coll.get(id, {}).get();
    CHECK(get_err.ec() == couchbase::errc::key_value::document_not_found);
  });
}

TEST_CASE("transactions public blocking API: insert then replace with illegal document "
          "modification in-between",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_replace_body_with_xattr()) {
    // If replace_body_with_xattr is not supported, we have the staged insert's content in memory,
    // so the transactional get will not fetch the document from the server, which would give the
    // up-to-date CAS.
    SKIP("the server does not support replace_body_with_xattr");
  }

  auto doc_id = test::utils::uniq_id("txn");
  auto txn_content_initial = tao::json::value{ { "num", 12 } };
  auto txn_content_updated = tao::json::value{ { "num", 20 } };
  auto illegal_content = tao::json::value{ { "illegal", "content" } };

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto [tx_err, result] = cluster.transactions()->run(
    [&doc_id, &collection, &txn_content_initial, &txn_content_updated, &illegal_content](
      std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
      // Stage an insert
      {
        auto [err, res] = ctx->insert(collection, doc_id, txn_content_initial);
        if (err) {
          return err;
        }
        REQUIRE(res.content_as<tao::json::value>() == txn_content_initial);
      }

      // Do an illegal non-transactional insert that will override any staged content and txn
      // metadata
      {
        auto [err, res] = collection.insert(doc_id, illegal_content).get();
        REQUIRE_SUCCESS(err.ec());
      }

      {
        // Now that we implement ExtReplaceBodyWithXattr, this will fetch the document from the
        // server (post-illegal mutation) as the staged content of the staged mutation is not stored
        // in memory.
        auto [get_err, get_res] = ctx->get(collection, doc_id);
        if (get_err) {
          return get_err;
        }
        REQUIRE(get_res.content_as<tao::json::value>() == illegal_content);

        // This replace will use the CAS from the transaction_get_result, which should be the one
        // after the illegal insert. This means the operation will succeed, and will result in a
        // staged insert with the CAS from the transaction_get_result.
        // When committing, the replace_body_with_xattr op, and the transaction, will succeed.
        auto [replace_err, replace_res] = ctx->replace(get_res, txn_content_updated);
        if (replace_err) {
          return replace_err;
        }
        REQUIRE(replace_res.content_as<tao::json::value>() == txn_content_updated);
      }

      return {};
    });

  REQUIRE_SUCCESS(tx_err.ec());
}
