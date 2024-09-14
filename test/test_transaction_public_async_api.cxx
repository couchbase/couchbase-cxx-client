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

#include "core/transactions/transaction_get_result.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/transactions.hxx>
#include <couchbase/transactions/transaction_options.hxx>

#include <memory>

static const tao::json::value async_content{ { "some_number", 0 } };
static const std::string async_content_json = couchbase::core::utils::json::generate(async_content);

couchbase::transactions::transaction_options
async_options()
{
  couchbase::transactions::transaction_options cfg;
  cfg.timeout(std::chrono::seconds(1));
  return cfg;
}

TEST_CASE("transactions public async API: can async get", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, coll](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [id](auto e, auto res) {
        CHECK_FALSE(e.ec());
        CHECK(res.id() == id);
        CHECK(res.template content_as<tao::json::value>() == async_content);
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(e.ec());
      CHECK_FALSE(res.transaction_id.empty());
      CHECK_FALSE(res.unstaging_complete);
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: can get fail as expected", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, coll](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [id](auto e, auto) {
        CHECK(e.ec() == couchbase::errc::transaction_op::document_not_found);
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(e.ec());
      CHECK_FALSE(res.transaction_id.empty());
      CHECK_FALSE(res.unstaging_complete);
      barrier->set_value();
    },
    async_options());
  f.get();
}
TEST_CASE("transactions public async API: can async remove", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [coll,
     id](std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [ctx](auto e, auto res) {
        CHECK_FALSE(e.ec());
        ctx->remove(res, [](auto remove_err) {
          CHECK_FALSE(remove_err.ec());
        });
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(res.transaction_id.empty());
      CHECK(res.unstaging_complete);
      CHECK_FALSE(e.ec());
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: async remove with bad cas fails as expected",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [coll,
     id](std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [ctx](auto, auto res) {
        // all this to change the cas...
        couchbase::core::transactions::transaction_get_result temp_doc(res);
        temp_doc.cas(100);
        ctx->remove(temp_doc.to_public_result(), [](auto remove_err) {
          CHECK(remove_err.ec());
        });
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(res.transaction_id.empty());
      CHECK_FALSE(res.unstaging_complete);
      CHECK(e.ec()); // sometimes, it is a FAIL, as it expires in rollback, other times an expiry
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: can async insert", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, coll](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->insert(coll, id, async_content, [coll, id](auto e, auto) {
        CHECK_FALSE(e.ec());
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(res.transaction_id.empty());
      CHECK(res.unstaging_complete);
      CHECK_FALSE(e.ec());
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE(
  "transactions public async API: async insert fails when doc already exists, but doesn't rollback",
  "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, coll](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->insert(coll, id, async_content, [coll, id](auto e, auto) {
        CHECK(e.ec() == couchbase::errc::transaction_op::document_exists);
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(res.transaction_id.empty());
      CHECK(res.unstaging_complete);
      CHECK_FALSE(e.ec());
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: can async replace", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  tao::json::value new_content = { { "Iam", "new content" } };
  c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [new_content, ctx, id](auto, auto res) {
        ctx->replace(res, new_content, [id](auto replace_e, auto replace_result) {
          CHECK(replace_result.id() == id);
          CHECK_FALSE(replace_e.ec());
        });
      });
      return {};
    },
    [barrier](auto e, auto tx_result) {
      CHECK_FALSE(tx_result.transaction_id.empty());
      CHECK(tx_result.unstaging_complete);
      CHECK_FALSE(e.ec());
      barrier->set_value();
    },
    async_options());
  f.get();
}
TEST_CASE("transactions public async API: async replace fails as expected with bad cas",
          "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  tao::json::value new_content = { { "Iam", "new content" } };
  c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [new_content, ctx](auto, auto res) {
        // all this to change the cas...
        couchbase::core::transactions::transaction_get_result temp_doc(res);
        temp_doc.cas(100);
        ctx->replace(temp_doc.to_public_result(), new_content, [](auto replace_e, auto) {
          CHECK(replace_e.ec());
        });
      });
      return {};
    },
    [barrier](auto e, auto tx_result) {
      CHECK_FALSE(tx_result.transaction_id.empty());
      CHECK_FALSE(tx_result.unstaging_complete);
      CHECK(e.ec());
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: uncaught exception will rollback", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  tao::json::value new_content = { { "Iam", "new content" } };
  c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [new_content, ctx](auto e, auto res) {
        CHECK_FALSE(e.ec());
        ctx->replace(res, new_content, [](auto replace_e, auto) {
          CHECK_FALSE(replace_e.ec());
          throw std::runtime_error("I wanna rollback");
        });
      });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK(e.ec() == couchbase::errc::transaction::failed);
      CHECK_FALSE(res.unstaging_complete);
      CHECK_FALSE(res.transaction_id.empty());
      barrier->set_value();
    },
    async_options());

  f.get();
}

TEST_CASE("transactions public async API: rerturning error will rollback", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  tao::json::value new_content = { { "Iam", "new content" } };
  c.transactions()->run(
    [id, coll, new_content](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [new_content, ctx](auto e, auto res) {
        CHECK_FALSE(e.ec());
        ctx->replace(res, new_content, [](auto replace_e, auto) {
          CHECK_FALSE(replace_e.ec());
        });
      });
      return couchbase::error(couchbase::errc::common::request_canceled);
    },
    [barrier](auto e, auto res) {
      CHECK(e.ec() == couchbase::errc::transaction::failed);
      CHECK_FALSE(res.unstaging_complete);
      CHECK_FALSE(res.transaction_id.empty());
      barrier->set_value();
    },
    async_options());

  f.get();
}

TEST_CASE("transactions public async API: can set transaction options", "[transactions]")
{
  test::utils::integration_test_guard integration;

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto begin = std::chrono::steady_clock::now();
  auto cfg = couchbase::transactions::transaction_options().timeout(std::chrono::seconds(2));
  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, coll](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->get(coll, id, [ctx](auto, auto doc) {
        // all this to change the cas...
        couchbase::core::transactions::transaction_get_result temp_doc(doc);
        temp_doc.cas(100);
        ctx->remove(temp_doc.to_public_result(), [](const couchbase::error& remove_err) {
          CHECK(remove_err.ec());
        });
      });
      return {};
    },
    [&begin, &cfg, barrier](auto e, auto res) {
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
      // should be greater than the timeout
      CHECK(elapsed > *cfg.timeout());
      // but not by too much (default is 15 seconds, we wanted 1, 2 is plenty)
      CHECK(elapsed < (2 * *cfg.timeout()));
      // and of course the txn should have expired
      CHECK_FALSE(res.transaction_id.empty());
      CHECK_FALSE(res.unstaging_complete);
      CHECK(e.ec()); // can be fail or expired, as we get a fail if expiring in rollback.
      barrier->set_value();
    },
    cfg);

  f.get();
}

TEST_CASE("transactions public async API: can do mutating query", "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES("{}", {}))",
                             test_ctx.bucket,
                             id,
                             async_content_json),
                 [](auto e, auto) {
                   CHECK_FALSE(e.ec());
                 });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK_FALSE(e.ec());
      CHECK_FALSE(res.transaction_id.empty());
      CHECK(res.unstaging_complete);
      barrier->set_value();
    },
    async_options());
  f.get();
}

TEST_CASE("transactions public async API: some query errors rollback", "[transactions]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_queries_in_transactions()) {
    SKIP("the server does not support queries inside transactions");
  }

  auto id = test::utils::uniq_id("txn");
  auto id2 = test::utils::uniq_id("txn");
  auto c = integration.public_cluster();
  auto coll = c.bucket(integration.ctx.bucket).default_collection();
  auto [err, upsert_res] = coll.upsert(id, async_content, {}).get();
  REQUIRE_SUCCESS(err.ec());

  auto barrier = std::make_shared<std::promise<void>>();
  auto f = barrier->get_future();
  c.transactions()->run(
    [id, id2, test_ctx = integration.ctx](
      std::shared_ptr<couchbase::transactions::async_attempt_context> ctx) -> couchbase::error {
      ctx->query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES("{}", {}))",
                             test_ctx.bucket,
                             id2,
                             async_content_json),
                 [id, ctx, &test_ctx](auto e, auto) {
                   CHECK_FALSE(e.ec());
                   ctx->query(fmt::format(R"(INSERT INTO `{}` (KEY, VALUE) VALUES("{}", {}))",
                                          test_ctx.bucket,
                                          id,
                                          async_content_json),
                              [](auto, auto) {
                              });
                 });
      return {};
    },
    [barrier](auto e, auto res) {
      CHECK(e.ec() == couchbase::errc::transaction::failed);
      CHECK_FALSE(res.transaction_id.empty());
      CHECK_FALSE(res.unstaging_complete);
      barrier->set_value();
    },
    async_options());
  f.get();
}
