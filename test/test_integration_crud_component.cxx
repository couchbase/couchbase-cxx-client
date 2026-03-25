/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/agent_group.hxx"
#include "core/bucket.hxx"
#include "core/document_id.hxx"
#include "core/impl/subdoc/path_flags.hxx"
#include "core/impl/with_cancellation.hxx"
#include "core/operations/document_exists.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/cmd_lookup_in.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/fail_fast_retry_strategy.hxx>
#include <couchbase/subdoc/mutate_in_macro.hxx>

TEST_CASE("integration: crud component get", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  std::string key = "test-key-get";
  std::string value = R"({"foo":"bar"})";

  auto [err, res] = collection
                      .upsert<couchbase::codec::raw_binary_transcoder>(
                        key, couchbase::core::utils::to_binary(value), {})
                      .get();
  REQUIRE_SUCCESS(err.ec());

  couchbase::core::get_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::get_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->get(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [get_res, get_ec] = f.get();
  REQUIRE_SUCCESS(get_ec);
  REQUIRE(get_res.value == couchbase::core::utils::to_binary(value));
}

TEST_CASE("integration: crud component upsert", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-upsert";
  std::string value = R"({"foo":"bar-upsert"})";

  couchbase::core::upsert_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.value = couchbase::core::utils::to_binary(value);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::upsert_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->upsert(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
}

TEST_CASE("integration: crud component insert", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-insert-" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::string value = R"({"foo":"bar-insert"})";

  couchbase::core::insert_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.value = couchbase::core::utils::to_binary(value);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  {
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::insert_result, std::error_code>>>();
    auto f = barrier->get_future();

    auto op = agent->insert(options, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);

    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    REQUIRE_FALSE(res.cas.empty());
  }

  // Second insert (duplicate)
  {
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::insert_result, std::error_code>>>();
    auto f = barrier->get_future();

    auto op = agent->insert(options, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);

    auto [res, ec] = f.get();
    REQUIRE(ec == couchbase::errc::key_value::document_exists);
  }
}

TEST_CASE("integration: crud component replace", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-replace";
  std::string value1 = R"({"foo":"bar-1"})";
  std::string value2 = R"({"foo":"bar-2"})";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary(value1);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::replace_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.value = couchbase::core::utils::to_binary(value2);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::replace_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->replace(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
}

TEST_CASE("integration: crud component remove", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-remove";

  SECTION("success")
  {
    // Ensure document exists
    {
      couchbase::core::upsert_options options;
      options.key = couchbase::core::utils::to_binary(key);
      options.value = couchbase::core::utils::to_binary("{}");
      options.scope_name = couchbase::scope::default_name;
      options.collection_name = couchbase::collection::default_name;
      auto barrier = std::make_shared<std::promise<std::error_code>>();
      auto op = agent->upsert(options, [barrier](auto, auto ec) {
        barrier->set_value(ec);
      });
      EXPECT_SUCCESS(op);
      auto ec = barrier->get_future().get();
      REQUIRE_SUCCESS(ec);
    }

    couchbase::core::remove_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;

    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::remove_result, std::error_code>>>();
    auto f = barrier->get_future();

    auto op = agent->remove(options, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);

    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
  }

  SECTION("document not found")
  {
    std::string non_existent_key =
      "non-existent-key-" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    couchbase::core::remove_options options;
    options.key = couchbase::core::utils::to_binary(non_existent_key);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;

    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->remove(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE(ec == couchbase::errc::key_value::document_not_found);
  }
}

TEST_CASE("integration: crud component touch", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-touch";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary("{}");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::touch_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.expiry = 10;
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::touch_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->touch(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
}

TEST_CASE("integration: crud component get_and_touch", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-gat";
  std::string value = R"({"foo":"bar"})";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary(value);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::get_and_touch_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.expiry = 10;
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier = std::make_shared<
    std::promise<std::pair<couchbase::core::get_and_touch_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->get_and_touch(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
  REQUIRE(res.value == couchbase::core::utils::to_binary(value));
}

TEST_CASE("integration: crud component get_and_lock", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-get-locked";
  std::string value = R"({"foo":"bar"})";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary(value);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::get_and_lock_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.lock_time = 15;
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier = std::make_shared<
    std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->get_and_lock(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
  REQUIRE(res.value == couchbase::core::utils::to_binary(value));

  // Verify it's locked (replace without CAS should fail with document_locked)
  {
    couchbase::core::replace_options replace_opts;
    replace_opts.key = couchbase::core::utils::to_binary(key);
    replace_opts.value = couchbase::core::utils::to_binary("{}");
    replace_opts.scope_name = couchbase::scope::default_name;
    replace_opts.collection_name = couchbase::collection::default_name;
    // No CAS set - server should reject with document_locked.
    // Use fail-fast to avoid retrying on document_locked.
    replace_opts.retry_strategy = couchbase::make_fail_fast_retry_strategy();
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op_replace = agent->replace(replace_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op_replace);
    auto ec_replace = b->get_future().get();
    // A replace with wrong CAS on a locked document may return:
    // - document_locked: when the server detects the lock before CAS check
    // - document_exists (cas_mismatch): when the server rejects the CAS (wrong CAS on locked doc)
    REQUIRE((ec_replace == couchbase::errc::key_value::document_exists ||
             ec_replace == couchbase::errc::key_value::document_locked ||
             ec_replace == couchbase::errc::common::cas_mismatch));
  }

  // Unlock
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = res.cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op_unlock = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op_unlock);
    auto ec_unlock = b->get_future().get();
    REQUIRE_SUCCESS(ec_unlock);
  }
}

TEST_CASE("integration: crud component increment/decrement", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-counter";

  // Ensure document doesn't exist
  {
    couchbase::core::remove_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    agent->remove(options, [b](auto, auto ec) {
      b->set_value(ec);
    });
    (void)b->get_future().get();
  }

  couchbase::core::counter_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.delta = 10;
  options.initial_value = 100;
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  // Increment (initial)
  {
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::counter_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->increment(options, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    REQUIRE(res.value == 100);
  }

  // Increment again
  {
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::counter_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->increment(options, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    REQUIRE(res.value == 110);
  }

  // Decrement
  {
    couchbase::core::counter_options dec_opts = options;
    dec_opts.delta = 5;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::counter_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->decrement(dec_opts, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    REQUIRE(res.value == 105);
  }
}

TEST_CASE("integration: crud component append/prepend", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-adjoin";

  // Ensure document exists (binary)
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary("foo");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = b->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  // Append
  {
    couchbase::core::adjoin_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary("bar");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::adjoin_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->append(options, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
  }

  // Prepend
  {
    couchbase::core::adjoin_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary("baz");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::adjoin_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->prepend(options, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
  }

  // Verify final value
  {
    couchbase::core::get_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::get_result, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent->get(options, [barrier](auto res, auto ec) {
      barrier->set_value(std::make_pair(std::move(res), ec));
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    REQUIRE(res.value == couchbase::core::utils::to_binary("bazfoobar"));
  }
}

TEST_CASE("integration: crud component get_with_meta", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-meta";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary("{}");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [barrier](auto, auto ec) {
      barrier->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::get_with_meta_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;

  auto barrier = std::make_shared<
    std::promise<std::pair<couchbase::core::get_with_meta_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->get_with_meta(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
  REQUIRE(res.deleted == 0);
}

TEST_CASE("integration: crud component lookup_in", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-lookup-in";
  std::string value = R"({"foo":"bar", "baz": 42})";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary(value);
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = b->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::lookup_in_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;
  options.operations = { { couchbase::core::protocol::subdoc_opcode::get, 0, "foo" },
                         { couchbase::core::protocol::subdoc_opcode::exists, 0, "baz" },
                         { couchbase::core::protocol::subdoc_opcode::get, 0, "non-existent" } };

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::lookup_in_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->lookup_in(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  // lookup_in returns success even if some paths failed (multi_path_failure is mapped to success)
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 3);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("bar")"));
  REQUIRE_SUCCESS(res.results[1].error);
  REQUIRE(res.results[2].error == couchbase::errc::key_value::path_not_found);
}

TEST_CASE("integration: crud component mutate_in", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = "test-key-mutate-in";

  // Ensure document exists
  {
    couchbase::core::upsert_options options;
    options.key = couchbase::core::utils::to_binary(key);
    options.value = couchbase::core::utils::to_binary(R"({"foo":"bar"})");
    options.scope_name = couchbase::scope::default_name;
    options.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(options, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    auto ec = b->get_future().get();
    REQUIRE_SUCCESS(ec);
  }

  couchbase::core::mutate_in_options options;
  options.key = couchbase::core::utils::to_binary(key);
  options.scope_name = couchbase::scope::default_name;
  options.collection_name = couchbase::collection::default_name;
  options.operations = { { couchbase::core::protocol::subdoc_opcode::replace,
                           0,
                           "foo",
                           couchbase::core::utils::to_binary(R"("baz")") },
                         { couchbase::core::protocol::subdoc_opcode::dict_add,
                           0,
                           "new",
                           couchbase::core::utils::to_binary("42") } };

  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::mutate_in_result, std::error_code>>>();
  auto f = barrier->get_future();

  auto op = agent->mutate_in(options, [barrier](auto res, auto ec) {
    barrier->set_value(std::make_pair(std::move(res), ec));
  });
  EXPECT_SUCCESS(op);

  auto [res, ec] = f.get();
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());

  // Verify changes
  {
    couchbase::core::get_options get_opts;
    get_opts.key = couchbase::core::utils::to_binary(key);
    get_opts.scope_name = couchbase::scope::default_name;
    get_opts.collection_name = couchbase::collection::default_name;
    auto b =
      std::make_shared<std::promise<std::pair<couchbase::core::get_result, std::error_code>>>();
    agent->get(get_opts, [b](auto res, auto ec) {
      b->set_value(std::make_pair(std::move(res), ec));
    });
    auto [get_res, get_ec] = b->get_future().get();
    REQUIRE_SUCCESS(get_ec);
    REQUIRE(get_res.value == couchbase::core::utils::to_binary(R"({"foo":"baz","new":42})"));
  }
}

TEST_CASE("integration: crud component lock - getAndLock returns different CAS", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_get_and_lock");
  std::string value = R"({"foo":"bar"})";

  // Insert document and record insert CAS
  couchbase::cas insert_cas{};
  {
    couchbase::core::insert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::insert_result, std::error_code>>>();
    auto op = agent->insert(opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [ins_res, ins_ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ins_ec);
    REQUIRE_FALSE(ins_res.cas.empty());
    insert_cas = ins_res.cas;
  }

  // get_and_lock returns a different (masked) CAS and the correct content
  couchbase::core::get_and_lock_options lock_opts;
  lock_opts.key = couchbase::core::utils::to_binary(key);
  lock_opts.lock_time = 30;
  lock_opts.scope_name = couchbase::scope::default_name;
  lock_opts.collection_name = couchbase::collection::default_name;

  auto barrier = std::make_shared<
    std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
  auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
    barrier->set_value({ std::move(res), ec });
  });
  EXPECT_SUCCESS(op);
  auto [lock_res, lock_ec] = barrier->get_future().get();
  REQUIRE_SUCCESS(lock_ec);
  REQUIRE_FALSE(lock_res.cas.empty());
  REQUIRE(lock_res.cas != insert_cas);
  REQUIRE(lock_res.value == couchbase::core::utils::to_binary(value));
}

TEST_CASE("integration: crud component lock - getAndLockNotFound", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  // get_and_lock on a missing document -> document_not_found
  std::string key = test::utils::uniq_id("lock_not_found");

  couchbase::core::get_and_lock_options lock_opts;
  lock_opts.key = couchbase::core::utils::to_binary(key);
  lock_opts.lock_time = 30;
  lock_opts.scope_name = couchbase::scope::default_name;
  lock_opts.collection_name = couchbase::collection::default_name;

  auto barrier = std::make_shared<
    std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
  auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
    barrier->set_value({ std::move(res), ec });
  });
  EXPECT_SUCCESS(op);
  auto [res, ec] = barrier->get_future().get();
  REQUIRE(ec == couchbase::errc::key_value::document_not_found);
}

TEST_CASE("integration: crud component lock - getAndLockTimeoutHasRetryReasonLocked",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_timeout_retry");
  std::string value = R"({"foo":"bar"})";

  // Create document
  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // First lock
  couchbase::cas locked_cas{};
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    locked_cas = res.cas;
  }

  // Second lock with short timeout - should timeout with key_value_locked retry reason
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    lock_opts.timeout = integration.ctx.use_wan_development_profile
                          ? std::chrono::seconds{ 2 }
                          : std::chrono::milliseconds{ 1000 };
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE(ec == couchbase::errc::common::ambiguous_timeout);
    REQUIRE(res.internal.retry_reasons.count(couchbase::retry_reason::key_value_locked) > 0);
  }

  // Cleanup: unlock
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = locked_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    (void)b->get_future().get(); // ignore result, cleanup
  }
}

TEST_CASE("integration: crud component lock - unlock then mutate without CAS", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_unlock_replace");
  std::string value = R"({"foo":"bar"})";

  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // Lock
  couchbase::cas locked_cas{};
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    locked_cas = res.cas;
  }

  // Unlock with correct CAS
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = locked_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // Replace without CAS should now succeed (doc is unlocked)
  {
    std::string new_value = R"({"foo":false})";
    couchbase::core::replace_options replace_opts;
    replace_opts.key = couchbase::core::utils::to_binary(key);
    replace_opts.value = couchbase::core::utils::to_binary(new_value);
    replace_opts.scope_name = couchbase::scope::default_name;
    replace_opts.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::replace_result, std::error_code>>>();
    auto op = agent->replace(replace_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [replace_res, replace_ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(replace_ec);
    REQUIRE_FALSE(replace_res.cas.empty());
  }
}

TEST_CASE("integration: crud component lock - unlockCasMismatch", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_unlock_cas_mismatch");
  std::string value = R"({"foo":"bar"})";

  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // Lock
  couchbase::cas locked_cas{};
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    locked_cas = res.cas;
  }

  // Unlock with wrong CAS -> cas_mismatch
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = couchbase::cas{ locked_cas.value() + 1 };
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE(b->get_future().get() == couchbase::errc::common::cas_mismatch);
  }

  // Cleanup: unlock with correct CAS
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = locked_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    (void)b->get_future().get();
  }
}

TEST_CASE("integration: crud component lock - upsertLockedDocumentTimeoutHasRetryReasonLocked",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_upsert_timeout");
  std::string value = R"({"foo":"bar"})";

  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // Lock
  couchbase::cas locked_cas{};
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    locked_cas = res.cas;
  }

  // Upsert on locked doc with short timeout should timeout with key_value_locked
  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(R"({})");
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    opts.timeout = integration.ctx.use_wan_development_profile ? std::chrono::seconds{ 2 }
                                                               : std::chrono::milliseconds{ 1000 };
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::upsert_result, std::error_code>>>();
    auto op = agent->upsert(opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE((ec == couchbase::errc::common::ambiguous_timeout ||
             ec == couchbase::errc::common::unambiguous_timeout));
    REQUIRE(res.internal.retry_reasons.count(couchbase::retry_reason::key_value_locked) > 0);
  }

  // Cleanup
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = locked_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    (void)b->get_future().get();
  }
}

TEST_CASE("integration: crud component lock - verifyGetAndLockDoubleLock", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_double_lock");
  std::string value = R"({"foo":"bar"})";

  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->upsert(opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // First lock
  couchbase::cas locked_cas{};
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    locked_cas = res.cas;
  }

  // Second lock - should timeout with key_value_locked
  {
    couchbase::core::get_and_lock_options lock_opts;
    lock_opts.key = couchbase::core::utils::to_binary(key);
    lock_opts.lock_time = 30;
    lock_opts.scope_name = couchbase::scope::default_name;
    lock_opts.collection_name = couchbase::collection::default_name;
    lock_opts.timeout = integration.ctx.use_wan_development_profile
                          ? std::chrono::seconds{ 2 }
                          : std::chrono::milliseconds{ 1000 };
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::get_and_lock_result, std::error_code>>>();
    auto op = agent->get_and_lock(lock_opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE(ec == couchbase::errc::common::ambiguous_timeout);
    REQUIRE(res.internal.retry_reasons.count(couchbase::retry_reason::key_value_locked) > 0);
  }

  // Cleanup
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = locked_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    (void)b->get_future().get();
  }
}

TEST_CASE("integration: crud component lock - verifyUnlockInvalidArgumentExceptions",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  // unlock with CAS=0 is invalid (client-side validated: server returns invalid_argument)
  std::string key = test::utils::uniq_id("lock_invalid_cas");

  couchbase::core::unlock_options unlock_opts;
  unlock_opts.key = couchbase::core::utils::to_binary(key);
  // CAS defaults to empty (zero) - should be rejected as invalid_argument
  unlock_opts.cas = couchbase::cas{ 0 };
  unlock_opts.scope_name = couchbase::scope::default_name;
  unlock_opts.collection_name = couchbase::collection::default_name;
  auto b = std::make_shared<std::promise<std::error_code>>();
  auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
    b->set_value(ec);
  });
  EXPECT_SUCCESS(op);
  REQUIRE(b->get_future().get() == couchbase::errc::common::invalid_argument);
}

TEST_CASE("integration: crud component lock - unlockNotFound", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  // unlock on a non-existent document with a non-zero CAS -> document_not_found
  std::string key = test::utils::uniq_id("lock_unlock_not_found");

  couchbase::core::unlock_options unlock_opts;
  unlock_opts.key = couchbase::core::utils::to_binary(key);
  unlock_opts.cas = couchbase::cas{ 1 };
  unlock_opts.scope_name = couchbase::scope::default_name;
  unlock_opts.collection_name = couchbase::collection::default_name;
  auto b = std::make_shared<std::promise<std::error_code>>();
  auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
    b->set_value(ec);
  });
  EXPECT_SUCCESS(op);
  REQUIRE(b->get_future().get() == couchbase::errc::key_value::document_not_found);
}

TEST_CASE("integration: crud component lock - unlockedDocRaisesNotLockedWhenSupported",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_document_not_locked_status()) {
    SKIP("cluster does not support document_not_locked status (requires >= 7.6.0-1815)");
  }

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("lock_not_locked");
  std::string value = R"({"foo":"bar"})";

  // Insert document
  couchbase::cas upsert_cas{};
  {
    couchbase::core::upsert_options opts;
    opts.key = couchbase::core::utils::to_binary(key);
    opts.value = couchbase::core::utils::to_binary(value);
    opts.scope_name = couchbase::scope::default_name;
    opts.collection_name = couchbase::collection::default_name;
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::core::upsert_result, std::error_code>>>();
    auto op = agent->upsert(opts, [barrier](auto res, auto ec) {
      barrier->set_value({ std::move(res), ec });
    });
    EXPECT_SUCCESS(op);
    auto [res, ec] = barrier->get_future().get();
    REQUIRE_SUCCESS(ec);
    upsert_cas = res.cas;
  }

  // Unlock an unlocked document -> document_not_locked
  {
    couchbase::core::unlock_options unlock_opts;
    unlock_opts.key = couchbase::core::utils::to_binary(key);
    unlock_opts.cas = upsert_cas;
    unlock_opts.scope_name = couchbase::scope::default_name;
    unlock_opts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->unlock(unlock_opts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    EXPECT_SUCCESS(op);
    REQUIRE(b->get_future().get() == couchbase::errc::key_value::document_not_locked);
  }
}

// ---------------------------------------------------------------------------
// Helpers shared by subdoc tests
// ---------------------------------------------------------------------------
namespace
{
// Standard document used across subdoc tests (mirrors Java test setup).
// {
//   "singleString": "initial",
//   "singleInt": 42,
//   "nestedObject": { "content": "initial" },
//   "nestedList": ["initial", "updated", "default"],
//   "quote\"name": { "key": "value" },
//   "`bracket[]name`": { "key": "value" }
// }
const std::string k_subdoc_doc =
  R"({"singleString":"initial","singleInt":42,"nestedObject":{"content":"initial"},"nestedList":["initial","updated","default"],"quote\"name":{"key":"value"},"`bracket[]name`":{"key":"value"}})";

// Upsert a document and return its key.
auto
upsert_doc(couchbase::core::agent* agent, const std::string& key, const std::string& value)
{
  couchbase::core::upsert_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.value = couchbase::core::utils::to_binary(value);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  auto b = std::make_shared<std::promise<std::error_code>>();
  auto op = agent->upsert(opts, [b](auto, auto ec) {
    b->set_value(ec);
  });
  (void)op;
  return b->get_future().get();
}

// Perform a lookup_in and return the result synchronously.
auto
do_lookup_in(couchbase::core::agent* agent, couchbase::core::lookup_in_options opts)
  -> std::pair<couchbase::core::lookup_in_result, std::error_code>
{
  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::lookup_in_result, std::error_code>>>();
  auto op = agent->lookup_in(opts, [barrier](auto res, auto ec) {
    barrier->set_value({ std::move(res), ec });
  });
  (void)op;
  return barrier->get_future().get();
}

// Perform a mutate_in and return the result synchronously.
auto
do_mutate_in(couchbase::core::agent* agent, couchbase::core::mutate_in_options opts)
  -> std::pair<couchbase::core::mutate_in_result, std::error_code>
{
  auto barrier =
    std::make_shared<std::promise<std::pair<couchbase::core::mutate_in_result, std::error_code>>>();
  auto op = agent->mutate_in(opts, [barrier](auto res, auto ec) {
    barrier->set_value({ std::move(res), ec });
  });
  (void)op;
  return barrier->get_future().get();
}

using op = couchbase::core::protocol::subdoc_opcode;
constexpr auto xattr_flag =
  static_cast<std::uint8_t>(couchbase::core::impl::subdoc::path_flag_xattr);
constexpr auto create_path_flag =
  static_cast<std::uint8_t>(couchbase::core::impl::subdoc::path_flag_create_parents);
constexpr auto expand_macro_flag =
  static_cast<std::uint8_t>(couchbase::core::impl::subdoc::path_flag_expand_macros);
} // anonymous namespace

// ---------------------------------------------------------------------------
// lookup_in: get variants
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc lookup_in simpleGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_simple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "singleString" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("initial")"));
}

TEST_CASE("integration: subdoc lookup_in nestedGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_nested");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "nestedObject.content" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("initial")"));
}

TEST_CASE("integration: subdoc lookup_in multipleGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_multiple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "singleString" }, { op::get, 0, "singleInt" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 2);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("initial")"));
  REQUIRE_SUCCESS(res.results[1].error);
  REQUIRE(res.results[1].value == couchbase::core::utils::to_binary("42"));
}

TEST_CASE("integration: subdoc lookup_in getWholeDocumentLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_whole_doc");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // get_doc with empty path returns the entire document body
  opts.operations = { { op::get_doc, 0, "" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  // The returned value should be the full document JSON
  REQUIRE_FALSE(res.results[0].value.empty());
}

TEST_CASE("integration: subdoc lookup_in arrayGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_array");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "nestedList[1]" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("updated")"));
}

TEST_CASE("integration: subdoc lookup_in arrayLastElementGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_array_last");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "nestedList[-1]" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("default")"));
}

TEST_CASE("integration: subdoc lookup_in pathNotFoundGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_path_not_found");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "doesNotExist" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  // Top-level ec should be success (multi_path_failure maps to success)
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_not_found);
}

TEST_CASE("integration: subdoc lookup_in pathInvalidGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_path_invalid");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // Double dot is syntactically invalid
  opts.operations = { { op::get, 0, "nestedObject..content" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_invalid);
}

TEST_CASE("integration: subdoc lookup_in xattrGetLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_xattr");

  // Insert doc and set an xattr value (flat path to avoid create_path requirement)
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));
  {
    couchbase::core::mutate_in_options mopts;
    mopts.key = couchbase::core::utils::to_binary(key);
    mopts.scope_name = couchbase::scope::default_name;
    mopts.collection_name = couchbase::collection::default_name;
    mopts.operations = {
      { op::dict_upsert, xattr_flag, "docId", couchbase::core::utils::to_binary(R"("doc-id-123")") }
    };
    auto [mres, mec] = do_mutate_in(&agent.value(), mopts);
    REQUIRE_SUCCESS(mec);
  }

  // Read the xattr back
  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, xattr_flag, "docId" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary(R"("doc-id-123")"));
}

TEST_CASE("integration: subdoc lookup_in tooManySpecsExistTest", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_get_too_many");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  // 17 specs should be rejected (max is 16)
  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  for (int i = 0; i < 17; ++i) {
    opts.operations.push_back({ op::get, 0, "singleString" });
  }

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE(ec == couchbase::errc::common::invalid_argument);
}

// ---------------------------------------------------------------------------
// lookup_in: exists variants
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc lookup_in simpleExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_simple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "singleString" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
}

TEST_CASE("integration: subdoc lookup_in multipleExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_multiple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "singleString" },
                      { op::exists, 0, "singleInt" },
                      { op::exists, 0, "nestedObject" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 3);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE_SUCCESS(res.results[1].error);
  REQUIRE_SUCCESS(res.results[2].error);
}

TEST_CASE("integration: subdoc lookup_in arrayExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_array");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "nestedList[0]" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
}

TEST_CASE("integration: subdoc lookup_in pathNotFoundExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_not_found");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "doesNotExist" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  // Top-level should be success; per-spec error indicates path_not_found (not-found = false)
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_not_found);
}

TEST_CASE("integration: subdoc lookup_in pathInvalidExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_path_invalid");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "nestedObject..content" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_invalid);
}

TEST_CASE("integration: subdoc lookup_in xattrExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_xattr");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  // Set an xattr (flat path to avoid needing create_path flag)
  {
    couchbase::core::mutate_in_options mopts;
    mopts.key = couchbase::core::utils::to_binary(key);
    mopts.scope_name = couchbase::scope::default_name;
    mopts.collection_name = couchbase::collection::default_name;
    mopts.operations = {
      { op::dict_upsert, xattr_flag, "docId", couchbase::core::utils::to_binary(R"("doc-id-123")") }
    };
    auto [mres, mec] = do_mutate_in(&agent.value(), mopts);
    REQUIRE_SUCCESS(mec);
  }

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, xattr_flag, "docId" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
}

TEST_CASE("integration: subdoc lookup_in accessDeletedExistsLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_exists_access_deleted");

  // Set a system xattr on a soon-to-be-deleted document.
  // System xattrs (path starting with "_") are preserved after document deletion;
  // regular user xattrs are NOT preserved on delete.
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));
  {
    couchbase::core::mutate_in_options mopts;
    mopts.key = couchbase::core::utils::to_binary(key);
    mopts.scope_name = couchbase::scope::default_name;
    mopts.collection_name = couchbase::collection::default_name;
    mopts.operations = {
      { op::dict_upsert, xattr_flag, "_txnId", couchbase::core::utils::to_binary(R"("txn-001")") }
    };
    auto [mres, mec] = do_mutate_in(&agent.value(), mopts);
    REQUIRE_SUCCESS(mec);
  }

  // Delete the document
  {
    couchbase::core::remove_options ropts;
    ropts.key = couchbase::core::utils::to_binary(key);
    ropts.scope_name = couchbase::scope::default_name;
    ropts.collection_name = couchbase::collection::default_name;
    auto b = std::make_shared<std::promise<std::error_code>>();
    auto op = agent->remove(ropts, [b](auto, auto ec) {
      b->set_value(ec);
    });
    (void)op;
    REQUIRE_SUCCESS(b->get_future().get());
  }

  // Access the system xattr of the deleted document using access_deleted flag.
  // System xattrs survive deletion; regular user xattrs do not.
  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.flags = couchbase::core::protocol::lookup_in_request_body::doc_flag_access_deleted;
  opts.operations = { { op::exists, xattr_flag, "_txnId" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
}

// ---------------------------------------------------------------------------
// lookup_in: count variants
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc lookup_in simpleCountLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_count_simple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get_count, 0, "nestedList" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary("3"));
}

TEST_CASE("integration: subdoc lookup_in multipleCountLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_count_multiple");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // Count array elements and object keys
  opts.operations = { { op::get_count, 0, "nestedList" }, { op::get_count, 0, "nestedObject" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 2);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary("3"));
  REQUIRE_SUCCESS(res.results[1].error);
  REQUIRE(res.results[1].value == couchbase::core::utils::to_binary("1"));
}

TEST_CASE("integration: subdoc lookup_in pathNotFoundCountLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_count_not_found");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get_count, 0, "doesNotExist" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_not_found);
}

TEST_CASE("integration: subdoc lookup_in pathMismatchCountLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_count_mismatch");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // count on a string (not array/object) -> path_mismatch
  opts.operations = { { op::get_count, 0, "singleString" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_mismatch);
}

// ---------------------------------------------------------------------------
// lookup_in: mixed
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc lookup_in mixedLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_mixed");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::exists, 0, "singleString" },
                      { op::get_count, 0, "nestedList" },
                      { op::get, 0, "nestedObject.content" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 3);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE_SUCCESS(res.results[1].error);
  REQUIRE(res.results[1].value == couchbase::core::utils::to_binary("3"));
  REQUIRE_SUCCESS(res.results[2].error);
  REQUIRE(res.results[2].value == couchbase::core::utils::to_binary(R"("initial")"));
}

TEST_CASE("integration: subdoc lookup_in exceptionsMixedLookup", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_mixed_errors");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::get, 0, "nestedObject..content" }, // path_invalid
    { op::get_count, 0, "singleString" },    // path_mismatch
    { op::get, 0, "doesNotExist" }           // path_not_found
  };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 3);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_invalid);
  REQUIRE(res.results[1].error == couchbase::errc::key_value::path_mismatch);
  REQUIRE(res.results[2].error == couchbase::errc::key_value::path_not_found);
}

// ---------------------------------------------------------------------------
// mutate_in: xattrOpsAreReordered
// At the crud_component (agent) level, xattr ops must be placed first in the
// operations list (server requirement). This test verifies that mixed xattr +
// non-xattr ops work correctly when the caller puts xattr specs first.
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc mutate_in xattrOpsAreReordered", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_xattr_reorder");

  // Seed the xattr counter field (use create_path | xattr since "x.foo" is nested)
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({})"));
  {
    couchbase::core::mutate_in_options seed;
    seed.key = couchbase::core::utils::to_binary(key);
    seed.scope_name = couchbase::scope::default_name;
    seed.collection_name = couchbase::collection::default_name;
    const auto seed_flag = static_cast<std::uint8_t>(xattr_flag | create_path_flag);
    seed.operations = {
      { op::dict_upsert, seed_flag, "x.foo", couchbase::core::utils::to_binary("0") }
    };
    auto [sr, sec] = do_mutate_in(&agent.value(), seed);
    REQUIRE_SUCCESS(sec);
  }

  // At the crud_component level the caller must place xattr specs first (server requirement).
  // spec[0]: xattr counter increment
  // spec[1]: non-xattr dict_add
  // Verify both specs succeed and the counter result contains the expected value.
  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    // spec[0]: xattr counter (must be first)
    { op::counter, xattr_flag, "x.foo", couchbase::core::utils::to_binary("5") },
    // spec[1]: non-xattr insert
    { op::dict_add, 0, "foo2", couchbase::core::utils::to_binary(R"("bar2")") },
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  // Only ops that produce a value send result entries; dict_add returns nothing,
  // counter returns the new value.  So only one result entry is expected.
  REQUIRE(res.results.size() == 1);

  // result[0] (the xattr counter at spec[0]) should contain the new counter value = 5
  REQUIRE(res.results[0].index == 0);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary("5"));
}

// ---------------------------------------------------------------------------
// mutate_in: basic mutation types
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc mutate_in insertString", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_insert");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::dict_add, 0, "newField", couchbase::core::utils::to_binary(R"("newValue")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
}

TEST_CASE("integration: subdoc mutate_in insertStringAlreadyThere", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_insert_exists");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // "singleString" already exists -> path_exists error
  opts.operations = {
    { op::dict_add, 0, "singleString", couchbase::core::utils::to_binary(R"("other")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_exists);
}

TEST_CASE("integration: subdoc mutate_in remove", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_remove");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::remove, 0, "singleString", {} } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE_FALSE(res.cas.empty());
}

TEST_CASE("integration: subdoc mutate_in replaceString", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_replace");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::replace, 0, "singleString", couchbase::core::utils::to_binary(R"("replaced")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in replaceStringDoesNotExist", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_replace_not_exist");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::replace, 0, "nonExistent", couchbase::core::utils::to_binary(R"("value")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_not_found);
}

TEST_CASE("integration: subdoc mutate_in upsertString", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_upsert");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::dict_upsert, 0, "singleString", couchbase::core::utils::to_binary(R"("upserted")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in upsertStringDoesNotExist", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_upsert_new");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::dict_upsert, 0, "brandNew", couchbase::core::utils::to_binary(R"("hello")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayAppend", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_append");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::array_push_last, 0, "nestedList", couchbase::core::utils::to_binary(R"("appended")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayPrepend", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_prepend");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::array_push_first, 0, "nestedList", couchbase::core::utils::to_binary(R"("prepended")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayInsert", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_insert");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::array_insert, 0, "nestedList[1]", couchbase::core::utils::to_binary(R"("inserted")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayInsertUniqueDoesNotExist", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_unique_new");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::array_add_unique, 0, "nestedList", couchbase::core::utils::to_binary(R"("uniqueNew")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayInsertUniqueDoesExist", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_unique_exists");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // "initial" already exists in nestedList -> path_exists
  opts.operations = {
    { op::array_add_unique, 0, "nestedList", couchbase::core::utils::to_binary(R"("initial")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_exists);
}

TEST_CASE("integration: subdoc mutate_in counterAdd", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_counter_add");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::counter, 0, "singleInt", couchbase::core::utils::to_binary("5") } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary("47"));
}

TEST_CASE("integration: subdoc mutate_in counterMinus", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_counter_minus");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::counter, 0, "singleInt", couchbase::core::utils::to_binary("-3") } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE_SUCCESS(res.results[0].error);
  REQUIRE(res.results[0].value == couchbase::core::utils::to_binary("39"));
}

TEST_CASE("integration: subdoc mutate_in invalidPath", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_invalid_path");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // x[1] is invalid for dict_add (only arrays support [N] as insert target via array_insert)
  opts.operations = {
    { op::dict_add, 0, "x[1]", couchbase::core::utils::to_binary(R"("value")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_invalid);
}

// ---------------------------------------------------------------------------
// mutate_in: xattr variants
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc mutate_in insertXattr", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_insert_xattr");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = {
    { op::dict_add, xattr_flag, "txn.status", couchbase::core::utils::to_binary(R"("pending")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in removeXattr", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_remove_xattr");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  // First insert the xattr
  {
    couchbase::core::mutate_in_options seed;
    seed.key = couchbase::core::utils::to_binary(key);
    seed.scope_name = couchbase::scope::default_name;
    seed.collection_name = couchbase::collection::default_name;
    seed.operations = { { op::dict_upsert,
                          xattr_flag,
                          "txn.status",
                          couchbase::core::utils::to_binary(R"("pending")") } };
    auto [sr, sec] = do_mutate_in(&agent.value(), seed);
    REQUIRE_SUCCESS(sec);
  }

  // Now remove it
  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::remove, xattr_flag, "txn.status", {} } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in upsertStringXattr", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_upsert_xattr");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::dict_upsert,
                        xattr_flag,
                        "txn.status",
                        couchbase::core::utils::to_binary(R"("committed")") } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

// ---------------------------------------------------------------------------
// mutate_in: create_path variants
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc mutate_in insertCreatePath", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_insert_create_path");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({})"));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  // create_path allows creating intermediate path components
  const auto flag = static_cast<std::uint8_t>(create_path_flag);
  opts.operations = {
    { op::dict_upsert, flag, "a.b.c", couchbase::core::utils::to_binary(R"("deep")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in insertXattrCreatePath", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_insert_xattr_create_path");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  const auto flag = static_cast<std::uint8_t>(xattr_flag | create_path_flag);
  opts.operations = {
    { op::dict_upsert, flag, "txn.nested.field", couchbase::core::utils::to_binary(R"("value")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

TEST_CASE("integration: subdoc mutate_in arrayAppendCreatePath", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_array_append_create_path");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({})"));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  const auto flag = static_cast<std::uint8_t>(create_path_flag);
  opts.operations = {
    { op::array_push_last, flag, "myArray", couchbase::core::utils::to_binary(R"("element")") }
  };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
}

// ---------------------------------------------------------------------------
// mutate_in: macro expansion
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc mutate_in insertExpandMacroCASXattr", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_macro_cas");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  // Insert xattr field using the CAS macro: value will be expanded to the document CAS
  // create_path is needed because "meta.cas" is a nested xattr path
  const auto flag = static_cast<std::uint8_t>(xattr_flag | expand_macro_flag | create_path_flag);
  auto macro_value = couchbase::subdoc::to_binary(couchbase::subdoc::mutate_in_macro::cas);

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::dict_upsert, flag, "meta.cas", macro_value } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);

  // Verify the xattr was expanded (not the literal macro string)
  couchbase::core::lookup_in_options lopts;
  lopts.key = couchbase::core::utils::to_binary(key);
  lopts.scope_name = couchbase::scope::default_name;
  lopts.collection_name = couchbase::collection::default_name;
  lopts.operations = { { op::get, xattr_flag, "meta.cas" } };

  auto [lres, lec] = do_lookup_in(&agent.value(), lopts);
  REQUIRE_SUCCESS(lec);
  REQUIRE(lres.results.size() == 1);
  REQUIRE_SUCCESS(lres.results[0].error);
  // Expanded CAS should NOT equal the literal macro string
  REQUIRE(lres.results[0].value != couchbase::core::utils::to_binary(R"("${Mutation.CAS}")"));
}

TEST_CASE("integration: subdoc mutate_in insertExpandMacroCRC32Xattr", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_macro_crc32");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, R"({"body":"value"})"));

  const auto flag = static_cast<std::uint8_t>(xattr_flag | expand_macro_flag | create_path_flag);
  auto macro_value = couchbase::subdoc::to_binary(couchbase::subdoc::mutate_in_macro::value_crc32c);

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::dict_upsert, flag, "meta.crc", macro_value } };

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);

  // Verify the xattr was expanded
  couchbase::core::lookup_in_options lopts;
  lopts.key = couchbase::core::utils::to_binary(key);
  lopts.scope_name = couchbase::scope::default_name;
  lopts.collection_name = couchbase::collection::default_name;
  lopts.operations = { { op::get, xattr_flag, "meta.crc" } };

  auto [lres, lec] = do_lookup_in(&agent.value(), lopts);
  REQUIRE_SUCCESS(lec);
  REQUIRE(lres.results.size() == 1);
  REQUIRE_SUCCESS(lres.results[0].error);
  REQUIRE(lres.results[0].value !=
          couchbase::core::utils::to_binary(R"("${Mutation.value_crc32c}")"));
}

// ---------------------------------------------------------------------------
// subdoc server error cases
// ---------------------------------------------------------------------------

TEST_CASE("integration: subdoc server errors lookupInTooManyCommands", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_server_err_too_many_lookup");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  for (int i = 0; i < 17; ++i) {
    opts.operations.push_back({ op::get, 0, "singleString" });
  }

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE(ec == couchbase::errc::common::invalid_argument);
}

TEST_CASE("integration: subdoc server errors mutateInTooManyCommands", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_server_err_too_many_mutate");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::mutate_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  for (int i = 0; i < 17; ++i) {
    opts.operations.push_back({ op::dict_upsert,
                                0,
                                "field" + std::to_string(i),
                                couchbase::core::utils::to_binary(std::to_string(i)) });
  }

  auto [res, ec] = do_mutate_in(&agent.value(), opts);
  REQUIRE(ec == couchbase::errc::common::invalid_argument);
}

TEST_CASE("integration: subdoc server errors lookupInBadPath", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  std::string key = test::utils::uniq_id("subdoc_server_err_bad_path");
  REQUIRE_SUCCESS(upsert_doc(&agent.value(), key, k_subdoc_doc));

  couchbase::core::lookup_in_options opts;
  opts.key = couchbase::core::utils::to_binary(key);
  opts.scope_name = couchbase::scope::default_name;
  opts.collection_name = couchbase::collection::default_name;
  opts.operations = { { op::get, 0, "bad..path" } };

  auto [res, ec] = do_lookup_in(&agent.value(), opts);
  REQUIRE_SUCCESS(ec);
  REQUIRE(res.results.size() == 1);
  REQUIRE(res.results[0].error == couchbase::errc::key_value::path_invalid);
}

// ---------------------------------------------------------------------------
// Regression tests: direct_execute on closed bucket must call handler exactly
// once (previously a use-after-move bug caused the handler to be called twice
// or invoked in a moved-from state when direct_dispatch() fails synchronously).
// ---------------------------------------------------------------------------

namespace
{
/// Opens integration.ctx.bucket, retrieves its shared_ptr, closes it, and
/// returns the closed bucket.  The bucket stays alive via the returned ptr even
/// after close().
auto
make_closed_bucket(test::utils::integration_test_guard& integration)
  -> std::shared_ptr<couchbase::core::bucket>
{
  auto open_barrier = std::make_shared<std::promise<std::error_code>>();
  integration.cluster.open_bucket(integration.ctx.bucket, [open_barrier](std::error_code ec) {
    open_barrier->set_value(ec);
  });
  auto open_ec = open_barrier->get_future().get();
  if (open_ec) {
    return nullptr;
  }
  auto bkt = integration.cluster.find_bucket_by_name(integration.ctx.bucket);
  if (bkt) {
    bkt->close();
  }
  return bkt;
}

/// Builds a minimal document_id for the closed-bucket tests.
auto
test_doc_id(const std::string& bucket, const std::string& key) -> couchbase::core::document_id
{
  return couchbase::core::document_id{
    bucket,
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    key,
  };
}
} // namespace

TEST_CASE("integration: direct_execute on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  // Open the bucket so it is registered in the cluster's bucket map
  auto open_barrier = std::make_shared<std::promise<std::error_code>>();
  integration.cluster.open_bucket(integration.ctx.bucket, [open_barrier](std::error_code ec) {
    open_barrier->set_value(ec);
  });
  REQUIRE_SUCCESS(open_barrier->get_future().get());

  // Retrieve a shared_ptr<bucket> *before* closing it so we can call direct_execute on it
  auto bkt = integration.cluster.find_bucket_by_name(integration.ctx.bucket);
  REQUIRE(bkt != nullptr);

  // Close the bucket directly (without removing it from the cluster map).
  // This sets the internal closed_ flag so that bucket_impl::direct_dispatch()
  // returns errc::network::bucket_closed synchronously.
  bkt->close();

  // Build a get_request targeting the default collection.
  couchbase::core::operations::get_request req{};
  req.id = couchbase::core::document_id{
    integration.ctx.bucket,
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    "test-direct-execute-closed",
  };

  // Call direct_execute directly — this bypasses bucket::execute()'s is_closed() guard and
  // goes straight to bucket_impl::direct_dispatch() which sees closed_==true.
  // Before the bug fix this would invoke a moved-from handler (use-after-move).
  // The test verifies the handler is called exactly once with the expected error code.
  std::atomic<int> handler_call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&handler_call_count, barrier](couchbase::core::operations::get_response resp) mutable {
      handler_call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  auto observed_ec = barrier->get_future().get();
  REQUIRE(handler_call_count.load() == 1);
  REQUIRE(observed_ec == couchbase::errc::network::bucket_closed);
}

TEST_CASE("integration: direct_execute(upsert) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::upsert_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-upsert-closed");
  req.value = couchbase::core::utils::to_binary(R"({"x":1})");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::upsert_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(insert) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::insert_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-insert-closed");
  req.value = couchbase::core::utils::to_binary(R"({"x":1})");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::insert_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(replace) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::replace_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-replace-closed");
  req.value = couchbase::core::utils::to_binary(R"({"x":1})");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::replace_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(remove) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::remove_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-remove-closed");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::remove_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(exists) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::exists_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-exists-closed");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::exists_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(lookup_in) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::lookup_in_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-lookup-in-closed");
  req.specs.push_back({ couchbase::core::impl::subdoc::opcode::get, "x", {}, std::byte{ 0 } });

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::lookup_in_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE("integration: direct_execute(mutate_in) on closed bucket calls handler exactly once",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::operations::mutate_in_request req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-mutate-in-closed");
  req.specs.push_back({ couchbase::core::impl::subdoc::opcode::dict_upsert,
                        "x",
                        couchbase::core::utils::to_binary("1"),
                        std::byte{ 0 } });

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::mutate_in_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE(
  "integration: direct_execute(with_cancellation<get>) on closed bucket calls handler exactly once",
  "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::impl::with_cancellation<couchbase::core::operations::get_request> req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-get-cancel-closed");

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req), [&call_count, barrier](couchbase::core::operations::get_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}

TEST_CASE(
  "integration: direct_execute(with_cancellation<lookup_in>) on closed bucket calls handler "
  "exactly once",
  "[integration]")
{
  test::utils::integration_test_guard integration;
  auto bkt = make_closed_bucket(integration);
  REQUIRE(bkt != nullptr);

  couchbase::core::impl::with_cancellation<couchbase::core::operations::lookup_in_request> req{};
  req.id = test_doc_id(integration.ctx.bucket, "test-direct-execute-lookup-in-cancel-closed");
  req.specs.push_back({ couchbase::core::impl::subdoc::opcode::get, "x", {}, std::byte{ 0 } });

  std::atomic<int> call_count{ 0 };
  auto barrier = std::make_shared<std::promise<std::error_code>>();
  bkt->direct_execute(
    std::move(req),
    [&call_count, barrier](couchbase::core::operations::lookup_in_response resp) mutable {
      call_count.fetch_add(1);
      barrier->set_value(resp.ctx.ec());
    });

  REQUIRE(barrier->get_future().get() == couchbase::errc::network::bucket_closed);
  REQUIRE(call_count.load() == 1);
}
