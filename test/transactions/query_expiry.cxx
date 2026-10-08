/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Client-side expiry in query-mode transactions, ported from FIT query.QueryExpiryTest. Expiry is
// injected through attempt_context_testing_hooks::has_expired_client_side at one stage per case.

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/integration_shortcuts.hxx"
#include "utils/integration_test_guard.hxx"
#include "utils/test_data.hxx"

#include "core/operations.hxx"
#include "core/transactions.hxx"
#include "core/transactions/attempt_context_testing_hooks.hxx"
#include "core/transactions/cleanup_testing_hooks.hxx"
#include "core/transactions/internal/exceptions_internal.hxx"
#include "core/utils/movable_function.hxx"

#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace couchbase::core::transactions;

const std::string select_statement{ "SELECT 'Hello World' AS Greeting" };

auto
initial_content() -> couchbase::codec::encoded_value
{
  return couchbase::codec::default_json_transcoder::encode(tao::json::value{ { "some", "thing" } });
}

using txn_body =
  std::function<void(const std::shared_ptr<attempt_context>&, const couchbase::core::document_id&)>;

// Runs body in a transaction with the hooks set_hooks configures, and returns the failure_type it
// raised. doc_exists seeds the document before the transaction, and the document is then required
// to be unchanged; otherwise it is required to be absent.
auto
run_with_hooks(const std::function<void(attempt_context_testing_hooks&)>& set_hooks,
               bool doc_exists,
               const txn_body& body) -> std::optional<failure_type>
{
  ::test::utils::integration_test_guard integration;
  ::test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  const couchbase::core::document_id id{
    integration.ctx.bucket, "_default", "_default", ::test::utils::uniq_id("txn")
  };
  const auto content = initial_content();
  if (doc_exists) {
    couchbase::core::operations::upsert_request req{ id, content.data };
    req.flags = content.flags;
    assert_success(::test::utils::execute(integration.cluster, req).ctx.ec(), "seed the document");
  }

  auto hooks = std::make_shared<attempt_context_testing_hooks>();
  set_hooks(*hooks);
  couchbase::transactions::transactions_config cfg{};
  cfg.test_factories(hooks, std::make_shared<cleanup_testing_hooks>());
  auto [ec, txns] =
    couchbase::core::transactions::transactions::create(integration.cluster, cfg).get();
  assert_success(ec, "transactions are created");

  std::optional<failure_type> raised{};
  try {
    txns->run([&](const std::shared_ptr<attempt_context>& ctx) {
      body(ctx, id);
    });
  } catch (const transaction_exception& e) {
    raised = e.type();
  }

  auto resp =
    ::test::utils::execute(integration.cluster, couchbase::core::operations::get_request{ id });
  if (doc_exists) {
    assert_success(resp.ctx.ec(), "the document still exists");
    assert_true(resp.value == content.data, "the document is unchanged");
  } else {
    assert_error(resp.ctx.ec(),
                 couchbase::errc::key_value::document_not_found,
                 "the document was not created");
  }
  return raised;
}

// The client-side expiry check fires only at stage.
auto
run_expiring_at(const std::string& stage, bool doc_exists, const txn_body& body)
  -> std::optional<failure_type>
{
  return run_with_hooks(
    [stage](attempt_context_testing_hooks& hooks) {
      hooks.has_expired_client_side = [stage](std::shared_ptr<attempt_context>,
                                              const std::string& place,
                                              std::optional<const std::string>) {
        return place == stage;
      };
    },
    doc_exists,
    body);
}

void
assert_expiry(const std::optional<failure_type>& raised)
{
  assert_true(raised.has_value(), "the transaction failed");
  assert_eq(raised.value(), failure_type::EXPIRY, "the transaction raised EXPIRY");
}

// FIT expiryBefore_rollback: an auto-rollback whose ROLLBACK statement expires raises EXPIRY, not
// the failure that started the rollback.
void
a_query_auto_rollback_that_expires_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_ROLLBACK, false, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->insert(id, initial_content());
    throw transaction_operation_failed(FAIL_OTHER, "force auto-rollback");
  }));
}

// The same for an exception other than transaction_operation_failed, which handle_error rolls back
// from a separate site.
void
a_query_auto_rollback_after_an_application_exception_that_expires_raises_expiry(
  [[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_ROLLBACK, false, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->insert(id, initial_content());
    throw std::runtime_error("force auto-rollback");
  }));
}

// The same for a thrown value not derived from std::exception, which handle_error rolls back from a
// third site.
void
a_query_auto_rollback_after_a_non_standard_exception_that_expires_raises_expiry(
  [[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_ROLLBACK, false, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->insert(id, initial_content());
    throw 42;
  }));
}

// A ROLLBACK that fails with a value not derived from std::exception is caught, and the
// transaction raises the failure that started the rollback.
void
a_query_auto_rollback_failing_with_a_non_standard_exception_raises_the_original_failure(
  [[maybe_unused]] context& ctx)
{
  const auto raised = run_with_hooks(
    [](attempt_context_testing_hooks& hooks) {
      hooks.before_query =
        [](std::shared_ptr<attempt_context>,
           const std::string& statement,
           couchbase::core::utils::movable_function<void(std::optional<error_class>)>&& cb) {
          if (statement == "ROLLBACK") {
            throw 42;
          }
          cb({});
        };
    },
    false,
    [](const auto& txn, const auto& id) {
      txn->query(select_statement);
      txn->insert(id, initial_content());
      throw transaction_operation_failed(FAIL_OTHER, "force auto-rollback");
    });
  assert_true(raised.has_value(), "the transaction failed");
  assert_eq(raised.value(), failure_type::FAIL, "the transaction raised the original FAIL");
}

void
query_expiry_before_begin_work_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_BEGIN_WORK, false, [](const auto& txn, const auto&) {
    txn->query(select_statement);
  }));
}

void
query_expiry_before_a_query_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY, false, [](const auto& txn, const auto& id) {
    txn->insert(id, initial_content());
    txn->query(select_statement);
  }));
}

void
query_expiry_before_get_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_KV_GET, true, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->get(id);
  }));
}

void
query_expiry_before_replace_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_KV_REPLACE, true, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->replace(txn->get(id),
                 couchbase::codec::default_json_transcoder::encode(
                   tao::json::value{ { "some", "thing else" } }));
  }));
}

void
query_expiry_before_remove_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_KV_REMOVE, true, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->remove(txn->get(id));
  }));
}

void
query_expiry_before_insert_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_KV_INSERT, false, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->insert(id, initial_content());
  }));
}

void
query_expiry_before_commit_raises_expiry([[maybe_unused]] context& ctx)
{
  assert_expiry(run_expiring_at(STAGE_QUERY_COMMIT, false, [](const auto& txn, const auto& id) {
    txn->query(select_statement);
    txn->insert(id, initial_content());
  }));
}
} // namespace

auto
tests() -> test_suite
{
  const std::vector<requirement_ptr> query_txn{ needs::real_cluster(),
                                                needs::service("n1ql"),
                                                needs::cluster_version(v7_1) };
  return {
    suite_name,
    {
      { CASE(a_query_auto_rollback_that_expires_raises_expiry), query_txn, timeout::integration },
      { CASE(a_query_auto_rollback_after_an_application_exception_that_expires_raises_expiry),
        query_txn,
        timeout::integration },
      { CASE(a_query_auto_rollback_after_a_non_standard_exception_that_expires_raises_expiry),
        query_txn,
        timeout::integration },
      { CASE(
          a_query_auto_rollback_failing_with_a_non_standard_exception_raises_the_original_failure),
        query_txn,
        timeout::integration },
      { CASE(query_expiry_before_begin_work_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_a_query_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_get_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_replace_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_remove_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_insert_raises_expiry), query_txn, timeout::integration },
      { CASE(query_expiry_before_commit_raises_expiry), query_txn, timeout::integration },
    },
  };
}

} // namespace couchbase::test
