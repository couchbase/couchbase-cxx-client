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

#include <couchbase/cluster.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/management/scope_spec.hxx>

#include <algorithm>
#include <atomic>
#include <vector>

namespace
{

auto
scope_exists_in_list(const std::vector<couchbase::management::bucket::scope_spec>& scopes,
                     const std::string& scope_name) -> bool
{
  return std::any_of(scopes.begin(), scopes.end(), [&scope_name](const auto& scope) -> bool {
    return scope.name == scope_name;
  });
}

auto
collection_exists_in_list(const std::vector<couchbase::management::bucket::scope_spec>& scopes,
                          const std::string& scope_name,
                          const std::string& collection_name) -> bool
{
  return std::any_of(scopes.begin(), scopes.end(), [&](const auto& scope) -> bool {
    if (scope.name != scope_name) {
      return false;
    }
    return std::any_of(scope.collections.begin(),
                       scope.collections.end(),
                       [&collection_name](const auto& coll) -> bool {
                         return coll.name == collection_name;
                       });
  });
}

} // namespace

TEST_CASE("integration: collection manager component create and drop scope", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  auto c = integration.public_cluster();
  auto manager = c.bucket(integration.ctx.bucket).collections();

  const auto scope_name = test::utils::uniq_id("scope");

  // create_scope succeeds
  {
    auto error = manager.create_scope(scope_name).get();
    REQUIRE_SUCCESS(error.ec());
  }

  // wait until scope appears
  {
    auto appeared = test::utils::wait_until([&]() -> bool {
      auto [err, scopes] = manager.get_all_scopes().get();
      return !err.ec() && scope_exists_in_list(scopes, scope_name);
    });
    REQUIRE(appeared);
  }

  // create_scope again → scope_exists
  {
    auto error = manager.create_scope(scope_name).get();
    REQUIRE(error.ec() == couchbase::errc::management::scope_exists);
  }

  // drop_scope succeeds
  {
    auto error = manager.drop_scope(scope_name).get();
    REQUIRE_SUCCESS(error.ec());
  }

  // wait until scope disappears, then drop_scope → scope_not_found
  {
    auto gone = test::utils::wait_until([&]() -> bool {
      auto error = manager.drop_scope(scope_name).get();
      return error.ec() == couchbase::errc::common::scope_not_found;
    });
    REQUIRE(gone);
  }
}

TEST_CASE("integration: collection manager component create and drop collection", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  auto c = integration.public_cluster();
  auto manager = c.bucket(integration.ctx.bucket).collections();

  const auto scope_name = test::utils::uniq_id("scope");
  const auto collection_name = test::utils::uniq_id("coll");

  // create the parent scope first
  {
    auto error = manager.create_scope(scope_name).get();
    REQUIRE_SUCCESS(error.ec());
    auto appeared = test::utils::wait_until([&]() -> bool {
      auto [err, scopes] = manager.get_all_scopes().get();
      return !err.ec() && scope_exists_in_list(scopes, scope_name);
    });
    REQUIRE(appeared);
  }

  // create_collection succeeds
  {
    couchbase::create_collection_settings settings{};
    auto error = manager.create_collection(scope_name, collection_name, settings).get();
    REQUIRE_SUCCESS(error.ec());
  }

  // wait until collection appears in get_all_scopes
  {
    auto appeared = test::utils::wait_until([&]() -> bool {
      auto [err, scopes] = manager.get_all_scopes().get();
      return !err.ec() && collection_exists_in_list(scopes, scope_name, collection_name);
    });
    REQUIRE(appeared);
  }

  // create_collection again → collection_exists
  {
    couchbase::create_collection_settings settings{};
    auto error = manager.create_collection(scope_name, collection_name, settings).get();
    REQUIRE(error.ec() == couchbase::errc::management::collection_exists);
  }

  // drop_collection succeeds
  {
    auto error = manager.drop_collection(scope_name, collection_name).get();
    REQUIRE_SUCCESS(error.ec());
  }

  // wait until collection is gone, then drop_collection → collection_not_found
  {
    auto gone = test::utils::wait_until([&]() -> bool {
      auto error = manager.drop_collection(scope_name, collection_name).get();
      return error.ec() == couchbase::errc::common::collection_not_found;
    });
    REQUIRE(gone);
  }

  // drop parent scope
  {
    auto error = manager.drop_scope(scope_name).get();
    REQUIRE_SUCCESS(error.ec());
  }
}

TEST_CASE("integration: collection manager component get_all_scopes", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  auto c = integration.public_cluster();
  auto manager = c.bucket(integration.ctx.bucket).collections();

  // get_all_scopes always includes _default scope
  auto [error, scopes] = manager.get_all_scopes().get();
  REQUIRE_SUCCESS(error.ec());
  REQUIRE_FALSE(scopes.empty());
  REQUIRE(scope_exists_in_list(scopes, "_default"));
}

TEST_CASE("integration: collection manager component update_collection", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }
  if (!integration.cluster_version().is_enterprise()) {
    SKIP("update_collection (max_expiry) requires enterprise edition");
  }
  if (!integration.cluster_version().supports_collection_update_max_expiry()) {
    SKIP("cluster does not support update_collection");
  }

  auto c = integration.public_cluster();
  auto manager = c.bucket(integration.ctx.bucket).collections();

  const auto* scope_name = "_default";
  const auto collection_name = test::utils::uniq_id("coll");

  // create a collection to update
  {
    couchbase::create_collection_settings settings{};
    auto error = manager.create_collection(scope_name, collection_name, settings).get();
    REQUIRE_SUCCESS(error.ec());
    auto appeared = test::utils::wait_until([&]() -> bool {
      auto [err, scopes] = manager.get_all_scopes().get();
      return !err.ec() && collection_exists_in_list(scopes, scope_name, collection_name);
    });
    REQUIRE(appeared);
  }

  // update_collection: set max_expiry = 3600
  {
    couchbase::update_collection_settings settings{ 3600 };
    auto error = manager.update_collection(scope_name, collection_name, settings).get();
    REQUIRE_SUCCESS(error.ec());
  }

  // verify the update was applied via get_all_scopes
  {
    auto verified = test::utils::wait_until([&]() -> bool {
      auto [err, scopes] = manager.get_all_scopes().get();
      if (err.ec()) {
        return false;
      }
      return std::any_of(scopes.begin(), scopes.end(), [&](const auto& scope) -> bool {
        if (scope.name != std::string_view(scope_name)) {
          return false;
        }
        return std::any_of(scope.collections.begin(),
                           scope.collections.end(),
                           [&collection_name](const auto& coll) -> bool {
                             return coll.name == collection_name && coll.max_expiry == 3600;
                           });
      });
    });
    REQUIRE(verified);
  }

  // cleanup
  {
    auto error = manager.drop_collection(scope_name, collection_name).get();
    REQUIRE_SUCCESS(error.ec());
  }
}

TEST_CASE("integration: collection manager component handler called exactly once on closed cluster",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  auto c = integration.public_cluster();
  auto manager = c.bucket(integration.ctx.bucket).collections();

  // Extract a core::cluster handle (shares the same cluster_impl) and close it.
  // This marks stopped_=true so that http_session_manager() returns cluster_closed,
  // while keeping the io_context alive (the public cluster `c` is still in scope,
  // so its cluster_impl — and its io_ — remain valid for the duration of this test).
  // do_http_request_buffered() will then return tl::unexpected(cluster_closed)
  // synchronously without consuming the callback, so execute_http must invoke
  // the handler exactly once.
  auto core_c = couchbase::core::get_core_cluster(c);
  test::utils::close_cluster(core_c);

  // get_all_scopes — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.get_all_scopes(
      {},
      [barrier, &call_count](
        couchbase::error err,
        const std::vector<couchbase::management::bucket::scope_spec>& /*result*/) mutable -> void {
        ++call_count;
        barrier->set_value(std::move(err));
      });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }

  // create_scope — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.create_scope(
      "test-scope", {}, [barrier, &call_count](couchbase::error err) mutable -> void {
        ++call_count;
        barrier->set_value(std::move(err));
      });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }

  // drop_scope — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.drop_scope(
      "test-scope", {}, [barrier, &call_count](couchbase::error err) mutable -> void {
        ++call_count;
        barrier->set_value(std::move(err));
      });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }

  // create_collection — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.create_collection("_default",
                              "test-coll",
                              {},
                              {},
                              [barrier, &call_count](couchbase::error err) mutable -> void {
                                ++call_count;
                                barrier->set_value(std::move(err));
                              });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }

  // drop_collection — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.drop_collection(
      "_default", "test-coll", {}, [barrier, &call_count](couchbase::error err) mutable -> void {
        ++call_count;
        barrier->set_value(std::move(err));
      });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }

  // update_collection — handler must be called exactly once
  {
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    std::atomic<int> call_count{ 0 };
    manager.update_collection("_default",
                              "test-coll",
                              {},
                              {},
                              [barrier, &call_count](couchbase::error err) mutable -> void {
                                ++call_count;
                                barrier->set_value(std::move(err));
                              });
    REQUIRE(barrier->get_future().get().ec() == couchbase::errc::network::cluster_closed);
    REQUIRE(call_count == 1);
  }
}
