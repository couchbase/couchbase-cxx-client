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

#include "couchbase/error_codes.hxx"
#include "test_helper_integration.hxx"
#include "utils/integration_shortcuts.hxx"
#include "utils/move_only_context.hxx"

#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_get_all_replicas.hxx"
#include "core/operations/document_get_any_replica.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_lookup_in_all_replicas.hxx"
#include "core/operations/document_lookup_in_any_replica.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/cluster.hxx>

#include <couchbase/certificate_authenticator.hxx>
#include <couchbase/get_all_replicas_options.hxx>
#include <couchbase/get_any_replica_options.hxx>
#include <couchbase/lookup_in_all_replicas_options.hxx>
#include <couchbase/lookup_in_any_replica_options.hxx>
#include <couchbase/password_authenticator.hxx>
#include <couchbase/read_preference.hxx>

#include <tao/json/value.hpp>

static const tao::json::value basic_doc = {
  { "a", 1.0 },
  { "b", 2.0 },
};
static const std::vector<std::byte> basic_doc_json =
  couchbase::core::utils::json::generate_binary(basic_doc);

//! [smuggling-transcoder]
struct smuggling_transcoder {
  using document_type = std::pair<std::vector<std::byte>, std::uint32_t>;

  static auto decode(const couchbase::codec::encoded_value& encoded) -> document_type
  {
    return { encoded.data, encoded.flags };
  }
};

template<>
struct couchbase::codec::is_transcoder<smuggling_transcoder> : public std::true_type {
};
//! [smuggling-transcoder]

TEST_CASE("unit: get any replica result with custom coder", "[integration]")
{
  couchbase::get_replica_result result{
    couchbase::cas{ 0 },
    true,
    { { std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xaf } }, 0xcafebebe },
  };

  // clang-format off
    //! [smuggling-transcoder-usage]
    auto [data, flags] = result.content_as<smuggling_transcoder>();
    //! [smuggling-transcoder-usage]
  // clang-format on

  REQUIRE(flags == 0xcafebebe);
  REQUIRE(data == std::vector{
                    std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xaf } });
}

TEST_CASE("integration: get any replica", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.number_of_replicas() == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     integration.number_of_replicas()));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  std::string scope_name{ "_default" };
  std::string collection_name{ "_default" };
  std::string key = test::utils::uniq_id("get_any_replica");

  {
    couchbase::core::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };

    couchbase::core::operations::insert_request req{ id, basic_doc_json };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto collection =
      cluster.bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
    auto [err, result] = collection.get_any_replica(key, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.content_as<smuggling_transcoder>().first == basic_doc_json);
  }
}

TEST_CASE("integration: get all replicas", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  std::string scope_name{ "_default" };
  std::string collection_name{ "_default" };
  std::string key = test::utils::uniq_id("get_all_replica");

  {
    couchbase::core::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };

    couchbase::core::operations::insert_request req{ id, basic_doc_json };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  if (integration.cluster_version().is_mock()) {
    // GOCAVES does not implement syncDurability. See
    // https://github.com/couchbaselabs/gocaves/issues/109
    std::this_thread::sleep_for(std::chrono::seconds{ 1 });
  }

  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto collection =
      cluster.bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
    auto [err, result] = collection.get_all_replicas(key, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.size() == number_of_replicas + 1);
    auto responses_from_active = std::count_if(result.begin(), result.end(), [](const auto& r) {
      return !r.is_replica();
    });
    REQUIRE(responses_from_active == 1);
  }
}

TEST_CASE("integration: get all replicas with missing key", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.number_of_replicas() == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     integration.number_of_replicas()));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    const std::string scope_name{ "_default" };
    const std::string collection_name{ "_default" };
    auto collection =
      cluster.bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);

    const std::string key = test::utils::uniq_id("get_all_replica_missing_key");
    auto [err, result] = collection.get_all_replicas(key, {}).get();
    REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);
    REQUIRE(result.empty());
  }
}

TEST_CASE("integration: get any replica with missing key", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     integration.number_of_replicas()));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  std::string scope_name{ "_default" };
  std::string collection_name{ "_default" };
  std::string key = test::utils::uniq_id("get_any_replica_missing_key");

  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto collection =
      cluster.bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
    auto [err, result] = collection.get_any_replica(key, {}).get();
    REQUIRE(err.ec() == couchbase::errc::key_value::document_irretrievable);
  }
}

TEST_CASE("integration: get any replica low-level version", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.number_of_replicas() == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     integration.number_of_replicas()));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  couchbase::core::document_id id{
    integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo")
  };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::json::generate_binary(value)
    };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  {
    couchbase::core::operations::get_any_replica_request req{ id };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(!resp.cas.empty());
    REQUIRE(resp.value == couchbase::core::utils::to_binary(R"({"a":1.0,"b":2.0})"));
  }
}

TEST_CASE("integration: get all replicas low-level version", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  couchbase::core::document_id id{
    integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo")
  };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::json::generate_binary(value)
    };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  if (integration.cluster_version().is_mock()) {
    // GOCAVES does not implement syncDurability. See
    // https://github.com/couchbaselabs/gocaves/issues/109
    std::this_thread::sleep_for(std::chrono::seconds{ 1 });
  }

  {
    couchbase::core::operations::get_all_replicas_request req{ id };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() == number_of_replicas + 1);
    auto responses_from_active =
      std::count_if(resp.entries.begin(), resp.entries.end(), [](const auto& r) {
        return !r.replica;
      });
    REQUIRE(responses_from_active == 1);
  }
}

TEST_CASE("integration: low-level zone-aware read replicas on balanced cluster", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().is_mock()) {
    SKIP("GOCAVES does not support server groups");
  }

  const auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  const auto server_groups = integration.server_groups();
  if (server_groups.size() != 2) {
    SKIP(fmt::format("This test expects exactly 2 server groups and at least one replica, "
                     "but found {} server groups",
                     integration.server_groups().size()));
  }

  asio::io_context io{};
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  auto connection_string =
    couchbase::core::utils::parse_connection_string(integration.ctx.connection_string);
  connection_string.options.server_group = server_groups.front();

  auto origin = couchbase::core::origin(integration.ctx.build_auth(), connection_string);
  test::utils::open_cluster(cluster, origin);
  test::utils::open_bucket(cluster, integration.ctx.bucket);

  couchbase::core::document_id id{
    integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo")
  };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::json::generate_binary(value)
    };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  {
    couchbase::core::operations::get_all_replicas_request req{
      id, {}, couchbase::read_preference::no_preference
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() == number_of_replicas + 1);
  }

  {
    couchbase::core::operations::get_all_replicas_request req{
      id, {}, couchbase::read_preference::selected_server_group
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() <= number_of_replicas + 1);
    REQUIRE(resp.entries.size() > 0);
  }

  {
    couchbase::core::operations::get_any_replica_request req{
      id, {}, couchbase::read_preference::no_preference
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.value.empty());
  }

  {
    couchbase::core::operations::get_any_replica_request req{
      id, {}, couchbase::read_preference::selected_server_group
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.value.empty());
  }

  {
    couchbase::core::operations::lookup_in_any_replica_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::no_preference,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.fields.empty());
  }

  {
    couchbase::core::operations::lookup_in_any_replica_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::selected_server_group,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.fields.empty());
  }

  {
    couchbase::core::operations::lookup_in_all_replicas_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::no_preference,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() <= number_of_replicas + 1);
    REQUIRE(resp.entries.size() > 0);
  }

  {
    couchbase::core::operations::lookup_in_all_replicas_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::selected_server_group,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() <= number_of_replicas + 1);
    REQUIRE(resp.entries.size() > 0);
  }

  test::utils::close_cluster(cluster);
  io_thread.join();
}

TEST_CASE("integration: low-level zone-aware read replicas on unbalanced cluster", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().is_mock()) {
    SKIP("GOCAVES does not support server groups");
  }

  const auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  const auto server_groups = integration.server_groups();
  if (server_groups.size() < 3 || number_of_replicas > 1) {
    SKIP(fmt::format(
      "{} server groups and {} replicas does not meet expected requirements of unbalanced cluster. "
      "The number of replicas + 1 has to be less than number of the groups",
      integration.server_groups().size(),
      number_of_replicas));
  }

  // Now we need to craft key, for which both active and replica vbuckets
  // are not bound to selected server group.
  const auto& selected_server_group = server_groups.front();
  const auto selected_key = integration.generate_key_not_in_server_group(selected_server_group);
  INFO(fmt::format("server group: \"{}\"\nkey: \"{}\"", selected_server_group, selected_key));

  asio::io_context io{};
  couchbase::core::cluster cluster(io);
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  auto connection_string =
    couchbase::core::utils::parse_connection_string(integration.ctx.connection_string);
  connection_string.options.server_group = selected_server_group;

  auto origin = couchbase::core::origin(integration.ctx.build_auth(), connection_string);
  test::utils::open_cluster(cluster, origin);
  test::utils::open_bucket(cluster, integration.ctx.bucket);

  couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", selected_key };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::json::generate_binary(value)
    };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  {
    couchbase::core::operations::get_all_replicas_request req{
      id, {}, couchbase::read_preference::no_preference
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() == number_of_replicas + 1);
  }

  {
    couchbase::core::operations::get_all_replicas_request req{
      id, {}, couchbase::read_preference::selected_server_group
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  {
    couchbase::core::operations::get_any_replica_request req{
      id, {}, couchbase::read_preference::no_preference
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.value.empty());
  }

  {
    couchbase::core::operations::get_any_replica_request req{
      id, {}, couchbase::read_preference::selected_server_group
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  {
    couchbase::core::operations::lookup_in_any_replica_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::no_preference,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.fields.empty());
  }

  {
    couchbase::core::operations::lookup_in_any_replica_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::selected_server_group,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  {
    couchbase::core::operations::lookup_in_all_replicas_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::no_preference,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE(resp.entries.size() == number_of_replicas + 1);
  }

  {
    couchbase::core::operations::lookup_in_all_replicas_request req{
      id,
      couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs(),
      {},
      {},
      couchbase::read_preference::selected_server_group,
    };
    auto resp = test::utils::execute(cluster, req);
    REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  test::utils::close_cluster(cluster);
  io_thread.join();
}

TEST_CASE("integration: zone-aware read replicas on balanced cluster", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().is_mock()) {
    SKIP("GOCAVES does not support server groups");
  }

  const auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  const auto server_groups = integration.server_groups();
  if (server_groups.size() != 2) {
    SKIP(fmt::format("This test expects exactly 2 server groups and at least one replica, "
                     "but found {} server groups",
                     integration.server_groups().size()));
  }

  couchbase::core::document_id id{
    integration.ctx.bucket,
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    test::utils::uniq_id("foo"),
  };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::json::generate_binary(value)
    };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  auto connection_string =
    couchbase::core::utils::parse_connection_string(integration.ctx.connection_string);
  // cppcheck-suppress unreadVariable
  connection_string.options.server_group = server_groups.front();

  auto cluster_options = integration.ctx.certificate_path.empty()
                           ? couchbase::cluster_options(couchbase::password_authenticator(
                               integration.ctx.username, integration.ctx.password))
                           : couchbase::cluster_options(couchbase::certificate_authenticator(
                               integration.ctx.certificate_path, integration.ctx.certificate_path));
  cluster_options.network().preferred_server_group(server_groups.front());
  auto [e, c] =
    couchbase::cluster::connect(integration.ctx.connection_string, cluster_options).get();
  REQUIRE_SUCCESS(e.ec());

  auto collection = c.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
  {
    auto [err, result] = collection.get_any_replica(id.key(), {}).get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] = collection
                           .get_any_replica(id.key(),
                                            couchbase::get_any_replica_options{}.read_preference(
                                              couchbase::read_preference::selected_server_group))
                           .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] = collection.get_all_replicas(id.key(), {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.size() == number_of_replicas + 1);
  }
  {
    auto [err, result] = collection
                           .get_all_replicas(id.key(),
                                             couchbase::get_all_replicas_options{}.read_preference(
                                               couchbase::read_preference::selected_server_group))
                           .get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.size() <= number_of_replicas + 1);
  }

  {
    auto [err, result] = collection
                           .lookup_in_any_replica(id.key(),
                                                  couchbase::lookup_in_specs{
                                                    couchbase::lookup_in_specs::get("a"),
                                                  },
                                                  {})
                           .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] =
      collection
        .lookup_in_any_replica(id.key(),
                               couchbase::lookup_in_specs{
                                 couchbase::lookup_in_specs::get("a"),
                               },
                               couchbase::lookup_in_any_replica_options{}.read_preference(
                                 couchbase::read_preference::selected_server_group))
        .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] = collection
                           .lookup_in_all_replicas(id.key(),
                                                   couchbase::lookup_in_specs{
                                                     couchbase::lookup_in_specs::get("a"),
                                                   })
                           .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] =
      collection
        .lookup_in_all_replicas(id.key(),
                                couchbase::lookup_in_specs{
                                  couchbase::lookup_in_specs::get("a"),
                                },
                                couchbase::lookup_in_all_replicas_options{}.read_preference(
                                  couchbase::read_preference::selected_server_group))
        .get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.size() <= number_of_replicas + 1);
  }

  c.close().get();
}

TEST_CASE("integration: zone-aware read replicas on unbalanced cluster", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().is_mock()) {
    SKIP("GOCAVES does not support server groups");
  }

  const auto number_of_replicas = integration.number_of_replicas();
  if (number_of_replicas == 0) {
    SKIP("bucket has zero replicas");
  }
  if (integration.number_of_nodes() <= number_of_replicas) {
    SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                     integration.number_of_nodes(),
                     number_of_replicas));
  }

  const auto server_groups = integration.server_groups();
  if (server_groups.size() < 3 || number_of_replicas > 1) {
    SKIP(fmt::format(
      "{} server groups and {} replicas does not meet expected requirements of unbalanced cluster. "
      "The number of replicas + 1 has to be less than number of the groups",
      integration.server_groups().size(),
      number_of_replicas));
  }

  // Now we need to craft key, for which both active and replica vbuckets
  // are not bound to selected server group.
  const auto& selected_server_group = server_groups.front();
  const auto selected_key = integration.generate_key_not_in_server_group(selected_server_group);
  INFO(fmt::format("server group: \"{}\"\nkey: \"{}\"", selected_server_group, selected_key));

  couchbase::core::document_id id{
    integration.ctx.bucket,
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    selected_key,
  };
  {
    const tao::json::value value = {
      { "a", 1.0 },
      { "b", 2.0 },
    };
    couchbase::core::operations::upsert_request req{
      id,
      couchbase::core::utils::json::generate_binary(value),
    };
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  auto cluster_options = integration.ctx.certificate_path.empty()
                           ? couchbase::cluster_options(couchbase::password_authenticator(
                               integration.ctx.username, integration.ctx.password))
                           : couchbase::cluster_options(couchbase::certificate_authenticator(
                               integration.ctx.certificate_path, integration.ctx.certificate_path));
  //! [select-preferred_server_group]
  cluster_options.network().preferred_server_group(selected_server_group);
  //! [select-preferred_server_group]
  auto [e, cluster] =
    couchbase::cluster::connect(integration.ctx.connection_string, cluster_options).get();
  REQUIRE_SUCCESS(e.ec());

  auto collection = cluster.bucket(id.bucket()).scope(id.scope()).collection(id.collection());
  {
    auto [err, result] = collection.get_any_replica(id.key(), {}).get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    //! [get_any_replica-with-read_preference]
    auto [err, result] = collection
                           .get_any_replica(id.key(),
                                            couchbase::get_any_replica_options{}.read_preference(
                                              couchbase::read_preference::selected_server_group))
                           .get();
    //! [get_any_replica-with-read_preference]
    REQUIRE(err.ec() == couchbase::errc::key_value::document_irretrievable);
  }
  {
    auto [err, result] = collection.get_all_replicas(id.key(), {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(result.size() == number_of_replicas + 1);
  }
  {
    //! [get_all_replicas-with-read_preference]
    auto [err, result] = collection
                           .get_all_replicas(id.key(),
                                             couchbase::get_all_replicas_options{}.read_preference(
                                               couchbase::read_preference::selected_server_group))
                           .get();
    //! [get_all_replicas-with-read_preference]
    REQUIRE(err.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  {
    auto [err, result] = collection
                           .lookup_in_any_replica(id.key(),
                                                  couchbase::lookup_in_specs{
                                                    couchbase::lookup_in_specs::get("a"),
                                                  },
                                                  {})
                           .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] =
      collection
        .lookup_in_any_replica(id.key(),
                               couchbase::lookup_in_specs{
                                 couchbase::lookup_in_specs::get("a"),
                               },
                               couchbase::lookup_in_any_replica_options{}.read_preference(
                                 couchbase::read_preference::selected_server_group))
        .get();
    REQUIRE(err.ec() == couchbase::errc::key_value::document_irretrievable);
  }
  {
    auto [err, result] = collection
                           .lookup_in_all_replicas(id.key(),
                                                   couchbase::lookup_in_specs{
                                                     couchbase::lookup_in_specs::get("a"),
                                                   })
                           .get();
    REQUIRE_SUCCESS(err.ec());
  }
  {
    auto [err, result] =
      collection
        .lookup_in_all_replicas(id.key(),
                                couchbase::lookup_in_specs{
                                  couchbase::lookup_in_specs::get("a"),
                                },
                                couchbase::lookup_in_all_replicas_options{}.read_preference(
                                  couchbase::read_preference::selected_server_group))
        .get();
    REQUIRE(err.ec() == couchbase::errc::key_value::document_irretrievable);
  }

  cluster.close().get();
}
