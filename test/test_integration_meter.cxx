/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/metrics/meter.hxx>

#include "core/logger/logger.hxx"

class test_value_recorder : public couchbase::metrics::value_recorder
{
public:
  test_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags)
    : name_(name)
    , tags_(tags)
  {
  }
  void record_value(std::int64_t value) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.emplace_back(value);
  }
  std::map<std::string, std::string> tags() const
  {
    return tags_;
  }
  std::list<std::uint64_t> values()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return values_;
  }
  void reset()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.clear();
  }

private:
  std::string name_;
  std::map<std::string, std::string> tags_;
  std::mutex mutex_;
  std::list<std::uint64_t> values_;
};

class test_meter : public couchbase::metrics::meter
{
public:
  test_meter()
    : couchbase::metrics::meter()
  {
  }
  std::shared_ptr<couchbase::metrics::value_recorder> get_value_recorder(
    const std::string& name,
    const std::map<std::string, std::string>& tags) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = value_recorders_.equal_range(name);
    if (it.first != it.second) {

      for (auto itr = it.first; itr != it.second; itr++) {
        if (tags == itr->second->tags())
          return itr->second;
      }
    }
    return value_recorders_.insert({ name, std::make_shared<test_value_recorder>(name, tags) })
      ->second;
  }

  void reset()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    value_recorders_.clear();
  }

  std::list<std::shared_ptr<test_value_recorder>> get_recorders(const std::string& name)
  {
    std::list<std::shared_ptr<test_value_recorder>> retval;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = value_recorders_.equal_range(name);
    for (auto itr = it.first; itr != it.second; itr++) {
      retval.push_back(itr->second);
    }
    return retval;
  }

private:
  std::multimap<std::string, std::shared_ptr<test_value_recorder>> value_recorders_;
  std::mutex mutex_;
};

void
assert_kv_recorder_tags(test::utils::integration_test_guard& guard,
                        std::list<std::shared_ptr<test_value_recorder>> recorders,
                        const std::string& op,
                        const std::optional<std::string>& expected_outcome = {})
{
  // you'd expect one of these (only one) to have a matching op
  REQUIRE(recorders.size() == 1);

  const auto& tags = recorders.front()->tags();

  REQUIRE(tags.at("couchbase.service") == "kv");
  REQUIRE(tags.at("db.operation.name") == op);
  if (expected_outcome.has_value()) {
    REQUIRE(tags.at("error.type") == expected_outcome.value());
  } else {
    REQUIRE(tags.find("error.type") == tags.end());
  }
  REQUIRE(tags.at("db.namespace") == guard.ctx.bucket);
  REQUIRE(tags.at("couchbase.scope.name") == "_default");
  REQUIRE(tags.at("couchbase.collection.name") == "_default");

  if (guard.cluster_version().supports_cluster_labels()) {
    REQUIRE_FALSE(tags.at("couchbase.cluster.name").empty());
    REQUIRE_FALSE(tags.at("couchbase.cluster.uuid").empty());
  } else {
    REQUIRE(tags.find("couchbase.cluster.name") == tags.end());
    REQUIRE(tags.find("couchbase.cluster.uuid") == tags.end());
  }
}

void
assert_http_recorder_tags(test::utils::integration_test_guard& guard,
                          std::list<std::shared_ptr<test_value_recorder>> recorders,
                          const std::string& op,
                          const std::string& service,
                          const std::optional<std::string>& expected_outcome = {},
                          const std::optional<std::string>& bucket_name = {},
                          const std::optional<std::string>& scope_name = {},
                          const std::optional<std::string>& collection_name = {})
{
  REQUIRE(recorders.size() == 1);

  const auto& tags = recorders.front()->tags();

  REQUIRE(tags.at("couchbase.service") == service);
  REQUIRE(tags.at("db.operation.name") == op);
  if (expected_outcome.has_value()) {
    REQUIRE(tags.at("error.type") == expected_outcome.value());
  } else {
    REQUIRE(tags.find("error.type") == tags.end());
  }
  if (bucket_name.has_value()) {
    REQUIRE(tags.at("db.namespace") == bucket_name.value());
  } else {
    REQUIRE(tags.find("db.namespace") == tags.end());
  }
  if (scope_name.has_value()) {
    REQUIRE(tags.at("couchbase.scope.name") == scope_name.value());
  } else {
    REQUIRE(tags.find("couchbase.scope.name") == tags.end());
  }
  if (collection_name.has_value()) {
    REQUIRE(tags.at("couchbase.collection.name") == collection_name.value());
  } else {
    REQUIRE(tags.find("couchbase.collection.name") == tags.end());
  }
  if (guard.cluster_version().supports_cluster_labels()) {
    REQUIRE_FALSE(tags.at("couchbase.cluster.name").empty());
    REQUIRE_FALSE(tags.at("couchbase.cluster.uuid").empty());
  } else {
    REQUIRE(tags.find("couchbase.cluster.name") == tags.end());
    REQUIRE(tags.find("couchbase.cluster.uuid") == tags.end());
  }
}

couchbase::core::document_id
make_id(const test::utils::test_context& ctx, std::string key = "")
{
  if (key.empty()) {
    key = test::utils::uniq_id("tracer");
  }
  return couchbase::core::document_id{ ctx.bucket, "_default", "_default", key };
}

TEST_CASE("integration: use external meter", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto meter = std::make_shared<test_meter>();
  auto cluster = integration.public_cluster([meter](couchbase::cluster_options& opts) {
    opts.metrics().meter(meter);
  });

  auto value = tao::json::value{
    { "some", "thing" },
  };
  auto existing_key = test::utils::uniq_id("meter");
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  {
    auto [err, res] = collection.upsert(existing_key, value, {}).get();
    REQUIRE_SUCCESS(err.ec());
  }

  meter->reset();

  SECTION("with KV ops")
  {
    SECTION("upsert")
    {
      auto [err, res] = collection.upsert(existing_key, value, {}).get();
      REQUIRE_FALSE(err.ec());

      auto recorders = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(integration, recorders, "upsert");
    }

    SECTION("insert")
    {
      auto new_key = test::utils::uniq_id("meter");
      auto [err, res] = collection.insert(new_key, value, {}).get();

      auto recorders = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(integration, recorders, "insert");
    }

    SECTION("replace")
    {
      auto new_value = tao::json::value{
        { "some", "thing else" },
      };
      auto [err, res] = collection.replace(existing_key, new_value, {}).get();
      REQUIRE_FALSE(err.ec());

      auto recorders = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(integration, recorders, "replace");
    }

    SECTION("get")
    {
      auto [err, res] = collection.get(existing_key, {}).get();
      REQUIRE_FALSE(err.ec());

      auto meters = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(meters.empty());
      assert_kv_recorder_tags(integration, meters, "get");
    }

    SECTION("get non-existent-document")
    {
      auto [err, res] = collection.get(test::utils::uniq_id("does-not-exist"), {}).get();
      REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);

      auto meters = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(meters.empty());
      assert_kv_recorder_tags(integration, meters, "get", "DocumentNotFound");
    }
  }

  SECTION("with HTTP ops")
  {
    SECTION("get_all_scopes")
    {
      if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
      }
      auto [err, res] = cluster.bucket(integration.ctx.bucket).collections().get_all_scopes().get();
      REQUIRE_SUCCESS(err.ec());

      auto meters = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(meters.empty());
      assert_http_recorder_tags(integration,
                                meters,
                                "manager_collections_get_all_scopes",
                                "management",
                                {},
                                integration.ctx.bucket);
    }

    SECTION("query")
    {
      SECTION("cluster-level")
      {
        auto [err, res] = cluster.query("SELECT 1=1", {}).get();
        REQUIRE_SUCCESS(err.ec());

        auto meters = meter->get_recorders("db.client.operation.duration");
        REQUIRE_FALSE(meters.empty());
        assert_http_recorder_tags(integration, meters, "query", "query");
      }

      SECTION("scope-level")
      {
        auto [err, res] =
          cluster.bucket(integration.ctx.bucket).default_scope().query("SELECT 1=1", {}).get();
        REQUIRE_SUCCESS(err.ec());

        auto meters = meter->get_recorders("db.client.operation.duration");
        REQUIRE_FALSE(meters.empty());
        assert_http_recorder_tags(
          integration, meters, "query", "query", {}, integration.ctx.bucket, "_default");
      }
    }

    SECTION("get_bucket fails with bucket_not_found")
    {
      auto [err, res] = cluster.buckets().get_bucket("non-existent").get();
      REQUIRE(err.ec() == couchbase::errc::common::bucket_not_found);

      auto meters = meter->get_recorders("db.client.operation.duration");
      REQUIRE_FALSE(meters.empty());
      assert_http_recorder_tags(integration,
                                meters,
                                "manager_buckets_get_bucket",
                                "management",
                                "BucketNotFound",
                                "non-existent");
    }
  }
}
