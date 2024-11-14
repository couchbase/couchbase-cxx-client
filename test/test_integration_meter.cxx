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

#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "core/operations/management/scope_get_all.hxx"
#include "core/platform/uuid.h"

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
                        const couchbase::core::document_id& id,
                        const std::string& expected_outcome = "Success")
{
  // you'd expect one of these (only one) to have a matching op
  REQUIRE(recorders.size() == 1);

  const auto& tags = recorders.front()->tags();

  REQUIRE(tags.at("db.couchbase.service") == "kv");
  REQUIRE(tags.at("db.operation") == op);
  REQUIRE(tags.at("outcome") == expected_outcome);
  REQUIRE(tags.at("db.name") == id.bucket());
  REQUIRE(tags.at("db.couchbase.scope") == id.scope());
  REQUIRE(tags.at("db.couchbase.collection") == id.collection());

  if (guard.cluster_version().supports_cluster_labels()) {
    REQUIRE_FALSE(tags.at("db.couchbase.cluster_name").empty());
    REQUIRE_FALSE(tags.at("db.couchbase.cluster_uuid").empty());
  } else {
    REQUIRE(tags.find("db.couchbase.cluster_name") == tags.end());
    REQUIRE(tags.find("db.couchbase.cluster_uuid") == tags.end());
  }
}

void
assert_http_recorder_tags(test::utils::integration_test_guard& guard,
                          std::list<std::shared_ptr<test_value_recorder>> recorders,
                          const std::string& op,
                          const std::string& service,
                          [[maybe_unused]] const std::string& expected_outcome = "Success")
{
  REQUIRE(recorders.size() == 1);

  const auto& tags = recorders.front()->tags();

  REQUIRE(tags.at("db.couchbase.service") == service);
  REQUIRE(tags.at("db.operation") == op);
  // TODO(CXXCBC-630): Enable assertion once bug recording all HTTP operations as 'Success' is
  // resolved. REQUIRE(tags.at("outcome") == expected_outcome);

  if (guard.cluster_version().supports_cluster_labels()) {
    REQUIRE_FALSE(tags.at("db.couchbase.cluster_name").empty());
    REQUIRE_FALSE(tags.at("db.couchbase.cluster_uuid").empty());
  } else {
    REQUIRE(tags.find("db.couchbase.cluster_name") == tags.end());
    REQUIRE(tags.find("db.couchbase.cluster_uuid") == tags.end());
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
  couchbase::core::cluster_options opts{};
  auto meter = std::make_shared<test_meter>();
  opts.meter = meter;
  test::utils::integration_test_guard guard(opts);
  test::utils::open_bucket(guard.cluster, guard.ctx.bucket);
  auto value = couchbase::core::utils::to_binary(R"({"some": "thing")");
  auto existing_id = make_id(guard.ctx, "foo");
  SECTION("add doc 'foo'")
  {
    const couchbase::core::operations::upsert_request r{ existing_id, value };
    auto response = test::utils::execute(guard.cluster, r);
    REQUIRE_FALSE(response.ctx.ec());
  }
  SECTION("with KV ops")
  {
    SECTION("upsert")
    {
      meter->reset();
      const couchbase::core::operations::upsert_request r{ existing_id, value };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_FALSE(response.ctx.ec());
      auto recorders = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(guard, recorders, "upsert", existing_id);
    }
    SECTION("insert")
    {
      meter->reset();
      auto new_id = make_id(guard.ctx);
      const couchbase::core::operations::insert_request r{ new_id, value };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_FALSE(response.ctx.ec());
      auto recorders = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(guard, recorders, "insert", new_id);
    }
    SECTION("replace")
    {
      meter->reset();
      auto new_value = couchbase::core::utils::to_binary("{\"some\": \"thing else\"");
      const couchbase::core::operations::replace_request r{ existing_id, new_value };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_FALSE(response.ctx.ec());
      auto recorders = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(recorders.empty());
      assert_kv_recorder_tags(guard, recorders, "replace", existing_id);
    }
    SECTION("get")
    {
      meter->reset();
      const couchbase::core::operations::get_request r{ existing_id };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_FALSE(response.ctx.ec());
      auto meters = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(meters.empty());
      assert_kv_recorder_tags(guard, meters, "get", existing_id);
    }
    SECTION("get non-existent-document")
    {
      meter->reset();
      auto new_id = make_id(guard.ctx);
      const couchbase::core::operations::get_request r{ new_id };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE(response.ctx.ec() == couchbase::errc::key_value::document_not_found);
      auto meters = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(meters.empty());
      assert_kv_recorder_tags(guard, meters, "get", new_id, "DocumentNotFound");
    }
  }

  SECTION("with HTTP ops")
  {
    SECTION("get_all_scopes")
    {
      if (!guard.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
      }
      meter->reset();
      const couchbase::core::operations::management::scope_get_all_request r{ guard.ctx.bucket };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_SUCCESS(response.ctx.ec);
      auto meters = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(meters.empty());
      assert_http_recorder_tags(guard, meters, "manager_collections_get_all_scopes", "management");
    }

    SECTION("query")
    {
      meter->reset();
      const couchbase::core::operations::query_request r{ "SELECT 1=1" };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE_SUCCESS(response.ctx.ec);
      auto meters = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(meters.empty());
      assert_http_recorder_tags(guard, meters, "query", "query");
    }

    SECTION("get_bucket fails with bucket_not_found")
    {
      meter->reset();
      couchbase::core::operations::management::bucket_get_request r{ "non-existent" };
      auto response = test::utils::execute(guard.cluster, r);
      REQUIRE(response.ctx.ec == couchbase::errc::common::bucket_not_found);
      auto meters = meter->get_recorders("db.couchbase.operations");
      REQUIRE_FALSE(meters.empty());
      assert_http_recorder_tags(
        guard, meters, "manager_buckets_get_bucket", "management", "BucketNotFound");
    }
  }
}
