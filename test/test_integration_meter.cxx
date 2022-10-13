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

#include "core/platform/uuid.h"

#include <couchbase/metrics/meter.hxx>

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
    std::shared_ptr<couchbase::metrics::value_recorder> get_value_recorder(const std::string& name,
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
        return value_recorders_.insert({ name, std::make_shared<test_value_recorder>(name, tags) })->second;
    }

    void reset()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto v : value_recorders_) {
            v.second->reset();
        }
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
assert_kv_recorder_tags(std::list<std::shared_ptr<test_value_recorder>> recorders, const std::string& op)
{
    // you'd expect one of these (only one) to have a matching op
    REQUIRE(recorders.size() == 1);
    REQUIRE(recorders.front()->tags()["db.couchbase.service"] == "kv");
    // db.operation always _starts_ with the op -- like '<op> 0x<NN'
    REQUIRE(recorders.front()->tags()["db.operation"].find(op, 0) == 0);
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
        couchbase::core::operations::upsert_request r{ existing_id, value };
        auto response = test::utils::execute(guard.cluster, r);
        REQUIRE_FALSE(response.ctx.ec());
    }
    SECTION("test KV ops")
    {
        SECTION("upsert")
        {
            meter->reset();
            couchbase::core::operations::upsert_request r{ existing_id, value };
            auto response = test::utils::execute(guard.cluster, r);
            REQUIRE_FALSE(response.ctx.ec());
            auto recorders = meter->get_recorders("db.couchbase.operations");
            REQUIRE_FALSE(recorders.empty());
            assert_kv_recorder_tags(recorders, "upsert");
        }
        SECTION("insert")
        {
            meter->reset();
            couchbase::core::operations::insert_request r{ make_id(guard.ctx), value };
            auto response = test::utils::execute(guard.cluster, r);
            REQUIRE_FALSE(response.ctx.ec());
            auto recorders = meter->get_recorders("db.couchbase.operations");
            REQUIRE_FALSE(recorders.empty());
            assert_kv_recorder_tags(recorders, "insert");
        }
        SECTION("replace")
        {
            meter->reset();
            auto new_value = couchbase::core::utils::to_binary("{\"some\": \"thing else\"");
            couchbase::core::operations::replace_request r{ existing_id, new_value };
            auto response = test::utils::execute(guard.cluster, r);
            REQUIRE_FALSE(response.ctx.ec());
            auto recorders = meter->get_recorders("db.couchbase.operations");
            REQUIRE_FALSE(recorders.empty());
            assert_kv_recorder_tags(recorders, "replace");
        }
        SECTION("get")
        {
            meter->reset();
            couchbase::core::operations::get_request r{ existing_id };
            auto response = test::utils::execute(guard.cluster, r);
            REQUIRE_FALSE(response.ctx.ec());
            auto meters = meter->get_recorders("db.couchbase.operations");
            REQUIRE_FALSE(meters.empty());
            assert_kv_recorder_tags(meters, "get");
        }
    }
}
