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

#include "utils/move_only_context.hxx"

static const tao::json::value basic_doc = {
    { "a", 1.0 },
    { "b", 2.0 },
};
static const std::string basic_doc_json = couchbase::utils::json::generate(basic_doc);

TEST_CASE("integration: crud on default collection", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    // create
    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("seqno=" << resp.token.sequence_number)
        REQUIRE(resp.token.sequence_number != 0);
    }

    // read
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.value == basic_doc_json);
    }

    // update
    {
        auto doc = basic_doc;
        auto json = couchbase::utils::json::generate(doc);
        doc["a"] = 2.0;

        {
            couchbase::operations::replace_request req{ id, json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.value == json);
        }

        {
            couchbase::operations::upsert_request req{ id, basic_doc_json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.value == basic_doc_json);
        }
    }

    // delete
    {
        {
            couchbase::operations::remove_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
        }
    }
}

TEST_CASE("integration: get", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("get") };

    SECTION("miss")
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message());
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }

    SECTION("hit")
    {
        auto flags = 0xdeadbeef;
        {
            couchbase::operations::insert_request req{ id, basic_doc_json };
            req.flags = flags;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
        {
            couchbase::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.value == basic_doc_json);
            REQUIRE(resp.flags == flags);
        }
    }
}

TEST_CASE("integration: touch", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("touch") };

    SECTION("miss")
    {
        couchbase::operations::touch_request req{ id };
        req.expiry = 666;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }

    SECTION("hit")
    {
        {
            couchbase::operations::insert_request req{ id, basic_doc_json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
        {
            couchbase::operations::touch_request req{ id };
            req.expiry = 666;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }
}

TEST_CASE("integration: pessimistic locking", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("locking") };
    uint32_t lock_time = 10;

    couchbase::cas cas;

    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    // lock and record CAS of the locked document
    {
        couchbase::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(cas != resp.cas);
        cas = resp.cas;
    }

    // real CAS is masked now and not visible by regular GET
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(cas != resp.cas);
    }

    // it is not allowed to lock the same key twice
    {
        couchbase::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::ambiguous_timeout);
        REQUIRE(resp.ctx.retry_reasons.count(couchbase::io::retry_reason::kv_locked) == 1);
    }

    // but unlock operation is not retried in this case, because it would never have succeeded
    {
        couchbase::operations::unlock_request req{ id };
        req.cas = couchbase::cas{ cas.value - 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_locked);
        REQUIRE(resp.ctx.retry_reasons.count(couchbase::io::retry_reason::kv_locked) == 0);
    }

    // but mutating the locked key is allowed with known cas
    {
        couchbase::operations::replace_request req{ id, basic_doc_json };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    // to unlock key without mutation, unlock might be used
    {
        couchbase::operations::unlock_request req{ id };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // now the key is not locked
    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: lock/unlock without lock time", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("locking") };

    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    couchbase::cas cas;

    {
        couchbase::operations::get_and_lock_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    {
        couchbase::operations::unlock_request req{ id };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: touch with zero expiry resets expiry", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("get_reset_expiry_key") };

    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // set expiry with touch
    {
        couchbase::operations::touch_request req{ id };
        req.expiry = 1;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // reset expiry
    {
        couchbase::operations::get_and_touch_request req{ id };
        req.expiry = 0;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // wait for original expiry to pass
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // check that the key still exists
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == basic_doc_json);
    }
}

TEST_CASE("integration: exists", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("exists") };

    {
        couchbase::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.exists());
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
        REQUIRE_FALSE(resp.deleted);
        REQUIRE(resp.cas.empty());
        REQUIRE(resp.sequence_number == 0);
    }

    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        req.expiry = 1878422400;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE_FALSE(resp.cas.empty());
    }

    {
        couchbase::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.exists());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE_FALSE(resp.deleted);
        REQUIRE_FALSE(resp.cas.empty());
        REQUIRE(resp.sequence_number != 0);
        REQUIRE(resp.expiry == 1878422400);
    }

    {
        couchbase::operations::remove_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.exists());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.deleted);
        REQUIRE_FALSE(resp.cas.empty());
        REQUIRE(resp.sequence_number != 0);
        REQUIRE(resp.expiry != 0);
    }
}

TEST_CASE("integration: zero length value", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("zero_length_value") };

    {
        couchbase::operations::insert_request req{ id, "" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == "");
    }
}

TEST_CASE("integration: ops on missing document", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

    SECTION("get")
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }

    SECTION("remove")
    {
        couchbase::operations::remove_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }

    SECTION("replace")
    {
        couchbase::operations::replace_request req{ id, "" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }
}

TEST_CASE("integration: cas replace", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("cas_replace") };
    couchbase::cas cas;

    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    SECTION("incorrect")
    {
        couchbase::operations::replace_request req{ id, "" };
        req.cas = couchbase::cas{ cas.value + 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::cas_mismatch);
    }

    SECTION("correct")
    {
        couchbase::operations::replace_request req{ id, "" };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: upsert preserve expiry", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_preserve_expiry()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("preserve_expiry") };
    uint32_t expiry = std::numeric_limits<uint32_t>::max();
    auto expiry_path = "$document.exptime";

    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        req.expiry = expiry;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, expiry_path);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(resp.fields[0].value));
    }

    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        req.preserve_expiry = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, expiry_path);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(resp.fields[0].value));
    }

    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, expiry_path);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(0 == std::stoul(resp.fields[0].value));
    }
}

TEST_CASE("integration: upsert with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
        couchbase::operations::upsert_request req{ id, R"({"foo":"bar"})" };
        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        auto handler = [barrier, ctx = std::move(ctx)](couchbase::operations::upsert_response&& resp) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(std::move(resp));
        };
        integration.cluster->execute(req, std::move(handler));
        auto resp = f.get();
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: upsert may trigger snappy compression", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    std::string compressible_json = R"(
{
  "name": "Emmy-lou Dickerson",
  "age": 26,
  "animals": ["cat", "dog", "parrot"],
  "attributes": {
    "hair": "brown",
    "dimensions": {
      "height": 67,
      "weight": 175
    },
    "hobbies": [
      {
        "type": "winter sports",
        "name": "curling"
      },
      {
        "type": "summer sports",
        "name": "water skiing",
        "details": {
          "location": {
            "lat": 49.282730,
            "long": -123.120735
          }
        }
      }
    ]
  }
}
)";

    // create
    {
        couchbase::operations::insert_request req{ id, compressible_json };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // read
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.value == compressible_json);
    }
}

TEST_CASE("integration: multi-threaded open/close bucket", "[integration]")
{
    test::utils::integration_test_guard integration;
    constexpr auto number_of_threads{ 100 };

    std::vector<std::thread> threads;
    threads.reserve(number_of_threads);

    for (auto i = 0; i < number_of_threads; ++i) {
        threads.emplace_back([&integration]() { test::utils::open_bucket(integration.cluster, integration.ctx.bucket); });
    }
    std::for_each(threads.begin(), threads.end(), [](auto& thread) { thread.join(); });

    threads.clear();

    for (auto i = 0; i < number_of_threads; ++i) {
        threads.emplace_back([&integration]() {
            couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
            couchbase::operations::upsert_request req{ id, basic_doc_json };
            req.timeout = std::chrono::seconds{ 10 };
            if (auto resp = test::utils::execute(integration.cluster, req); resp.ctx.ec) {
                if (resp.ctx.ec != couchbase::error::common_errc::ambiguous_timeout) {
                    throw std::system_error(resp.ctx.ec);
                }
            }
        });
    }

    std::for_each(threads.begin(), threads.end(), [](auto& thread) { thread.join(); });

    threads.clear();

    for (auto i = 0; i < number_of_threads; ++i) {
        threads.emplace_back([&integration]() { test::utils::close_bucket(integration.cluster, integration.ctx.bucket); });
    }
    std::for_each(threads.begin(), threads.end(), [](auto& thread) { thread.join(); });
}

TEST_CASE("integration: open bucket that does not exist", "[integration]")
{
    test::utils::integration_test_guard integration;

    auto bucket_name = test::utils::uniq_id("missing_bucket");

    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    integration.cluster->open_bucket(bucket_name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    REQUIRE(rc == couchbase::error::common_errc::bucket_not_found);
}

TEST_CASE("integration: upsert returns valid mutation token", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("upsert_mt") };

    couchbase::mutation_token token{};
    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        token = resp.token;
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(token.bucket_name == integration.ctx.bucket);
        REQUIRE(token.partition_uuid > 0);
        REQUIRE(token.sequence_number > 0);
    }
    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, "$document.vbucket_uuid");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, true, "$document.seqno");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.fields[0].value.find("\"0x") == 0);
        REQUIRE(std::strtoull(resp.fields[0].value.data() + 3, nullptr, 16) == token.partition_uuid);
        REQUIRE(resp.fields[1].value.find("\"0x") == 0);
        REQUIRE(std::strtoull(resp.fields[1].value.data() + 3, nullptr, 16) == token.sequence_number);
    }
    {
        couchbase::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_exists);
        REQUIRE(resp.token.bucket_name.empty());
        REQUIRE(resp.token.partition_id == 0);
        REQUIRE(resp.token.partition_uuid == 0);
        REQUIRE(resp.token.sequence_number == 0);
    }
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, true, false, "foo", "42");
        req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::insert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_exists);
        REQUIRE(resp.token.bucket_name.empty());
        REQUIRE(resp.token.partition_id == 0);
        REQUIRE(resp.token.partition_uuid == 0);
        REQUIRE(resp.token.sequence_number == 0);
    }
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_add, false, false, false, "a", "{}");
        req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::replace;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::path_exists);
        REQUIRE(resp.token.bucket_name.empty());
        REQUIRE(resp.token.partition_id == 0);
        REQUIRE(resp.token.partition_uuid == 0);
        REQUIRE(resp.token.sequence_number == 0);
        REQUIRE(resp.first_error_index == 0);
        REQUIRE(resp.fields.size() == 1);
        REQUIRE(resp.fields[0].path == "a");
        REQUIRE(resp.fields[0].status == couchbase::protocol::status::subdoc_path_exists);
    }
}

TEST_CASE("integration: upsert is cancelled immediately if the cluster was closed", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    test::utils::close_cluster(integration.cluster);

    {
        couchbase::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::network_errc::cluster_closed);
    }
}
