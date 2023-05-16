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

#include <couchbase/cluster.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>

static const tao::json::value basic_doc = {
    { "a", 1.0 },
    { "b", 2.0 },
};
static const std::vector<std::byte> basic_doc_json = couchbase::core::utils::json::generate_binary(basic_doc);

TEST_CASE("integration: switching off mutation token", "[integration]")
{
    couchbase::core::cluster_options opts{};
    opts.enable_mutation_tokens = false;
    test::utils::integration_test_guard integration(opts);

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES does not allow to switching off mutation tokens. See https://github.com/couchbaselabs/gocaves/issues/100");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() == 0);
        REQUIRE(resp.token.partition_uuid() == 0);
        REQUIRE_FALSE(resp.token.partition_id() == 0);
        REQUIRE_FALSE(resp.token.bucket_name().empty());
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.value == basic_doc_json);
    }
}

TEST_CASE("integration: crud on default collection", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    // create
    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }

    // read
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.value == basic_doc_json);
    }

    // update
    {
        auto doc = basic_doc;
        auto json = couchbase::core::utils::json::generate_binary(doc);
        doc["a"] = 2.0;

        {
            couchbase::core::operations::replace_request req{ id, json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.value == json);
        }

        {
            couchbase::core::operations::upsert_request req{ id, basic_doc_json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.value == basic_doc_json);
        }
    }

    // delete
    {
        {
            couchbase::core::operations::remove_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
        }
    }
}

TEST_CASE("integration: get", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("get") };

    SECTION("miss")
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }

    SECTION("hit")
    {
        auto flags = 0xdeadbeef;
        {
            couchbase::core::operations::insert_request req{ id, basic_doc_json };
            req.flags = flags;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }
        {
            couchbase::core::operations::get_request req{ id };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.value == basic_doc_json);
            REQUIRE(resp.flags == flags);
        }
    }
}

TEST_CASE("integration: touch", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("touch") };

    SECTION("miss")
    {
        couchbase::core::operations::touch_request req{ id };
        req.expiry = 666;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }

    SECTION("hit")
    {
        {
            couchbase::core::operations::insert_request req{ id, basic_doc_json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }
        {
            couchbase::core::operations::touch_request req{ id };
            req.expiry = 666;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }
    }
}

TEST_CASE("integration: pessimistic locking", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("locking") };
    uint32_t lock_time = 10;

    couchbase::cas cas{};

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    // lock and record CAS of the locked document
    {
        couchbase::core::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(cas != resp.cas);
        cas = resp.cas;
    }

    // real CAS is masked now and not visible by regular GET
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(cas != resp.cas);
    }

    // it is not allowed to lock the same key twice
    {
        couchbase::core::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        if (integration.ctx.deployment == test::utils::deployment_type::capella ||
            integration.ctx.deployment == test::utils::deployment_type::elixir) {
            req.timeout = std::chrono::seconds{ 2 };
        }
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::ambiguous_timeout);
        REQUIRE(resp.ctx.retried_because_of(couchbase::retry_reason::key_value_locked));
    }

    // but unlock operation is not retried in this case, because it would never have succeeded
    {
        couchbase::core::operations::unlock_request req{ id };
        req.cas = couchbase::cas{ cas.value() - 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_locked);
        REQUIRE_FALSE(resp.ctx.retried_because_of(couchbase::retry_reason::key_value_locked));
    }

    // but mutating the locked key is allowed with known cas
    {
        couchbase::core::operations::replace_request req{ id, basic_doc_json };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_and_lock_request req{ id };
        req.lock_time = lock_time;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    // to unlock key without mutation, unlock might be used
    {
        couchbase::core::operations::unlock_request req{ id };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // now the key is not locked
    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
}

TEST_CASE("integration: lock/unlock without lock time", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("locking") };

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    couchbase::cas cas{};

    {
        couchbase::core::operations::get_and_lock_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    {
        couchbase::core::operations::unlock_request req{ id };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
}

TEST_CASE("integration: touch with zero expiry resets expiry", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("get_reset_expiry_key") };

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // set expiry with touch
    {
        couchbase::core::operations::touch_request req{ id };
        req.expiry = 1;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // reset expiry
    {
        couchbase::core::operations::get_and_touch_request req{ id };
        req.expiry = 0;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // wait for original expiry to pass
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // check that the key still exists
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == basic_doc_json);
    }
}

TEST_CASE("integration: exists", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("exists") };

    {
        couchbase::core::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.exists());
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE_FALSE(resp.deleted);
        REQUIRE(resp.cas.empty());
        REQUIRE(resp.sequence_number == 0);
    }

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        req.expiry = 1878422400;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE_FALSE(resp.cas.empty());
    }

    {
        couchbase::core::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.exists());
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE_FALSE(resp.deleted);
        REQUIRE_FALSE(resp.cas.empty());
        REQUIRE(resp.sequence_number != 0);
        REQUIRE(resp.expiry == 1878422400);
    }

    {
        couchbase::core::operations::remove_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::exists_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.exists());
        REQUIRE_SUCCESS(resp.ctx.ec());
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

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("zero_length_value") };

    {
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary("") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary(""));
    }
}

TEST_CASE("integration: ops on missing document", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

    SECTION("get")
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }

    SECTION("remove")
    {
        couchbase::core::operations::remove_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }

    SECTION("replace")
    {
        couchbase::core::operations::replace_request req{ id, couchbase::core::utils::to_binary("") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("integration: cas replace", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("cas_replace") };
    couchbase::cas cas{};

    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    SECTION("incorrect")
    {
        couchbase::core::operations::replace_request req{ id, couchbase::core::utils::to_binary("") };
        req.cas = couchbase::cas{ cas.value() + 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::cas_mismatch);
    }

    SECTION("correct")
    {
        couchbase::core::operations::replace_request req{ id, couchbase::core::utils::to_binary("") };
        req.cas = cas;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
}

TEST_CASE("integration: upsert preserve expiry", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_preserve_expiry()) {
        SKIP("cluster does not support preserve expiry");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("preserve_expiry") };
    uint32_t expiry = std::numeric_limits<uint32_t>::max();

    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        req.expiry = expiry;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::expiry_time).xattr(),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(test::utils::to_string(resp.fields[0].value)));
    }

    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        req.preserve_expiry = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::expiry_time).xattr(),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(test::utils::to_string(resp.fields[0].value)));
    }

    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::expiry_time).xattr(),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(0 == std::stoul(test::utils::to_string(resp.fields[0].value)));
    }
}

TEST_CASE("integration: upsert with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(R"({"foo":"bar"})") };
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::upsert_response>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        auto handler = [barrier, ctx = std::move(ctx)](couchbase::core::operations::upsert_response&& resp) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(std::move(resp));
        };
        integration.cluster->execute(req, std::move(handler));
        auto resp = f.get();
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
}

TEST_CASE("integration: upsert may trigger snappy compression", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    auto compressible_json = couchbase::core::utils::to_binary(R"(
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
)");

    // create
    {
        couchbase::core::operations::insert_request req{ id, compressible_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // read
    {
        couchbase::core::operations::get_request req{ id };
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
            couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
            couchbase::core::operations::upsert_request req{ id, basic_doc_json };
            req.timeout = std::chrono::seconds{ 10 };
            if (auto resp = test::utils::execute(integration.cluster, req); resp.ctx.ec()) {
                if (resp.ctx.ec() != couchbase::errc::common::ambiguous_timeout) {
                    throw std::system_error(resp.ctx.ec());
                }
            }
        });
    }

    std::for_each(threads.begin(), threads.end(), [](auto& thread) { thread.join(); });

    threads.clear();

    for (auto i = 0; i < number_of_threads; ++i) {
        auto close_bucket = [&integration]() { test::utils::close_bucket(integration.cluster, integration.ctx.bucket); };
        std::thread closer(std::move(close_bucket));
        threads.emplace_back(std::move(closer));
    }
    std::for_each(threads.begin(), threads.end(), [](auto& thread) { thread.join(); });
}

TEST_CASE("integration: open bucket that does not exist", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES returns not_found (0x01) instead of no_access (0x24). See https://github.com/couchbaselabs/gocaves/issues/102");
    }

    auto bucket_name = test::utils::uniq_id("missing_bucket");

    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    integration.cluster->open_bucket(bucket_name, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    REQUIRE(rc == couchbase::errc::common::bucket_not_found);
}

TEST_CASE("integration: upsert returns valid mutation token", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("upsert_mt") };

    couchbase::mutation_token token{};
    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        token = resp.token;
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(token.bucket_name() == integration.ctx.bucket);
        REQUIRE(token.partition_uuid() > 0);
        REQUIRE(token.sequence_number() > 0);
    }
    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::vbucket_uuid).xattr(),
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::sequence_number).xattr(),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        auto vbucket_uuid = test::utils::to_string(resp.fields[0].value);
        REQUIRE(vbucket_uuid.find("\"0x") == 0);
        REQUIRE(std::strtoull(vbucket_uuid.data() + 3, nullptr, 16) == token.partition_uuid());
        auto sequence_number = test::utils::to_string(resp.fields[1].value);
        REQUIRE(sequence_number.find("\"0x") == 0);
        REQUIRE(std::strtoull(sequence_number.data() + 3, nullptr, 16) == token.sequence_number());
    }
    {
        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_exists);
        REQUIRE(resp.token.bucket_name().empty());
        REQUIRE(resp.token.partition_id() == 0);
        REQUIRE(resp.token.partition_uuid() == 0);
        REQUIRE(resp.token.sequence_number() == 0);
    }
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("foo", "42").create_path() }.specs();
        req.store_semantics = couchbase::store_semantics::insert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_exists);
        REQUIRE(resp.token.bucket_name().empty());
        REQUIRE(resp.token.partition_id() == 0);
        REQUIRE(resp.token.partition_uuid() == 0);
        REQUIRE(resp.token.sequence_number() == 0);
    }
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::insert("a", tao::json::empty_object) }.specs();
        req.store_semantics = couchbase::store_semantics::replace;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::path_exists);
        REQUIRE(resp.token.bucket_name().empty());
        REQUIRE(resp.token.partition_id() == 0);
        REQUIRE(resp.token.partition_uuid() == 0);
        REQUIRE(resp.token.sequence_number() == 0);
        REQUIRE(resp.ctx.first_error_index() == 0);
        REQUIRE(resp.fields.size() == 1);
        REQUIRE(resp.fields[0].path == "a");
        REQUIRE(resp.fields[0].status == couchbase::key_value_status_code::subdoc_path_exists);
    }
}

TEST_CASE("integration: upsert is cancelled immediately if the cluster was closed", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };

    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    test::utils::close_cluster(integration.cluster);

    {
        couchbase::core::operations::upsert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::network::cluster_closed);
    }
}

TEST_CASE("integration: pessimistic locking with public API", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("counter");
    std::chrono::seconds lock_time{ 10 };

    couchbase::cas cas{};

    {
        auto [ctx, resp] = collection.insert(id, basic_doc, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        cas = resp.cas();
    }

    // lock and record CAS of the locked document
    {
        auto [ctx, resp] = collection.get_and_lock(id, lock_time, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(cas != resp.cas());
        cas = resp.cas();
    }

    // real CAS is masked now and not visible by regular GET
    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(cas != resp.cas());
    }

    // it is not allowed to lock the same key twice
    {
        couchbase::get_and_lock_options options{};
        if (integration.ctx.deployment == test::utils::deployment_type::capella ||
            integration.ctx.deployment == test::utils::deployment_type::elixir) {
            options.timeout(std::chrono::seconds{ 2 });
        }
        auto [ctx, resp] = collection.get_and_lock(id, lock_time, options).get();
        REQUIRE(ctx.ec() == couchbase::errc::common::ambiguous_timeout);
        REQUIRE(ctx.retried_because_of(couchbase::retry_reason::key_value_locked));
    }

    // but unlock operation is not retried in this case, because it would never have succeeded
    {
        auto wrong_cas = couchbase::cas{ cas.value() - 1 };
        auto ctx = collection.unlock(id, wrong_cas, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_locked);
        REQUIRE_FALSE(ctx.retried_because_of(couchbase::retry_reason::key_value_locked));
    }

    // and yet mutating the locked key is allowed with known cas
    {
        auto [ctx, resp] = collection.replace(id, basic_doc, couchbase::replace_options{}.cas(cas)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.get_and_lock(id, lock_time, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        cas = resp.cas();
    }

    // to unlock key without mutation, unlock might be used
    {
        auto ctx = collection.unlock(id, cas, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
    }

    // now the key is not locked
    {
        auto [ctx, resp] = collection.upsert(id, basic_doc, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
    }
}

TEST_CASE("integration: exists with public API", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("exists");

    {
        auto [ctx, resp] = collection.exists(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.exists());
        REQUIRE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.insert(id, basic_doc, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.exists(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(resp.exists());
        REQUIRE_FALSE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.remove(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
    }

    {
        auto [ctx, resp] = collection.exists(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.exists());
    }
}

TEST_CASE("integration: get with expiry with public API", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("get_expiry");

    auto get_options = couchbase::get_options{}.with_expiry(true);

    SECTION("no expiry set on the document")
    {
        {
            auto [ctx, resp] = collection.insert(id, basic_doc, {}).get();
            REQUIRE_SUCCESS(ctx.ec());
        }

        {
            auto [ctx, resp] = collection.get(id, get_options).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE_FALSE(resp.expiry_time().has_value());
        }
    }

    SECTION("some expiry is set on the document")
    {
        auto the_expiry = std::chrono::system_clock::from_time_t(1878422400);
        auto insert_options = couchbase::insert_options{}.expiry(the_expiry);
        {
            auto [ctx, resp] = collection.insert(id, basic_doc, insert_options).get();
            REQUIRE_SUCCESS(ctx.ec());
        }

        {
            auto [ctx, resp] = collection.get(id, get_options).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(resp.expiry_time().has_value());
            REQUIRE(resp.expiry_time().value() == the_expiry);
        }
    }
}
