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

#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>

template<typename SubdocumentOperation>
void
assert_single_lookup_success(test::utils::integration_test_guard& integration,
                             const couchbase::core::document_id& id,
                             const SubdocumentOperation& spec,
                             std::optional<std::string> expected_value = std::nullopt)
{
    couchbase::core::operations::lookup_in_request req{ id };
    req.specs = couchbase::lookup_in_specs{ spec }.specs();
    auto resp = test::utils::execute(integration.cluster, req);
    INFO(fmt::format("assert_single_lookup_success(\"{}\", \"{}\")", id, req.specs[0].path_));
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE(resp.fields[0].exists);
    REQUIRE(resp.fields[0].path == req.specs[0].path_);
    REQUIRE(resp.fields[0].status == couchbase::key_value_status_code::success);
    REQUIRE_SUCCESS(resp.fields[0].ec);
    if (expected_value.has_value()) {
        REQUIRE(couchbase::core::utils::to_binary(expected_value.value()) == resp.fields[0].value);
    }
}

template<typename SubdocumentOperation>
void
assert_single_lookup_error(test::utils::integration_test_guard& integration,
                           const couchbase::core::document_id& id,
                           const SubdocumentOperation& spec,
                           couchbase::key_value_status_code expected_status,
                           std::error_code expected_ec)
{
    couchbase::core::operations::lookup_in_request req{ id };
    req.specs = couchbase::lookup_in_specs{ spec }.specs();
    auto resp = test::utils::execute(integration.cluster, req);
    INFO(fmt::format("assert_single_lookup_error(\"{}\", \"{}\")", id, req.specs[0].path_));
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE_FALSE(resp.fields[0].exists);
    REQUIRE(resp.fields[0].path == req.specs[0].path_);
    REQUIRE(resp.fields[0].value.empty());
    REQUIRE(resp.fields[0].status == expected_status);
    REQUIRE(resp.fields[0].ec == expected_ec);
}

void
assert_single_mutate_success(couchbase::core::operations::mutate_in_response resp, const std::string& path, const std::string& value = "")
{
    REQUIRE_SUCCESS(resp.ctx.ec());
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE(resp.fields[0].path == path);
    REQUIRE(resp.fields[0].status == couchbase::key_value_status_code::success);
    REQUIRE_SUCCESS(resp.fields[0].ec);
    REQUIRE(resp.fields[0].value == couchbase::core::utils::to_binary(value));
}

void
assert_single_mutate_error(couchbase::core::operations::mutate_in_response resp,
                           const std::string& path,
                           couchbase::key_value_status_code expected_status,
                           std::error_code expected_ec)
{
    REQUIRE(resp.ctx.ec() == expected_ec);
    REQUIRE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE(resp.fields[0].path == path);
    REQUIRE(resp.fields[0].value.empty());
    REQUIRE(resp.fields[0].status == expected_status);
    REQUIRE(resp.fields[0].ec == expected_ec);
}

TEST_CASE("integration: subdoc get & exists", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("dict get")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("dictkey"), R"("dictval")");
    }

    SECTION("dict exists")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::exists("dictkey"));
    }

    SECTION("array get")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("array"), "[1,2,3,4,[10,20,30,[100,200,300]]]");
    }

    SECTION("array exists")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::exists("array"));
    }

    SECTION("array index get")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("array[0]"), "1");
    }

    SECTION("array index exists")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::exists("array[0]"));
    }

    SECTION("non existent path get")
    {
        assert_single_lookup_error(integration,
                                   id,
                                   couchbase::lookup_in_specs::get("non-exist"),
                                   couchbase::key_value_status_code::subdoc_path_not_found,
                                   couchbase::errc::key_value::path_not_found);
    }

    SECTION("non existent path exists")
    {
        assert_single_lookup_error(integration,
                                   id,
                                   couchbase::lookup_in_specs::exists("non-exist"),
                                   couchbase::key_value_status_code::subdoc_path_not_found,
                                   couchbase::errc::key_value::path_not_found);
    }

    SECTION("non existent doc")
    {
        couchbase::core::document_id missing_id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

        SECTION("non existent doc get")
        {
            couchbase::core::operations::lookup_in_request req{ missing_id };
            req.specs =
              couchbase::lookup_in_specs{
                  couchbase::lookup_in_specs::get("non-exist"),
              }
                .specs();
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
            REQUIRE(resp.fields.empty());
        }

        SECTION("non existent doc exists")
        {
            couchbase::core::operations::lookup_in_request req{ missing_id };
            req.specs =
              couchbase::lookup_in_specs{
                  couchbase::lookup_in_specs::exists("non-exist"),
              }
                .specs();
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
            REQUIRE(resp.fields.empty());
        }
    }

    SECTION("non json")
    {
        couchbase::core::document_id non_json_id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("non_json") };
        auto non_json_doc = couchbase::core::utils::to_binary("string");

        {
            couchbase::core::operations::insert_request req{ non_json_id, non_json_doc };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        SECTION("non json get")
        {
            if (integration.cluster_version().is_mock()) {
                SKIP("GOCAVES does not handle subdocument operations for non-JSON documents. See "
                     "https://github.com/couchbaselabs/gocaves/issues/103");
            }
            assert_single_lookup_error(integration,
                                       non_json_id,
                                       couchbase::lookup_in_specs::get("non-exist"),
                                       couchbase::key_value_status_code::subdoc_doc_not_json,
                                       couchbase::errc::key_value::document_not_json);
        }

        SECTION("non json exists")
        {
            if (integration.cluster_version().is_mock()) {
                SKIP("GOCAVES does not handle subdocument operations for non-JSON documents. See "
                     "https://github.com/couchbaselabs/gocaves/issues/103");
            }
            assert_single_lookup_error(integration,
                                       non_json_id,
                                       couchbase::lookup_in_specs::exists("non-exist"),
                                       couchbase::key_value_status_code::subdoc_doc_not_json,
                                       couchbase::errc::key_value::document_not_json);
        }
    }

    SECTION("invalid path")
    {
        std::vector<std::string> invalid_paths = { "invalid..path", "invalid[-2]" };
        for (const auto& path : invalid_paths) {
            if (integration.cluster_version().is_mock()) {
                assert_single_lookup_error(integration,
                                           id,
                                           couchbase::lookup_in_specs::get(path),
                                           couchbase::key_value_status_code::subdoc_path_not_found,
                                           couchbase::errc::key_value::path_not_found);
            } else {
                assert_single_lookup_error(integration,
                                           id,
                                           couchbase::lookup_in_specs::get(path),
                                           couchbase::key_value_status_code::subdoc_path_invalid,
                                           couchbase::errc::key_value::path_invalid);
            }
        }
    }

    SECTION("negative paths")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("array[-1][-1][-1]"), "300");
    }

    SECTION("nested arrays")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("array[4][3][2]"), "300");
    }

    SECTION("path mismatch")
    {
        assert_single_lookup_error(integration,
                                   id,
                                   couchbase::lookup_in_specs::get("array.key"),
                                   couchbase::key_value_status_code::subdoc_path_mismatch,
                                   couchbase::errc::key_value::path_mismatch);
    }
}

TEST_CASE("integration: subdoc store", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    couchbase::cas cas{};

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    SECTION("dict add")
    {
        std::string path{ "newpath" };
        std::string value{ "123" };

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::insert(path, 123) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::insert(path, 123) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, path, couchbase::key_value_status_code::subdoc_path_exists, couchbase::errc::key_value::path_exists);
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert(path, 123) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
        }

        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get(path), value);
    }

    SECTION("bad cas")
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.cas = couchbase::cas{ cas.value() + 1 };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("newpath", 123) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::cas_mismatch);
    }

    SECTION("compound value")
    {

        std::string path{ "dict" };
        tao::json::value value{
            { "key", "value" },
        };
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert(path, value) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "dict");
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("dict.key"), R"("value")");
    }

    SECTION("non json")
    {
        if (integration.cluster_version().is_mock()) {
            SKIP("GOCAVES does not handle subdocument operations for non-JSON documents. See "
                 "https://github.com/couchbaselabs/gocaves/issues/103");
        }
        std::string path{ "dict" };
        auto value = couchbase::core::utils::to_binary("non-json");
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert_raw(path, value) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, path, couchbase::key_value_status_code::subdoc_value_cannot_insert, couchbase::errc::key_value::value_invalid);
    }

    SECTION("unknown parent")
    {
        std::string path{ "parent.with.missing.children" };
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert(path, tao::json::null) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, path, couchbase::key_value_status_code::subdoc_path_not_found, couchbase::errc::key_value::path_not_found);
    }

    SECTION("create parents")
    {
        std::string path{ "parent.with.missing.children" };
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert(path, tao::json::null).create_path() }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, path);
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get(path), "null");
    }

    SECTION("replace")
    {
        SECTION("existing path")
        {
            std::string path{ "dictkey" };
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::replace(path, 123) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
            assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get(path), "123");
        }

        SECTION("missing path")
        {
            std::string path = "not-exists";
            std::string value = "123";
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::replace(path, 123) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, path, couchbase::key_value_status_code::subdoc_path_not_found, couchbase::errc::key_value::path_not_found);
        }

        SECTION("array element")
        {
            std::string path{ "array[1]" };
            std::string value{ "true" };
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::replace(path, true) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
            assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get(path), value);
        }

        SECTION("root")
        {
            std::string path;
            tao::json::value value{
                { "key", 42 },
            };
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::replace(path, value) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }
    }
}

TEST_CASE("integration: subdoc mutate in store semantics", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    couchbase::core::operations::mutate_in_request req{ id };
    req.store_semantics = couchbase::store_semantics::upsert;
    req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("pth", 123) }.specs();
    auto resp = test::utils::execute(integration.cluster, req);
    assert_single_mutate_success(resp, "pth");
    assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("pth"), "123");
}

TEST_CASE("integration: subdoc unique", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES does not support subdocument create_path feature. See https://github.com/couchbaselabs/gocaves/issues/17");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // Push to a non-existent array (without parent)
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique("a", 1).create_path() }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "a");
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("a[0]"), "1");
    }

    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique("a", 1).create_path() }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::key_value_status_code::subdoc_path_exists, couchbase::errc::key_value::path_exists);
    }

    // try adding object, can't be unique compared
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique("a", tao::json::empty_object) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::key_value_status_code::subdoc_value_cannot_insert, couchbase::errc::key_value::value_invalid);
    }

    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs =
          couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_append("a", tao::json::empty_object).create_path() }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "a");
    }

    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique("a", tao::json::null).create_path() }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::key_value_status_code::subdoc_path_mismatch, couchbase::errc::key_value::path_mismatch);
    }
}

TEST_CASE("integration: subdoc counter", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json =
          integration.cluster_version().is_mock() // kv_engine creates counters automatically
            ? couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]],"counter":0})")
            : couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("simple increment")
    {
        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", 42) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "42");
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", 42) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "84");
        }
    }

    SECTION("max value")
    {
        if (integration.cluster_version().is_mock()) {
            SKIP("GOCAVES incorrectly handles limits for subdoc counters. See https://github.com/couchbaselabs/gocaves/issues/104");
        }
        {
            int64_t max_value = std::numeric_limits<int64_t>::max();
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", max_value) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", std::to_string(max_value));
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", 1) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, "counter", couchbase::key_value_status_code::subdoc_value_cannot_insert, couchbase::errc::key_value::value_invalid);
        }
    }

    SECTION("invalid delta")
    {
        if (integration.cluster_version().is_mock()) {
            SKIP("GOCAVES incorrectly handles zero delta for subdoc counters. See https://github.com/couchbaselabs/gocaves/issues/105");
        }
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", 0) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "counter", couchbase::key_value_status_code::subdoc_delta_invalid, couchbase::errc::key_value::delta_invalid);
    }

    SECTION("increase number already too big")
    {
        if (integration.cluster_version().is_mock()) {
            SKIP("GOCAVES incorrectly handles big values for subdoc counters. See https://github.com/couchbaselabs/gocaves/issues/106");
        }
        {
            auto big_value = R"({"counter":)" + std::to_string(std::numeric_limits<int64_t>::max()) + "999999999999999999999999999999}";
            auto value_json = couchbase::core::utils::to_binary(big_value);
            couchbase::core::operations::upsert_request req{ id, value_json };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("counter", 1) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, "counter", couchbase::key_value_status_code::subdoc_num_range_error, couchbase::errc::key_value::number_too_big);
        }
    }

    SECTION("non-numeric existing value")
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("dictkey", 1) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "dictkey", couchbase::key_value_status_code::subdoc_path_mismatch, couchbase::errc::key_value::path_mismatch);
    }

    SECTION("simple decrement")
    {
        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::decrement("counter", 42) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "-42");
        }

        {
            couchbase::core::operations::mutate_in_request req{ id };
            req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::decrement("counter", 42) }.specs();
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "-84");
        }
    }
}

TEST_CASE("integration: subdoc multi lookup", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("simple multi lookup")
    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get("dictkey"),
              couchbase::lookup_in_specs::exists("array[0]"),
              couchbase::lookup_in_specs::get("nonexist"),
              couchbase::lookup_in_specs::get("array[1]"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.fields.size() == 4);

        REQUIRE(resp.fields[0].value == couchbase::core::utils::to_binary(R"("dictval")"));
        REQUIRE(resp.fields[0].status == couchbase::key_value_status_code::success);

        REQUIRE(resp.fields[1].value.empty());
        REQUIRE(resp.fields[1].status == couchbase::key_value_status_code::success);
        REQUIRE(resp.fields[1].exists);

        REQUIRE(resp.fields[2].value.empty());
        REQUIRE(resp.fields[2].status == couchbase::key_value_status_code::subdoc_path_not_found);

        REQUIRE(resp.fields[3].value == couchbase::core::utils::to_binary("2"));
        REQUIRE(resp.fields[3].status == couchbase::key_value_status_code::success);
    }

    SECTION("mismatched type and opcode")
    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::remove("array[0]"),
              couchbase::mutate_in_specs::remove("array[0]"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        if (integration.cluster_version().is_mock()) {
            REQUIRE(resp.ctx.ec() == couchbase::errc::common::unsupported_operation);
        } else {
            REQUIRE(resp.ctx.ec() == couchbase::errc::common::invalid_argument);
        }
    }

    SECTION("missing key")
    {
        couchbase::core::document_id missing_id{ integration.ctx.bucket, "_default", "_default", "missing_key" };
        couchbase::core::operations::lookup_in_request req{ missing_id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get("dictkey"),
              couchbase::lookup_in_specs::get("dictkey"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
        REQUIRE(resp.fields.empty());
    }
}

TEST_CASE("integration: subdoc multi mutation", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json =
          integration.cluster_version().is_mock() // kv_engine creates counters automatically
            ? couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]],"counter":0})")
            : couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("simple multi mutation")
    {
        couchbase::core::operations::mutate_in_request req{ id };

        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::upsert("newpath", true),
              couchbase::mutate_in_specs::increment("counter", 42),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.fields.size() == 2);

        REQUIRE(resp.fields[1].value == couchbase::core::utils::to_binary("42"));
        REQUIRE(resp.fields[1].status == couchbase::key_value_status_code::success);

        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("newpath"), "true");
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::get("counter"), "42");
    }

    SECTION("replace with errors")
    {
        if (integration.cluster_version().is_mock()) {
            SKIP("GOCAVES incorrectly uses error indexes for subdoc mutations. See https://github.com/couchbaselabs/gocaves/issues/107");
        }
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::replace("dictkey", tao::json::null),
              couchbase::mutate_in_specs::replace("dested.nonexist", tao::json::null),
              couchbase::mutate_in_specs::replace("bad..bad", tao::json::null),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::path_not_found);
        REQUIRE(resp.fields.size() == 3);
        REQUIRE(resp.ctx.first_error_index() == 1);
        REQUIRE(resp.fields[1].status == couchbase::key_value_status_code::subdoc_path_not_found);
    }
}

TEST_CASE("integration: subdoc expiry")
{
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES does not support subdoc mutations with expiry. See https://github.com/couchbaselabs/gocaves/issues/85");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.expiry = 10;
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::insert("tmppath", tao::json::null) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "tmppath");
    }
}

TEST_CASE("integration: subdoc get count", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = couchbase::core::utils::to_binary(R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})");
        couchbase::core::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("top level get count")
    {
        assert_single_lookup_success(integration, id, couchbase::lookup_in_specs::count(""), "2");
    }

    SECTION("multi")
    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::count("404"),
              couchbase::lookup_in_specs::count("array"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.fields.size() == 2);

        REQUIRE(resp.fields[0].value.empty());
        REQUIRE(resp.fields[0].status == couchbase::key_value_status_code::subdoc_path_not_found);

        REQUIRE(resp.fields[1].value == couchbase::core::utils::to_binary("5"));
        REQUIRE(resp.fields[1].status == couchbase::key_value_status_code::success);
    }
}

TEST_CASE("integration: subdoc insert error consistency", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_err") };

    couchbase::cas cas{};
    {
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary("{}") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        cas = resp.cas;
    }

    // try to upsert path "foo"=42 with INSERT semantics and zero CAS, expected code is DOCUMENT_EXISTS
    SECTION("insert semantics")
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("foo", 42) }.specs();
        req.store_semantics = couchbase::store_semantics::insert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_exists);
    }

    // subdocument operation with UPSERT semantics rejects CAS earlier
    SECTION("upsert semantics invalid cas")
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("foo", 42) }.specs();
        req.cas = couchbase::cas{ cas.value() + 1 };
        req.store_semantics = couchbase::store_semantics::upsert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::invalid_argument);
    }

    // try to upsert path "foo"=42 with default (REPLACE) semantics and invalid CAS, expected code is CAS_MISMATCH
    SECTION("replace semantics invalid cas")
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("foo", 42) }.specs();
        req.cas = couchbase::cas{ cas.value() + 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::cas_mismatch);
    }
}

TEST_CASE("integration: subdoc remove with empty path", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_err") };
    std::string empty_path;
    tao::json::value value{
        { "foo", "bar" },
    };

    // create initial document
    {
        auto initial_value = couchbase::core::utils::to_binary(R"({"bar":"foo"})");
        couchbase::core::operations::insert_request req{ id, initial_value };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    // replace with empty path sets root value
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::replace(empty_path, value) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::json::generate_binary(value));
    }

    // remove with empty path removes the document
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::remove(empty_path) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("integration: subdoc top level array", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_tl_ary") };
    std::string empty_path;

    // add number 1 to top-level array (and initialize the document)
    {
        std::string value{ "1" };
        couchbase::core::operations::mutate_in_request req{ id };
        req.store_semantics = couchbase::store_semantics::upsert;
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_prepend(empty_path, 1) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary("[1]"));
    }

    // try to add number 1 but only if it is not in the array yet
    {
        std::string value{ "1" };
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique(empty_path, 1) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::path_exists);
    }
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_add_unique(empty_path, 42) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary("[1,42]"));
    }

    // add number 2 to the end of the array
    {
        std::string value{ "2" };
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::array_append(empty_path, 2) }.specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary("[1,42,2]"));
    }

    // check size of the top-level array
    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::count(empty_path),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.fields.size() == 1);
        REQUIRE(resp.fields[0].value == couchbase::core::utils::to_binary("3"));
    }
}
