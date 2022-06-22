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

void
assert_single_lookup_success(test::utils::integration_test_guard& integration,
                             couchbase::protocol::subdoc_opcode opcode,
                             const couchbase::document_id& id,
                             const std::string& path,
                             std::optional<std::string> expected_value = std::nullopt)
{
    couchbase::operations::lookup_in_request req{ id };
    req.specs.add_spec(opcode, false, path);
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_FALSE(resp.ctx.ec);
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE(resp.fields[0].exists);
    REQUIRE(resp.fields[0].path == path);
    REQUIRE(resp.fields[0].status == couchbase::protocol::status::success);
    REQUIRE_FALSE(resp.fields[0].ec);
    if (expected_value.has_value()) {
        REQUIRE(expected_value == resp.fields[0].value);
    }
}

void
assert_single_lookup_error(test::utils::integration_test_guard& integration,
                           couchbase::protocol::subdoc_opcode opcode,
                           const couchbase::document_id& id,
                           const std::string& path,
                           couchbase::protocol::status expected_status,
                           std::error_code expected_ec)
{
    couchbase::operations::lookup_in_request req{ id };
    req.specs.add_spec(opcode, false, path);
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_FALSE(resp.ctx.ec);
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE_FALSE(resp.fields[0].exists);
    REQUIRE(resp.fields[0].path == path);
    REQUIRE(resp.fields[0].value.empty());
    REQUIRE(resp.fields[0].status == expected_status);
    REQUIRE(resp.fields[0].ec == expected_ec);
}

void
assert_single_mutate_success(couchbase::operations::mutate_in_response resp, const std::string& path, const std::string& value = "")
{
    REQUIRE_FALSE(resp.ctx.ec);
    REQUIRE_FALSE(resp.cas.empty());
    REQUIRE(resp.fields.size() == 1);
    REQUIRE(resp.fields[0].path == path);
    REQUIRE(resp.fields[0].status == couchbase::protocol::status::success);
    REQUIRE_FALSE(resp.fields[0].ec);
    REQUIRE(resp.fields[0].value == value);
}

void
assert_single_mutate_error(couchbase::operations::mutate_in_response resp,
                           const std::string& path,
                           couchbase::protocol::status expected_status,
                           std::error_code expected_ec)
{
    REQUIRE(resp.ctx.ec == expected_ec);
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
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };
    std::string value_json{ R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})" };
    auto value = couchbase::utils::json::parse(value_json);

    {
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("dict get")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "dictkey", R"("dictval")");
    }

    SECTION("dict exists")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::exists, id, "dictkey");
    }

    SECTION("array get")
    {
        assert_single_lookup_success(
          integration, couchbase::protocol::subdoc_opcode::get, id, "array", "[1,2,3,4,[10,20,30,[100,200,300]]]");
    }

    SECTION("array exists")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::exists, id, "array");
    }

    SECTION("array index get")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "array[0]", "1");
    }

    SECTION("array index exists")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::exists, id, "array[0]");
    }

    SECTION("non existent path get")
    {
        assert_single_lookup_error(integration,
                                   couchbase::protocol::subdoc_opcode::get,
                                   id,
                                   "non-exist",
                                   couchbase::protocol::status::subdoc_path_not_found,
                                   couchbase::error::key_value_errc::path_not_found);
    }

    SECTION("non existent path exists")
    {
        assert_single_lookup_error(integration,
                                   couchbase::protocol::subdoc_opcode::exists,
                                   id,
                                   "non-exist",
                                   couchbase::protocol::status::subdoc_path_not_found,
                                   couchbase::error::key_value_errc::path_not_found);
    }

    SECTION("non existent doc")
    {
        couchbase::document_id missing_id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

        SECTION("non existent doc get")
        {
            couchbase::operations::lookup_in_request req{ missing_id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "non-exist");
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
            REQUIRE(resp.fields.empty());
        }

        SECTION("non existent doc exists")
        {
            couchbase::operations::lookup_in_request req{ missing_id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::exists, false, "non-exist");
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
            REQUIRE(resp.fields.empty());
        }
    }

    SECTION("non json")
    {
        couchbase::document_id non_json_id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("non_json") };
        std::string non_json_doc{ "string" };

        {
            couchbase::operations::insert_request req{ non_json_id, non_json_doc };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        SECTION("non json get")
        {
            assert_single_lookup_error(integration,
                                       couchbase::protocol::subdoc_opcode::get,
                                       non_json_id,
                                       "non-exist",
                                       couchbase::protocol::status::subdoc_doc_not_json,
                                       couchbase::error::key_value_errc::document_not_json);
        }

        SECTION("non json exists")
        {
            assert_single_lookup_error(integration,
                                       couchbase::protocol::subdoc_opcode::exists,
                                       non_json_id,
                                       "non-exist",
                                       couchbase::protocol::status::subdoc_doc_not_json,
                                       couchbase::error::key_value_errc::document_not_json);
        }
    }

    SECTION("invalid path")
    {
        std::vector<std::string> invalid_paths = { "invalid..path", "invalid[-2]" };
        for (const auto& path : invalid_paths) {
            assert_single_lookup_error(integration,
                                       couchbase::protocol::subdoc_opcode::get,
                                       id,
                                       path,
                                       couchbase::protocol::status::subdoc_path_invalid,
                                       couchbase::error::key_value_errc::path_invalid);
        }
    }

    SECTION("negative paths")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "array[-1][-1][-1]", "300");
    }

    SECTION("nested arrays")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "array[4][3][2]", "300");
    }

    SECTION("path mismatch")
    {
        assert_single_lookup_error(integration,
                                   couchbase::protocol::subdoc_opcode::get,
                                   id,
                                   "array.key",
                                   couchbase::protocol::status::subdoc_path_mismatch,
                                   couchbase::error::key_value_errc::path_mismatch);
    }
}

TEST_CASE("integration: subdoc store", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    couchbase::cas cas{};

    {
        std::string value_json{ R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})" };
        auto value = couchbase::utils::json::parse(value_json);
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    SECTION("dict add")
    {
        std::string path{ "newpath" };
        std::string value{ "123" };

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_add, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_add, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, path, couchbase::protocol::status::subdoc_path_exists, couchbase::error::key_value_errc::path_exists);
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
        }

        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, path, value);
    }

    SECTION("bad cas")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.cas = couchbase::cas{ cas.value + 1 };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "newpath", "123");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::cas_mismatch);
    }

    SECTION("compound value")
    {

        std::string path{ "dict" };
        std::string value{ R"({"key":"value"})" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, path);
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "dict.key", R"("value")");
    }

    SECTION("non json")
    {
        std::string path{ "dict" };
        std::string value{ "non-json" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, path, couchbase::protocol::status::subdoc_value_cannot_insert, couchbase::error::key_value_errc::value_invalid);
    }

    SECTION("unknown parent")
    {
        std::string path{ "parent.with.missing.children" };
        std::string value{ "null" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, path, couchbase::protocol::status::subdoc_path_not_found, couchbase::error::key_value_errc::path_not_found);
    }

    SECTION("create parents")
    {
        std::string path{ "parent.with.missing.children" };
        std::string value{ "null" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, true, false, path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, path);
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, path, value);
    }

    SECTION("replace")
    {
        SECTION("existing path")
        {
            std::string path{ "dictkey" };
            std::string value{ "123" };
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
            assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, path, value);
        }

        SECTION("missing path")
        {
            std::string path = "not-exists";
            std::string value = "123";
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, path, couchbase::protocol::status::subdoc_path_not_found, couchbase::error::key_value_errc::path_not_found);
        }

        SECTION("array element")
        {
            std::string path{ "array[1]" };
            std::string value{ "true" };
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, path);
            assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, path, value);
        }

        SECTION("root")
        {
            std::string path;
            std::string value{ R"({"foo":42})" };
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, path, value);
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }
}

TEST_CASE("integration: subdoc mutate in store semantics", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    couchbase::operations::mutate_in_request req{ id };
    req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
    req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "pth", "123");
    auto resp = test::utils::execute(integration.cluster, req);
    assert_single_mutate_success(resp, "pth");
    assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "pth", "123");
}

TEST_CASE("integration: subdoc unique", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
        auto value = couchbase::utils::json::parse(value_json);
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // Push to a non-existent array (without parent)
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, true, false, "a", "1");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "a");
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "a[0]", "1");
    }

    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, true, false, "a", "1");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::protocol::status::subdoc_path_exists, couchbase::error::key_value_errc::path_exists);
    }

    // try adding object, can't be unique compared
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, true, false, "a", "{}");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::protocol::status::subdoc_value_cannot_insert, couchbase::error::key_value_errc::value_invalid);
    }

    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_push_last, false, true, false, "a", "{}");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "a");
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "a[-1]", "{}");
    }

    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, true, false, "a", "null");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "a", couchbase::protocol::status::subdoc_path_mismatch, couchbase::error::key_value_errc::path_mismatch);
    }
}

TEST_CASE("integration: subdoc counter", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };

    {
        auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
        auto value = couchbase::utils::json::parse(value_json);
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("simple increment")
    {
        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "42");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "42");
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "42");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "84");
        }
    }

    SECTION("max value")
    {
        {
            int64_t max_value = std::numeric_limits<int64_t>::max();
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", std::to_string(max_value));
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", std::to_string(max_value));
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "1");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, "counter", couchbase::protocol::status::subdoc_value_cannot_insert, couchbase::error::key_value_errc::value_invalid);
        }
    }

    SECTION("invalid delta")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "0");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "counter", couchbase::protocol::status::subdoc_delta_invalid, couchbase::error::key_value_errc::delta_invalid);
    }

    SECTION("increase number already too big")
    {
        {
            auto big_value = std::to_string(std::numeric_limits<int64_t>::max()) + "999999999999999999999999999999";
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_add, false, false, false, "counter", big_value);
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter");
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "1");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_error(
              resp, "counter", couchbase::protocol::status::subdoc_num_range_error, couchbase::error::key_value_errc::number_too_big);
        }
    }

    SECTION("non-numeric existing value")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "dictkey", "1");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_error(
          resp, "dictkey", couchbase::protocol::status::subdoc_path_mismatch, couchbase::error::key_value_errc::path_mismatch);
    }

    SECTION("simple decrement")
    {
        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "-42");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "-42");
        }

        {
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "-42");
            auto resp = test::utils::execute(integration.cluster, req);
            assert_single_mutate_success(resp, "counter", "-84");
        }
    }
}

TEST_CASE("integration: subdoc multi lookup", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };
    auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
    auto value = couchbase::utils::json::parse(value_json);

    {
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("simple multi lookup")
    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "dictkey");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::exists, false, "array[0]");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "nonexist");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "array[1]");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.fields.size() == 4);

        REQUIRE(resp.fields[0].value == R"("dictval")");
        REQUIRE(resp.fields[0].status == couchbase::protocol::status::success);

        REQUIRE(resp.fields[1].value.empty());
        REQUIRE(resp.fields[1].status == couchbase::protocol::status::success);
        REQUIRE(resp.fields[1].exists);

        REQUIRE(resp.fields[2].value.empty());
        REQUIRE(resp.fields[2].status == couchbase::protocol::status::subdoc_path_not_found);

        REQUIRE(resp.fields[3].value == "2");
        REQUIRE(resp.fields[3].status == couchbase::protocol::status::success);
    }

    SECTION("mismatched type and opcode")
    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::remove, false, "array[0]");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::remove, false, "array[0]");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    SECTION("missing key")
    {
        couchbase::document_id missing_id{ integration.ctx.bucket, "_default", "_default", "missing_key" };
        couchbase::operations::lookup_in_request req{ missing_id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "dictkey");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "dictkey");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
        REQUIRE(resp.fields.empty());
    }
}

TEST_CASE("integration: subdoc multi mutation", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };
    auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
    auto value = couchbase::utils::json::parse(value_json);

    {
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("simple multi mutation")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "newpath", "true");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::counter, false, false, false, "counter", "42");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.fields.size() == 2);

        REQUIRE(resp.fields[1].value == "42");
        REQUIRE(resp.fields[1].status == couchbase::protocol::status::success);

        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "newpath", "true");
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get, id, "counter", "42");
    }

    SECTION("replace with errors")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, "dictkey", "null");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, "nested.nonexist", "null");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, "bad..bad", "null");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::path_not_found);
        REQUIRE(resp.fields.size() == 3);
        REQUIRE(resp.fields[1].status == couchbase::protocol::status::subdoc_path_not_found);
    }
}

TEST_CASE("integration: subdoc expiry")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };
    auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
    auto value = couchbase::utils::json::parse(value_json);

    {
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::mutate_in_request req{ id };
        req.expiry = 10;
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_add, false, false, false, "tmppath", "null");
        auto resp = test::utils::execute(integration.cluster, req);
        assert_single_mutate_success(resp, "tmppath");
    }
}

TEST_CASE("integration: subdoc get count", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("subdoc") };
    auto value_json = R"({"dictkey":"dictval","array":[1,2,3,4,[10,20,30,[100,200,300]]]})";
    auto value = couchbase::utils::json::parse(value_json);

    {
        couchbase::operations::insert_request req{ id, value_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("top level get count")
    {
        assert_single_lookup_success(integration, couchbase::protocol::subdoc_opcode::get_count, id, "", "2");
    }

    SECTION("multi")
    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get_count, false, "404");
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get_count, false, "array");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.fields.size() == 2);

        REQUIRE(resp.fields[0].value.empty());
        REQUIRE(resp.fields[0].status == couchbase::protocol::status::subdoc_path_not_found);

        REQUIRE(resp.fields[1].value == "5");
        REQUIRE(resp.fields[1].status == couchbase::protocol::status::success);
    }
}

TEST_CASE("integration: subdoc insert error consistency", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_err") };

    couchbase::cas cas;
    {
        couchbase::operations::insert_request req{ id, "{}" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        cas = resp.cas;
    }

    // try to upsert path "foo"=42 with INSERT semantics and zero CAS, expected code is DOCUMENT_EXISTS
    SECTION("insert semantics")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "foo", "42");
        req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::insert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_exists);
    }

    // subdocument operation with UPSERT semantics rejects CAS earlier
    SECTION("upsert semantics invalid cas")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "foo", "42");
        req.cas = couchbase::cas{ cas.value + 1 };
        req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    // try to upsert path "foo"=42 with default (REPLACE) semantics and invalid CAS, expected code is CAS_MISMATCH
    SECTION("replace semantics invalid cas")
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "foo", "42");
        req.cas = couchbase::cas{ cas.value + 1 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::cas_mismatch);
    }
}

TEST_CASE("integration: subdoc remove with empty path", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_err") };
    std::string empty_path;
    std::string value{ R"({"foo":"bar"})" };

    // create initial document
    {
        std::string initial_value{ R"({"bar":"foo"})" };
        couchbase::operations::insert_request req{ id, initial_value };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // replace with empty path sets root value
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::replace, false, false, false, empty_path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == value);
    }

    // remove with empty path removes the document
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::remove, false, empty_path);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }
}

TEST_CASE("integration: subdoc top level array", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("sd_tl_ary") };
    std::string empty_path;

    // add number 1 to top-level array (and initialize the document)
    {
        std::string value{ "1" };
        couchbase::operations::mutate_in_request req{ id };
        req.store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_push_first, false, false, false, empty_path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == "[1]");
    }

    // try to add number 1 but only if it is not in the array yet
    {
        std::string value{ "1" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, false, false, empty_path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::path_exists);
    }
    {
        std::string value{ "42" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_add_unique, false, false, false, empty_path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == "[1,42]");
    }

    // add number 2 to the end of the array
    {
        std::string value{ "2" };
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::array_push_last, false, false, false, empty_path, value);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.value == "[1,42,2]");
    }

    // check size of the top-level array
    {
        couchbase::operations::lookup_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::get_count, false, empty_path);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.fields.size() == 1);
        REQUIRE(resp.fields[0].value == "3");
    }
}
