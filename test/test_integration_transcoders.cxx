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

#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_exception.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>

#include <tao/json/contrib/vector_traits.hpp>

#include "profile.hxx"

using Catch::Matchers::ContainsSubstring;

TEST_CASE("integration: upsert/get with json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    {
        auto [ctx, resp] = collection.upsert(id, albert, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == albert);
    }
}

TEST_CASE("integration: insert/get with json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    {
        auto [ctx, resp] = collection.insert(id, albert, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == albert);
    }
}

TEST_CASE("integration: insert/replace with json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    couchbase::cas original_cas;
    {
        auto [ctx, resp] = collection.insert(id, albert, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        original_cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.cas() == original_cas);
        REQUIRE(resp.content_as<profile>() == albert);
    }

    {
        albert.username += " (clone)";
        auto [ctx, resp] = collection.replace(id, albert, couchbase::replace_options{}.cas(original_cas)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.cas() != original_cas);
        REQUIRE(resp.content_as<profile>() == albert);
    }

    {
        albert.username += " (copy)";
        auto [ctx, resp] = collection.replace(id, albert, couchbase::replace_options{}.cas(original_cas)).get();
        REQUIRE(ctx.ec() == couchbase::errc::common::cas_mismatch);
        REQUIRE(resp.cas().empty());
        REQUIRE_FALSE(resp.mutation_token().has_value());
    }
}

TEST_CASE("integration: upsert/remove with json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    couchbase::cas original_cas;
    {
        auto [ctx, resp] = collection.upsert(id, albert, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        original_cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection.remove(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        REQUIRE(resp.cas() != original_cas);
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}

TEST_CASE("integration: upsert/append/prepend with raw binary transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    std::vector data{ std::byte{ 20 }, std::byte{ 21 } };

    {
        auto [ctx, resp] = collection.upsert<couchbase::codec::raw_binary_transcoder>(id, data, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<couchbase::codec::raw_binary_transcoder>() == std::vector{ std::byte{ 20 }, std::byte{ 21 } });
    }

    {
        auto [ctx, resp] = collection.binary().prepend(id, std::vector{ std::byte{ 10 }, std::byte{ 11 } }, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<couchbase::codec::raw_binary_transcoder>() ==
                std::vector{ std::byte{ 10 }, std::byte{ 11 }, std::byte{ 20 }, std::byte{ 21 } });
    }

    {
        auto [ctx, resp] = collection.binary().append(id, std::vector{ std::byte{ 30 }, std::byte{ 31 } }, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<couchbase::codec::raw_binary_transcoder>() ==
                std::vector{ std::byte{ 10 }, std::byte{ 11 }, std::byte{ 20 }, std::byte{ 21 }, std::byte{ 30 }, std::byte{ 31 } });
    }
}

TEST_CASE("integration: get with expiry and json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };
    auto skynet_birthday = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1807056000 } };

    {
        auto [ctx, resp] = collection.upsert(id, albert, couchbase::upsert_options{}.expiry(skynet_birthday)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == albert);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == skynet_birthday);
    }
}

TEST_CASE("integration: get with projections and json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);
    auto id = test::utils::uniq_id("foo");
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    {
        auto [ctx, resp] = collection.upsert(id, albert, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    SECTION("all fields present")
    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.project({ "username", "full_name" })).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        auto light_albert = resp.content_as<profile>();
        REQUIRE_FALSE(light_albert == albert);
        REQUIRE(light_albert.username == albert.username);
        REQUIRE(light_albert.full_name == albert.full_name);
        REQUIRE(light_albert.birth_year != albert.birth_year);
        REQUIRE(light_albert.birth_year == 0);
    }

    SECTION("with non-existent field in projections")
    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.project({ "username", "full_name", "non_existent_field" })).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        auto light_albert = resp.content_as<profile>();
        REQUIRE_FALSE(light_albert == albert);
        REQUIRE(light_albert.username == albert.username);
        REQUIRE(light_albert.full_name == albert.full_name);
        REQUIRE(light_albert.birth_year != albert.birth_year);
        REQUIRE(light_albert.birth_year == 0);
    }
}

TEST_CASE("integration: get_and_touch and json transcoder", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    const auto skynet_birthday = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1807056000 } };

    auto id = test::utils::uniq_id("cecilia");
    profile cecilia{ "cecilia", "Cecilia Payne-Gaposchkin", 1900 };

    {
        auto [ctx, resp] = collection.upsert(id, cecilia, couchbase::upsert_options{}.expiry(skynet_birthday)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == skynet_birthday);
    }

    const auto asteroid_99942_apophis_passage = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1870722000 } };

    {
        auto [ctx, resp] = collection.get_and_touch(id, asteroid_99942_apophis_passage, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE_FALSE(resp.expiry_time().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == asteroid_99942_apophis_passage);
    }

    {
        auto [ctx, resp] = collection.get_and_touch(test::utils::uniq_id("unknown_profile"), asteroid_99942_apophis_passage, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_not_found);
        REQUIRE(resp.cas().empty());
    }
}

TEST_CASE("integration: touch with public API", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    const auto skynet_birthday = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1807056000 } };

    auto id = test::utils::uniq_id("cecilia");
    profile cecilia{ "cecilia", "Cecilia Payne-Gaposchkin", 1900 };

    {
        auto [ctx, resp] = collection.upsert(id, cecilia, couchbase::upsert_options{}.expiry(skynet_birthday)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == skynet_birthday);
    }

    const auto asteroid_99942_apophis_passage = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1870722000 } };

    {
        auto [ctx, resp] = collection.touch(id, asteroid_99942_apophis_passage, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == asteroid_99942_apophis_passage);
    }

    {
        auto [ctx, resp] = collection.touch(test::utils::uniq_id("unknown_profile"), asteroid_99942_apophis_passage, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_not_found);
        REQUIRE(resp.cas().empty());
    }
}

TEST_CASE("integration: subdoc with public API", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES incorrectly uses error indexes for subdoc mutations. See https://github.com/couchbaselabs/gocaves/issues/107");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("liu_cixin");
    profile cixin{ "liu_cixin", "刘慈欣", 1963 };

    couchbase::mutation_token token{};
    {
        auto [ctx, resp] = collection.upsert(id, cixin, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        token = resp.mutation_token().value();
    }

    {
        auto [ctx, resp] = collection
                             .lookup_in(id,
                                        couchbase::lookup_in_specs{
                                          couchbase::lookup_in_specs::get("full_name"),
                                          couchbase::lookup_in_specs::exists("birth_year"),
                                          couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::sequence_number),
                                          couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::value_size_bytes),
                                        },
                                        {})
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());

        REQUIRE_FALSE(resp.is_deleted());

        REQUIRE(resp.exists(0));
        REQUIRE(resp.exists("full_name"));
        REQUIRE(resp.content_as<std::string>(0) == "刘慈欣");
        REQUIRE(resp.content_as<std::string>("full_name") == "刘慈欣");

        REQUIRE(resp.exists(1));
        REQUIRE(resp.exists("birth_year"));

        REQUIRE(resp.exists(2));
        REQUIRE(resp.exists(couchbase::subdoc::lookup_in_macro::sequence_number));
        REQUIRE(resp.content_as<std::string>(2) == fmt::format("0x{:016x}", token.sequence_number()));
        REQUIRE(resp.content_as<std::string>(couchbase::subdoc::lookup_in_macro::sequence_number) ==
                fmt::format("0x{:016x}", token.sequence_number()));

        REQUIRE(resp.exists(3));
        REQUIRE(resp.exists(couchbase::subdoc::lookup_in_macro::value_size_bytes));
        REQUIRE(resp.content_as<std::uint32_t>(3) == 66);
        REQUIRE(resp.content_as<std::uint32_t>(couchbase::subdoc::lookup_in_macro::value_size_bytes) == 66);
    }

    {
        auto [ctx, resp] = collection
                             .mutate_in(id,
                                        couchbase::mutate_in_specs{
                                          couchbase::mutate_in_specs::increment("views", 1).create_path(),
                                          couchbase::mutate_in_specs::remove("missing_field"),
                                        },
                                        {})
                             .get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::path_not_found);
        REQUIRE(ctx.first_error_index().has_value());
        REQUIRE(ctx.first_error_index().value() == 1);
        REQUIRE(ctx.first_error_path().has_value());
        REQUIRE(ctx.first_error_path() == "missing_field");
        REQUIRE(resp.cas().empty());
        REQUIRE_THROWS_WITH(resp.has_value(0), ContainsSubstring("path_invalid"));
        REQUIRE_THROWS_WITH(resp.has_value("views"), ContainsSubstring("path_invalid"));
        REQUIRE_THROWS_WITH(resp.content_as<std::uint32_t>(0), ContainsSubstring("path_invalid"));
        REQUIRE_THROWS_WITH(resp.content_as<std::uint32_t>("views"), ContainsSubstring("path_invalid"));
    }

    {
        auto [ctx, resp] = collection
                             .mutate_in(id,
                                        couchbase::mutate_in_specs{
                                          couchbase::mutate_in_specs::increment("views", 1).create_path(),
                                          couchbase::mutate_in_specs::upsert("references", 100'500).create_path(),
                                        },
                                        {})
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(ctx.first_error_index().has_value());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.has_value(0));
        REQUIRE(resp.has_value("views"));
        REQUIRE(resp.content_as<std::uint32_t>(0) == 1);
        REQUIRE(resp.content_as<std::uint32_t>("views") == 1);
        REQUIRE_FALSE(resp.has_value(1));
        REQUIRE_FALSE(resp.has_value("references"));
    }

    couchbase::cas cas{};
    {
        auto [ctx, resp] = collection
                             .mutate_in(id,
                                        couchbase::mutate_in_specs{
                                          couchbase::mutate_in_specs::remove("birth_year"),
                                          couchbase::mutate_in_specs::upsert("my_cas", couchbase::subdoc::mutate_in_macro::cas).xattr(),
                                        },
                                        {})
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(ctx.first_error_index());
        REQUIRE_FALSE(ctx.first_error_path());
        REQUIRE_FALSE(resp.cas().empty());
        cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection
                             .lookup_in(id,
                                        couchbase::lookup_in_specs{
                                          couchbase::lookup_in_specs::get("my_cas").xattr(),
                                          couchbase::lookup_in_specs::exists("birth_year"),
                                        },
                                        {})
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());

        REQUIRE(resp.exists(0));
        REQUIRE(resp.exists("my_cas"));
        REQUIRE(resp.content_as<std::string>(0) == fmt::format("0x{:016x}", couchbase::core::utils::byte_swap(cas.value())));
        REQUIRE(resp.content_as<std::string>("my_cas") == fmt::format("0x{:016x}", couchbase::core::utils::byte_swap(cas.value())));

        REQUIRE_FALSE(resp.exists(1));
        REQUIRE_FALSE(resp.exists("birth_year"));
    }

    {
        auto [ctx, resp] = collection.remove(id, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection
                             .lookup_in(id,
                                        couchbase::lookup_in_specs{
                                          couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::cas).xattr(),
                                        },
                                        couchbase::lookup_in_options{}.access_deleted(true))
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());

        REQUIRE(resp.is_deleted());
        REQUIRE(resp.exists(0));
        REQUIRE(resp.exists(couchbase::subdoc::lookup_in_macro::cas));
        REQUIRE(resp.content_as<std::string>(0) == fmt::format("0x{:016x}", cas.value()));
        REQUIRE(resp.content_as<std::string>(couchbase::subdoc::lookup_in_macro::cas) == fmt::format("0x{:016x}", cas.value()));
    }
}
