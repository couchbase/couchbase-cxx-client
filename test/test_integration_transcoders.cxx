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

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>

#include <tao/json/contrib/vector_traits.hpp>

struct profile {
    std::string username{};
    std::string full_name{};
    std::uint32_t birth_year{};

    bool operator==(const profile& other) const
    {
        return username == other.username && full_name == other.full_name && birth_year == other.birth_year;
    }
};

template<>
struct tao::json::traits<profile> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const profile& p)
    {
        v = {
            { "username", p.username },
            { "full_name", p.full_name },
            { "birth_year", p.birth_year },
        };
    }

    template<template<typename...> class Traits>
    static profile as(const tao::json::basic_value<Traits>& v)
    {
        profile result;
        const auto& object = v.get_object();
        result.username = object.at("username").template as<std::string>();
        result.full_name = object.at("full_name").template as<std::string>();
        if (object.count("birth_year") != 0) {
            // expect incomplete JSON here, as we might use projections to fetch reduced document
            // as an alternative we might use std::optional<> here
            result.birth_year = object.at("birth_year").template as<std::uint32_t>();
        }
        return result;
    }
};

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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        original_cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.cas() == original_cas);
        REQUIRE(resp.content_as<profile>() == albert);
    }

    {
        albert.username += " (clone)";
        auto [ctx, resp] = collection.replace(id, albert, couchbase::replace_options{}.cas(original_cas)).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
        original_cas = resp.cas();
    }

    {
        auto [ctx, resp] = collection.remove(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<couchbase::codec::raw_binary_transcoder>() == std::vector{ std::byte{ 20 }, std::byte{ 21 } });
    }

    {
        auto [ctx, resp] = collection.binary().prepend(id, std::vector{ std::byte{ 10 }, std::byte{ 11 } }, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<couchbase::codec::raw_binary_transcoder>() ==
                std::vector{ std::byte{ 10 }, std::byte{ 11 }, std::byte{ 20 }, std::byte{ 21 } });
    }

    {
        auto [ctx, resp] = collection.binary().append(id, std::vector{ std::byte{ 30 }, std::byte{ 31 } }, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, {}).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.project({ "username", "full_name" })).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        auto light_albert = resp.content_as<profile>();
        REQUIRE_FALSE(light_albert == albert);
        REQUIRE(light_albert.username == albert.username);
        REQUIRE(light_albert.full_name == albert.full_name);
        REQUIRE(light_albert.birth_year != albert.birth_year);
        REQUIRE(light_albert.birth_year == 0);
    }
}
