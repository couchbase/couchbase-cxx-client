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

#include "profile.hxx"

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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == skynet_birthday);
    }

    const auto asteroid_99942_apophis_passage = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1870722000 } };

    {
        auto [ctx, resp] = collection.get_and_touch(id, asteroid_99942_apophis_passage, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE_FALSE(resp.expiry_time().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_FALSE(ctx.ec());
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
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.mutation_token().has_value());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
        REQUIRE(resp.content_as<profile>() == cecilia);
        REQUIRE(resp.expiry_time().has_value());
        REQUIRE(resp.expiry_time().value() == skynet_birthday);
    }

    const auto asteroid_99942_apophis_passage = std::chrono::system_clock::time_point{ std::chrono::seconds{ 1870722000 } };

    {
        auto [ctx, resp] = collection.touch(id, asteroid_99942_apophis_passage, {}).get();
        REQUIRE_FALSE(ctx.ec());
        REQUIRE_FALSE(resp.cas().empty());
    }

    {
        auto [ctx, resp] = collection.get(id, couchbase::get_options{}.with_expiry(true)).get();
        REQUIRE_FALSE(ctx.ec());
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
