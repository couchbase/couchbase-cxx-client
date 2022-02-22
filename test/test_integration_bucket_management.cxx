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
#include <couchbase/operations/management/bucket.hxx>

TEST_CASE("integration: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    SECTION("crud")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.ram_quota_mb = 100;
        bucket_settings.num_replicas = 1;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;
        bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::value_only;
        bucket_settings.flush_enabled = true;
        bucket_settings.max_expiry = 10;
        bucket_settings.compression_mode = couchbase::operations::management::bucket_settings::compression_mode::active;
        bucket_settings.replica_indexes = true;
        bucket_settings.conflict_resolution_type =
          couchbase::operations::management::bucket_settings::conflict_resolution_type::sequence_number;

        {
            couchbase::operations::management::bucket_create_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(bucket_settings.bucket_type == resp.bucket.bucket_type);
            REQUIRE(bucket_settings.name == resp.bucket.name);
            REQUIRE(Approx(bucket_settings.ram_quota_mb).margin(5) == resp.bucket.ram_quota_mb);
            REQUIRE(bucket_settings.num_replicas == resp.bucket.num_replicas);
            REQUIRE(bucket_settings.flush_enabled == resp.bucket.flush_enabled);
            REQUIRE(bucket_settings.max_expiry == resp.bucket.max_expiry);
            REQUIRE(bucket_settings.eviction_policy == resp.bucket.eviction_policy);
            REQUIRE(bucket_settings.compression_mode == resp.bucket.compression_mode);
            REQUIRE(bucket_settings.replica_indexes == resp.bucket.replica_indexes);
        }
        std::uint64_t old_quota_mb{ 0 };
        {
            couchbase::operations::management::bucket_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            INFO(resp.ctx.http_body)
            REQUIRE_FALSE(resp.ctx.ec);
            bool found = false;
            for (const auto& bucket : resp.buckets) {
                if (bucket.name != bucket_name) {
                    continue;
                }
                found = true;
                REQUIRE(bucket_settings.bucket_type == bucket.bucket_type);
                REQUIRE(bucket_settings.name == bucket.name);
                REQUIRE(bucket_settings.ram_quota_mb == bucket.ram_quota_mb);
                old_quota_mb = bucket_settings.ram_quota_mb;
                REQUIRE(bucket_settings.num_replicas == bucket.num_replicas);
                REQUIRE(bucket_settings.flush_enabled == bucket.flush_enabled);
                REQUIRE(bucket_settings.max_expiry == bucket.max_expiry);
                REQUIRE(bucket_settings.eviction_policy == bucket.eviction_policy);
                REQUIRE(bucket_settings.compression_mode == bucket.compression_mode);
                REQUIRE(bucket_settings.replica_indexes == bucket.replica_indexes);
                break;
            }
            REQUIRE(found);
        }

        {
            bucket_settings.ram_quota_mb = old_quota_mb + 20;
            couchbase::operations::management::bucket_update_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        auto ram_quota_updated = test::utils::wait_until([&integration, &bucket_name, old_quota_mb]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return !resp.ctx.ec && resp.bucket.ram_quota_mb > old_quota_mb;
        });
        REQUIRE(ram_quota_updated);

        {
            couchbase::operations::management::bucket_drop_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::retry_on_error(integration.cluster, req, {});
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
        }

        {
            couchbase::operations::management::bucket_get_all_request req;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(!resp.buckets.empty());
            auto known_buckets =
              std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](auto& entry) { return entry.name == bucket_name; });
            REQUIRE(known_buckets == 0);
        }
    }

    SECTION("flush")
    {
        SECTION("flush item")
        {
            couchbase::document_id id{ bucket_name, "_default", "_default", test::utils::uniq_id("foo") };

            {
                couchbase::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

            test::utils::open_bucket(integration.cluster, bucket_name);

            {
                const tao::json::value value = {
                    { "a", 1.0 },
                };
                couchbase::operations::insert_request req{ id, couchbase::utils::json::generate(value) };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            auto flushed = test::utils::wait_until([&integration, id]() {
                couchbase::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec == couchbase::error::key_value_errc::document_not_found;
            });
            REQUIRE(flushed);
        }

        SECTION("no bucket")
        {
            couchbase::operations::management::bucket_flush_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
        }

        SECTION("flush disabled")
        {
            {
                couchbase::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

            {
                couchbase::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::error::management_errc::bucket_not_flushable);
            }
        }
    }

    SECTION("memcached")
    {
        {
            couchbase::operations::management::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;
            bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::memcached;
            bucket_settings.num_replicas = 0;
            couchbase::operations::management::bucket_create_request req{ bucket_settings };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::memcached);
        }
    }

    SECTION("ephemeral")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::ephemeral;

        SECTION("default eviction")
        {
            {
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message)
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::no_eviction);
            }
        }

        SECTION("nru eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy ==
                        couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                {
                    bucket_settings.storage_backend = couchbase::operations::management::bucket_settings::storage_backend_type::couchstore;
                    couchbase::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                }

                {
                    auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                    REQUIRE_FALSE(resp.ctx.ec);
                    REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                    REQUIRE(resp.bucket.storage_backend ==
                            couchbase::operations::management::bucket_settings::storage_backend_type::unknown);
                }
            }
        }
    }

    SECTION("couchbase")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;

        SECTION("default eviction")
        {
            {

                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message)
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::value_only);
            }
        }

        SECTION("full eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::full;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::full);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                SECTION("couchstore")
                {
                    {
                        bucket_settings.storage_backend =
                          couchbase::operations::management::bucket_settings::storage_backend_type::couchstore;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend ==
                                couchbase::operations::management::bucket_settings::storage_backend_type::couchstore);
                    }
                }

                SECTION("magma")
                {
                    {
                        bucket_settings.ram_quota_mb = 256;
                        bucket_settings.storage_backend = couchbase::operations::management::bucket_settings::storage_backend_type::magma;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend ==
                                couchbase::operations::management::bucket_settings::storage_backend_type::magma);
                    }
                }
            }
        }
    }

    SECTION("update no bucket")
    {

        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        couchbase::operations::management::bucket_update_request req;
        req.bucket = bucket_settings;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
    }

    if (integration.cluster_version().supports_minimum_durability_level()) {
        SECTION("minimum durability level")
        {
            couchbase::operations::management::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;

            SECTION("default")
            {
                {
                    couchbase::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                }

                {
                    auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                    REQUIRE_FALSE(resp.ctx.ec);
                    REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::none);
                }
            }

            if (integration.number_of_nodes() >= 2) {
                SECTION("majority")
                {
                    {
                        bucket_settings.minimum_durability_level = couchbase::protocol::durability_level::majority;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        INFO(resp.error_message)
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = test::utils::wait_for_bucket_created(integration.cluster, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::majority);
                    }
                }
            }
        }
    }

    test::utils::close_bucket(integration.cluster, bucket_name);

    // drop bucket if not already dropped
    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        test::utils::execute(integration.cluster, req);
    }
}
