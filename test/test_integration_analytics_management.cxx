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
#include <couchbase/operations/management/analytics.hxx>
#include <couchbase/operations/management/analytics_link.hxx>

TEST_CASE("integration: analytics index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics()) {
        return;
    }

    // MB-47718
    if (integration.storage_backend() == couchbase::operations::management::bucket_settings::storage_backend_type::magma) {
        return;
    }

    SECTION("crud")
    {
        auto dataverse_name = test::utils::uniq_id("dataverse");
        auto dataset_name = test::utils::uniq_id("dataset");
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_exists);
        }

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataset_exists);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_link_connect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.datasets.empty());
            auto dataset = std::find_if(resp.datasets.begin(), resp.datasets.end(), [dataset_name](const auto& exp_dataset) {
                return exp_dataset.name == dataset_name;
            });
            REQUIRE(dataset != resp.datasets.end());
            REQUIRE(dataset->dataverse_name == dataverse_name);
            REQUIRE(dataset->link_name == "Local");
            REQUIRE(dataset->bucket_name == integration.ctx.bucket);
        }

        {
            couchbase::operations::management::analytics_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());
            auto index = std::find_if(
              resp.indexes.begin(), resp.indexes.end(), [index_name](const auto& exp_index) { return exp_index.name == index_name; });
            REQUIRE(index != resp.indexes.end());
            REQUIRE(index->dataverse_name == dataverse_name);
            REQUIRE(index->dataset_name == dataset_name);
            REQUIRE_FALSE(index->is_primary);
        }

        if (integration.cluster_version().supports_analytics_pending_mutations()) {
            couchbase::operations::management::analytics_get_pending_mutations_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.stats[index_name] == 0);
        }

        {
            couchbase::operations::management::analytics_link_disconnect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataset_not_found);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_not_found);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("compound names")
        {
            auto dataverse_name = fmt::format("{}/{}", test::utils::uniq_id("dataverse"), test::utils::uniq_id("dataverse"));
            auto dataset_name = test::utils::uniq_id("dataset");
            auto index_name = test::utils::uniq_id("index");

            {
                couchbase::operations::management::analytics_dataverse_create_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataset_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_index_create_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                req.fields["testkey"] = "string";
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_link_connect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_link_disconnect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_index_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataset_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataverse_drop_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }
    }
}

void
run_s3_link_test(test::utils::integration_test_guard& integration, std::string dataverse_name, std::string link_name)
{
    {
        couchbase::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_exists);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].link_name == link_name);
        REQUIRE(resp.s3[0].dataverse == dataverse_name);
        REQUIRE(resp.s3[0].access_key_id == "access_key");
        REQUIRE(resp.s3[0].secret_access_key.empty());
        REQUIRE(resp.s3[0].region == "us-east-1");
        REQUIRE(resp.s3[0].service_endpoint == "service_endpoint");
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "s3";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "eu-west-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].region == "eu-west-1");
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_not_found);
    }
}

void
run_azure_link_test(test::utils::integration_test_guard& integration, std::string dataverse_name, std::string link_name)
{
    {
        couchbase::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_exists);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.azure_blob[0].link_name == link_name);
        REQUIRE(resp.azure_blob[0].dataverse == dataverse_name);
        REQUIRE_FALSE(resp.azure_blob[0].connection_string.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].account_name.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].account_key.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].shared_access_signature.has_value());
        REQUIRE(resp.azure_blob[0].blob_endpoint == "blob_endpoint");
        REQUIRE(resp.azure_blob[0].endpoint_suffix == "endpoint_suffix");
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "azureblob";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "new_blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.azure_blob[0].blob_endpoint == "new_blob_endpoint");
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_not_found);
    }
}

TEST_CASE("integration: analytics external link management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics_links()) {
        return;
    }

    // MB-47718
    if (integration.storage_backend() == couchbase::operations::management::bucket_settings::storage_backend_type::magma) {
        return;
    }

    // MB-40198
    if (!integration.cluster_version().supports_analytics_links_cert_auth() && integration.origin.credentials().uses_certificate()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto link_name = test::utils::uniq_id("link");

    SECTION("missing dataverse")
    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = "missing_dataverse";
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;

        {
            couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
              req{};
            req.link = link;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_not_found);
        }
    }

    SECTION("missing argument")
    {
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = couchbase::operations::management::analytics_link::s3_external{};
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    SECTION("link crud")
    {
        auto dataverse_name = test::utils::uniq_id("dataverse");

        SECTION("s3")
        {
            run_s3_link_test(integration, dataverse_name, link_name);
        }

        if (integration.cluster_version().supports_analytics_link_azure_blob()) {
            SECTION("azure")
            {
                run_azure_link_test(integration, dataverse_name, link_name);
            }
        }
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("link crud scopes")
        {
            auto scope_name = test::utils::uniq_id("scope");

            {
                couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto created =
                  test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
                REQUIRE(created);
            }

            auto dataverse_name = fmt::format("{}/{}", integration.ctx.bucket, scope_name);

            SECTION("s3")
            {
                run_s3_link_test(integration, dataverse_name, link_name);
            }

            if (integration.cluster_version().supports_analytics_link_azure_blob()) {
                SECTION("azure")
                {
                    run_azure_link_test(integration, dataverse_name, link_name);
                }
            }
        }
    }
}
