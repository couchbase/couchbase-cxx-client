/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2024. Couchbase, Inc.
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

#include "core/logger/logger.hxx"
#include "core/operations/management/eventing.hxx"
#include "test_helper_integration.hxx"

static couchbase::core::operations::management::bucket_get_response
wait_for_bucket_created(test::utils::integration_test_guard& integration,
                        const std::string& bucket_name)
{
  test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name);
  couchbase::core::operations::management::bucket_get_request req{ bucket_name };
  auto resp = test::utils::execute(integration.cluster, req);
  return resp;
}

static bool
wait_for_function_reach_status(test::utils::integration_test_guard& integration,
                               const std::string& function_name,
                               const std::optional<std::string>& bucket_name,
                               const std::optional<std::string>& scope_name,
                               couchbase::core::management::eventing::function_status status)
{
  return test::utils::wait_until(
    [&integration, function_name, bucket_name, scope_name, status]() {
      couchbase::core::operations::management::eventing_get_status_request req{ bucket_name,
                                                                                scope_name };
      auto resp = test::utils::execute(integration.cluster, req);
      if (resp.ctx.ec) {
        return false;
      }
      auto function = std::find_if(resp.status.functions.begin(),
                                   resp.status.functions.end(),
                                   [function_name](const auto& fun) {
                                     return function_name == fun.name;
                                   });
      if (function == resp.status.functions.end()) {
        return false;
      }
      return function->status == status;
    },
    std::chrono::minutes(3));
}

static void
run_core_eventing_management_lifecycle_test(test::utils::integration_test_guard& integration,
                                            std::optional<std::string> bucket_name,
                                            std::optional<std::string> scope_name)
{
  auto function_name = test::utils::uniq_id("name");

  {
    couchbase::core::operations::management::eventing_drop_function_request req{ function_name,
                                                                                 bucket_name,
                                                                                 scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    if (integration.cluster_version().is_cheshire_cat()) {
      REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_deployed);
    } else {
      REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
    }
  }

  {
    couchbase::core::operations::management::eventing_get_function_request req{ function_name,
                                                                                bucket_name,
                                                                                scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
  }

  auto meta_bucket_name = test::utils::uniq_id("meta");
  {

    couchbase::core::management::cluster::bucket_settings bucket_settings;
    bucket_settings.name = meta_bucket_name;
    bucket_settings.ram_quota_mb = 256;

    {
      couchbase::core::operations::management::bucket_create_request req;
      req.bucket = bucket_settings;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }
  }

  {
    auto resp = wait_for_bucket_created(integration, meta_bucket_name);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  std::string source_code = R"(
function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}
)";

  INFO(fmt::format("function_name: {}\nbucket_name: {}\nscope_name: {}",
                   function_name,
                   bucket_name.value_or("(not specified)"),
                   scope_name.value_or("(not specified)")));

  {
    couchbase::core::operations::management::eventing_upsert_function_request req{};
    req.bucket_name = bucket_name;
    req.scope_name = scope_name;
    req.function.source_keyspace.bucket = integration.ctx.bucket;
    req.function.metadata_keyspace.bucket = meta_bucket_name;
    req.function.name = function_name;
    req.function.code = source_code;
    req.function.settings.handler_headers = { "// generated by Couchbase C++ SDK" };
    req.function.constant_bindings.emplace_back(
      couchbase::core::management::eventing::function_constant_binding{ "PI", "3.14" });
    req.function.bucket_bindings.emplace_back(
      couchbase::core::management::eventing::function_bucket_binding{
        "data",
        { integration.ctx.bucket },
        couchbase::core::management::eventing::function_bucket_access::read_write });
    req.function.url_bindings.emplace_back(
      couchbase::core::management::eventing::function_url_binding{ "home",
                                                                   "https://couchbase.com" });
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  {
    REQUIRE(test::utils::wait_for_function_created(
      integration.cluster, function_name, bucket_name, scope_name));
    auto resp =
      test::utils::execute(integration.cluster,
                           couchbase::core::operations::management::eventing_get_function_request{
                             function_name,
                             bucket_name,
                             scope_name,
                           });
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  {
    couchbase::core::operations::management::eventing_get_all_functions_request req{ bucket_name,
                                                                                     scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto function =
      std::find_if(resp.functions.begin(), resp.functions.end(), [&function_name](const auto& fun) {
        return function_name == fun.name;
      });
    REQUIRE(function != resp.functions.end());
    REQUIRE(function->code == source_code);
    REQUIRE(function->source_keyspace.bucket == integration.ctx.bucket);
    REQUIRE(function->metadata_keyspace.bucket == meta_bucket_name);
    REQUIRE(function->settings.deployment_status ==
            couchbase::core::management::eventing::function_deployment_status::undeployed);
    REQUIRE(function->settings.processing_status ==
            couchbase::core::management::eventing::function_processing_status::paused);
    REQUIRE(!function->settings.handler_headers.empty());
    REQUIRE(function->settings.handler_headers[0] == "// generated by Couchbase C++ SDK");
    REQUIRE(!function->constant_bindings.empty());
    REQUIRE(function->constant_bindings[0].alias == "PI");
    REQUIRE(function->constant_bindings[0].literal == "3.14");
    REQUIRE(!function->bucket_bindings.empty());
    REQUIRE(function->bucket_bindings[0].alias == "data");
    REQUIRE(function->bucket_bindings[0].name.bucket == "default");
    REQUIRE(function->bucket_bindings[0].access ==
            couchbase::core::management::eventing::function_bucket_access::read_write);
    REQUIRE(!function->url_bindings.empty());
    REQUIRE(function->url_bindings[0].alias == "home");
    REQUIRE(function->url_bindings[0].hostname == "https://couchbase.com");
    REQUIRE(std::holds_alternative<couchbase::core::management::eventing::function_url_no_auth>(
      function->url_bindings[0].auth));
  }

  {
    couchbase::core::operations::management::eventing_get_status_request req{ bucket_name,
                                                                              scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.status.num_eventing_nodes > 0);
    auto function = std::find_if(resp.status.functions.begin(),
                                 resp.status.functions.end(),
                                 [&function_name](const auto& fun) {
                                   return function_name == fun.name;
                                 });
    REQUIRE(function != resp.status.functions.end());
    REQUIRE(function->status == couchbase::core::management::eventing::function_status::undeployed);
    REQUIRE(function->deployment_status ==
            couchbase::core::management::eventing::function_deployment_status::undeployed);
    REQUIRE(function->processing_status ==
            couchbase::core::management::eventing::function_processing_status::paused);
  }

  {
    couchbase::core::operations::management::eventing_undeploy_function_request req{ function_name,
                                                                                     bucket_name,
                                                                                     scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_deployed);
  }

  {
    couchbase::core::operations::management::eventing_deploy_function_request req{ function_name,
                                                                                   bucket_name,
                                                                                   scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  REQUIRE(wait_for_function_reach_status(
    integration,
    function_name,
    bucket_name,
    scope_name,
    couchbase::core::management::eventing::function_status::deployed));

  {
    couchbase::core::operations::management::eventing_drop_function_request req{ function_name,
                                                                                 bucket_name,
                                                                                 scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_deployed);
  }

  {
    couchbase::core::operations::management::eventing_resume_function_request req{ function_name,
                                                                                   bucket_name,
                                                                                   scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_deployed);
  }

  {
    couchbase::core::operations::management::eventing_pause_function_request req{ function_name,
                                                                                  bucket_name,
                                                                                  scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  REQUIRE(
    wait_for_function_reach_status(integration,
                                   function_name,
                                   bucket_name,
                                   scope_name,
                                   couchbase::core::management::eventing::function_status::paused));

  {
    couchbase::core::operations::management::eventing_pause_function_request req{ function_name,
                                                                                  bucket_name,
                                                                                  scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_paused);
  }

  {
    couchbase::core::operations::management::eventing_resume_function_request req{ function_name,
                                                                                   bucket_name,
                                                                                   scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  REQUIRE(wait_for_function_reach_status(
    integration,
    function_name,
    bucket_name,
    scope_name,
    couchbase::core::management::eventing::function_status::deployed));

  {
    couchbase::core::operations::management::eventing_undeploy_function_request req{ function_name,
                                                                                     bucket_name,
                                                                                     scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  REQUIRE(wait_for_function_reach_status(
    integration,
    function_name,
    bucket_name,
    scope_name,
    couchbase::core::management::eventing::function_status::undeployed));

  {
    couchbase::core::operations::management::eventing_drop_function_request req{ function_name,
                                                                                 bucket_name,
                                                                                 scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  {
    auto function_not_found = test::utils::wait_until([&]() {
      auto resp =
        test::utils::execute(integration.cluster,
                             couchbase::core::operations::management::eventing_get_function_request{
                               function_name,
                               bucket_name,
                               scope_name,
                             });
      return resp.ctx.ec == couchbase::errc::management::eventing_function_not_found;
    });
    REQUIRE(function_not_found);
  }

  {
    couchbase::core::operations::management::eventing_get_function_request req{ function_name,
                                                                                bucket_name,
                                                                                scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
  }

  {
    couchbase::core::operations::management::bucket_drop_request req{ meta_bucket_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }
}

TEST_CASE("integration: eventing functions management", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_eventing_functions()) {
    SKIP("cluster does not support eventing service");
  }
  if (!integration.has_eventing_service()) {
    SKIP("cluster does not have eventing service");
  }

  if (!integration.cluster_version().supports_gcccp()) {
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }

  SECTION("lifecycle")
  {
    run_core_eventing_management_lifecycle_test(integration, {}, {});
  }
}

TEST_CASE("integration: scoped eventing functions management", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_scoped_eventing_functions()) {
    SKIP("cluster does not support scoped eventing functions");
  }
  if (!integration.has_eventing_service()) {
    SKIP("cluster does not have eventing service");
  }

  if (!integration.cluster_version().supports_gcccp()) {
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }

  SECTION("lifecycle")
  {
    run_core_eventing_management_lifecycle_test(integration, integration.ctx.bucket, "_default");
  }

  SECTION("filtering for get_all_functions and get_status")
  {
    auto admin_function_name = test::utils::uniq_id("admin");
    auto scoped_function_name = test::utils::uniq_id("scoped");

    auto meta_bucket_name = test::utils::uniq_id("meta");
    {

      couchbase::core::management::cluster::bucket_settings bucket_settings;
      bucket_settings.name = meta_bucket_name;
      bucket_settings.ram_quota_mb = 256;

      {
        couchbase::core::operations::management::bucket_create_request req;
        req.bucket = bucket_settings;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }
    }

    {
      auto resp = wait_for_bucket_created(integration, meta_bucket_name);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    std::string source_code = R"(
function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}
)";

    // Upsert a function in the admin function scope (unset bucket/scope names)
    {
      couchbase::core::operations::management::eventing_upsert_function_request req{};
      req.function.source_keyspace.bucket = integration.ctx.bucket;
      req.function.metadata_keyspace.bucket = meta_bucket_name;
      req.function.name = admin_function_name;
      req.function.code = source_code;
      req.function.settings.handler_headers = { "// generated by Couchbase C++ SDK" };
      req.function.constant_bindings.emplace_back(
        couchbase::core::management::eventing::function_constant_binding{ "PI", "3.14" });
      req.function.bucket_bindings.emplace_back(
        couchbase::core::management::eventing::function_bucket_binding{
          "data",
          { integration.ctx.bucket },
          couchbase::core::management::eventing::function_bucket_access::read_write });
      req.function.url_bindings.emplace_back(
        couchbase::core::management::eventing::function_url_binding{ "home",
                                                                     "https://couchbase.com" });
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      REQUIRE(test::utils::wait_for_function_created(integration.cluster, admin_function_name));
      auto resp =
        test::utils::execute(integration.cluster,
                             couchbase::core::operations::management::eventing_get_function_request{
                               admin_function_name,
                             });
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    // Upsert a scoped function
    // Upsert a function in the admin function scope (unset bucket/scope names)
    {
      couchbase::core::operations::management::eventing_upsert_function_request req{};
      req.bucket_name = integration.ctx.bucket;
      req.scope_name = "_default";
      req.function.source_keyspace.bucket = integration.ctx.bucket;
      req.function.metadata_keyspace.bucket = meta_bucket_name;
      req.function.name = scoped_function_name;
      req.function.code = source_code;
      req.function.settings.handler_headers = { "// generated by Couchbase C++ SDK" };
      req.function.constant_bindings.emplace_back(
        couchbase::core::management::eventing::function_constant_binding{ "PI", "3.14" });
      req.function.bucket_bindings.emplace_back(
        couchbase::core::management::eventing::function_bucket_binding{
          "data",
          { integration.ctx.bucket },
          couchbase::core::management::eventing::function_bucket_access::read_write });
      req.function.url_bindings.emplace_back(
        couchbase::core::management::eventing::function_url_binding{ "home",
                                                                     "https://couchbase.com" });
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      REQUIRE(test::utils::wait_for_function_created(
        integration.cluster, scoped_function_name, integration.ctx.bucket, "_default"));
      auto resp =
        test::utils::execute(integration.cluster,
                             couchbase::core::operations::management::eventing_get_function_request{
                               scoped_function_name, integration.ctx.bucket, "_default" });
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      couchbase::core::operations::management::eventing_get_all_functions_request req{
        integration.ctx.bucket, "_default"
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);

      // The scoped function should be in the results of a scoped get_all_functions
      {
        auto function = std::find_if(
          resp.functions.begin(), resp.functions.end(), [&scoped_function_name](const auto& fun) {
            return scoped_function_name == fun.name;
          });
        REQUIRE(function != resp.functions.end());
      }

      // The admin function should not be in the results of a scoped get_all_functions
      {
        auto function = std::find_if(
          resp.functions.begin(), resp.functions.end(), [&admin_function_name](const auto& fun) {
            return admin_function_name == fun.name;
          });
        REQUIRE(function == resp.functions.end());
      }
    }

    {
      couchbase::core::operations::management::eventing_get_all_functions_request req{};
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);

      // The scoped function should not be in the results of a non-scoped get_all_functions
      {
        auto function = std::find_if(
          resp.functions.begin(), resp.functions.end(), [&scoped_function_name](const auto& fun) {
            return scoped_function_name == fun.name;
          });
        REQUIRE(function == resp.functions.end());
      }

      // The admin function should be in the results of a non-scoped get_all_functions
      {
        auto function = std::find_if(
          resp.functions.begin(), resp.functions.end(), [&admin_function_name](const auto& fun) {
            return admin_function_name == fun.name;
          });
        REQUIRE(function != resp.functions.end());
      }
    }

    {
      couchbase::core::operations::management::eventing_get_status_request req{
        integration.ctx.bucket, "_default"
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);

      // The scoped function should be in the results of a scoped get_status
      {
        auto function = std::find_if(resp.status.functions.begin(),
                                     resp.status.functions.end(),
                                     [&scoped_function_name](const auto& fun) {
                                       return scoped_function_name == fun.name;
                                     });
        REQUIRE(function != resp.status.functions.end());
      }

      // The admin function should not be in the results of a scoped get_status
      {
        auto function = std::find_if(resp.status.functions.begin(),
                                     resp.status.functions.end(),
                                     [&admin_function_name](const auto& fun) {
                                       return admin_function_name == fun.name;
                                     });
        REQUIRE(function == resp.status.functions.end());
      }
    }

    {
      couchbase::core::operations::management::eventing_get_status_request req{};
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);

      // The scoped function should not be in the results of a non-scoped get_status
      {
        auto function = std::find_if(resp.status.functions.begin(),
                                     resp.status.functions.end(),
                                     [&scoped_function_name](const auto& fun) {
                                       return scoped_function_name == fun.name;
                                     });
        REQUIRE(function == resp.status.functions.end());
      }

      // The admin function should be in the results of a non-scoped get_status
      {
        auto function = std::find_if(resp.status.functions.begin(),
                                     resp.status.functions.end(),
                                     [&admin_function_name](const auto& fun) {
                                       return admin_function_name == fun.name;
                                     });
        REQUIRE(function != resp.status.functions.end());
      }
    }

    {
      couchbase::core::operations::management::eventing_drop_function_request req{
        scoped_function_name, integration.ctx.bucket, "_default"
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      couchbase::core::operations::management::eventing_drop_function_request req{
        admin_function_name
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      couchbase::core::operations::management::bucket_drop_request req{ meta_bucket_name };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }
  }
}
