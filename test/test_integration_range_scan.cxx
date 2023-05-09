/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/agent_group.hxx"
#include "core/agent_unit_test_api.hxx"
#include "core/collections_component_unit_test_api.hxx"
#include "core/range_scan_orchestrator.hxx"
#include "core/scan_options.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>

#include <chrono>

static auto
populate_documents_for_range_scan(const couchbase::collection& collection,
                                  const std::vector<std::string>& ids,
                                  const std::vector<std::byte>& value,
                                  std::optional<std::chrono::seconds> expiry = {})
{
    couchbase::upsert_options options{};
    if (expiry) {
        options.expiry(expiry.value());
    }

    std::map<std::vector<std::byte>, couchbase::mutation_token> mutations;
    for (const auto& id : ids) {
        auto [ctx, resp] = collection.upsert<couchbase::codec::raw_binary_transcoder>(id, value, options).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(resp.mutation_token().has_value());
        mutations[couchbase::core::utils::to_binary(id)] = resp.mutation_token().value();
    }
    return mutations;
}

std::vector<couchbase::core::range_scan_item>
do_range_scan(couchbase::core::agent agent,
              std::uint16_t vbucket_id,
              const couchbase::core::range_scan_create_options& create_options,
              const couchbase::core::range_scan_continue_options& continue_options)
{
    std::vector<std::byte> scan_uuid;

    {
        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent.range_scan_create(vbucket_id, create_options, [barrier](auto res, auto error) {
            barrier->set_value({ std::move(res), error });
        });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        REQUIRE_SUCCESS(ec);
        REQUIRE_FALSE(res.scan_uuid.empty());
        scan_uuid = res.scan_uuid;
    }

    std::vector<couchbase::core::range_scan_item> data;

    auto options = continue_options;
    options.ids_only = create_options.ids_only; // support servers before MB-54267. TODO: remove after server GA

    do {
        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent.range_scan_continue(
          scan_uuid,
          vbucket_id,
          options,
          [&data](auto item) { data.emplace_back(std::move(item)); },
          [barrier](auto res, auto error) {
              barrier->set_value({ std::move(res), error });
          });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        REQUIRE_SUCCESS(ec);

        if (res.complete) {
            break;
        }
    } while (true);

    REQUIRE_FALSE(data.empty());

    return data;
}

static auto
make_binary_value(std::size_t number_of_bytes)
{
    std::vector<std::byte> value(number_of_bytes);
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }
    return value;
}

TEST_CASE("integration: range scan large values", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto value = make_binary_value(16'384);

    std::vector<std::string> ids{
        "largevalues-2960", "largevalues-3064", "largevalues-3686", "largevalues-3716", "largevalues-5354",
        "largevalues-5426", "largevalues-6175", "largevalues-6607", "largevalues-6797", "largevalues-7871",
    };
    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan_create_options create_options{
        couchbase::scope::default_name,
        couchbase::collection::default_name,
        couchbase::core::range_scan{
          { couchbase::core::utils::to_binary("largevalues") },
          { couchbase::core::utils::to_binary("largevalues\xff") },
        },
    };
    create_options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
        highest_mutation->second.partition_uuid(),
        highest_mutation->second.sequence_number(),
    };

    couchbase::core::range_scan_continue_options continue_options{};
    continue_options.batch_time_limit = std::chrono::seconds{ 10 };

    auto data = do_range_scan(agent.value(), 12, create_options, continue_options);
    REQUIRE_FALSE(data.empty());
    for (const auto& item : data) {
        REQUIRE(item.body.has_value());
        REQUIRE(item.body->value == value);
        auto it = mutations.find(item.key);
        REQUIRE(it != mutations.end());
        REQUIRE(it->second.sequence_number() == item.body->sequence_number);
    }
}

TEST_CASE("integration: range scan small values", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangesmallvalues-1023", "rangesmallvalues-1751", "rangesmallvalues-2202", "rangesmallvalues-2392", "rangesmallvalues-2570",
        "rangesmallvalues-4132", "rangesmallvalues-4640", "rangesmallvalues-5836", "rangesmallvalues-7283", "rangesmallvalues-7313",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan_create_options create_options{
        couchbase::scope::default_name,
        couchbase::collection::default_name,
        couchbase::core::range_scan{
          { couchbase::core::utils::to_binary("rangesmallvalues") },
          { couchbase::core::utils::to_binary("rangesmallvalues\xff") },
        },
    };
    create_options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
        highest_mutation->second.partition_uuid(),
        highest_mutation->second.sequence_number(),
    };

    couchbase::core::range_scan_continue_options continue_options{};
    continue_options.batch_time_limit = std::chrono::seconds{ 10 };

    auto data = do_range_scan(agent.value(), 12, create_options, continue_options);
    REQUIRE_FALSE(data.empty());
    for (const auto& item : data) {
        REQUIRE(item.body.has_value());
        REQUIRE(item.body->value == value);
        auto it = mutations.find(item.key);
        REQUIRE(it != mutations.end());
        REQUIRE(it->second.sequence_number() == item.body->sequence_number);
    }
}

class collection_guard
{
  public:
    explicit collection_guard(test::utils::integration_test_guard& integration)
      : integration_{ integration }
    {
        auto resp = test::utils::execute(integration_.cluster,
                                         couchbase::core::operations::management::collection_create_request{
                                           integration_.ctx.bucket,
                                           couchbase::scope::default_name,
                                           collection_name_,
                                         });
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(test::utils::wait_until_collection_manifest_propagated(integration_.cluster, integration_.ctx.bucket, resp.uid));
    }

    ~collection_guard()
    {
        auto resp = test::utils::execute(integration_.cluster,
                                         couchbase::core::operations::management::collection_drop_request{
                                           integration_.ctx.bucket,
                                           couchbase::scope::default_name,
                                           collection_name_,
                                         });
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(test::utils::wait_until_collection_manifest_propagated(integration_.cluster, integration_.ctx.bucket, resp.uid));
    }

    [[nodiscard]] auto name() const -> const std::string&
    {
        return collection_name_;
    }

  private:
    test::utils::integration_test_guard& integration_;
    std::string collection_name_{ test::utils::uniq_id("collection") };
};

TEST_CASE("integration: range scan collection retry", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    collection_guard new_collection(integration);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(new_collection.name());

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangecollectionretry-9695",   "rangecollectionretry-24520",  "rangecollectionretry-90825",  "rangecollectionretry-119677",
        "rangecollectionretry-150939", "rangecollectionretry-170176", "rangecollectionretry-199557", "rangecollectionretry-225568",
        "rangecollectionretry-231302", "rangecollectionretry-245898",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    // we're going to force a refresh, so we need to delete the collection from our cache.
    agent->unit_test_api().collections().remove_collection_from_cache(couchbase::scope::default_name, new_collection.name());

    couchbase::core::range_scan_create_options create_options{
        couchbase::scope::default_name,
        new_collection.name(),
        couchbase::core::range_scan{
          { couchbase::core::utils::to_binary("rangecollectionretry") },
          { couchbase::core::utils::to_binary("rangecollectionretry\xff") },
        },
    };
    create_options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
        highest_mutation->second.partition_uuid(),
        highest_mutation->second.sequence_number(),
    };

    couchbase::core::range_scan_continue_options continue_options{};
    continue_options.batch_time_limit = std::chrono::seconds{ 10 };

    auto data = do_range_scan(agent.value(), 12, create_options, continue_options);
    REQUIRE_FALSE(data.empty());
    for (const auto& item : data) {
        REQUIRE(item.body.has_value());
        REQUIRE(item.body->value == value);
        auto it = mutations.find(item.key);
        REQUIRE(it != mutations.end());
        REQUIRE(it->second.sequence_number() == item.body->sequence_number);
    }
}

TEST_CASE("integration: range scan only keys", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangekeysonly-1269", "rangekeysonly-2048",  "rangekeysonly-4378",  "rangekeysonly-7159",  "rangekeysonly-8898",
        "rangekeysonly-8908", "rangekeysonly-19559", "rangekeysonly-20808", "rangekeysonly-20998", "rangekeysonly-25889",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan_create_options create_options{
        couchbase::scope::default_name,
        couchbase::collection::default_name,
        couchbase::core::range_scan{
          { couchbase::core::utils::to_binary("rangekeysonly") },
          { couchbase::core::utils::to_binary("rangekeysonly\xff") },
        },
    };
    create_options.ids_only = true;
    create_options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
        highest_mutation->second.partition_uuid(),
        highest_mutation->second.sequence_number(),
    };

    couchbase::core::range_scan_continue_options continue_options{};
    continue_options.batch_time_limit = std::chrono::seconds{ 10 };

    auto data = do_range_scan(agent.value(), 12, create_options, continue_options);
    REQUIRE_FALSE(data.empty());
    for (const auto& item : data) {
        REQUIRE_FALSE(item.body.has_value());
        auto it = mutations.find(item.key);
        REQUIRE(it != mutations.end());
    }
}

TEST_CASE("integration: range scan cancellation before continue", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",  "rangescancancel-116036", "rangescancancel-136879",
        "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428", "rangescancancel-257242",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    const std::uint16_t vbucket_id{ 12 };
    std::vector<std::byte> scan_uuid;

    {
        couchbase::core::range_scan_create_options options{
            couchbase::scope::default_name,
            couchbase::collection::default_name,
            couchbase::core::range_scan{
              { couchbase::core::utils::to_binary("rangescancancel") },
              { couchbase::core::utils::to_binary("rangescancancel\xff") },
            },
        };
        options.ids_only = true;
        options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
            highest_mutation->second.partition_uuid(),
            highest_mutation->second.sequence_number(),
        };

        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_create(vbucket_id, options, [barrier](auto res, auto ec) {
            barrier->set_value({ std::move(res), ec });
        });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        REQUIRE_SUCCESS(ec);
        REQUIRE_FALSE(res.scan_uuid.empty());
        scan_uuid = res.scan_uuid;
    }

    {
        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_cancel_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_cancel(scan_uuid, vbucket_id, {}, [barrier](auto res, auto ec) {
            barrier->set_value({ std::move(res), ec });
        });
        EXPECT_SUCCESS(op);

        auto [resp, ec] = f.get();
        REQUIRE_SUCCESS(ec);
    }

    couchbase::core::range_scan_continue_options options{};
    options.batch_time_limit = std::chrono::seconds{ 10 };
    options.ids_only = true; // support servers before MB-54267. TODO: remove after server GA

    bool items_callback_invoked{ false };
    {
        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_continue(
          scan_uuid,
          vbucket_id,
          options,
          [&items_callback_invoked](auto) { items_callback_invoked = true; },
          [barrier](auto res, auto ec) {
              barrier->set_value({ std::move(res), ec });
          });
        EXPECT_SUCCESS(op);

        auto [resp, ec] = f.get();
        REQUIRE(ec == couchbase::errc::key_value::document_not_found);
    }

    REQUIRE_FALSE(items_callback_invoked);
}

TEST_CASE("integration: range scan cancel during streaming using protocol cancel", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",  "rangescancancel-116036", "rangescancancel-136879",
        "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428", "rangescancancel-257242",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    const std::uint16_t vbucket_id{ 12 };
    std::vector<std::byte> scan_uuid;

    {
        couchbase::core::range_scan_create_options options{
            couchbase::scope::default_name,
            couchbase::collection::default_name,
            couchbase::core::range_scan{
              { couchbase::core::utils::to_binary("rangescancancel") },
              { couchbase::core::utils::to_binary("rangescancancel\xff") },
            },
        };
        options.ids_only = true;
        options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
            highest_mutation->second.partition_uuid(),
            highest_mutation->second.sequence_number(),
        };

        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_create(vbucket_id, options, [barrier](auto res, auto ec) {
            barrier->set_value({ std::move(res), ec });
        });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        REQUIRE_SUCCESS(ec);
        REQUIRE_FALSE(res.scan_uuid.empty());
        scan_uuid = res.scan_uuid;
    }

    auto execute_protocol_cancel = [agent, scan_uuid, vbid = vbucket_id]() {
        auto op = agent->range_scan_cancel(scan_uuid, vbid, {}, [](auto /* res */, auto ec) { REQUIRE_SUCCESS(ec); });
        EXPECT_SUCCESS(op);
    };

    std::vector<couchbase::core::range_scan_item> data;
    std::size_t iteration = 0;

    do {
        ++iteration;

        couchbase::core::range_scan_continue_options options{};
        options.batch_time_limit = std::chrono::seconds{ 10 };
        options.batch_item_limit = 3; // limit batch to 3 items, while range expected to be larger
        options.ids_only = true;      // support servers before MB-54267. TODO: remove after server GA

        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_continue(
          scan_uuid,
          vbucket_id,
          options,
          [&data, do_cancel = execute_protocol_cancel](auto item) {
              if (data.empty()) {
                  do_cancel(); // cancel scan after first document, but continue scanning
              }
              data.emplace_back(std::move(item));
          },
          [barrier](auto res, auto ec) {
              barrier->set_value({ std::move(res), ec });
          });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        if (iteration == 1) {
            REQUIRE(res.complete == false);
            REQUIRE_SUCCESS(ec);
        } else {
            REQUIRE(ec == couchbase::errc::key_value::document_not_found); // scan has been cancelled
            break;
        }
    } while (true);

    REQUIRE(data.size() == 3);
}

TEST_CASE("integration: range scan cancel during streaming using pending_operation cancel", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",  "rangescancancel-116036", "rangescancancel-136879",
        "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428", "rangescancancel-257242",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    const std::uint16_t vbucket_id{ 12 };
    std::vector<std::byte> scan_uuid;

    {
        couchbase::core::range_scan_create_options options{
            couchbase::scope::default_name,
            couchbase::collection::default_name,
            couchbase::core::range_scan{
              { couchbase::core::utils::to_binary("rangescancancel") },
              { couchbase::core::utils::to_binary("rangescancancel\xff") },
            },
        };
        options.ids_only = true;
        options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
            highest_mutation->second.partition_uuid(),
            highest_mutation->second.sequence_number(),
        };

        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_create(vbucket_id, options, [barrier](auto res, auto ec) {
            barrier->set_value({ std::move(res), ec });
        });
        EXPECT_SUCCESS(op);

        auto [res, ec] = f.get();
        REQUIRE_SUCCESS(ec);
        REQUIRE_FALSE(res.scan_uuid.empty());
        scan_uuid = res.scan_uuid;
    }

    std::shared_ptr<couchbase::core::pending_operation> operation_holder{};

    auto execute_operation_cancel = [&operation_holder]() { operation_holder->cancel(); };

    std::vector<couchbase::core::range_scan_item> data;

    {
        couchbase::core::range_scan_continue_options options{};
        options.batch_time_limit = std::chrono::seconds{ 10 };
        options.batch_item_limit = 3; // limit batch to 3 items, while range expected to be larger
        options.ids_only = true;      // support servers before MB-54267. TODO: remove after server GA

        auto barrier = std::make_shared<std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
        auto f = barrier->get_future();

        auto op = agent->range_scan_continue(
          scan_uuid,
          vbucket_id,
          options,
          [&data, do_cancel = execute_operation_cancel](auto item) {
              data.emplace_back(std::move(item));
              do_cancel(); // cancel operation after first document
          },
          [barrier](auto res, auto ec) {
              barrier->set_value({ std::move(res), ec });
          });
        EXPECT_SUCCESS(op);
        std::swap(operation_holder, op.value()); // store the operation for cancellation

        auto [res, ec] = f.get();
        REQUIRE(res.complete == false);
        REQUIRE(ec == couchbase::errc::common::request_canceled);
    }

    REQUIRE(data.size() >= 1);
}

TEST_CASE("integration: sampling scan keys only", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
    for (std::size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<std::byte>(i);
    }

    std::vector<std::string> ids{
        "samplescankeys-170",  "samplescankeys-602",   "samplescankeys-792",   "samplescankeys-3978",  "samplescankeys-6869",
        "samplescankeys-9038", "samplescankeys-10806", "samplescankeys-10996", "samplescankeys-11092", "samplescankeys-11102",
    };

    auto mutations = populate_documents_for_range_scan(collection, ids, value);

    auto highest_mutation = std::max_element(
      mutations.begin(), mutations.end(), [](auto a, auto b) { return a.second.sequence_number() < b.second.sequence_number(); });

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan_create_options create_options{
        couchbase::scope::default_name,
        couchbase::collection::default_name,
        couchbase::core::sampling_scan{ 10 },
    };
    create_options.ids_only = true;
    create_options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
        highest_mutation->second.partition_uuid(),
        highest_mutation->second.sequence_number(),
    };

    couchbase::core::range_scan_continue_options continue_options{};
    continue_options.batch_time_limit = std::chrono::seconds{ 10 };

    auto data = do_range_scan(agent.value(), 12, create_options, continue_options);
    REQUIRE_FALSE(data.empty());
}

static auto
make_doc_ids(std::size_t number_of_keys, const std::string& prefix)
{
    std::vector<std::string> keys{ number_of_keys };
    for (std::size_t i = 0; i < number_of_keys; ++i) {
        keys[i] = prefix + std::to_string(i);
    }
    return keys;
}

static auto
mutations_to_mutation_state(std::map<std::vector<std::byte>, couchbase::mutation_token> mutations)
{
    couchbase::core::mutation_state state;
    for (const auto& [key, token] : mutations) {
        state.tokens.emplace_back(token);
    }
    return state;
}

TEST_CASE("integration: manager scan range without content", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto ids = make_doc_ids(100, "rangescanwithoutcontent-");
    auto value = make_binary_value(1);
    auto mutations = populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

    auto barrier = std::make_shared<std::promise<tl::expected<std::size_t, std::error_code>>>();
    auto f = barrier->get_future();
    integration.cluster->with_bucket_configuration(
      integration.ctx.bucket, [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
          if (ec) {
              return barrier->set_value(tl::unexpected(ec));
          }
          if (!config.vbmap || config.vbmap->empty()) {
              return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
          }
          barrier->set_value(config.vbmap->size());
      });
    auto number_of_vbuckets = f.get();
    EXPECT_SUCCESS(number_of_vbuckets);

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan scan{ "rangescanwithoutcontent", "rangescanwithoutcontent\xff" };
    couchbase::core::range_scan_orchestrator_options options{};
    options.consistent_with = mutations_to_mutation_state(mutations);
    options.ids_only = true;
    couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                          agent.value(),
                                                          number_of_vbuckets.value(),
                                                          couchbase::scope::default_name,
                                                          couchbase::collection::default_name,
                                                          scan,
                                                          options);

    auto result = orchestrator.scan();
    EXPECT_SUCCESS(result);

    std::set<std::vector<std::byte>> entry_ids{};

    do {
        auto entry = result->next();
        if (!entry) {
            break;
        }

        auto [_, inserted] = entry_ids.insert(entry->key);
        REQUIRE(inserted);
        REQUIRE_FALSE(entry->body.has_value());
    } while (true);

    for (const auto& id : ids) {
        REQUIRE(entry_ids.count(couchbase::core::utils::to_binary(id)) == 1);
    }
    REQUIRE(ids.size() == entry_ids.size());
}

TEST_CASE("integration: manager scan range with content", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto ids = make_doc_ids(100, "rangescanwithcontent-");
    auto value = make_binary_value(100);
    auto mutations = populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

    auto barrier = std::make_shared<std::promise<tl::expected<std::size_t, std::error_code>>>();
    auto f = barrier->get_future();
    integration.cluster->with_bucket_configuration(
      integration.ctx.bucket, [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
          if (ec) {
              return barrier->set_value(tl::unexpected(ec));
          }
          if (!config.vbmap || config.vbmap->empty()) {
              return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
          }
          barrier->set_value(config.vbmap->size());
      });
    auto number_of_vbuckets = f.get();
    EXPECT_SUCCESS(number_of_vbuckets);

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan scan{ "rangescanwithcontent", "rangescanwithcontent\xff" };
    couchbase::core::range_scan_orchestrator_options options{};
    options.consistent_with = mutations_to_mutation_state(mutations);
    couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                          agent.value(),
                                                          number_of_vbuckets.value(),
                                                          couchbase::scope::default_name,
                                                          couchbase::collection::default_name,
                                                          scan,
                                                          options);

    auto result = orchestrator.scan();
    EXPECT_SUCCESS(result);

    std::set<std::vector<std::byte>> entry_ids{};

    do {
        auto entry = result->next();
        if (!entry) {
            break;
        }

        auto [_, inserted] = entry_ids.insert(entry->key);
        REQUIRE(inserted);
        REQUIRE(entry->body.has_value());
    } while (true);

    for (const auto& id : ids) {
        INFO(id);
        REQUIRE(entry_ids.count(couchbase::core::utils::to_binary(id)) == 1);
    }
    REQUIRE(ids.size() == entry_ids.size());
}

TEST_CASE("integration: manager sampling scan with custom collection", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    collection_guard new_collection(integration);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(new_collection.name());

    auto ids = make_doc_ids(100, "samplingscan-");
    auto value = make_binary_value(100);
    auto mutations = populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

    auto barrier = std::make_shared<std::promise<tl::expected<std::size_t, std::error_code>>>();
    auto f = barrier->get_future();
    integration.cluster->with_bucket_configuration(
      integration.ctx.bucket, [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
          if (ec) {
              return barrier->set_value(tl::unexpected(ec));
          }
          if (!config.vbmap || config.vbmap->empty()) {
              return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
          }
          barrier->set_value(config.vbmap->size());
      });
    auto number_of_vbuckets = f.get();
    EXPECT_SUCCESS(number_of_vbuckets);

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::sampling_scan scan{ 10, 50 };
    couchbase::core::range_scan_orchestrator_options options{};
    options.consistent_with = mutations_to_mutation_state(mutations);
    couchbase::core::range_scan_orchestrator orchestrator(
      integration.io, agent.value(), number_of_vbuckets.value(), couchbase::scope::default_name, new_collection.name(), scan, options);

    auto result = orchestrator.scan();
    EXPECT_SUCCESS(result);

    std::set<std::vector<std::byte>> entry_ids{};

    auto now = std::chrono::system_clock::now();
    do {
        auto entry = result->next();
        if (!entry) {
            break;
        }

        REQUIRE(entry->body);
        REQUIRE_FALSE(entry->body->cas.empty());
        REQUIRE(entry->body->value == value);
        REQUIRE(entry->body->expiry_time() > now);

        auto [_, inserted] = entry_ids.insert(entry->key);
        REQUIRE(inserted);
    } while (true);

    REQUIRE(ids.size() >= 10);

    for (const auto& id : entry_ids) {
        REQUIRE(std::find(ids.begin(), ids.end(), std::string(reinterpret_cast<const char*>(id.data()), id.size())) != ids.end());
    }
}

TEST_CASE("integration: manager range scan with sort", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.has_bucket_capability("range_scan")) {
        SKIP("cluster does not support range_scan");
    }

    collection_guard new_collection(integration);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(new_collection.name());

    auto ids = make_doc_ids(100, "rangescansort-");
    auto value = make_binary_value(100);
    auto mutations = populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

    auto barrier = std::make_shared<std::promise<tl::expected<std::size_t, std::error_code>>>();
    auto f = barrier->get_future();
    integration.cluster->with_bucket_configuration(
      integration.ctx.bucket, [barrier](std::error_code ec, const couchbase::core::topology::configuration& config) mutable {
          if (ec) {
              return barrier->set_value(tl::unexpected(ec));
          }
          if (!config.vbmap || config.vbmap->empty()) {
              return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
          }
          barrier->set_value(config.vbmap->size());
      });
    auto number_of_vbuckets = f.get();
    EXPECT_SUCCESS(number_of_vbuckets);

    auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
    ag.open_bucket(integration.ctx.bucket);
    auto agent = ag.get_agent(integration.ctx.bucket);
    REQUIRE(agent.has_value());

    couchbase::core::range_scan scan{ "rangescansort", "rangescansort\xff" };
    couchbase::core::range_scan_orchestrator_options options{};
    options.consistent_with = mutations_to_mutation_state(mutations);
    options.ids_only = true;
    options.sort = couchbase::core::scan_sort::ascending;
    couchbase::core::range_scan_orchestrator orchestrator(
      integration.io, agent.value(), number_of_vbuckets.value(), couchbase::scope::default_name, new_collection.name(), scan, options);

    auto result = orchestrator.scan();
    EXPECT_SUCCESS(result);

    std::vector<std::string> entry_ids{};
    entry_ids.reserve(ids.size());

    do {
        auto entry = result->next();
        if (!entry) {
            break;
        }

        entry_ids.emplace_back(reinterpret_cast<const char*>(entry->key.data()), entry->key.size());
        REQUIRE_FALSE(entry->body.has_value());
    } while (true);

    REQUIRE(ids.size() == entry_ids.size());
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == entry_ids);
}
