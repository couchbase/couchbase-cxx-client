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
#include "core/topology/configuration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/scan_type.hxx>

#include <chrono>
#include <utility>

namespace
{
auto
populate_documents_for_range_scan(const couchbase::collection& collection,
                                  const std::vector<std::string>& ids,
                                  const std::vector<std::byte>& value,
                                  std::optional<std::chrono::seconds> expiry = {})
{
  couchbase::upsert_options options{};
  if (expiry) {
    options.expiry(expiry.value());
  }

  std::map<std::string, couchbase::mutation_token> mutations;
  for (const auto& id : ids) {
    auto [err, resp] =
      collection.upsert<couchbase::codec::raw_binary_transcoder>(id, value, options).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(resp.mutation_token().has_value());
    mutations[id] = resp.mutation_token().value();
  }
  return mutations;
}

auto
do_range_scan(couchbase::core::agent agent,
              std::uint16_t vbucket_id,
              const couchbase::core::range_scan_create_options& create_options,
              const couchbase::core::range_scan_continue_options& continue_options)
  -> std::vector<couchbase::core::range_scan_item>
{
  std::vector<std::byte> scan_uuid;

  {
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
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

  const auto options = continue_options;

  do {
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
    auto f = barrier->get_future();

    auto op = agent.range_scan_continue(
      scan_uuid,
      vbucket_id,
      options,
      [&data](auto item) {
        data.emplace_back(std::move(item));
      },
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

auto
make_binary_value(std::size_t number_of_bytes)
{
  std::vector<std::byte> value(number_of_bytes);
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }
  return value;
}

auto
get_vbucket_map(const test::utils::integration_test_guard& integration)
  -> couchbase::core::topology::configuration::vbucket_map
{
  auto barrier = std::make_shared<std::promise<
    tl::expected<couchbase::core::topology::configuration::vbucket_map, std::error_code>>>();
  auto f = barrier->get_future();
  integration.cluster.with_bucket_configuration(
    integration.ctx.bucket,
    [barrier](std::error_code ec,
              const std::shared_ptr<couchbase::core::topology::configuration>& config) mutable {
      if (ec) {
        return barrier->set_value(tl::unexpected(ec));
      }
      if (!config->vbmap || config->vbmap->empty()) {
        return barrier->set_value(tl::unexpected(couchbase::errc::common::feature_not_available));
      }
      barrier->set_value(config->vbmap.value());
    });
  auto vbucket_map = f.get();
  EXPECT_SUCCESS(vbucket_map);
  return vbucket_map.value();
}
} // namespace

TEST_CASE("integration: range scan large values", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto value = make_binary_value(16'384);

  const std::vector<std::string> ids{
    "largevalues-2960", "largevalues-3064", "largevalues-3686", "largevalues-3716",
    "largevalues-5354", "largevalues-5426", "largevalues-6175", "largevalues-6607",
    "largevalues-6797", "largevalues-7871",
  };
  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::range_scan_create_options create_options{
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    couchbase::core::range_scan{
      couchbase::core::scan_term{ "largevalues" },
      couchbase::core::scan_term{ "largevalues\xff" },
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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangesmallvalues-1023", "rangesmallvalues-1751", "rangesmallvalues-2202",
    "rangesmallvalues-2392", "rangesmallvalues-2570", "rangesmallvalues-4132",
    "rangesmallvalues-4640", "rangesmallvalues-5836", "rangesmallvalues-7283",
    "rangesmallvalues-7313",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::range_scan_create_options create_options{
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    couchbase::core::range_scan{
      couchbase::core::scan_term{ "rangesmallvalues" },
      couchbase::core::scan_term{ "rangesmallvalues\xff" },
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

TEST_CASE("integration: range scan collection retry", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  const test::utils::collection_guard new_collection(integration);

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(new_collection.collection_name());

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangecollectionretry-9695",   "rangecollectionretry-24520",  "rangecollectionretry-90825",
    "rangecollectionretry-119677", "rangecollectionretry-150939", "rangecollectionretry-170176",
    "rangecollectionretry-199557", "rangecollectionretry-225568", "rangecollectionretry-231302",
    "rangecollectionretry-245898",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  // we're going to force a refresh, so we need to delete the collection from our cache.
  agent->unit_test_api().collections().remove_collection_from_cache(
    couchbase::scope::default_name, new_collection.collection_name());

  couchbase::core::range_scan_create_options create_options{
    couchbase::scope::default_name,
    new_collection.collection_name(),
    couchbase::core::range_scan{
      couchbase::core::scan_term{ "rangecollectionretry" },
      couchbase::core::scan_term{ "rangecollectionretry\xff" },
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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangekeysonly-1269",  "rangekeysonly-2048",  "rangekeysonly-4378",  "rangekeysonly-7159",
    "rangekeysonly-8898",  "rangekeysonly-8908",  "rangekeysonly-19559", "rangekeysonly-20808",
    "rangekeysonly-20998", "rangekeysonly-25889",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::range_scan_create_options create_options{
    couchbase::scope::default_name,
    couchbase::collection::default_name,
    couchbase::core::range_scan{
      couchbase::core::scan_term{ "rangekeysonly" },
      couchbase::core::scan_term{ "rangekeysonly\xff" },
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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",
    "rangescancancel-116036", "rangescancancel-136879", "rangescancancel-156589",
    "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428",
    "rangescancancel-257242",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

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
        couchbase::core::scan_term{ "rangescancancel" },
        couchbase::core::scan_term{ "rangescancancel\xff" },
      },
    };
    options.ids_only = true;
    options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
      highest_mutation->second.partition_uuid(),
      highest_mutation->second.sequence_number(),
    };

    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
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
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_cancel_result, std::error_code>>>();
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

  bool items_callback_invoked{ false };
  {
    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
    auto f = barrier->get_future();

    auto op = agent->range_scan_continue(
      scan_uuid,
      vbucket_id,
      options,
      [&items_callback_invoked](auto) {
        items_callback_invoked = true;
      },
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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",
    "rangescancancel-116036", "rangescancancel-136879", "rangescancancel-156589",
    "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428",
    "rangescancancel-257242",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

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
        couchbase::core::scan_term{ "rangescancancel" },
        couchbase::core::scan_term{ "rangescancancel\xff" },
      },
    };
    options.ids_only = true;
    options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
      highest_mutation->second.partition_uuid(),
      highest_mutation->second.sequence_number(),
    };

    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
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
    auto op = agent->range_scan_cancel(scan_uuid, vbid, {}, [](auto /* res */, auto ec) {
      REQUIRE_SUCCESS(ec);
    });
    EXPECT_SUCCESS(op);
  };

  std::vector<couchbase::core::range_scan_item> data;
  std::size_t iteration = 0;

  do {
    ++iteration;

    couchbase::core::range_scan_continue_options options{};
    options.batch_time_limit = std::chrono::seconds{ 10 };
    options.batch_item_limit = 3; // limit batch to 3 items, while range expected to be larger

    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
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

TEST_CASE("integration: range scan cancel during streaming using pending_operation cancel",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "rangescancancel-2746",   "rangescancancel-37795",  "rangescancancel-63440",
    "rangescancancel-116036", "rangescancancel-136879", "rangescancancel-156589",
    "rangescancancel-196316", "rangescancancel-203197", "rangescancancel-243428",
    "rangescancancel-257242",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

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
        couchbase::core::scan_term{ "rangescancancel" },
        couchbase::core::scan_term{ "rangescancancel\xff" },
      },
    };
    options.ids_only = true;
    options.snapshot_requirements = couchbase::core::range_snapshot_requirements{
      highest_mutation->second.partition_uuid(),
      highest_mutation->second.sequence_number(),
    };

    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_create_result, std::error_code>>>();
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
  std::mutex operation_holder_mutex{};

  auto execute_operation_cancel = [&operation_holder, &operation_holder_mutex]() {
    std::scoped_lock lock(operation_holder_mutex);
    operation_holder->cancel();
  };

  std::vector<couchbase::core::range_scan_item> data;

  {
    couchbase::core::range_scan_continue_options options{};
    options.batch_time_limit = std::chrono::seconds{ 10 };
    options.batch_item_limit = 3; // limit batch to 3 items, while range expected to be larger

    auto barrier = std::make_shared<
      std::promise<std::pair<couchbase::core::range_scan_continue_result, std::error_code>>>();
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
    {
      std::scoped_lock lock(operation_holder_mutex);
      std::swap(operation_holder, op.value()); // store the operation for cancellation
    }

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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  std::vector<std::byte> value = couchbase::core::utils::to_binary(R"({"barry":"sheen")");
  for (std::size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<std::byte>(i);
  }

  const std::vector<std::string> ids{
    "samplescankeys-170",   "samplescankeys-602",   "samplescankeys-792",   "samplescankeys-3978",
    "samplescankeys-6869",  "samplescankeys-9038",  "samplescankeys-10806", "samplescankeys-10996",
    "samplescankeys-11092", "samplescankeys-11102",
  };

  auto mutations = populate_documents_for_range_scan(collection, ids, value);

  auto highest_mutation = std::max_element(mutations.begin(), mutations.end(), [](auto a, auto b) {
    return a.second.sequence_number() < b.second.sequence_number();
  });

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
mutations_to_mutation_state(const std::map<std::string, couchbase::mutation_token>& mutations)
{
  couchbase::core::mutation_state state;
  for (const auto& [key, token] : mutations) {
    state.tokens.emplace_back(token);
  }
  return state;
}

TEST_CASE("integration: orchestrator scan range without content", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(100, "rangescanwithoutcontent-");
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::range_scan scan{
    couchbase::core::scan_term{ "rangescanwithoutcontent" },
    couchbase::core::scan_term{ "rangescanwithoutcontent\xff" },
  };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.ids_only = true;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

  do {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
    REQUIRE_FALSE(entry->body.has_value());
  } while (true);

  REQUIRE(ids.size() == entry_ids.size());

  for (const auto& id : ids) {
    REQUIRE(entry_ids.count(id) == 1);
  }
}

TEST_CASE("integration: orchestrator scan range with content", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(100, "rangescanwithcontent-");
  auto value = make_binary_value(100);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::range_scan scan{
    couchbase::core::scan_term{ "rangescanwithcontent" },
    couchbase::core::scan_term{ "rangescanwithcontent\xff" },
  };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

  do {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
    REQUIRE(entry->body.has_value());
  } while (true);

  REQUIRE(ids.size() == entry_ids.size());

  for (const auto& id : ids) {
    INFO(id);
    REQUIRE(entry_ids.count(id) == 1);
  }
}

TEST_CASE("integration: orchestrator sampling scan with custom collection", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  const test::utils::collection_guard new_collection(integration);

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(new_collection.collection_name());

  auto ids = make_doc_ids(100, "samplingscan-");
  auto value = make_binary_value(100);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::sampling_scan scan{ 10 };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        new_collection.collection_name(),
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

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
    REQUIRE(std::find(ids.begin(), ids.end(), id) != ids.end());
  }
}

TEST_CASE("integration: orchestrator sampling scan with seed & custom collection", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  const test::utils::collection_guard new_collection(integration);

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(new_collection.collection_name());

  auto ids = make_doc_ids(100, "samplingscan-");
  auto value = make_binary_value(100);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::sampling_scan scan{
    10, // limit
    50, // seed
  };
  couchbase::core::range_scan_orchestrator_options options{};
  options.concurrency = 1;
  options.ids_only = true;

  options.consistent_with = mutations_to_mutation_state(mutations);
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        new_collection.collection_name(),
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};
  while (true) {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
  }

  REQUIRE(entry_ids.size() >= 10);

  for (const auto& id : entry_ids) {
    REQUIRE(std::find(ids.begin(), ids.end(), id) != ids.end());
  }

  // Doing the scan again with the same seed & concurrency 1 should yield the same documents
  couchbase::core::sampling_scan scan2{
    10, // limit
    50, // seed
  };
  couchbase::core::range_scan_orchestrator orchestrator2(integration.io,
                                                         agent.value(),
                                                         vbucket_map,
                                                         couchbase::scope::default_name,
                                                         new_collection.collection_name(),
                                                         scan2,
                                                         options);
  auto result2 = orchestrator2.scan();
  EXPECT_SUCCESS(result2);

  std::set<std::string> entry_ids2{};
  while (true) {
    auto entry = result2->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids2.insert(entry->key);
    REQUIRE(inserted);
  }
  REQUIRE(entry_ids2.size() >= 10);
  REQUIRE(entry_ids == entry_ids2);
}

TEST_CASE("integration: orchestrator prefix scan without content", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(100, "prefixscanwithoutcontent-");
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::prefix_scan scan{ "prefixscanwithoutcontent" };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.ids_only = true;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

  do {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
    REQUIRE_FALSE(entry->body.has_value());
  } while (true);

  REQUIRE(ids.size() == entry_ids.size());

  for (const auto& id : ids) {
    REQUIRE(entry_ids.count(id) == 1);
  }
}

TEST_CASE(
  "integration: orchestrator sampling scan with custom collection and up to 10 concurrent streams",
  "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  const test::utils::collection_guard new_collection(integration);

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(new_collection.collection_name());

  auto ids = make_doc_ids(100, "samplingscan-");
  auto value = make_binary_value(100);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::sampling_scan scan{ 10, 50 };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.concurrency = 10;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        new_collection.collection_name(),
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

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
    REQUIRE(std::find(ids.begin(), ids.end(), id) != ids.end());
  }
}

TEST_CASE("integration: orchestrator sampling scan with custom collection and up to 128 concurrent "
          "streams and batch "
          "item limit 0",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  const test::utils::collection_guard new_collection(integration);

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(new_collection.collection_name());

  auto ids = make_doc_ids(100, "samplingscan-");
  auto value = make_binary_value(100);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 300 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::sampling_scan scan{ 10, 50 };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.concurrency = 128;
  options.batch_item_limit = 0;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        new_collection.collection_name(),
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

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
    REQUIRE(std::find(ids.begin(), ids.end(), id) != ids.end());
  }
}

TEST_CASE("integration: orchestrator prefix scan without content and up to 5 concurrent streams",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(100, "prefixscanwithoutcontent-");
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::prefix_scan scan{ "prefixscanwithoutcontent" };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.ids_only = true;
  options.concurrency = 5;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};

  do {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
    REQUIRE_FALSE(entry->body.has_value());
  } while (true);

  REQUIRE(ids.size() == entry_ids.size());

  for (const auto& id : ids) {
    REQUIRE(entry_ids.count(id) == 1);
  }
}

TEST_CASE("integration: orchestrator prefix scan, get 10 items and cancel", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(15, "prefixscancancel-");
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io, { { integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::prefix_scan scan{ "prefixscancancel" };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.ids_only = true;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  EXPECT_SUCCESS(result);

  std::set<std::string> entry_ids{};
  const std::size_t expected_id_count = 10;

  for (std::size_t i = 0; i < expected_id_count; i++) {
    auto entry = result->next();
    if (!entry) {
      break;
    }

    auto [_, inserted] = entry_ids.insert(entry->key);
    REQUIRE(inserted);
    REQUIRE_FALSE(entry->body.has_value());
  }

  result->cancel();

  REQUIRE(expected_id_count == entry_ids.size());

  for (const auto& id : entry_ids) {
    REQUIRE(std::count(ids.begin(), ids.end(), id) == 1);
  }

  auto next_item = result->next();
  REQUIRE(!next_item.has_value());
  REQUIRE(next_item.error() == couchbase::errc::key_value::range_scan_completed);
  REQUIRE(result->is_cancelled());
}

TEST_CASE("integration: orchestrator prefix scan with concurrency 0 (invalid argument)",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range_scan");
  }

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

  auto collection = cluster.bucket(integration.ctx.bucket)
                      .scope(couchbase::scope::default_name)
                      .collection(couchbase::collection::default_name);

  auto ids = make_doc_ids(100, "prefixscaninvalidconcurrency-");
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  auto vbucket_map = get_vbucket_map(integration);

  auto ag = couchbase::core::agent_group(integration.io,
                                         { couchbase::core::core_sdk_shim{ integration.cluster } });
  ag.open_bucket(integration.ctx.bucket);
  auto agent = ag.get_agent(integration.ctx.bucket);
  REQUIRE(agent.has_value());

  couchbase::core::prefix_scan scan{ "prefixscaninvalidconcurrency" };
  couchbase::core::range_scan_orchestrator_options options{};
  options.consistent_with = mutations_to_mutation_state(mutations);
  options.ids_only = true;
  options.concurrency = 0;
  couchbase::core::range_scan_orchestrator orchestrator(integration.io,
                                                        agent.value(),
                                                        vbucket_map,
                                                        couchbase::scope::default_name,
                                                        couchbase::collection::default_name,
                                                        scan,
                                                        options);

  auto result = orchestrator.scan();
  REQUIRE(!result.has_value());
  REQUIRE(result.error() == couchbase::errc::common::invalid_argument);
}

TEST_CASE("integration: range scan public API feature not available", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.has_bucket_capability("range_scan")) {
    SKIP("cluster supports range scan");
  }

  auto test_ctx = integration.ctx;
  auto [e, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto [err, res] = collection.scan(couchbase::prefix_scan{ "foo" }, {}).get();
  REQUIRE(err.ec() == couchbase::errc::common::feature_not_available);
}

std::vector<couchbase::scan_result_item>
scan_and_store_results(const couchbase::collection& collection,
                       const couchbase::scan_type& scan_type,
                       const couchbase::scan_options& options)
{
  auto [err, res] = collection.scan(scan_type, options).get();
  REQUIRE_SUCCESS(err.ec());
  std::vector<couchbase::scan_result_item> items{};
  while (true) {
    auto [iter_err, item] = res.next().get();
    REQUIRE_SUCCESS(iter_err.ec());
    if (!item.has_value()) {
      break;
    }
    items.push_back(item.value());
  }
  return items;
}

std::vector<couchbase::scan_result_item>
scan_and_store_results_with_iterator(const couchbase::collection& collection,
                                     const couchbase::scan_type& scan_type,
                                     const couchbase::scan_options& options)
{
  auto [err, res] = collection.scan(scan_type, options).get();
  REQUIRE_SUCCESS(err.ec());
  std::vector<couchbase::scan_result_item> items{};
  for (auto [iter_err, item] : res) {
    REQUIRE_SUCCESS(iter_err.ec());
    items.push_back(item);
  }
  return items;
}

[[maybe_unused]] static auto
mutations_to_public_mutation_state(
  const std::map<std::string, couchbase::mutation_token>& mutations)
{
  couchbase::mutation_state state{};
  for (const auto& [key, token] : mutations) {
    couchbase::mutation_result mut_res{ {}, token };
    state.add(mut_res);
  }
  return state;
}

static void
next_item(couchbase::scan_result res,
          std::function<void(couchbase::scan_result_item)> validator,
          std::function<void()>&& callback)
{
  res.next([res, callback = std::move(callback), validator = std::move(validator)](
             couchbase::error err, std::optional<couchbase::scan_result_item> item) mutable {
    REQUIRE_SUCCESS(err.ec());
    if (!item.has_value()) {
      return callback();
    }
    validator(item.value());
    return next_item(res, std::move(validator), std::move(callback));
  });
}

TEST_CASE("integration: range scan public API", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_bucket_capability("range_scan")) {
    SKIP("cluster does not support range scan");
  }

  auto test_ctx = integration.ctx;
  auto [e, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto prefix = "scan-public-api-";
  auto ids = make_doc_ids(100, prefix);
  auto value = make_binary_value(1);
  auto mutations =
    populate_documents_for_range_scan(collection, ids, value, std::chrono::seconds{ 30 });

  SECTION("prefix scan")
  {
    auto scan_type = couchbase::prefix_scan(prefix);
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20);
    auto [err, res] = collection.scan(scan_type, options).get();
    REQUIRE_SUCCESS(err.ec());
    auto item_count = 0;
    for (auto [iter_err, item] : res) {
      REQUIRE_SUCCESS(iter_err.ec());
      item_count++;
      REQUIRE(!item.id().empty());
      auto content =
        item.content_as<couchbase::codec::binary, couchbase::codec::raw_binary_transcoder>();
      REQUIRE(!item.id_only());
      REQUIRE(content == value);
      REQUIRE(item.cas().value() != 0);
    }
    REQUIRE(item_count == 100);
  }

  SECTION("range scan")
  {
    auto scan_type =
      couchbase::range_scan(couchbase::scan_term("scan-public-api-1"),
                            couchbase::scan_term("scan-public-api-2").exclusive(true));
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20);
    auto [err, res] = collection.scan(scan_type, options).get();
    REQUIRE_SUCCESS(err.ec());
    auto item_count = 0;
    for (auto [iter_err, item] : res) {
      REQUIRE_SUCCESS(iter_err.ec());
      item_count++;
      REQUIRE(!item.id().empty());
      auto content =
        item.content_as<couchbase::codec::binary, couchbase::codec::raw_binary_transcoder>();
      REQUIRE(!item.id_only());
      REQUIRE(content == value);
      REQUIRE(item.cas().value() != 0);
    }
    REQUIRE(item_count == 11);
  }

  SECTION("sampling scan")
  {
    auto scan_type = couchbase::sampling_scan(35);
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20);
    auto [err, res] = collection.scan(scan_type, options).get();
    REQUIRE_SUCCESS(err.ec());
    auto item_count = 0;

    for (auto [iter_err, item] : res) {
      REQUIRE_SUCCESS(iter_err.ec());
      item_count++;
      REQUIRE(!item.id().empty());
      REQUIRE(!item.id_only());
      // Not checking the content value as the sample might contain any documents from the
      // collection
      REQUIRE(item.cas().value() != 0);
    }
    REQUIRE(item_count <= 35);
  }

  SECTION("range scan with no results")
  {
    // Using a 'from' that is bigger than 'to'
    auto scan_type =
      couchbase::range_scan(couchbase::scan_term("scan-public-api-2"),
                            couchbase::scan_term("scan-public-api-1").exclusive(true));
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20);
    auto [err, res] = collection.scan(scan_type, options).get();
    REQUIRE_SUCCESS(err.ec());
    auto item_count = 0;
    for (auto _item : res) {
      // Should not be reached
      REQUIRE(false);
    }
    REQUIRE(item_count == 0);
  }

  SECTION("prefix scan ids only")
  {
    auto scan_type = couchbase::prefix_scan(prefix);
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20)
                     .ids_only(true);
    auto [err, res] = collection.scan(scan_type, options).get();
    REQUIRE_SUCCESS(err.ec());
    auto item_count = 0;
    for (auto [iter_err, item] : res) {
      REQUIRE_SUCCESS(iter_err.ec());
      item_count++;
      REQUIRE(!item.id().empty());
      auto content =
        item.content_as<couchbase::codec::binary, couchbase::codec::raw_binary_transcoder>();
      REQUIRE(item.id_only());
      REQUIRE(content.empty());
      REQUIRE(item.cas().value() == 0);
    }
    REQUIRE(item_count == 100);
  }

  SECTION("range scan async")
  {
    auto item_count = 0;
    auto barrier = std::make_shared<std::promise<void>>();
    auto callback = [barrier]() {
      barrier->set_value();
    };

    auto scan_type = couchbase::prefix_scan(prefix);
    auto options = couchbase::scan_options()
                     .consistent_with(mutations_to_public_mutation_state(mutations))
                     .concurrency(20);

    collection.scan(
      scan_type,
      options,
      [&item_count, callback = std::move(callback)](couchbase::error err,
                                                    couchbase::scan_result res) mutable {
        REQUIRE_SUCCESS(err.ec());
        return next_item(
          std::move(res),
          [&item_count](const couchbase::scan_result_item& item) {
            item_count++;
            REQUIRE(!item.id().empty());
            auto content =
              item.content_as<couchbase::codec::binary, couchbase::codec::raw_binary_transcoder>();
            REQUIRE(!content.empty());
            REQUIRE(!item.id_only());
            REQUIRE(item.cas().value() != 0);
          },
          std::move(callback));
      });

    auto fut = barrier->get_future();
    fut.get();
    REQUIRE(item_count == 100);
  }
}
