/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Live probes of search index management over the couchbase2:// transport (CXXCBC-901), against
// admin.search.v1.
//
// Two things here are only observable against a real server.
//
// Whether an upsert updates is decided by FTS, not by the client: the gateway forwards the
// definition to PUT /api/index/<name>, and FTS reads the body's "uuid" member as prevIndexUUID and
// refuses a create for a name that already exists. Which of CreateIndex/UpdateIndex the client
// chooses is therefore visible only in whether the second upsert of a name succeeds.
//
// And FTS augments an index definition with its own defaults, so what comes back from get_index is
// never byte-identical to what was sent. Whether the three parameter blobs stay separate can only
// be checked against a real definition, by looking for each blob's own member in its own blob.

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/operations/management/search_index_analyze_document.hxx"
#include "core/operations/management/search_index_control_ingest.hxx"
#include "core/operations/management/search_index_control_plan_freeze.hxx"
#include "core/operations/management/search_index_control_query.hxx"
#include "core/operations/management/search_index_drop.hxx"
#include "core/operations/management/search_index_get.hxx"
#include "core/operations/management/search_index_get_all.hxx"
#include "core/operations/management/search_index_get_documents_count.hxx"
#include "core/operations/management/search_index_get_stats.hxx"
#include "core/operations/management/search_index_upsert.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
using ::couchbase::core::management::search::index;

// Distinct probe members, one per parameter blob, so a converter that crossed two of them is
// visible in what comes back. FTS keeps all three verbatim alongside the defaults it adds.
constexpr auto plan_params_probe = R"({"maxPartitionsPerPIndex":512})";
constexpr auto source_params_probe = R"({"feedBufferSizeBytes":1048576})";

[[nodiscard]] auto
unique_name(const std::string& prefix) -> std::string
{
  static int counter{ 0 };
  return prefix + "_cng_" + std::to_string(++counter) + "_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() %
                        1'000'000'000);
}

// The member of a JSON object, or nothing when the blob is unset, is not an object, or has no such
// member. Used to look for a probe in a blob it does not belong to as well as in the one it does,
// so "absent" has to be a value rather than a failure.
[[nodiscard]] auto
member(const std::string& json, const std::string& key) -> std::optional<tao::json::value>
{
  if (json.empty()) {
    return {};
  }
  tao::json::value parsed;
  try {
    parsed = couchbase::core::utils::json::parse(json);
  } catch (...) {
    return {};
  }
  if (!parsed.is_object()) {
    return {};
  }
  const auto* found = parsed.find(key);
  if (found == nullptr) {
    return {};
  }
  return *found;
}

[[nodiscard]] auto
definition(const std::string& name, const std::string& bucket) -> index
{
  index definition;
  definition.name = name;
  definition.type = "fulltext-index";
  definition.source_type = "couchbase";
  definition.source_name = bucket;
  return definition;
}

// Lists the indexes, failing the case for anything but the gateway not serving the service.
//
// Nothing on the client side answers this request with feature_not_available -- cluster.cxx routes
// it and the component has no refusal path for it -- so the code can only have come from a gateway
// that does not serve admin.search.v1. Every other case calls this first and then asserts rather
// than skips: once the service is known to be served, a later refusal is the client declining to
// send something, which is a failure and not a reason to skip.
[[nodiscard]] auto
indexes_or_skip(live_cluster_fixture& fixture) -> std::vector<index>
{
  ops::management::search_index_get_all_request request{};
  auto listed = fixture.execute(std::move(request));
  if (listed.ctx.ec == couchbase::errc::common::feature_not_available) {
    skip("gateway does not implement admin.search.v1 (feature_not_available)");
  }
  assert_false(static_cast<bool>(listed.ctx.ec), "list search indexes over couchbase2 succeeds");
  return std::move(listed.indexes);
}

[[nodiscard]] auto
find_index(const std::vector<index>& indexes, const std::string& name) -> std::optional<index>
{
  const auto found = std::find_if(indexes.begin(), indexes.end(), [&name](const auto& candidate) {
    return candidate.name == name;
  });
  if (found == indexes.end()) {
    return {};
  }
  return *found;
}

// Drops the index however the case leaves -- an assertion failure unwinds, and an index left
// behind changes what every later case lists.
class index_guard
{
public:
  index_guard(live_cluster_fixture& fixture, std::string name)
    : fixture_{ fixture }
    , name_{ std::move(name) }
  {
  }

  index_guard(const index_guard&) = delete;
  index_guard(index_guard&&) = delete;
  auto operator=(const index_guard&) -> index_guard& = delete;
  auto operator=(index_guard&&) -> index_guard& = delete;

  ~index_guard()
  {
    ops::management::search_index_drop_request request{};
    request.index_name = name_;
    [[maybe_unused]] auto dropped = fixture_.execute(std::move(request));
  }

private:
  live_cluster_fixture& fixture_;
  std::string name_;
};

[[nodiscard]] auto
get_index(live_cluster_fixture& fixture, const std::string& name)
  -> ops::management::search_index_get_response
{
  ops::management::search_index_get_request request{};
  request.index_name = name;
  return fixture.execute(std::move(request));
}

void
list_search_indexes_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto indexes = indexes_or_skip(fixture);
}

void
the_search_index_lifecycle_round_trips_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("lifecycle");
  {
    index_guard guard{ fixture, name };

    ops::management::search_index_upsert_request create{};
    create.index = definition(name, fixture.bucket());
    const auto created = fixture.execute(std::move(create));
    assert_false(static_cast<bool>(created.ctx.ec), "create search index over couchbase2 succeeds");
    assert_eq(created.status, std::string{ "ok" }, "a successful upsert reports status ok");

    const auto fetched = get_index(fixture, name);
    assert_false(static_cast<bool>(fetched.ctx.ec), "get search index succeeds");
    assert_eq(fetched.index.name, name, "the index comes back under the name it was created with");
    assert_eq(fetched.index.type, std::string{ "fulltext-index" }, "type round-trips");
    assert_false(fetched.index.uuid.empty(), "the server assigned a uuid");
    assert_eq(fetched.status, std::string{ "ok" }, "a successful get reports status ok");

    assert_true(find_index(indexes_or_skip(fixture), name).has_value(),
                "the new index appears in the listing");

    ops::management::search_index_get_documents_count_request count{};
    count.index_name = name;
    const auto counted = fixture.execute(std::move(count));
    assert_false(static_cast<bool>(counted.ctx.ec), "indexed documents count succeeds");
    assert_eq(counted.status, std::string{ "ok" }, "a successful count reports status ok");
  }

  ops::management::search_index_drop_request drop{};
  drop.index_name = name;
  const auto dropped = fixture.execute(std::move(drop));
  // The guard already dropped it, so this second drop is expected to fail; what it must not do is
  // succeed against an index that is still there.
  assert_false(find_index(indexes_or_skip(fixture), name).has_value(),
               "the index is gone from the listing after the drop");
  assert_true(static_cast<bool>(dropped.ctx.ec), "dropping an index that is gone reports an error");
}

void
an_existing_index_is_updated_through_upsert_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("update");
  index_guard guard{ fixture, name };

  ops::management::search_index_upsert_request create{};
  create.index = definition(name, fixture.bucket());
  const auto created = fixture.execute(std::move(create));
  assert_false(static_cast<bool>(created.ctx.ec), "create succeeds");

  const auto fetched = get_index(fixture, name);
  assert_false(static_cast<bool>(fetched.ctx.ec), "get after create succeeds");
  assert_false(fetched.index.uuid.empty(), "the created index has a uuid to update against");

  // The upsert the review describes: take what get_index returned, change one thing, put it back.
  // Sending this through CreateIndex is refused by FTS because the name exists, so a success here
  // is the client having chosen UpdateIndex and carried the uuid.
  auto modified = fetched.index;
  modified.plan_params_json = plan_params_probe;
  ops::management::search_index_upsert_request update{};
  update.index = std::move(modified);
  const auto updated = fixture.execute(std::move(update));
  assert_false(static_cast<bool>(updated.ctx.ec),
               "upserting a definition that carries its uuid updates it");
  assert_eq(updated.status, std::string{ "ok" }, "a successful update reports status ok");

  const auto reread = get_index(fixture, name);
  assert_false(static_cast<bool>(reread.ctx.ec), "get after update succeeds");
  const auto partitions = member(reread.index.plan_params_json, "maxPartitionsPerPIndex");
  assert_true(partitions.has_value(), "the updated plan_params reached the server");
  assert_true(partitions->is_number() && partitions->as<int>() == 512,
              "the updated plan_params holds the value that was sent");
  assert_true(reread.index.uuid != fetched.index.uuid,
              "the server issued a new uuid for the updated definition");
}

void
an_upsert_without_a_uuid_for_an_existing_name_is_refused_against_live_gateway(
  [[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("exists");
  index_guard guard{ fixture, name };

  ops::management::search_index_upsert_request create{};
  create.index = definition(name, fixture.bucket());
  assert_false(static_cast<bool>(fixture.execute(std::move(create)).ctx.ec), "create succeeds");

  // Same definition, still no uuid: this is a create of a name that exists, and it must stay an
  // error. Routing every upsert through UpdateIndex would turn it into a silent overwrite.
  ops::management::search_index_upsert_request again{};
  again.index = definition(name, fixture.bucket());
  const auto refused = fixture.execute(std::move(again));
  assert_eq(refused.ctx.ec,
            std::error_code{ couchbase::errc::common::index_exists },
            "a second create of the same name reports index_exists");
}

void
the_three_parameter_blobs_stay_separate_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("blobs");
  index_guard guard{ fixture, name };

  auto definition_with_blobs = definition(name, fixture.bucket());
  definition_with_blobs.plan_params_json = plan_params_probe;
  definition_with_blobs.source_params_json = source_params_probe;

  ops::management::search_index_upsert_request create{};
  create.index = std::move(definition_with_blobs);
  assert_false(static_cast<bool>(fixture.execute(std::move(create)).ctx.ec),
               "create with plan and source params succeeds");

  const auto fetched = get_index(fixture, name);
  assert_false(static_cast<bool>(fetched.ctx.ec), "get succeeds");

  // Each probe in its own blob. FTS adds its own defaults, so this is a membership check rather
  // than an equality one -- what must hold is that the value sent is the value stored.
  const auto partitions = member(fetched.index.plan_params_json, "maxPartitionsPerPIndex");
  assert_true(partitions.has_value(), "plan_params survives the round trip");
  assert_true(partitions->is_number() && partitions->as<int>() == 512,
              "plan_params holds the value that was sent");
  const auto feed_buffer = member(fetched.index.source_params_json, "feedBufferSizeBytes");
  assert_true(feed_buffer.has_value(), "source_params survives the round trip");
  assert_true(feed_buffer->is_number() && feed_buffer->as<int>() == 1048576,
              "source_params holds the value that was sent");

  // And in no other blob. A converter that read all three maps into one string, or wrote one blob
  // into another's map, passes every assertion above and fails these.
  assert_false(member(fetched.index.params_json, "maxPartitionsPerPIndex").has_value(),
               "the plan_params probe did not leak into params");
  assert_false(member(fetched.index.params_json, "feedBufferSizeBytes").has_value(),
               "the source_params probe did not leak into params");
  assert_false(member(fetched.index.source_params_json, "maxPartitionsPerPIndex").has_value(),
               "the plan_params probe did not leak into source_params");
  assert_false(member(fetched.index.plan_params_json, "feedBufferSizeBytes").has_value(),
               "the source_params probe did not leak into plan_params");
}

void
a_definition_that_cannot_be_represented_is_refused_before_it_is_sent([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  const auto before = indexes_or_skip(fixture);

  const auto name = unique_name("unrepresentable");
  auto broken = definition(name, fixture.bucket());
  // Valid JSON with nowhere to go in a map<string,bytes> keyed by an object's members. Dropping it
  // would create an index whose definition is missing everything the caller put in that blob.
  broken.params_json = R"(["not","an","object"])";

  ops::management::search_index_upsert_request create{};
  create.index = std::move(broken);
  const auto refused = fixture.execute(std::move(create));
  assert_eq(refused.ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "a params blob that is not an object is reported as invalid_argument");

  assert_false(find_index(indexes_or_skip(fixture), name).has_value(), "and no index was created");
  assert_eq(indexes_or_skip(fixture).size(), before.size(), "the listing is unchanged");
}

void
an_empty_index_name_is_reported_as_invalid_argument_against_live_gateway(
  [[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  // Parity with the classic transport, which rejects an empty name in encode_to. Which side
  // refuses it is not visible here -- the gateway answers an empty name with InvalidArgument too --
  // so that this transport sends nothing is pinned by the component suite instead.
  ops::management::search_index_get_request get{};
  get.index_name = "";
  assert_eq(fixture.execute(std::move(get)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "get with an empty index name is invalid_argument");

  ops::management::search_index_drop_request drop{};
  drop.index_name = "";
  assert_eq(fixture.execute(std::move(drop)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "drop with an empty index name is invalid_argument");

  ops::management::search_index_upsert_request upsert{};
  upsert.index = definition("", fixture.bucket());
  assert_eq(fixture.execute(std::move(upsert)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "upsert with an empty index name is invalid_argument");

  ops::management::search_index_control_ingest_request ingest{};
  ingest.index_name = "";
  ingest.pause = true;
  assert_eq(fixture.execute(std::move(ingest)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "control ingest with an empty index name is invalid_argument");

  ops::management::search_index_control_query_request query{};
  query.index_name = "";
  query.allow = true;
  assert_eq(fixture.execute(std::move(query)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "control query with an empty index name is invalid_argument");

  ops::management::search_index_control_plan_freeze_request freeze{};
  freeze.index_name = "";
  freeze.freeze = true;
  assert_eq(fixture.execute(std::move(freeze)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "control plan freeze with an empty index name is invalid_argument");

  ops::management::search_index_analyze_document_request analyze{};
  analyze.index_name = "";
  analyze.encoded_document = R"({"a":1})";
  assert_eq(fixture.execute(std::move(analyze)).ctx.ec,
            std::error_code{ couchbase::errc::common::invalid_argument },
            "analyze document with an empty index name is invalid_argument");
}

void
getting_a_missing_index_reports_index_not_found_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto fetched = get_index(fixture, unique_name("absent"));
  assert_eq(fetched.ctx.ec,
            std::error_code{ couchbase::errc::common::index_not_found },
            "getting an index that does not exist reports index_not_found");
  assert_true(fetched.index.name.empty(), "and hands back no definition");
}

void
the_control_operations_round_trip_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("control");
  index_guard guard{ fixture, name };

  ops::management::search_index_upsert_request create{};
  create.index = definition(name, fixture.bucket());
  assert_false(static_cast<bool>(fixture.execute(std::move(create)).ctx.ec), "create succeeds");

  // Each pair goes through both RPCs, so a bool wired to the wrong branch is visible: the two
  // calls would hit the same endpoint and the second would not be a no-op the server accepts.
  for (const auto pause : { true, false }) {
    ops::management::search_index_control_ingest_request request{};
    request.index_name = name;
    request.pause = pause;
    const auto response = fixture.execute(std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec),
                 pause ? "pause ingest succeeds" : "resume ingest succeeds");
    assert_eq(response.status, std::string{ "ok" }, "control ingest reports status ok");
  }

  for (const auto allow : { false, true }) {
    ops::management::search_index_control_query_request request{};
    request.index_name = name;
    request.allow = allow;
    const auto response = fixture.execute(std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec),
                 allow ? "allow querying succeeds" : "disallow querying succeeds");
    assert_eq(response.status, std::string{ "ok" }, "control query reports status ok");
  }

  for (const auto freeze : { true, false }) {
    ops::management::search_index_control_plan_freeze_request request{};
    request.index_name = name;
    request.freeze = freeze;
    const auto response = fixture.execute(std::move(request));
    assert_false(static_cast<bool>(response.ctx.ec),
                 freeze ? "freeze plan succeeds" : "unfreeze plan succeeds");
    assert_eq(response.status, std::string{ "ok" }, "control plan freeze reports status ok");
  }
}

void
get_stats_is_refused_over_couchbase2_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  // The listing establishes that the gateway serves admin.search.v1 before anything is asserted
  // about a refusal. Without it, feature_not_available below would read the same whether the one
  // operation is deliberately unrouted or the whole service is missing -- and the case could not
  // fail. With it, the two are distinguishable, which is the only reason it is worth asserting.
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  ops::management::search_index_get_stats_request stats{};
  stats.index_name = unique_name("stats");
  const auto refused = fixture.execute(std::move(stats));
  assert_eq(refused.ctx.ec,
            std::error_code{ couchbase::errc::common::feature_not_available },
            "get_stats has no admin.search.v1 RPC and is refused as feature_not_available");
}

void
analyze_document_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();
  [[maybe_unused]] auto listed = indexes_or_skip(fixture);

  const auto name = unique_name("analyze");
  index_guard guard{ fixture, name };

  ops::management::search_index_upsert_request create{};
  create.index = definition(name, fixture.bucket());
  assert_false(static_cast<bool>(fixture.execute(std::move(create)).ctx.ec), "create succeeds");

  // A freshly created index has no partitions for a moment, and FTS answers analyzeDoc for one
  // with "Search index is still being built, try again later" -- Unavailable at the gateway,
  // temporary_failure here. Retried rather than tolerated: accepting it as an outcome would leave
  // the case unable to fail, since every assertion below would be skipped.
  ops::management::search_index_analyze_document_response analyzed;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 20 };
  do {
    ops::management::search_index_analyze_document_request analyze{};
    analyze.index_name = name;
    analyze.encoded_document = R"({"name":"the quick brown fox"})";
    analyzed = fixture.execute(std::move(analyze));
    if (analyzed.ctx.ec != couchbase::errc::common::temporary_failure) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 200 });
  } while (std::chrono::steady_clock::now() < deadline);
  assert_false(static_cast<bool>(analyzed.ctx.ec), "analyze document succeeds");
  // FTS answers analyzeDoc with "ok", which is also what this transport reports when it infers a
  // status from a successful RPC, so this pins the value and not which of the two produced it.
  assert_eq(analyzed.status, std::string{ "ok" }, "a successful analyze reports status ok");
  assert_false(analyzed.analysis.empty(), "the analysis is relayed");
  assert_true(couchbase::core::utils::json::parse(analyzed.analysis).is_array(),
              "the analysis is the JSON array FTS returns");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_search_index_admin",
    {
      { "list_search_indexes_against_live_gateway",
        list_search_indexes_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "the_search_index_lifecycle_round_trips_against_live_gateway",
        the_search_index_lifecycle_round_trips_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "an_existing_index_is_updated_through_upsert_against_live_gateway",
        an_existing_index_is_updated_through_upsert_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "an_upsert_without_a_uuid_for_an_existing_name_is_refused_against_live_gateway",
        an_upsert_without_a_uuid_for_an_existing_name_is_refused_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "the_three_parameter_blobs_stay_separate_against_live_gateway",
        the_three_parameter_blobs_stay_separate_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "a_definition_that_cannot_be_represented_is_refused_before_it_is_sent",
        a_definition_that_cannot_be_represented_is_refused_before_it_is_sent,
        { needs::real_cluster() },
        timeout::integration },
      { "an_empty_index_name_is_reported_as_invalid_argument_against_live_gateway",
        an_empty_index_name_is_reported_as_invalid_argument_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "getting_a_missing_index_reports_index_not_found_against_live_gateway",
        getting_a_missing_index_reports_index_not_found_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "the_control_operations_round_trip_against_live_gateway",
        the_control_operations_round_trip_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "get_stats_is_refused_over_couchbase2_against_live_gateway",
        get_stats_is_refused_over_couchbase2_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "analyze_document_against_live_gateway",
        analyze_document_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
