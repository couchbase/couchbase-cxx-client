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

// Live probes of query index management over the couchbase2:// transport (CXXCBC-901), against
// admin.query.v1.
//
// Two things here are only observable against a real server.
//
// The gateway builds the CREATE INDEX statement rather than taking one, and splices the field list
// into it verbatim. Whether an index key is escaped correctly is therefore a property of the
// statement the query parser reads, not of the request the client sends -- a key that leaves its
// own quoting produces a valid statement describing a different index, which no assertion on the
// request can see.
//
// And build_deferred_indexes() is two round trips over HTTP and a single RPC here, so which core
// requests the public API issues is what decides whether the operation reaches the gateway at all.
// The case below drives the public manager rather than the core request, because the routing hole
// it covers was invisible from the core request that was already wired.

#include "framework/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/error_codes.hxx>

#include <algorithm>
#include <chrono>
#include <future>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::cng::test
{
namespace
{
namespace ops = ::couchbase::core::operations;

// Lists the bucket's indexes, failing the case for anything but the gateway not serving the
// service.
//
// Nothing on the client side answers this request with feature_not_available -- cluster.cxx routes
// it and the component has no refusal path for it -- so the code can only have come from a gateway
// that does not serve admin.query.v1. Every other case in this file calls this first and then
// asserts rather than skips: once the service is known to be served, a later refusal is the client
// declining to send something, which is a failure and not a reason to skip.
[[nodiscard]] auto
indexes_or_skip(live_cluster_fixture& fixture) -> std::vector<couchbase::management::query_index>
{
  ops::management::query_index_get_all_request request{};
  request.bucket_name = fixture.bucket();
  auto listed = fixture.execute(std::move(request));
  if (listed.ctx.ec == couchbase::errc::common::feature_not_available) {
    skip("gateway does not implement admin.query.v1 (feature_not_available)");
  }
  assert_false(static_cast<bool>(listed.ctx.ec), "list query indexes over couchbase2 succeeds");
  return std::move(listed.indexes);
}

[[nodiscard]] auto
find_index(const std::vector<couchbase::management::query_index>& indexes, const std::string& name)
  -> std::optional<couchbase::management::query_index>
{
  const auto found = std::find_if(indexes.begin(), indexes.end(), [&name](const auto& index) {
    return index.name == name;
  });
  if (found == indexes.end()) {
    return {};
  }
  return *found;
}

// Drops the index however the case leaves -- an assertion failure unwinds, and an index left
// behind makes the next run fail on "already exists".
class index_guard
{
public:
  index_guard(live_cluster_fixture& fixture, std::string name, bool is_primary = false)
    : fixture_{ fixture }
    , name_{ std::move(name) }
    , is_primary_{ is_primary }
  {
    drop();
  }

  index_guard(const index_guard&) = delete;
  index_guard(index_guard&&) = delete;
  auto operator=(const index_guard&) -> index_guard& = delete;
  auto operator=(index_guard&&) -> index_guard& = delete;

  ~index_guard()
  {
    drop();
  }

private:
  void drop()
  {
    ops::management::query_index_drop_request request{};
    request.bucket_name = fixture_.bucket();
    request.index_name = name_;
    request.is_primary = is_primary_;
    request.ignore_if_does_not_exist = true;
    static_cast<void>(fixture_.execute(std::move(request)));
  }

  live_cluster_fixture& fixture_;
  std::string name_;
  bool is_primary_;
};

[[nodiscard]] auto
create_index(live_cluster_fixture& fixture,
             const std::string& index_name,
             std::vector<std::string> keys,
             bool ignore_if_exists = false) -> ops::management::query_index_create_response
{
  ops::management::query_index_create_request request{};
  request.bucket_name = fixture.bucket();
  request.index_name = index_name;
  request.keys = std::move(keys);
  request.ignore_if_exists = ignore_if_exists;
  // Deferred, so the case does not wait for a build it is not measuring.
  request.deferred = true;
  return fixture.execute(std::move(request));
}

[[nodiscard]] auto
drop_index(live_cluster_fixture& fixture,
           const std::string& index_name,
           bool is_primary,
           bool ignore_if_does_not_exist) -> ops::management::query_index_drop_response
{
  ops::management::query_index_drop_request request{};
  request.bucket_name = fixture.bucket();
  request.index_name = index_name;
  request.is_primary = is_primary;
  request.ignore_if_does_not_exist = ignore_if_does_not_exist;
  return fixture.execute(std::move(request));
}

void
list_query_indexes_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  static_cast<void>(indexes_or_skip(fixture));
}

// The defect this covers: the keys were reaching the gateway unquoted, and the gateway splices
// them into the statement as they arrive. "two words" is the case the classic path's own
// integration test pins, and it is a syntax error without the quoting.
void
an_index_key_that_needs_escaping_round_trips_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  static_cast<void>(indexes_or_skip(fixture));

  const std::string index_name{ "cng_escaped_key" };
  const index_guard guard{ fixture, index_name };

  const auto created = create_index(fixture, index_name, { "two words" });
  assert_false(static_cast<bool>(created.ctx.ec),
               "an index on a key that needs quoting is created over couchbase2");

  const auto index = find_index(indexes_or_skip(fixture), index_name);
  assert_true(index.has_value(), "the created index is listed");
  assert_eq(index->index_key.size(), std::size_t{ 1 }, "the index has exactly one key");
  assert_true(index->index_key.at(0).find("two words") != std::string::npos,
              "the key names the field the caller asked for, not a mangling of it: " +
                index->index_key.at(0));
}

// The injection case. A key carrying its own quoting -- a`,`b -- becomes two fields if the encode
// merely wraps it in backticks, and the server builds a valid two-key index under the requested
// name: measured against this cluster, the broken form creates an index whose index_key reads
// ["`a`", "`b`"].
//
// Escaped correctly it is one identifier whose name contains a backtick. The parser reads that --
// SELECT 1 AS `a\`b` returns the field a`b -- but the indexer refuses to index it, so the safe
// outcome here is a refusal rather than a one-key index. Both are asserted, because which one a
// server gives is the server's business and neither is what broken escaping produces.
void
a_hostile_index_key_cannot_add_terms_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  const auto before = indexes_or_skip(fixture);

  const std::string index_name{ "cng_hostile_key" };
  const index_guard guard{ fixture, index_name };

  const auto created = create_index(fixture, index_name, { "a`,`b" });
  const auto after = indexes_or_skip(fixture);
  const auto index = find_index(after, index_name);

  // Whatever else happens, the request has to have been sent: feature_not_available is the client
  // declining to build the statement at all, which would make this case pass without testing it.
  assert_true(created.ctx.ec != couchbase::errc::common::feature_not_available,
              "the request reached the gateway rather than being refused by the client");

  if (created.ctx.ec) {
    assert_false(index.has_value(), "the refused index was not created");
    assert_eq(after.size(), before.size(), "no index was created");
    return;
  }

  assert_true(index.has_value(), "the created index is listed");
  std::string keys;
  for (const auto& key : index->index_key) {
    keys += keys.empty() ? key : ", " + key;
  }
  assert_eq(index->index_key.size(),
            std::size_t{ 1 },
            fmt::format("the hostile key is one index key and not two, so it added no term to the "
                        "statement: [{}]",
                        keys));
  assert_eq(after.size(), before.size() + 1, "exactly one index was created");
}

// The one refusal this converter emits. It is asserted rather than skipped: the service is known
// to be served by the time this runs, so feature_not_available here is the client declining to
// send a request it cannot express -- which is the behaviour under test.
void
a_conditional_secondary_index_is_refused_over_couchbase2()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  static_cast<void>(indexes_or_skip(fixture));

  const std::string index_name{ "cng_conditional" };
  const index_guard guard{ fixture, index_name };

  ops::management::query_index_create_request request{};
  request.bucket_name = fixture.bucket();
  request.index_name = index_name;
  request.keys = { "country" };
  request.condition = "country = \"US\"";
  const auto created = fixture.execute(std::move(request));
  assert_true(created.ctx.ec == couchbase::errc::common::feature_not_available,
              "a conditional secondary index is refused rather than created without its WHERE");

  assert_false(find_index(indexes_or_skip(fixture), index_name).has_value(),
               "the refused index was not created");
}

// Borrowed from gocb's TestCollectionQueryIndexManagerCrud. Over couchbase2 the ignore flags are
// proto fields the gateway acts on rather than error codes the client swallows, so whether they
// arrive is only visible against a server -- and the metadata below is the decode read back from a
// real GetAllIndexes rather than a hand-built message.
void
the_secondary_index_lifecycle_round_trips_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  static_cast<void>(indexes_or_skip(fixture));

  const std::string index_name{ "cng_lifecycle" };
  const index_guard guard{ fixture, index_name };

  assert_false(static_cast<bool>(create_index(fixture, index_name, { "field" }).ctx.ec),
               "the index is created");
  assert_true(create_index(fixture, index_name, { "field" }).ctx.ec ==
                couchbase::errc::common::index_exists,
              "creating it again reports index_exists");
  assert_false(static_cast<bool>(create_index(fixture, index_name, { "field" }, true).ctx.ec),
               "ignore_if_exists suppresses it");

  const auto index = find_index(indexes_or_skip(fixture), index_name);
  assert_true(index.has_value(), "the created index is listed");
  assert_false(index->is_primary, "a secondary index is not reported as primary");
  assert_eq(index->type, std::string{ "gsi" }, "the index type is decoded");
  assert_eq(index->bucket_name, fixture.bucket(), "the index names its bucket");
  assert_eq(index->index_key.size(), std::size_t{ 1 }, "one index key");
  assert_eq(index->index_key.at(0),
            std::string{ "`field`" },
            "the key is reported in the quoted form the classic path reports");
  assert_false(index->condition.has_value(), "an unconditional index has no condition");
  assert_false(index->partition.has_value(), "an unpartitioned index has no partition");

  assert_false(static_cast<bool>(drop_index(fixture, index_name, false, false).ctx.ec),
               "the index is dropped");
  assert_true(drop_index(fixture, index_name, false, false).ctx.ec ==
                couchbase::errc::common::index_not_found,
              "dropping it again reports index_not_found");
  assert_false(static_cast<bool>(drop_index(fixture, index_name, false, true).ctx.ec),
               "ignore_if_does_not_exist suppresses it");
}

// The primary/secondary branch picks between two RPCs, and a request sent to the wrong one still
// succeeds against a server that has both -- it just manages a different index. Named rather than
// unnamed so the case does not touch a #primary another test may be relying on.
void
the_primary_index_lifecycle_round_trips_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  static_cast<void>(indexes_or_skip(fixture));

  const std::string index_name{ "cng_lifecycle_primary" };
  const index_guard guard{ fixture, index_name, true };

  ops::management::query_index_create_request request{};
  request.bucket_name = fixture.bucket();
  request.index_name = index_name;
  request.is_primary = true;
  request.deferred = true;
  auto create = [&fixture, request](bool ignore_if_exists) {
    auto copy = request;
    copy.ignore_if_exists = ignore_if_exists;
    return fixture.execute(std::move(copy));
  };

  assert_false(static_cast<bool>(create(false).ctx.ec), "the primary index is created");
  assert_true(create(false).ctx.ec == couchbase::errc::common::index_exists,
              "creating it again reports index_exists");
  assert_false(static_cast<bool>(create(true).ctx.ec), "ignore_if_exists suppresses it");

  const auto index = find_index(indexes_or_skip(fixture), index_name);
  assert_true(index.has_value(), "the created primary index is listed");
  assert_true(index->is_primary, "it is reported as primary");
  assert_true(index->index_key.empty(), "a primary index has no keys");

  assert_false(static_cast<bool>(drop_index(fixture, index_name, true, false).ctx.ec),
               "the primary index is dropped");
  assert_true(drop_index(fixture, index_name, true, false).ctx.ec ==
                couchbase::errc::common::index_not_found,
              "dropping it again reports index_not_found");

  // The named index above does not separate the two drop RPCs -- DropIndex drops an index by name
  // whether or not it is primary. An *unnamed* primary drop does: DropPrimaryIndex resolves the
  // name server-side, and DropIndex is rejected for an empty one. That means this half owns
  // #primary on the test bucket while it runs, so it creates the index it drops.
  const index_guard unnamed{ fixture, {}, true };
  ops::management::query_index_create_request primary{};
  primary.bucket_name = fixture.bucket();
  primary.is_primary = true;
  primary.deferred = true;
  primary.ignore_if_exists = true;
  assert_false(static_cast<bool>(fixture.execute(std::move(primary)).ctx.ec),
               "an unnamed primary index is created");
  assert_false(static_cast<bool>(drop_index(fixture, {}, true, false).ctx.ec),
               "an unnamed primary index is dropped by the primary RPC");
  assert_true(drop_index(fixture, {}, true, false).ctx.ec ==
                couchbase::errc::common::index_not_found,
              "dropping it again reports index_not_found, not a rejected empty name");
}

// The routing hole: query_index_build_deferred_request was wired to BuildDeferredIndexes, but the
// public build_deferred_indexes() issued two other request types that nothing routed, so the
// operation failed at its first step over couchbase2 while the RPC it needed sat unused. This
// drives the public manager, which is the only place that hole was visible.
void
build_deferred_indexes_through_the_public_api_against_live_gateway()
{
  const auto connection_string = safe_getenv("TEST_CONNECTION_STRING");
  if (!connection_string.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  if (connection_string->rfind("couchbase2://", 0) != 0) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }

  couchbase::cluster_options options{ env_or("TEST_CB2_USERNAME", "Administrator"),
                                      env_or("TEST_CB2_PASSWORD", "password") };
  options.security().tls_verify(couchbase::tls_verify_mode::none);

  auto [connect_err, cluster] = couchbase::cluster::connect(*connection_string, options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds");

  const auto bucket = env_or("TEST_CB2_BUCKET", "default");
  const std::string index_name{ "cng_build_deferred" };
  auto manager = cluster.query_indexes();

  const auto listed = manager.get_all_indexes(bucket, {}).get();
  if (listed.first.ec() == couchbase::errc::common::feature_not_available) {
    cluster.close().get();
    skip("gateway does not implement admin.query.v1 (feature_not_available)");
  }
  assert_false(listed.first.ec().operator bool(), "the public manager lists indexes");

  static_cast<void>(manager.drop_index(bucket, index_name, {}).get());
  const auto created = manager
                         .create_index(bucket,
                                       index_name,
                                       { "field" },
                                       couchbase::create_query_index_options{}.build_deferred(true))
                         .get();
  assert_false(created.ec().operator bool(), "a deferred index is created through the public API");

  const auto built = manager.build_deferred_indexes(bucket, {}).get();
  static_cast<void>(manager.drop_index(bucket, index_name, {}).get());
  cluster.close().get();

  // The failure this replaces was feature_not_available, produced by cluster.cxx for a request
  // type it did not route -- not by the gateway, which serves the RPC this needs.
  assert_false(built.ec().operator bool(),
               "build_deferred_indexes() reaches the gateway over couchbase2");
}

// A closed cluster is the one input build_deferred_indexes() answers without sending anything, and
// the answer has to be handed back on the calling thread. cluster::close() runs io_.stop() and
// joins the io thread before it returns (public_cluster.cxx do_close), so nothing posted to that
// context afterwards is ever run: deferring the completion would replace a reported error with a
// handler that never fires and a future that never becomes ready.
//
// The assertion is therefore that the handler arrives at all. The bounded wait is what keeps a
// dropped completion a failure instead of a hung suite.
void
build_deferred_indexes_on_a_closed_cluster_answers_on_the_calling_thread()
{
  const auto connection_string = safe_getenv("TEST_CONNECTION_STRING");
  if (!connection_string.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  if (connection_string->rfind("couchbase2://", 0) != 0) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }

  couchbase::cluster_options options{ env_or("TEST_CB2_USERNAME", "Administrator"),
                                      env_or("TEST_CB2_PASSWORD", "password") };
  options.security().tls_verify(couchbase::tls_verify_mode::none);

  auto [connect_err, cluster] = couchbase::cluster::connect(*connection_string, options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds");

  // The manager holds its own reference to the core cluster, so it outlives the close.
  auto manager = cluster.query_indexes();
  cluster.close().get();

  std::promise<couchbase::error> barrier;
  auto answered = barrier.get_future();
  manager.build_deferred_indexes(
    env_or("TEST_CB2_BUCKET", "default"), {}, [&barrier](auto err) mutable {
      barrier.set_value(std::move(err));
    });

  assert_true(answered.wait_for(std::chrono::seconds{ 5 }) == std::future_status::ready,
              "a closed cluster answers build_deferred_indexes rather than dropping the handler");
  assert_true(answered.get().ec() == couchbase::errc::network::cluster_closed,
              "and answers it with cluster_closed");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_query_index_admin",
    {
      { "list_query_indexes_against_live_gateway",
        list_query_indexes_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "the_secondary_index_lifecycle_round_trips_against_live_gateway",
        the_secondary_index_lifecycle_round_trips_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "the_primary_index_lifecycle_round_trips_against_live_gateway",
        the_primary_index_lifecycle_round_trips_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "an_index_key_that_needs_escaping_round_trips_against_live_gateway",
        an_index_key_that_needs_escaping_round_trips_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "a_hostile_index_key_cannot_add_terms_against_live_gateway",
        a_hostile_index_key_cannot_add_terms_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "a_conditional_secondary_index_is_refused_over_couchbase2",
        a_conditional_secondary_index_is_refused_over_couchbase2,
        timeout::integration,
        test_env::cluster_only },
      { "build_deferred_indexes_through_the_public_api_against_live_gateway",
        build_deferred_indexes_through_the_public_api_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "build_deferred_indexes_on_a_closed_cluster_answers_on_the_calling_thread",
        build_deferred_indexes_on_a_closed_cluster_answers_on_the_calling_thread,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::cng::test
