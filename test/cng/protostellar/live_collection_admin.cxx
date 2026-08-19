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

// Live probes of scope and collection management over the couchbase2:// transport (CXXCBC-900),
// against admin.collection.v1: that the manifest arrives, and that a collection's max_expiry is
// the value it was created with when it is read back.
//
// max_expiry is where the two protocols disagree in shape. The core struct spends a sign on it
// (-1 no expiry, 0 inherit the bucket default, a positive value a TTL) and the proto field is an
// unsigned optional, so the sentinel has to travel as the field's presence. Only a round trip
// against a real gateway shows whether both halves of that mapping agree with the server.
//
// Expiry is layered: a bucket carries one and a collection may carry its own. A scope carries
// none -- there is no expiry field on CreateScopeRequest and none in the manifest -- so the layers
// are bucket and collection only, and the cases below cover both of them. What the layering does
// to a *document* is out of reach here: the effective expiry of a stored document is only
// observable through subdoc ($document.exptime), which the couchbase2 transport does not carry
// yet.
//
// History retention is here for a different reason. It is the one collection setting the classic
// path gates on a bucket capability before it sends anything, and that capability is read from a
// cluster map couchbase2 does not have -- so the request has to be sent and the server's answer
// taken, and only a live case shows what that answer is.

#include "fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/management/bucket_settings.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_drop.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"
#include "core/operations/management/scope_get_all.hxx"

#include <couchbase/error_codes.hxx>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace mgmt = ::couchbase::core::management::cluster;
namespace topology = ::couchbase::core::topology;

// Lists the manifest, failing the case for anything but the gateway not serving the service.
[[nodiscard]] auto
manifest_or_skip(live_cluster_fixture& fixture) -> topology::collections_manifest
{
  ops::management::scope_get_all_request request{};
  request.bucket_name = fixture.bucket();
  auto listed = fixture.execute(std::move(request));
  // Nothing on the client side answers this request with feature_not_available -- cluster.cxx
  // routes it and the component has no refusal path for it -- so the code can only have come from
  // a gateway that does not serve admin.collection.v1.
  if (listed.ctx.ec == couchbase::errc::common::feature_not_available) {
    skip("gateway does not implement admin.collection.v1 (feature_not_available)");
  }
  assert_false(static_cast<bool>(listed.ctx.ec), "list collections over couchbase2 succeeds");
  return std::move(listed.manifest);
}

[[nodiscard]] auto
find_collection(const topology::collections_manifest& manifest,
                const std::string& scope_name,
                const std::string& collection_name)
  -> std::optional<topology::collections_manifest::collection>
{
  const auto scope =
    std::find_if(manifest.scopes.begin(), manifest.scopes.end(), [&scope_name](const auto& s) {
      return s.name == scope_name;
    });
  if (scope == manifest.scopes.end()) {
    return {};
  }
  const auto collection = std::find_if(
    scope->collections.begin(), scope->collections.end(), [&collection_name](const auto& c) {
      return c.name == collection_name;
    });
  if (collection == scope->collections.end()) {
    return {};
  }
  return *collection;
}

// -999 for a collection that is not in the manifest at all, so a missing collection cannot pass an
// assertion by matching one of the three states the mapping produces.
[[nodiscard]] auto
find_max_expiry(const topology::collections_manifest& manifest,
                const std::string& scope_name,
                const std::string& collection_name) -> std::int32_t
{
  const auto collection = find_collection(manifest, scope_name, collection_name);
  if (!collection.has_value()) {
    return -999;
  }
  return collection->max_expiry;
}

// Reports history retention as a word rather than an optional<bool>, so a failure names which of
// the four states it found: on, off, a manifest entry carrying no history field, or no such
// collection. The last two are different defects and comparing optionals cannot tell them apart --
// both read as "not what was expected".
[[nodiscard]] auto
find_history(const topology::collections_manifest& manifest,
             const std::string& scope_name,
             const std::string& collection_name) -> std::string
{
  const auto collection = find_collection(manifest, scope_name, collection_name);
  if (!collection.has_value()) {
    return "no such collection";
  }
  if (!collection->history.has_value()) {
    return "no history field";
  }
  return *collection->history ? "on" : "off";
}

// Every case builds on the fixture's bucket, so each one begins by proving it is there and served:
// skipped if the gateway does not implement admin.collection.v1, failed if the bucket answers with
// a manifest that has no default scope and collection. Without it a bucket that is missing, empty
// or not the one the suite was pointed at surfaces several requests later, as whatever the first
// create happens to report.
void
require_default_collection(live_cluster_fixture& fixture)
{
  const auto manifest = manifest_or_skip(fixture);
  assert_true(find_collection(manifest, "_default", "_default").has_value(),
              "the fixture's bucket has a default scope and collection");
}

// A killed run leaves its scope or bucket behind and the next create then fails on "already
// exists" rather than on anything the case is about, so each one drops first. Not-found is the
// ordinary answer; any other error is the transport rather than the leftover, and discarding it
// here would make the create that follows fail for a reason this run had already seen.
void
drop_stale_scope(live_cluster_fixture& fixture, const std::string& scope_name)
{
  ops::management::scope_drop_request request{};
  request.bucket_name = fixture.bucket();
  request.scope_name = scope_name;
  const auto dropped = fixture.execute(std::move(request));
  assert_true(!dropped.ctx.ec || dropped.ctx.ec == couchbase::errc::common::scope_not_found,
              "dropping a leftover scope reports success or scope_not_found");
}

void
drop_stale_bucket(live_cluster_fixture& fixture, const std::string& bucket_name)
{
  ops::management::bucket_drop_request request{};
  request.name = bucket_name;
  const auto dropped = fixture.execute(std::move(request));
  assert_true(!dropped.ctx.ec || dropped.ctx.ec == couchbase::errc::common::bucket_not_found,
              "dropping a leftover bucket reports success or bucket_not_found");
}

// Lists the manifest of a named bucket. Polls, because a bucket does not serve its manifest the
// instant create() returns, and every caller here has just made one.
[[nodiscard]] auto
manifest_of(live_cluster_fixture& fixture, const std::string& bucket_name)
  -> topology::collections_manifest
{
  ops::management::scope_get_all_request request{};
  request.bucket_name = bucket_name;
  topology::collections_manifest manifest;
  for (int attempt = 0; attempt < 30; ++attempt) {
    auto listed = fixture.execute(request);
    if (!listed.ctx.ec) {
      manifest = std::move(listed.manifest);
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });
  }
  assert_false(manifest.scopes.empty(), "the new bucket serves its manifest");
  return manifest;
}

// Drops the scope however the case leaves -- an assertion failure unwinds, and a scope left behind
// makes the next run fail on "already exists". Dropping the scope takes its collections with it.
class scope_guard
{
public:
  scope_guard(live_cluster_fixture& fixture, std::string name)
    : fixture_{ fixture }
    , name_{ std::move(name) }
  {
  }

  scope_guard(const scope_guard&) = delete;
  scope_guard(scope_guard&&) = delete;
  auto operator=(const scope_guard&) -> scope_guard& = delete;
  auto operator=(scope_guard&&) -> scope_guard& = delete;

  // The one place the result cannot be asserted on: this runs while a failed assertion is
  // unwinding, and throwing a second exception out of a destructor terminates the process, which
  // would replace the failure message with a crash. A drop that fails here is picked up by the
  // next run instead, where drop_stale_scope() reports it before anything else.
  ~scope_guard()
  {
    ops::management::scope_drop_request request{};
    request.bucket_name = fixture_.bucket();
    request.scope_name = name_;
    static_cast<void>(fixture_.execute(std::move(request)));
  }

private:
  live_cluster_fixture& fixture_;
  std::string name_;
};

// Drops the bucket however the case leaves, for the reason scope_guard gives above.
class bucket_guard
{
public:
  bucket_guard(live_cluster_fixture& fixture, std::string name)
    : fixture_{ fixture }
    , name_{ std::move(name) }
  {
  }

  bucket_guard(const bucket_guard&) = delete;
  bucket_guard(bucket_guard&&) = delete;
  auto operator=(const bucket_guard&) -> bucket_guard& = delete;
  auto operator=(bucket_guard&&) -> bucket_guard& = delete;

  // Unasserted for the reason scope_guard's destructor gives.
  ~bucket_guard()
  {
    ops::management::bucket_drop_request request{};
    request.name = name_;
    static_cast<void>(fixture_.execute(std::move(request)));
  }

private:
  live_cluster_fixture& fixture_;
  std::string name_;
};

// A no-expiry collection needs a server that accepts maxTTL=-1, which is 7.6 and later. That is
// asserted rather than skipped on: collection_create_response carries no message field, so the
// server's own account of a refusal is not reachable here, and a skip keyed on the error code
// alone would also swallow the encode bug these cases exist to catch.
void
create_collection(live_cluster_fixture& fixture,
                  const std::string& scope_name,
                  const std::string& collection_name,
                  std::optional<std::int32_t> max_expiry)
{
  ops::management::collection_create_request request{};
  request.bucket_name = fixture.bucket();
  request.scope_name = scope_name;
  request.collection_name = collection_name;
  request.max_expiry = max_expiry;
  const auto created = fixture.execute(std::move(request));
  assert_false(static_cast<bool>(created.ctx.ec), "create collection over couchbase2 succeeds");
}

void
create_scope(live_cluster_fixture& fixture, const std::string& scope_name)
{
  drop_stale_scope(fixture, scope_name);

  ops::management::scope_create_request create{};
  create.bucket_name = fixture.bucket();
  create.scope_name = scope_name;
  const auto created = fixture.execute(std::move(create));
  assert_false(static_cast<bool>(created.ctx.ec), "create scope over couchbase2 succeeds");
}

void
list_collections_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto manifest = manifest_or_skip(fixture);
  // Naming the default scope and collection, rather than counting: a non-empty manifest is also
  // what a gateway pointed at some other bucket returns.
  assert_true(find_collection(manifest, "_default", "_default").has_value(),
              "the _default scope and collection are present");
}

// The case the two halves of the max_expiry mapping exist for: each of the three states written
// through create() is the state read back by the manifest. The two that collapse into each other
// are "no expiry" and "inherit the bucket default" -- a decode that reads max_expiry_secs without
// its presence bit reports every collection as the second, and an encode that writes an explicit 0
// for the second stores the first. Either way the collection has an expiry policy its creator did
// not ask for, and nothing fails.
void
collection_max_expiry_round_trips_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  require_default_collection(fixture);

  const std::string scope_name{ "cng_expiry" };
  create_scope(fixture, scope_name);
  scope_guard guard{ fixture, scope_name };

  // Unset and an explicit 0 are separate cases even though both mean "inherit the bucket default".
  // Only the second one reaches the sentinel encode, and it is the one that goes wrong quietly: an
  // explicit 0 written to the wire is what the gateway reads as no expiry.
  create_collection(fixture, scope_name, "inherits", {});
  create_collection(fixture, scope_name, "inherits_explicitly", 0);
  create_collection(fixture, scope_name, "never_expires", -1);
  create_collection(fixture, scope_name, "hourly", 3600);

  const auto manifest = manifest_or_skip(fixture);
  assert_eq(find_max_expiry(manifest, scope_name, "inherits"),
            std::int32_t{ 0 },
            "a collection created without a max expiry inherits the bucket default");
  assert_eq(find_max_expiry(manifest, scope_name, "inherits_explicitly"),
            std::int32_t{ 0 },
            "a collection created with max expiry 0 inherits the bucket default");
  assert_eq(find_max_expiry(manifest, scope_name, "never_expires"),
            std::int32_t{ -1 },
            "a collection created with no expiry reads back as no expiry");
  assert_eq(find_max_expiry(manifest, scope_name, "hourly"),
            std::int32_t{ 3600 },
            "a collection created with a TTL reads back with that TTL");
}

// The update path has its own encode, and one state it cannot express: an explicit 0 means "put
// this collection back on the bucket default", and on UpdateCollection an unset field means "leave
// it alone" instead. The component refuses it, and the collection has to be left as it was.
void
collection_update_max_expiry_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  require_default_collection(fixture);

  const std::string scope_name{ "cng_expiry_update" };
  create_scope(fixture, scope_name);
  scope_guard guard{ fixture, scope_name };

  create_collection(fixture, scope_name, "retuned", 3600);

  ops::management::collection_update_request to_no_expiry{};
  to_no_expiry.bucket_name = fixture.bucket();
  to_no_expiry.scope_name = scope_name;
  to_no_expiry.collection_name = "retuned";
  to_no_expiry.max_expiry = -1;
  const auto updated = fixture.execute(std::move(to_no_expiry));
  assert_false(static_cast<bool>(updated.ctx.ec), "update collection over couchbase2 succeeds");
  assert_eq(find_max_expiry(manifest_or_skip(fixture), scope_name, "retuned"),
            std::int32_t{ -1 },
            "an updated max expiry reads back as no expiry");

  ops::management::collection_update_request to_bucket_default{};
  to_bucket_default.bucket_name = fixture.bucket();
  to_bucket_default.scope_name = scope_name;
  to_bucket_default.collection_name = "retuned";
  to_bucket_default.max_expiry = 0;
  const auto refused = fixture.execute(std::move(to_bucket_default));
  assert_true(refused.ctx.ec == couchbase::errc::common::feature_not_available,
              "resetting a collection to the bucket default is refused over couchbase2");
  // The refusal is only worth anything if it kept its hands off the collection: a request that
  // reported an error and applied something anyway is worse than one that applied nothing.
  assert_eq(find_max_expiry(manifest_or_skip(fixture), scope_name, "retuned"),
            std::int32_t{ -1 },
            "the refused update left the collection as it was");
}

// -1 is the lowest value max_expiry gives a meaning, and the classic path refuses anything below
// it before encoding. The sentinel mapping cannot: every negative produces the same wire form, so
// without the refusal -2 would create a never-expiring collection and report success. Both RPCs
// carry the field, so both are checked.
void
collection_max_expiry_below_the_sentinel_is_refused_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  require_default_collection(fixture);

  const std::string scope_name{ "cng_expiry_invalid" };
  create_scope(fixture, scope_name);
  scope_guard guard{ fixture, scope_name };

  ops::management::collection_create_request create{};
  create.bucket_name = fixture.bucket();
  create.scope_name = scope_name;
  create.collection_name = "below_the_sentinel";
  create.max_expiry = -2;
  const auto created = fixture.execute(std::move(create));
  assert_true(created.ctx.ec == couchbase::errc::common::invalid_argument,
              "creating a collection with a max expiry below -1 is refused");
  assert_false(
    find_collection(manifest_or_skip(fixture), scope_name, "below_the_sentinel").has_value(),
    "the refused create sent nothing");

  create_collection(fixture, scope_name, "retuned", 3600);
  ops::management::collection_update_request update{};
  update.bucket_name = fixture.bucket();
  update.scope_name = scope_name;
  update.collection_name = "retuned";
  update.max_expiry = -2;
  const auto updated = fixture.execute(std::move(update));
  assert_true(updated.ctx.ec == couchbase::errc::common::invalid_argument,
              "updating a collection to a max expiry below -1 is refused");
  assert_eq(find_max_expiry(manifest_or_skip(fixture), scope_name, "retuned"),
            std::int32_t{ 3600 },
            "the refused update left the collection as it was");
}

// The second layer. A bucket carries its own maximum expiry, and a collection either takes it
// (the manifest reports 0, not the bucket's value) or overrides it. The two never merge on the
// wire, so a collection read back carrying the bucket's TTL would mean the layers had been
// flattened somewhere between the server and the core struct.
void
collection_max_expiry_is_layered_over_the_bucket_default()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  require_default_collection(fixture);

  const std::string bucket_name{ "cng_expiry_bucket" };
  mgmt::bucket_settings settings;
  settings.name = bucket_name;
  settings.ram_quota_mb = 100;
  settings.num_replicas = 0;
  settings.max_expiry = 300;

  // Dropped first for the reason create_scope gives: a killed run leaves the bucket behind.
  drop_stale_bucket(fixture, bucket_name);

  ops::management::bucket_create_request create_bucket{};
  create_bucket.bucket = std::move(settings);
  const auto bucket_created = fixture.execute(std::move(create_bucket));
  // A cluster with no spare KV quota is asserted on rather than skipped over. It is a property of
  // the cluster and not of the client, but a case that skips itself is a case that cannot report a
  // regression, and the quota is something the test cluster is configured to have: the CI action
  // raises it and fails the job if the new value never applies.
  assert_false(static_cast<bool>(bucket_created.ctx.ec),
               "create bucket over couchbase2 succeeds (" + bucket_created.error_message + ")");

  bucket_guard guard{ fixture, bucket_name };

  assert_eq(find_max_expiry(manifest_of(fixture, bucket_name), "_default", "_default"),
            std::int32_t{ 0 },
            "the bucket's own maximum expiry is not reported as the collection's");
}

// History retention is the one collection setting the classic path gates before it sends anything:
// cluster.cxx checks the bucket's non_deduped_history capability, which is read from a cluster map
// this transport does not have. Over couchbase2 the request is sent and the server answers, so
// these two cases cover what that answer is on each kind of bucket.
//
// The bucket sets history_retention_collection_default false so that a new collection starts with
// history off. Against a magma bucket's own default -- on -- a case asserting "on" would pass just
// as well against a client that dropped the field before sending it.
//
// Only "on" is observable. The gateway populates history_retention_enabled when the collection has
// history and leaves it unset otherwise (collectionadminserver.go:86-88), so off and "the server
// did not say" arrive identically, and the manifest cannot report a collection as history-off. The
// classic path reads ns_server's own `"history": false` and does report it.
void
collection_history_retention_round_trips_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  require_default_collection(fixture);

  const std::string bucket_name{ "cng_history_bucket" };
  mgmt::bucket_settings settings;
  settings.name = bucket_name;
  // 1024 is the gateway's own floor for a magma bucket (bucketadminserver.go), not the server's --
  // ns_server creates one at 100. The CI cluster's data quota is sized for this bucket.
  settings.ram_quota_mb = 1024;
  settings.num_replicas = 0;
  // History retention is a magma feature; the same requests against the couchstore bucket are the
  // case below.
  settings.storage_backend = mgmt::bucket_storage_backend::magma;
  settings.history_retention_collection_default = false;

  drop_stale_bucket(fixture, bucket_name);

  ops::management::bucket_create_request create_bucket{};
  create_bucket.bucket = std::move(settings);
  const auto bucket_created = fixture.execute(std::move(create_bucket));
  assert_false(static_cast<bool>(bucket_created.ctx.ec),
               "create magma bucket over couchbase2 succeeds (" + bucket_created.error_message +
                 ")");
  bucket_guard guard{ fixture, bucket_name };

  ops::management::collection_create_request with_history{};
  with_history.bucket_name = bucket_name;
  with_history.scope_name = "_default";
  with_history.collection_name = "history_on";
  with_history.history = true;
  const auto created = fixture.execute(std::move(with_history));
  assert_false(static_cast<bool>(created.ctx.ec),
               "create collection with history retention over couchbase2 succeeds");
  assert_eq(find_history(manifest_of(fixture, bucket_name), "_default", "history_on"),
            std::string{ "on" },
            "a collection created with history retention on reads back on");

  ops::management::collection_create_request without_history{};
  without_history.bucket_name = bucket_name;
  without_history.scope_name = "_default";
  without_history.collection_name = "defaulted";
  const auto defaulted = fixture.execute(std::move(without_history));
  assert_false(static_cast<bool>(defaulted.ctx.ec), "create collection over couchbase2 succeeds");
  // A tripwire rather than a property of the client: this collection has history off, and no
  // client change can make the manifest say so. It fails the day the gateway starts sending the
  // field for an off collection, which is the day the state becomes readable.
  assert_eq(find_history(manifest_of(fixture, bucket_name), "_default", "defaulted"),
            std::string{ "no history field" },
            "a collection with history retention off is not reported as off");

  ops::management::collection_update_request update{};
  update.bucket_name = bucket_name;
  update.scope_name = "_default";
  update.collection_name = "defaulted";
  update.history = true;
  const auto updated = fixture.execute(std::move(update));
  assert_false(static_cast<bool>(updated.ctx.ec),
               "update collection history retention over couchbase2 succeeds");
  assert_eq(find_history(manifest_of(fixture, bucket_name), "_default", "defaulted"),
            std::string{ "on" },
            "an updated history retention reads back on");
}

// The other side of the same routing, and the one that shows where the refusal now comes from.
// Couchstore does not carry history retention: the classic path reports that from the bucket
// capability without sending anything, and over couchbase2 the server says it instead. What the
// caller sees is therefore not the same code, and it is not the same code on the two RPCs either --
// the gateway maps this refusal on UpdateCollection and not on CreateCollection
// (collectionadminserver.go: only the update chain tests cbmgmtx.ErrServerInvalidArg).
//
// These assertions record what the codes are, not what they should be. They fail if the gateway
// starts mapping them alike, which is the point: the difference is worth noticing.
void
collection_history_retention_on_a_couchstore_bucket_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();
  const auto manifest = manifest_or_skip(fixture);
  // The fixture's bucket is whatever the suite was pointed at, and nothing here guarantees its
  // storage backend. Asserting history is off on _default names what makes this case mean
  // something: a magma bucket reports it on, and the refusals below would then not be about
  // couchstore at all.
  const auto default_history = find_history(manifest, "_default", "_default");
  assert_true(default_history != "on",
              "the fixture's bucket does not have history retention on its default collection (" +
                default_history + ")");

  const std::string scope_name{ "cng_history_refused" };
  create_scope(fixture, scope_name);
  scope_guard guard{ fixture, scope_name };

  ops::management::collection_create_request create{};
  create.bucket_name = fixture.bucket();
  create.scope_name = scope_name;
  create.collection_name = "history_on";
  create.history = true;
  const auto created = fixture.execute(std::move(create));
  assert_true(created.ctx.ec == couchbase::errc::common::internal_server_failure,
              "creating a collection with history retention on couchstore is refused (" +
                created.ctx.ec.message() + ")");
  assert_false(find_collection(manifest_or_skip(fixture), scope_name, "history_on").has_value(),
               "the refused create left no collection behind");

  create_collection(fixture, scope_name, "plain", {});
  ops::management::collection_update_request update{};
  update.bucket_name = fixture.bucket();
  update.scope_name = scope_name;
  update.collection_name = "plain";
  update.history = true;
  const auto updated = fixture.execute(std::move(update));
  assert_true(updated.ctx.ec == couchbase::errc::common::invalid_argument,
              "updating a collection to history retention on couchstore is refused (" +
                updated.ctx.ec.message() + ")");
  assert_true(find_history(manifest_or_skip(fixture), scope_name, "plain") != "on",
              "the refused update did not turn history retention on");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_collection_admin",
    {
      { "list_collections_against_live_gateway",
        list_collections_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "collection_max_expiry_round_trips_against_live_gateway",
        collection_max_expiry_round_trips_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "collection_update_max_expiry_against_live_gateway",
        collection_update_max_expiry_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "collection_max_expiry_below_the_sentinel_is_refused_against_live_gateway",
        collection_max_expiry_below_the_sentinel_is_refused_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "collection_max_expiry_is_layered_over_the_bucket_default",
        collection_max_expiry_is_layered_over_the_bucket_default,
        timeout::integration,
        test_env::cluster_only },
      { "collection_history_retention_round_trips_against_live_gateway",
        collection_history_retention_round_trips_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "collection_history_retention_on_a_couchstore_bucket_against_live_gateway",
        collection_history_retention_on_a_couchstore_bucket_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::test
