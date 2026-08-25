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

// Live probes of bucket management over the couchbase2:// transport (CXXCBC-900), against
// admin.bucket.v1: that the bucket list arrives, and that settings written by create() are the
// settings read back by get().

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/management/bucket_settings.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_drop.hxx"
#include "core/operations/management/bucket_flush.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "core/operations/management/bucket_get_all.hxx"
#include "core/operations/management/bucket_update.hxx"

#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace mgmt = ::couchbase::core::management::cluster;

void
list_buckets_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(ops::management::bucket_get_all_request{});
  // Nothing on the client side answers this request with feature_not_available -- cluster.cxx
  // routes it and the component has no refusal path for it -- so the code can only have come from a
  // gateway that does not serve admin.bucket.v1.
  if (result.ctx.ec == couchbase::errc::common::feature_not_available) {
    skip("gateway does not implement admin.bucket.v1 (feature_not_available)");
  }
  assert_false(static_cast<bool>(result.ctx.ec), "list buckets over couchbase2 succeeds");

  // Naming the bucket, rather than counting the list: a non-empty list is also what a gateway
  // pointed at some other cluster returns.
  const auto& expected = fixture.bucket();
  const auto found = std::any_of(
    result.buckets.begin(), result.buckets.end(), [&expected](const mgmt::bucket_settings& bucket) {
      return bucket.name == expected;
    });
  assert_true(found, "the bucket under test appears in the list");
}

// Drops the bucket however the case leaves -- an assertion failure unwinds, and a bucket left
// behind makes the next run fail on "already exists".
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

// Creates the bucket, or skips the case for a condition that belongs to the cluster rather than to
// the client.
void
create_or_skip(live_cluster_fixture& fixture, const mgmt::bucket_settings& settings)
{
  ops::management::bucket_create_request create{};
  create.bucket = settings;
  const auto created = fixture.execute(std::move(create));
  if (created.ctx.ec == couchbase::errc::common::feature_not_available) {
    skip("gateway does not implement admin.bucket.v1 (feature_not_available)");
  }
  // A cluster whose whole KV quota is already committed cannot host another bucket, which is the
  // state a stock cbdinocluster allocation is in. That is a property of the cluster and not of the
  // client, so it is a skip -- but only on the server's own account of it: the message is quoted
  // from the gateway's status, and no client-side refusal produces one.
  if (created.ctx.ec == couchbase::errc::common::invalid_argument &&
      created.error_message.find("ram quota") != std::string::npos) {
    skip("cluster has no spare KV quota for a test bucket (" + created.error_message + ")");
  }
  // A minimum durability level needs enough data servers to satisfy it, which a single-node cluster
  // does not have. Same reasoning as the quota above: a property of the cluster, reported in the
  // server's own words, so it cannot be confused with the client refusing the setting.
  if (created.ctx.ec == couchbase::errc::common::invalid_argument &&
      created.error_message.find("DurabilityMinLevel") != std::string::npos) {
    skip("cluster cannot satisfy the requested durability (" + created.error_message + ")");
  }
  assert_false(static_cast<bool>(created.ctx.ec),
               "create bucket over couchbase2 succeeds: " + created.error_message);
}

// Neither a create nor an update is visible the instant its RPC returns, so a case that reads
// straight back is asserting on whichever of the two it happened to win. Poll until the bucket is
// there and carries what the case is waiting for.
auto
wait_for_bucket(live_cluster_fixture& fixture,
                const std::string& name,
                const std::function<bool(const mgmt::bucket_settings&)>& ready)
  -> mgmt::bucket_settings
{
  ops::management::bucket_get_response fetched;
  for (int attempt = 0; attempt < 30; ++attempt) {
    ops::management::bucket_get_request get{};
    get.name = name;
    fetched = fixture.execute(std::move(get));
    if (!fetched.ctx.ec && ready(fetched.bucket)) {
      return fetched.bucket;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });
  }
  assert_false(static_cast<bool>(fetched.ctx.ec), "get bucket over couchbase2 succeeds");
  assert_true(false, "bucket '" + name + "' did not reach the expected state");
  return {};
}

void
bucket_settings_round_trip_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const std::string name{ "cng-round-trip" };

  mgmt::bucket_settings settings;
  settings.name = name;
  settings.bucket_type = mgmt::bucket_type::couchbase;
  settings.ram_quota_mb = 100; // the gateway's floor for a couchstore bucket
  settings.num_replicas = 0;
  settings.flush_enabled = true;
  settings.max_expiry = 3600;
  settings.compression_mode = mgmt::bucket_compression::active;
  // Not what the gateway would choose by itself -- it defaults conflict resolution to
  // sequence_number -- so this fails if the field is written but not read back. Minimum durability
  // is the other such field, and it has its own case below because not every cluster can satisfy
  // one; keeping it out of here means a single-node cluster still runs everything else.
  settings.conflict_resolution_type = mgmt::bucket_conflict_resolution::timestamp;

  create_or_skip(fixture, settings);
  const bucket_guard guard{ fixture, name };

  const auto read_back = wait_for_bucket(fixture, name, [](const mgmt::bucket_settings&) {
    return true;
  });
  assert_eq(read_back.name, name, "name round-trips");
  assert_true(read_back.conflict_resolution_type == mgmt::bucket_conflict_resolution::timestamp,
              "conflict resolution round-trips");
  assert_true(read_back.flush_enabled.has_value() && read_back.flush_enabled.value(),
              "flush_enabled round-trips");
  assert_eq(read_back.ram_quota_mb, std::uint64_t{ 100 }, "ram quota round-trips");
  assert_true(read_back.compression_mode == mgmt::bucket_compression::active,
              "compression mode round-trips");
  // storage_backend and the history-retention settings are not asserted here. Reading them back
  // only proves anything for magma, which the gateway requires a 1024 MB quota for (and history
  // retention a further 2048 MB), more than a development cluster reliably has spare. The
  // converter unit tests cover both against a constructed response.
}

// Its own case rather than part of the round trip above, because the server refuses a minimum
// durability the cluster has too few data servers for, and a single-node cluster has too few for
// any of them. Folding it in would make the whole round trip skip there instead of just this.
void
minimum_durability_round_trips_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const std::string name{ "cng-durability" };

  mgmt::bucket_settings settings;
  settings.name = name;
  settings.bucket_type = mgmt::bucket_type::couchbase;
  settings.ram_quota_mb = 100;
  settings.num_replicas = 1;
  // A bucket has no minimum durability unless one is asked for, so reading this back is only
  // possible if the field survived both directions.
  settings.minimum_durability_level = couchbase::durability_level::majority;

  create_or_skip(fixture, settings);
  const bucket_guard guard{ fixture, name };

  const auto read_back = wait_for_bucket(fixture, name, [](const mgmt::bucket_settings& bucket) {
    return bucket.minimum_durability_level.has_value();
  });
  assert_true(read_back.minimum_durability_level.value() == couchbase::durability_level::majority,
              "minimum durability round-trips");
}

// update -> flush -> drop over one bucket, which is the shape gocbcoreps' bucket admin tests use.
// Each step is checked through a subsequent read rather than on its own status alone.
void
bucket_lifecycle_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const std::string name{ "cng-lifecycle" };

  mgmt::bucket_settings settings;
  settings.name = name;
  settings.bucket_type = mgmt::bucket_type::couchbase;
  settings.ram_quota_mb = 100;
  settings.num_replicas = 0;
  settings.flush_enabled = false;

  create_or_skip(fixture, settings);
  const bucket_guard guard{ fixture, name };
  static_cast<void>(wait_for_bucket(fixture, name, [](const mgmt::bucket_settings& bucket) {
    return bucket.flush_enabled.has_value() && !bucket.flush_enabled.value();
  }));

  // FLUSH_DISABLED arrives as a FAILED_PRECONDITION whose violation type names it. Without that
  // mapping the caller is told the cluster failed, rather than that the bucket has flush turned off
  // and they can turn it on.
  {
    ops::management::bucket_flush_request flush{};
    flush.name = name;
    const auto refused = fixture.execute(std::move(flush));
    assert_true(refused.ctx.ec == couchbase::errc::management::bucket_not_flushable,
                "flushing a bucket with flush disabled reports bucket_not_flushable");
  }

  settings.flush_enabled = true;
  settings.ram_quota_mb = 128;
  {
    ops::management::bucket_update_request update{};
    update.bucket = settings;
    const auto updated = fixture.execute(std::move(update));
    assert_false(static_cast<bool>(updated.ctx.ec),
                 "update bucket over couchbase2 succeeds: " + updated.error_message);
  }

  const auto after_update = wait_for_bucket(fixture, name, [](const mgmt::bucket_settings& bucket) {
    return bucket.flush_enabled.value_or(false);
  });
  assert_eq(
    after_update.ram_quota_mb, std::uint64_t{ 128 }, "the updated quota is what get reports");

  // Enabling flush is visible in the bucket list before the server will act on it, so the first
  // flush after the update can still be refused. Retried rather than slept on, and bounded so a
  // permanent failure is still a failure.
  {
    std::error_code flush_ec{};
    for (int attempt = 0; attempt < 20; ++attempt) {
      ops::management::bucket_flush_request flush{};
      flush.name = name;
      flush_ec = fixture.execute(std::move(flush)).ctx.ec;
      if (!flush_ec) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });
    }
    assert_false(static_cast<bool>(flush_ec),
                 "flush succeeds once the bucket allows it: " + flush_ec.message());
  }

  {
    ops::management::bucket_drop_request drop{};
    drop.name = name;
    const auto dropped = fixture.execute(std::move(drop));
    assert_false(static_cast<bool>(dropped.ctx.ec), "drop bucket over couchbase2 succeeds");
  }

  ops::management::bucket_get_request get{};
  get.name = name;
  const auto gone = fixture.execute(std::move(get));
  assert_true(gone.ctx.ec == couchbase::errc::common::bucket_not_found,
              "a dropped bucket is no longer reported");
}

// The gateway distinguishes these by attaching a ResourceInfo naming the resource type, which is
// what turns a bare NOT_FOUND into bucket_not_found rather than document_not_found. None of them
// needs spare quota: each is refused before anything is allocated.
void
bucket_errors_carry_the_bucket_specific_code([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const std::string missing{ "cng-no-such-bucket" };

  {
    mgmt::bucket_settings settings;
    settings.name = fixture.bucket(); // already exists
    settings.bucket_type = mgmt::bucket_type::couchbase;
    settings.ram_quota_mb = 100;
    ops::management::bucket_create_request create{};
    create.bucket = std::move(settings);
    const auto created = fixture.execute(std::move(create));
    if (created.ctx.ec == couchbase::errc::common::feature_not_available) {
      skip("gateway does not implement admin.bucket.v1 (feature_not_available)");
    }
    assert_true(created.ctx.ec == couchbase::errc::management::bucket_exists,
                "creating an existing bucket reports bucket_exists, not document_exists");
  }
  {
    mgmt::bucket_settings settings;
    settings.name = missing;
    ops::management::bucket_update_request update{};
    update.bucket = std::move(settings);
    const auto updated = fixture.execute(std::move(update));
    assert_true(updated.ctx.ec == couchbase::errc::common::bucket_not_found,
                "updating a missing bucket reports bucket_not_found");
  }
  {
    ops::management::bucket_drop_request drop{};
    drop.name = missing;
    const auto dropped = fixture.execute(std::move(drop));
    assert_true(dropped.ctx.ec == couchbase::errc::common::bucket_not_found,
                "dropping a missing bucket reports bucket_not_found");
  }
  {
    ops::management::bucket_flush_request flush{};
    flush.name = missing;
    const auto flushed = fixture.execute(std::move(flush));
    assert_true(flushed.ctx.ec == couchbase::errc::common::bucket_not_found,
                "flushing a missing bucket reports bucket_not_found");
  }
  {
    ops::management::bucket_get_request get{};
    get.name = missing;
    const auto fetched = fixture.execute(std::move(get));
    assert_true(fetched.ctx.ec == couchbase::errc::common::bucket_not_found,
                "getting a missing bucket reports bucket_not_found");
  }
  {
    // A replica count the cluster cannot satisfy. The gateway forwards it and the server rejects
    // it, so this also pins that an INVALID_ARGUMENT is not flattened into a generic failure.
    mgmt::bucket_settings settings;
    settings.name = "cng-too-many-replicas";
    settings.bucket_type = mgmt::bucket_type::couchbase;
    settings.ram_quota_mb = 100;
    settings.num_replicas = 5;
    ops::management::bucket_create_request create{};
    create.bucket = std::move(settings);
    const auto created = fixture.execute(std::move(create));
    assert_true(created.ctx.ec == couchbase::errc::common::invalid_argument,
                "an impossible replica count reports invalid_argument");
  }
}

// memcached buckets have no couchbase2 representation, so the request is refused by the client
// before anything is sent, on the io context like every other completion.
void
a_memcached_bucket_is_refused_by_the_client([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  mgmt::bucket_settings settings;
  settings.name = "cng-memcached";
  settings.bucket_type = mgmt::bucket_type::memcached;
  settings.ram_quota_mb = 100;

  ops::management::bucket_create_request create{};
  create.bucket = std::move(settings);
  const auto [created, handler_thread] = fixture.execute_on(std::move(create));

  assert_true(created.ctx.ec == couchbase::errc::common::feature_not_available,
              "a memcached bucket is refused rather than sent");
  assert_true(created.error_message.empty(),
              "the refusal is the client's own, so it carries no gateway message");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_bucket_admin",
    {
      { "list_buckets_against_live_gateway",
        list_buckets_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "bucket_settings_round_trip_against_live_gateway",
        bucket_settings_round_trip_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "minimum_durability_round_trips_against_live_gateway",
        minimum_durability_round_trips_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "bucket_lifecycle_against_live_gateway",
        bucket_lifecycle_against_live_gateway,
        { needs::real_cluster() },
        timeout::integration },
      { "bucket_errors_carry_the_bucket_specific_code",
        bucket_errors_carry_the_bucket_specific_code,
        { needs::real_cluster() },
        timeout::integration },
      { "a_memcached_bucket_is_refused_by_the_client",
        a_memcached_bucket_is_refused_by_the_client,
        { needs::real_cluster() },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
