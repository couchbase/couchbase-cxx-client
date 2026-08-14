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

// Unit tests for the collection-admin <-> couchbase.admin.collection.v1 manifest decode and the
// max_expiry encode that has to agree with it (CXXCBC-900). Pure, no server.

#include "framework/test_registry.hxx"

#include "core/protostellar/collection_admin_converter.hxx"

#include <cstdint>
#include <string>

namespace couchbase::test
{
namespace
{
namespace ca = ::couchbase::core::protostellar::collection_admin;
namespace v1 = ::couchbase::admin::collection::v1;

void
decode_manifest_maps_scopes_and_collections([[maybe_unused]] context& ctx)
{
  v1::ListCollectionsResponse proto;
  proto.set_manifest_uid(42);
  auto* scope = proto.add_scopes();
  scope->set_name("inventory");
  auto* products = scope->add_collections();
  products->set_name("products");
  products->set_max_expiry_secs(3600);
  products->set_history_retention_enabled(true);
  auto* orders = scope->add_collections();
  orders->set_name("orders");

  const auto manifest = ca::decode_manifest(proto);
  assert_eq(manifest.uid, std::uint64_t{ 42 }, "manifest uid decoded");
  assert_eq(manifest.scopes.size(), std::size_t{ 1 }, "one scope decoded");
  assert_eq(manifest.scopes.at(0).name, std::string{ "inventory" }, "scope name decoded");
  assert_eq(manifest.scopes.at(0).collections.size(), std::size_t{ 2 }, "two collections decoded");
  assert_eq(manifest.scopes.at(0).collections.at(0).name,
            std::string{ "products" },
            "collection name decoded");
  assert_eq(manifest.scopes.at(0).collections.at(0).max_expiry,
            std::int32_t{ 3600 },
            "collection max_expiry decoded");
  assert_true(manifest.scopes.at(0).collections.at(0).history.has_value() &&
                manifest.scopes.at(0).collections.at(0).history.value(),
              "collection history decoded");
  assert_false(manifest.scopes.at(0).collections.at(1).history.has_value(),
               "unset history stays nullopt");
}

// max_expiry is the one collection field whose meaning is carried by a sign the wire cannot hold,
// so the two halves of the mapping are pinned separately and then against each other.
void
encode_max_expiry_moves_the_sentinel_into_field_presence([[maybe_unused]] context& ctx)
{
  const auto no_expiry = ca::encode_max_expiry(-1);
  assert_true(no_expiry.has_value(), "no-expiry is written to the wire");
  assert_eq(no_expiry.value(), std::uint32_t{ 0 }, "no-expiry is an explicit 0");

  assert_false(ca::encode_max_expiry(0).has_value(), "inherit-the-bucket-default leaves it unset");

  const auto ttl = ca::encode_max_expiry(3600);
  assert_true(ttl.has_value(), "a positive TTL is written to the wire");
  assert_eq(ttl.value(), std::uint32_t{ 3600 }, "a positive TTL is written unchanged");
}

void
decode_max_expiry_reads_field_presence_back_as_the_sentinel([[maybe_unused]] context& ctx)
{
  v1::ListCollectionsResponse proto;
  auto* scope = proto.add_scopes();
  scope->set_name("_default");
  scope->add_collections()->set_name("inherits");
  auto* never = scope->add_collections();
  never->set_name("never_expires");
  never->set_max_expiry_secs(0);
  auto* hour = scope->add_collections();
  hour->set_name("hourly");
  hour->set_max_expiry_secs(3600);

  const auto manifest = ca::decode_manifest(proto);
  const auto& collections = manifest.scopes.at(0).collections;
  assert_eq(collections.at(0).max_expiry,
            std::int32_t{ 0 },
            "an absent field is inherit-the-bucket-default");
  assert_eq(collections.at(1).max_expiry, std::int32_t{ -1 }, "an explicit 0 is no-expiry");
  assert_eq(collections.at(2).max_expiry, std::int32_t{ 3600 }, "a positive TTL survives");
}

// The encode and decode above are only correct together: reading a collection back has to return
// the max_expiry it was created with, which is what a caller comparing the two ever sees.
void
max_expiry_round_trips_through_the_wire_form([[maybe_unused]] context& ctx)
{
  for (const auto expected : { std::int32_t{ -1 }, std::int32_t{ 0 }, std::int32_t{ 900 } }) {
    v1::ListCollectionsResponse proto;
    auto* collection = proto.add_scopes()->add_collections();
    collection->set_name("c");
    if (const auto secs = ca::encode_max_expiry(expected); secs.has_value()) {
      collection->set_max_expiry_secs(secs.value());
    }

    assert_eq(ca::decode_manifest(proto).scopes.at(0).collections.at(0).max_expiry,
              expected,
              "max_expiry " + std::to_string(expected) + " round-trips");
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(decode_manifest_maps_scopes_and_collections) },
      { CASE(encode_max_expiry_moves_the_sentinel_into_field_presence) },
      { CASE(decode_max_expiry_reads_field_presence_back_as_the_sentinel) },
      { CASE(max_expiry_round_trips_through_the_wire_form) },
    },
  };
}

} // namespace couchbase::test
