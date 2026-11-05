/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "framework/test_registry.hxx"
#include "utils/topology_fixtures.hxx"

#include "core/document_id.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/crc32.hxx"

#include <couchbase/read_preference.hxx>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::read_preference;
using couchbase::core::topology::configuration;
using ::test::utils::config_with_vbmap;
using ::test::utils::vbucket_map;

void
an_absent_vbucket_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(std::nullopt, 1);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "there is no map to look in");
}

void
a_vbucket_beyond_the_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(1, 0).has_value(), "the map holds one row");
}

void
a_chain_index_equal_to_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 2).has_value(), "the chain holds two entries");
}

void
a_chain_index_beyond_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 42).has_value(), "the chain holds two entries");
}

void
an_empty_chain_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ {} }, 0);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "the row lists no node at all");
}

void
an_unassigned_chain_slot_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, -1 } }, 1);
  assert_false(config.server_by_vbucket(0, 1).has_value(), "-1 marks the slot unassigned");
}

void
an_assigned_chain_slot_names_its_node_index([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 3, 7 } }, 1);
  assert_true(config.server_by_vbucket(0, 0) == 3, "the active copy's node");
  assert_true(config.server_by_vbucket(0, 1) == 7, "the first replica's node");
}

void
replica_reads_stop_at_the_end_of_a_short_vbucket_row([[maybe_unused]] context& ctx)
{
  // A configuration may advertise more replicas than its vbucket-map rows
  // list: the two fields are parsed independently. Callers derive the replica
  // index from num_replicas, so the surplus indexes must not be read.
  const auto config =
    std::make_shared<configuration>(config_with_vbmap(vbucket_map{ { 0, 1 } }, 2, 3));
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };

  const auto nodes = couchbase::core::impl::effective_nodes(
    id, config, read_preference::no_preference, /* preferred_server_group */ "");

  assert_eq(nodes.size(), std::size_t{ 2 }, "one node per entry the row actually lists");
  assert_false(nodes[0].is_replica, "the first entry is the active copy");
  assert_eq(nodes[0].index, std::size_t{ 0 }, "the active copy's node");
  assert_true(nodes[1].is_replica, "the second entry is a replica");
  assert_eq(nodes[1].index, std::size_t{ 1 }, "the first replica's node");
}

void
a_vbucket_map_without_rows_maps_no_key([[maybe_unused]] context& ctx)
{
  // The vbucket count comes from the map itself, so a map present but without
  // rows must not be asked which vbucket a key belongs to.
  const auto config = config_with_vbmap(vbucket_map{}, 1);

  const auto [vbucket, server] = config.map_key("foo", 0);
  assert_eq(vbucket, 0, "no row can be chosen");
  assert_false(server.has_value(), "no row means no server");

  const std::vector<std::byte> binary_key{ std::byte{ 'f' }, std::byte{ 'o' }, std::byte{ 'o' } };
  const auto [binary_vbucket, binary_server] = config.map_key(binary_key, 0);
  assert_eq(binary_vbucket, 0, "no row can be chosen");
  assert_false(binary_server.has_value(), "no row means no server");

  const auto [hash_vbucket, hash_server] =
    config.map_key_by_hash(couchbase::core::utils::hash_crc32("foo", 3), 0);
  assert_eq(hash_vbucket, 0, "no row can be chosen");
  assert_false(hash_server.has_value(), "no row means no server");
}

auto
bitwise_ieee8023_couchbase_crc32(const std::uint8_t* data, std::size_t len) noexcept
  -> std::uint32_t
{
  std::uint32_t crc = 0xffffffffU;
  for (std::size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1U) ^ ((crc & 1U) != 0U ? 0xedb88320U : 0U);
    }
  }
  return ((~crc) >> 16U) & 0x7fffU;
}

void
crc32_hardware_and_software_slicing_agree_across_lengths_and_alignments(
  [[maybe_unused]] context& ctx)
{
  // Standard IEEE 802.3 CRC-32 of "123456789" is 0xcbf43926; Couchbase hash is
  // (0xcbf43926 >> 16) & 0x7fff = 0x4bf4.
  assert_eq(couchbase::core::utils::hash_crc32("123456789", 9),
            std::uint32_t{ 0x4bf4U },
            "known-answer test vector for 123456789");
  assert_eq(couchbase::core::utils::hash_crc32_software("123456789", 9),
            std::uint32_t{ 0x4bf4U },
            "software known-answer test vector for 123456789");

  std::vector<std::uint8_t> raw(264);
  for (std::size_t i = 0; i < raw.size(); ++i) {
    raw[i] = static_cast<std::uint8_t>(((i * 131U) + 17U) & 0xffU);
  }

  for (std::size_t align = 0; align < 8; ++align) {
    for (std::size_t len = 0; len <= 250; ++len) {
      const auto* u8_ptr = raw.data() + align;
      const auto* char_ptr = reinterpret_cast<const char*>(u8_ptr);
      const std::uint32_t expected = bitwise_ieee8023_couchbase_crc32(u8_ptr, len);

      const auto sw = couchbase::core::utils::hash_crc32_software(char_ptr, len);
      const auto hw = couchbase::core::utils::hash_crc32(char_ptr, len);
      assert_eq(sw, expected, "software slicing-by-8 matches bitwise IEEE 802.3 reference");
      assert_eq(hw, expected, "hash_crc32 matches bitwise IEEE 802.3 reference");
    }
  }
}

void
map_key_by_hash_matches_map_key_across_vbuckets_and_replicas([[maybe_unused]] context& ctx)
{
  vbucket_map map(64);
  for (std::size_t vb = 0; vb < 64; ++vb) {
    map[vb] = {
      static_cast<std::int16_t>(vb % 3),
      static_cast<std::int16_t>((vb + 1) % 3),
    };
  }
  const auto config = config_with_vbmap(std::move(map), 1, 3);

  for (int i = 0; i < 200; ++i) {
    const std::string key = "document_key_" + std::to_string(i);
    const auto crc = couchbase::core::utils::hash_crc32(key.data(), key.size());
    const auto expected_vbucket = static_cast<std::uint16_t>(crc % 64U);
    for (const std::size_t index : { std::size_t{ 0 }, std::size_t{ 1 }, std::size_t{ 2 } }) {
      const std::optional<std::size_t> expected_server =
        index < 2 ? std::optional<std::size_t>{ (expected_vbucket + index) % 3U } : std::nullopt;
      const auto from_key = config.map_key(key, index);
      const auto from_hash = config.map_key_by_hash(crc, index);
      assert_eq(from_key.first, expected_vbucket, "map_key vbucket matches fixture formula");
      assert_true(from_key.second == expected_server,
                  "map_key server index matches fixture formula");
      assert_eq(from_hash.first, expected_vbucket, "map_key_by_hash vbucket matches fixture");
      assert_true(from_hash.second == expected_server,
                  "map_key_by_hash server index matches fixture");
    }
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_absent_vbucket_map_names_no_server), {}, timeout::instant },
      { CASE(a_vbucket_beyond_the_map_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_equal_to_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_beyond_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(an_empty_chain_names_no_server), {}, timeout::instant },
      { CASE(an_unassigned_chain_slot_names_no_server), {}, timeout::instant },
      { CASE(an_assigned_chain_slot_names_its_node_index), {}, timeout::instant },
      { CASE(replica_reads_stop_at_the_end_of_a_short_vbucket_row), {}, timeout::instant },
      { CASE(a_vbucket_map_without_rows_maps_no_key), {}, timeout::instant },
      { CASE(crc32_hardware_and_software_slicing_agree_across_lengths_and_alignments),
        {},
        timeout::instant },
      { CASE(map_key_by_hash_matches_map_key_across_vbuckets_and_replicas), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
