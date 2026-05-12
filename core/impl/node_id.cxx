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

#include "node_id.hxx"

#include "core/utils/crc32.hxx"

#include <array>
#include <cstdint>
#include <string>
#include <utility>

namespace couchbase
{

namespace
{
/**
 * Produces a stable, opaque hex string from hostname + KV port.
 *
 * Uses CRC32 (already in-tree for vBucket mapping) rather than a
 * cryptographic hash — collision resistance across a handful of cluster
 * nodes is more than sufficient, and this avoids pulling in OpenSSL headers.
 */
auto
derive_fallback_id(const std::string& hostname, std::uint16_t port) -> std::string
{
  auto input = hostname + ':' + std::to_string(port);
  auto crc = core::utils::hash_crc32(input.data(), input.size());

  // Format as 8 lowercase hex chars, zero-padded — avoids a heavy fmt include
  // for what amounts to a two-line hex conversion.
  static constexpr std::array<char, 16> hex_digits{ '0', '1', '2', '3', '4', '5', '6', '7',
                                                    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
  std::string out;
  out.reserve(8);
  for (int shift = 28; shift >= 0; shift -= 4) {
    out.push_back(hex_digits[(crc >> shift) & 0xFU]);
  }
  return out;
}
} // namespace

node_id::node_id(std::string node_uuid, std::string hostname, std::uint16_t port)
  : node_uuid_{ std::move(node_uuid) }
  , hostname_{ std::move(hostname) }
  , port_{ port }
  , id_{ node_uuid_.empty() ? derive_fallback_id(hostname_, port_) : node_uuid_ }
{
}

auto
node_id::id() const noexcept -> const std::string&
{
  return id_;
}

auto
node_id::node_uuid() const noexcept -> const std::string&
{
  return node_uuid_;
}

auto
node_id::hostname() const noexcept -> const std::string&
{
  return hostname_;
}

auto
node_id::port() const noexcept -> std::uint16_t
{
  return port_;
}

node_id::
operator bool() const noexcept
{
  return !id_.empty();
}

auto
node_id::operator==(const node_id& other) const noexcept -> bool
{
  return id_ == other.id_;
}

auto
node_id::operator!=(const node_id& other) const noexcept -> bool
{
  return !(*this == other);
}

auto
node_id::operator<(const node_id& other) const noexcept -> bool
{
  return id_ < other.id_;
}

auto
node_id::operator<=(const node_id& other) const noexcept -> bool
{
  return !(other < *this);
}

auto
node_id::operator>(const node_id& other) const noexcept -> bool
{
  return other < *this;
}

auto
node_id::operator>=(const node_id& other) const noexcept -> bool
{
  return !(*this < other);
}

auto
internal_node_id::build(std::string node_uuid, std::string hostname, std::uint16_t port) -> node_id
{
  // Preserve the "falsy = unknown" contract: a node that has neither a
  // server-assigned UUID nor a usable hostname+port pair cannot be uniquely
  // identified. Returning a default-constructed node_id keeps such an input
  // falsy so request- and response-side identity construction agree on the
  // same "empty" answer for not-yet-bound or KV-less topology entries.
  if (node_uuid.empty() && (hostname.empty() || port == 0)) {
    return {};
  }
  return { std::move(node_uuid), std::move(hostname), port };
}

} // namespace couchbase
