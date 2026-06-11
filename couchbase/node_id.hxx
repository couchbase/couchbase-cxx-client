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

#pragma once

#include <cstdint>
#include <functional>
#include <string>

namespace couchbase
{

/**
 * @brief Identifies a cluster node.
 *
 * On Couchbase Server 8.0.1+ the identifier is the server-assigned node UUID.
 * On older releases, a stable opaque token is derived from the node's hostname
 * and KV port.
 *
 * The type is equality-comparable, totally ordered, and hashable.
 *
 * @since 1.3.2
 * @uncommitted
 */
class node_id
{
public:
  /**
   * Creates an empty (invalid) node_id.
   *
   * @see operator bool()
   *
   * @since 1.3.2
   * @uncommitted
   */
  node_id() = default;

  // Copy and move are noexcept: all members (std::string + std::uint16_t)
  // have noexcept moves on every conforming implementation.
  node_id(const node_id&) = default;
  node_id(node_id&&) noexcept = default;
  auto operator=(const node_id&) -> node_id& = default;
  auto operator=(node_id&&) noexcept -> node_id& = default;
  ~node_id() = default;

  /**
   * User-facing identifier string.
   *
   * Returns node_uuid when the server provides one, otherwise a stable
   * hex-encoded hash derived from hostname and KV port.
   *
   * @return opaque identifier string (empty for default-constructed instances)
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto id() const noexcept -> const std::string&;

  /**
   * The server-assigned node UUID (empty on servers before 8.0.1).
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto node_uuid() const noexcept -> const std::string&;

  /**
   * The hostname of the node.
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto hostname() const noexcept -> const std::string&;

  /**
   * The port of the node's key-value service.
   *
   * This reflects the port actually used by the client: the TLS port when the
   * cluster connection is TLS-enabled, otherwise the plain port. A value of
   * @c 0 indicates the port is unknown or the node is not bound; in that case
   * @c operator @c bool() also returns @c false.
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto port() const noexcept -> std::uint16_t;

  /**
   * Returns true when the node_id was properly initialized (not default-constructed).
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] explicit operator bool() const noexcept;

  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator==(const node_id& other) const noexcept -> bool;
  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator!=(const node_id& other) const noexcept -> bool;
  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator<(const node_id& other) const noexcept -> bool;
  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator<=(const node_id& other) const noexcept -> bool;
  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator>(const node_id& other) const noexcept -> bool;
  /** @since 1.3.2 @uncommitted */
  [[nodiscard]] auto operator>=(const node_id& other) const noexcept -> bool;

private:
  // Construction from core-level data is restricted to internal_node_id,
  // declared in core/impl/node_id.hxx. Public users cannot construct node_id
  // directly.
  friend class internal_node_id;

  node_id(std::string node_uuid, std::string hostname, std::uint16_t port);

  std::string node_uuid_{};
  std::string hostname_{};
  std::uint16_t port_{ 0 };
  // Computed once at construction: node_uuid_ if non-empty, otherwise a
  // deterministic hex-encoded CRC32 hash of "hostname:port".
  std::string id_{};
};

} // namespace couchbase

template<>
struct std::hash<couchbase::node_id> {
  // Hashes the full id_ string. For the fallback case id_ is 8 hex chars;
  // for a server-assigned UUID it is typically 32-36 chars. The cost is well
  // under 100ns per lookup and not on a critical path; if profiling shows
  // otherwise, cache the hash on node_id and return it from a friend accessor.
  auto operator()(const couchbase::node_id& nid) const noexcept -> std::size_t
  {
    return std::hash<std::string>{}(nid.id());
  }
};
