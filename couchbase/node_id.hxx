/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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
 * Uniquely identifies a cluster node.
 *
 * On Couchbase Server 8.0.1+ the identifier is the server-assigned node UUID.
 * On older releases, a stable opaque token is derived from the node's hostname
 * and management port.
 *
 * The type is equality-comparable, ordered, and hashable.
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
   * @since 1.3.2
   * @uncommitted
   */
  node_id() = default;

  /**
   * User-facing identifier string.
   *
   * Returns node_uuid when the server provides one, otherwise a stable
   * hex-encoded hash derived from hostname and management port.
   *
   * @return opaque identifier string (empty for default-constructed instances)
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto id() const -> const std::string&;

  /**
   * The server-assigned node UUID (empty on servers before 8.0.1).
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto node_uuid() const -> const std::string&;

  /**
   * The hostname of the node.
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto hostname() const -> const std::string&;

  /**
   * The port of the node's key-value service.
   *
   * This reflects the port actually used by the client: the TLS port when the
   * cluster connection is TLS-enabled, otherwise the plain port.
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto port() const -> std::uint16_t;

  /**
   * Returns true when the node_id was properly initialized (not default-constructed).
   *
   * @since 1.3.2
   * @uncommitted
   */
  explicit operator bool() const;

  auto operator==(const node_id& other) const -> bool;
  auto operator!=(const node_id& other) const -> bool;
  auto operator<(const node_id& other) const -> bool;

private:
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
  auto operator()(const couchbase::node_id& nid) const noexcept -> std::size_t
  {
    return std::hash<std::string>{}(nid.id());
  }
};
