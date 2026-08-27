/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "cluster_credentials.hxx"
#include "cluster_options.hxx"

#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::core
{
namespace utils
{
struct connection_string;
} // namespace utils

namespace topology
{
struct configuration;
} // namespace topology

struct origin {
  using node_entry = std::pair<std::string, std::string>;
  using node_list = std::vector<node_entry>;

  origin() = default;
  ~origin() = default;

  origin(origin&& other) noexcept;
  origin(const origin& other);
  origin(origin other, const topology::configuration& config);
  origin(cluster_credentials auth,
         const std::string& hostname,
         std::uint16_t port,
         cluster_options options);
  origin(cluster_credentials auth,
         const std::string& hostname,
         const std::string& port,
         cluster_options options);
  origin(cluster_credentials auth, const utils::connection_string& connstr);
  auto operator=(origin&& other) noexcept -> origin&;
  auto operator=(const origin& other) -> origin&;

  [[nodiscard]] auto connection_string() const -> const std::string&;
  // True when built from a couchbase2:// connection string (Protostellar transport). Lets the
  // connect path branch to the gRPC client without re-parsing the connection string.
  [[nodiscard]] auto uses_protostellar() const -> bool;
  [[nodiscard]] auto username() const -> std::string;
  [[nodiscard]] auto password() const -> std::string;
  [[nodiscard]] auto certificate_path() const -> std::string;
  [[nodiscard]] auto key_path() const -> std::string;
  [[nodiscard]] auto jwt_token() const -> std::string;

  [[nodiscard]] auto get_hostnames() const -> std::vector<std::string>;
  [[nodiscard]] auto get_nodes() const -> std::vector<std::string>;
  // The same addresses as get_nodes(), without the display quotes baked into each entry. Log
  // statements want them bare so that logger::system_data_list() can put a tag inside the quotes it
  // renders; a tag around a pre-quoted entry hashes the punctuation with the value.
  [[nodiscard]] auto get_node_addresses() const -> std::vector<std::string>;

  void shuffle_nodes();
  void set_nodes(node_list nodes);
  void set_nodes_from_config(const topology::configuration& config);

  void update_credentials(cluster_credentials auth);

  [[nodiscard]] auto next_address() -> std::pair<std::string, std::string>;

  [[nodiscard]] auto exhausted() const -> bool;

  void restart();

  [[nodiscard]] auto options() const -> const couchbase::core::cluster_options&;
  [[nodiscard]] auto options() -> couchbase::core::cluster_options&;
  [[nodiscard]] auto credentials() const -> couchbase::core::cluster_credentials;
  // A JSON dump of the whole origin, less the credentials. Its only callers are the log
  // statements in the cluster open paths, and they annotate the whole document: a tag written
  // into one of these values instead would be read back as part of that value by anything that
  // parses the line as JSON. So nothing in here is annotated.
  [[nodiscard]] auto to_json() const -> std::string;

private:
  couchbase::core::cluster_options options_{};
  node_list nodes_{};
  node_list::iterator next_node_{};
  bool exhausted_{ false };
  std::string connection_string_{};
  bool uses_protostellar_{ false };
  cluster_credentials credentials_{};
  mutable std::shared_mutex credentials_mutex_{};
};

} // namespace couchbase::core
