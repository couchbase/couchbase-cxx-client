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

#pragma once

// What a case needs from the environment it runs in, declared at registration rather than decided
// inside the case body.
//
// A guard at the top of a body answers only for itself: the runner cannot see it, and the reason a
// case did not run survives only as a line of output. The reporting matters less than the third
// outcome. A predicate returning bool has to fold "I could not find out" into one of two answers,
// and in test infrastructure it is always folded into the one that does not fail the build -- so a
// cluster that cannot be reached skips every case and the leg goes green having verified nothing.
//
// check() therefore returns three states. `not_satisfied` skips the case; `undetermined` fails it.

#include "context.hxx"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace couchbase::test
{

struct check_result {
  enum class status : std::uint8_t {
    satisfied,
    not_satisfied,
    undetermined,
  };

  status value{ status::satisfied };
  std::string detail{};

  [[nodiscard]] static auto ok() -> check_result
  {
    return {};
  }
  [[nodiscard]] static auto missing(std::string why) -> check_result
  {
    return { status::not_satisfied, std::move(why) };
  }
  [[nodiscard]] static auto unknown(std::string why) -> check_result
  {
    return { status::undetermined, std::move(why) };
  }
};

class requirement
{
public:
  requirement() = default;
  requirement(const requirement&) = delete;
  auto operator=(const requirement&) -> requirement& = delete;
  virtual ~requirement() = default;

  // What the case needs, phrased so the skip report reads as a list of gaps in the environment.
  [[nodiscard]] virtual auto describe() const -> std::string = 0;

  // Whether this environment provides it. A probe_failure thrown here is caught by the runner and
  // reported as undetermined, so an implementation handles no errors of its own.
  [[nodiscard]] virtual auto check(context& ctx) const -> check_result = 0;
};

using requirement_ptr = std::shared_ptr<const requirement>;

// The built-in vocabulary. The names come from the predicates the suite already uses rather than
// from a taxonomy invented for the framework; a file that needs something else writes its own
// requirement in the translation unit that needs it.
namespace needs
{
// A configured cluster. Says nothing about whether it answers -- that is what the probing
// requirements below find out, and their answer to an unreachable endpoint is a failure.
[[nodiscard]] auto
real_cluster() -> requirement_ptr;

// The gocaves mock rather than a real server, for behaviour only the mock can produce.
[[nodiscard]] auto
mock() -> requirement_ptr;

// A service by its cluster-map name: "kv", "n1ql", "index", "fts", "cbas", "eventing".
[[nodiscard]] auto
service(std::string name) -> requirement_ptr;

// A version range, half-open: [from, until). Passing only `from` leaves it unbounded above.
[[nodiscard]] auto
cluster_version(server_version from) -> requirement_ptr;
[[nodiscard]] auto
cluster_version(server_version from, server_version until) -> requirement_ptr;

// A bucket capability as the cluster map spells it, e.g. "durableWrite", "subdoc.ReplicaRead".
[[nodiscard]] auto
bucket_capability(std::string capability) -> requirement_ptr;

[[nodiscard]] auto
deployment(deployment_type type) -> requirement_ptr;

[[nodiscard]] auto
edition(server_edition wanted) -> requirement_ptr;

// "couchstore" or "magma".
[[nodiscard]] auto
storage_backend(std::string backend) -> requirement_ptr;

[[nodiscard]] auto
replicas(std::size_t at_least) -> requirement_ptr;

[[nodiscard]] auto
nodes(std::size_t at_least) -> requirement_ptr;

[[nodiscard]] auto
server_groups(std::size_t at_least) -> requirement_ptr;

[[nodiscard]] auto
developer_preview() -> requirement_ptr;
} // namespace needs

} // namespace couchbase::test
