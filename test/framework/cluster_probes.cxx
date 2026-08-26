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

// The probe backend for a test executable that links the client library: one connection, opened on
// the first probe, answering from the cluster map and the management endpoints.
//
// This is the only file under test/framework/ that includes core headers, and it is a .cxx --
// context.hxx stays lean, so declaring a requirement costs a test file nothing in compile time.
//
// cmake/TestFramework.cmake links this or null_probes.cxx, never both.

#include "context.hxx"

#include <spdlog/fmt/fmt.h>

#include "utils/integration_test_guard.hxx"
#include "utils/server_version.hxx"

#include <system_error>
#include <utility>

namespace couchbase::test
{
namespace
{
// ::test::utils, never test::utils: inside namespace couchbase::test the leading `test` binds to
// this namespace, and lookup stops there rather than falling through to the global one.
namespace utils = ::test::utils;

auto
to_edition(utils::server_edition edition) -> server_edition
{
  switch (edition) {
    case utils::server_edition::enterprise:
      return server_edition::enterprise;
    case utils::server_edition::community:
      return server_edition::community;
    case utils::server_edition::columnar:
      return server_edition::columnar;
    case utils::server_edition::unknown:
      break;
  }
  return server_edition::unknown;
}

auto
to_deployment(utils::deployment_type deployment) -> deployment_type
{
  switch (deployment) {
    case utils::deployment_type::capella:
      return deployment_type::capella;
    case utils::deployment_type::elixir:
      return deployment_type::elixir;
    case utils::deployment_type::on_prem:
      break;
  }
  return deployment_type::on_prem;
}

auto
to_storage_backend(couchbase::core::management::cluster::bucket_storage_backend backend)
  -> std::string
{
  switch (backend) {
    case couchbase::core::management::cluster::bucket_storage_backend::couchstore:
      return "couchstore";
    case couchbase::core::management::cluster::bucket_storage_backend::magma:
      return "magma";
    case couchbase::core::management::cluster::bucket_storage_backend::unknown:
      break;
  }
  return "unknown";
}

class cluster_probes : public probe_backend
{
public:
  [[nodiscard]] auto server_version() -> couchbase::test::server_version override
  {
    return guarded([](utils::integration_test_guard& guard) {
      const auto version = guard.cluster_version();
      return couchbase::test::server_version{
        static_cast<std::uint32_t>(version.major),
        static_cast<std::uint32_t>(version.minor),
        static_cast<std::uint32_t>(version.micro),
        version.developer_preview,
        to_edition(version.edition),
        to_deployment(version.deployment),
      };
    });
  }

  [[nodiscard]] auto has_service(const std::string& name) -> bool override
  {
    // By the cluster-map spelling ("n1ql", "cbas", "fts") rather than by service_type, so a
    // requirement names the service the way the server does and no enum mapping can drift.
    return guarded([&name](utils::integration_test_guard& guard) {
      return guard.number_of_nodes_with_service(name) > 0;
    });
  }

  [[nodiscard]] auto has_bucket_capability(const std::string& capability) -> bool override
  {
    return guarded([&capability](utils::integration_test_guard& guard) {
      return guard.has_bucket_capability(capability);
    });
  }

  [[nodiscard]] auto number_of_replicas() -> std::size_t override
  {
    return guarded([](utils::integration_test_guard& guard) {
      return guard.number_of_replicas();
    });
  }

  [[nodiscard]] auto number_of_nodes() -> std::size_t override
  {
    return guarded([](utils::integration_test_guard& guard) {
      return guard.number_of_nodes();
    });
  }

  [[nodiscard]] auto server_groups() -> std::vector<std::string> override
  {
    return guarded([](utils::integration_test_guard& guard) {
      return guard.server_groups();
    });
  }

  [[nodiscard]] auto storage_backend() -> std::string override
  {
    return guarded([](utils::integration_test_guard& guard) {
      return to_storage_backend(guard.storage_backend());
    });
  }

private:
  // The guard opens the connection in its constructor, so it is built on the first probe and not
  // before. Anything either step throws -- a refused connection, a management endpoint that
  // answered with an error -- becomes probe_failure, which the runner reports as undetermined and
  // therefore as a failed case. An unreachable cluster must not read as an inapplicable one.
  //
  // The two steps are told apart in what they throw. A constructor that failed reports
  // probe_backend_unavailable, which the context remembers for every probe kind: there is no
  // second endpoint to try, and its answer caches are per kind, so a suite asking two different
  // things would otherwise wait out the same connection attempt once for each. A probe that threw
  // on an open connection is that one question failing and stays a plain probe_failure.
  template<typename Fn>
  auto guarded(Fn&& fn) -> decltype(fn(std::declval<utils::integration_test_guard&>()))
  {
    if (!guard_) {
      try {
        guard_ = std::make_unique<utils::integration_test_guard>();
      } catch (const std::exception& e) {
        throw probe_backend_unavailable(
          fmt::format("the cluster could not be reached: {}", e.what()));
      }
    }
    try {
      return std::forward<Fn>(fn)(*guard_);
    } catch (const std::system_error& e) {
      throw probe_failure(fmt::format("the cluster could not answer: {}", e.what()));
    } catch (const std::exception& e) {
      throw probe_failure(fmt::format("the cluster could not be reached: {}", e.what()));
    }
  }

  std::unique_ptr<utils::integration_test_guard> guard_{};
};
} // namespace

auto
make_probe_backend(const configuration& /* config */) -> std::unique_ptr<probe_backend>
{
  // The connection details come from test::utils::test_context, which reads the same environment
  // variables the configuration did. One reader will remain once the Catch2 suites are gone.
  return std::make_unique<cluster_probes>();
}

} // namespace couchbase::test
