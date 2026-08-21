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

// The probe backend for a test executable that does not link the client library. Every probe
// reports that it cannot answer, which the runner turns into `undetermined` and therefore a
// failure -- a test asking about the server from a binary that cannot reach one is a registration
// mistake, and silently skipping it would hide the mistake for as long as the test existed.
//
// cmake/TestFramework.cmake links this or cluster_probes.cxx, never both.

#include "context.hxx"

namespace couchbase::test
{
namespace
{
class null_probes : public probe_backend
{
public:
  [[nodiscard]] auto server_version() -> couchbase::test::server_version override
  {
    fail();
  }
  [[nodiscard]] auto has_service(const std::string& /* name */) -> bool override
  {
    fail();
  }
  [[nodiscard]] auto has_bucket_capability(const std::string& /* capability */) -> bool override
  {
    fail();
  }
  [[nodiscard]] auto number_of_replicas() -> std::size_t override
  {
    fail();
  }
  [[nodiscard]] auto number_of_nodes() -> std::size_t override
  {
    fail();
  }
  [[nodiscard]] auto server_groups() -> std::vector<std::string> override
  {
    fail();
  }
  [[nodiscard]] auto storage_backend() -> std::string override
  {
    fail();
  }

private:
  [[noreturn]] static void fail()
  {
    throw probe_failure("this test executable does not link the client library, so it cannot ask "
                        "the server anything (add LINK_CLIENT to its couchbase_add_test call)");
  }
};
} // namespace

auto
make_probe_backend(const configuration& /* config */) -> std::unique_ptr<probe_backend>
{
  return std::make_unique<null_probes>();
}

} // namespace couchbase::test
