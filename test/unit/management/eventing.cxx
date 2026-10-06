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

#include "framework/test_registry.hxx"

#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/management/eventing_deploy_function.hxx"
#include "core/operations/management/eventing_drop_function.hxx"
#include "core/operations/management/eventing_get_function.hxx"
#include "core/operations/management/eventing_pause_function.hxx"
#include "core/operations/management/eventing_resume_function.hxx"
#include "core/operations/management/eventing_undeploy_function.hxx"
#include "core/operations/management/eventing_upsert_function.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
template<typename Request>
auto
mapped_error(std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  const Request request{};
  return couchbase::core::io::complete_http_response(
           request,
           [] {
             return couchbase::core::error_context::http{};
           },
           encoded)
    .ctx.ec;
}

using deploy = couchbase::core::operations::management::eventing_deploy_function_request;
using get = couchbase::core::operations::management::eventing_get_function_request;

const std::string unknown_problem =
  R"({"name":"ERR_SOMETHING_NEW","code":1,"description":"not a known problem"})";

void
an_unrecognised_rejection_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error<deploy>(400, unknown_problem),
            couchbase::errc::common::invalid_argument,
            "deploy: a 400 naming a problem with no specific mapping");
  assert_eq(mapped_error<get>(400, unknown_problem),
            couchbase::errc::common::invalid_argument,
            "get: a 400 naming a problem with no specific mapping");
}

void
a_rejection_that_names_no_problem_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  namespace mgmt = couchbase::core::operations::management;
  assert_eq(mapped_error<deploy>(400, ""),
            couchbase::errc::common::invalid_argument,
            "deploy: a 400 with an empty body");
  assert_eq(mapped_error<mgmt::eventing_drop_function_request>(400, ""),
            couchbase::errc::common::invalid_argument,
            "drop: a 400 with an empty body");
  assert_eq(mapped_error<mgmt::eventing_pause_function_request>(400, ""),
            couchbase::errc::common::invalid_argument,
            "pause: a 400 with an empty body");
  assert_eq(mapped_error<mgmt::eventing_resume_function_request>(400, ""),
            couchbase::errc::common::invalid_argument,
            "resume: a 400 with an empty body");
  assert_eq(mapped_error<mgmt::eventing_undeploy_function_request>(400, ""),
            couchbase::errc::common::invalid_argument,
            "undeploy: a 400 with an empty body");
  assert_eq(mapped_error<mgmt::eventing_upsert_function_request>(400, ""),
            couchbase::errc::common::invalid_argument,
            "upsert: a 400 with an empty body");
  assert_eq(mapped_error<deploy>(400, "{}"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose body has no problem name");
  assert_eq(mapped_error<deploy>(400, R"({"name":"ERR_SOMETHING_NEW"})"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose problem has no code");
  assert_eq(mapped_error<deploy>(400, R"({"name":"ERR_SOMETHING_NEW","code":"x"})"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose problem code is not a number");
}

void
a_recognised_problem_keeps_its_own_error([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error<deploy>(
              400, R"({"name":"ERR_APP_NOT_FOUND_TS","code":2,"description":"not found"})"),
            couchbase::errc::management::eventing_function_not_found,
            "the catch-all for a 400 does not shadow the specific problems");
}

void
a_server_fault_is_still_an_internal_server_failure([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error<deploy>(500, unknown_problem),
            couchbase::errc::common::internal_server_failure,
            "only a 400 is the caller's error");
  assert_eq(mapped_error<deploy>(200, ""), std::error_code{}, "a 200 with no body is a success");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_unrecognised_rejection_is_an_invalid_argument) },
      { CASE(a_rejection_that_names_no_problem_is_an_invalid_argument) },
      { CASE(a_recognised_problem_keeps_its_own_error) },
      { CASE(a_server_fault_is_still_an_internal_server_failure) },
    },
  };
}

} // namespace couchbase::test
