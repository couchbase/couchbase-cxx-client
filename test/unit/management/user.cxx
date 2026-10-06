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

#include "core/io/http_message.hxx"
#include "core/operations/management/change_password.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
auto
change_password_error(std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  return couchbase::core::operations::management::change_password_request{}
    .make_response({}, encoded)
    .ctx.ec;
}

void
an_unrecognised_password_rejection_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_eq(change_password_error(
              400, R"({"errors":{"password":"The password must be at least 6 characters long."}})"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose text matches none of the known reasons");
}

void
a_cluster_that_cannot_change_passwords_reports_the_feature([[maybe_unused]] context& ctx)
{
  assert_eq(change_password_error(400, "Not allowed on this version of cluster"),
            couchbase::errc::common::feature_not_available,
            "the catch-all for a 400 does not shadow the specific reason");
  assert_eq(change_password_error(200, ""), std::error_code{}, "a 200 is a success");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_unrecognised_password_rejection_is_an_invalid_argument) },
      { CASE(a_cluster_that_cannot_change_passwords_reports_the_feature) },
    },
  };
}

} // namespace couchbase::test
