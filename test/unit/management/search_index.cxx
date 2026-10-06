/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "utils/test_data.hxx"

#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"
#include "core/management/search_index.hxx"
#include "core/operations/management/search_get_stats.hxx"
#include "core/operations/management/search_index_drop.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
auto
index_with_params(const std::string& file) -> couchbase::core::management::search::index
{
  couchbase::core::management::search::index search_index{};
  search_index.params_json = ::test::utils::read_test_data(file);
  return search_index;
}

void
an_index_with_a_vector_field_is_a_vector_index([[maybe_unused]] context& ctx)
{
  assert_true(index_with_params("sample_vector_index_params.json").is_vector_index(),
              "a vector field at the top level of the mapping");
}

void
a_vector_field_under_a_nested_property_is_found([[maybe_unused]] context& ctx)
{
  assert_true(
    index_with_params("sample_vector_index_with_nested_properties_params.json").is_vector_index(),
    "a vector field reached only by descending into a property");
}

void
an_index_without_a_vector_field_is_not_a_vector_index([[maybe_unused]] context& ctx)
{
  assert_false(index_with_params("travel_sample_index_params.json").is_vector_index(),
               "an ordinary full-text index");
}

// search_index_drop stands for every management request that falls back to
// extract_common_error_code().
auto
drop_error(std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  return couchbase::core::operations::management::search_index_drop_request{}
    .make_response({}, encoded)
    .ctx.ec;
}

void
an_unrecognised_management_rejection_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_eq(drop_error(400, R"({"status":"fail","error":"rest_auth: unsupported request"})"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose text matches none of the known reasons");
}

void
a_management_server_fault_is_still_an_internal_server_failure([[maybe_unused]] context& ctx)
{
  assert_eq(drop_error(500, R"({"status":"fail","error":"internal error"})"),
            couchbase::errc::common::internal_server_failure,
            "only a 400 is the caller's error");
}

auto
completed_drop_error(std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  const couchbase::core::operations::management::search_index_drop_request request{};
  return couchbase::core::io::complete_http_response(
           request,
           [] {
             return couchbase::core::error_context::http{};
           },
           encoded)
    .ctx.ec;
}

void
a_management_rejection_without_the_expected_fields_is_an_invalid_argument(
  [[maybe_unused]] context& ctx)
{
  assert_eq(completed_drop_error(400, "{}"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose body has no status or error");
  assert_eq(completed_drop_error(400, R"({"status":"fail","error":42})"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose error is not a string");
}

void
a_rejected_stats_request_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = 400;
  encoded.body.append(R"({"status":"fail","error":"rest_auth: unsupported request"})");
  auto response =
    couchbase::core::operations::management::search_get_stats_request{}.make_response({}, encoded);
  assert_eq(response.ctx.ec,
            couchbase::errc::common::invalid_argument,
            "a 400 is not reported as the stats document");
  assert_true(response.stats.empty(), "the rejection text is not returned as stats");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_index_with_a_vector_field_is_a_vector_index) },
      { CASE(a_vector_field_under_a_nested_property_is_found) },
      { CASE(an_index_without_a_vector_field_is_not_a_vector_index) },
      { CASE(an_unrecognised_management_rejection_is_an_invalid_argument) },
      { CASE(a_management_server_fault_is_still_an_internal_server_failure) },
      { CASE(a_management_rejection_without_the_expected_fields_is_an_invalid_argument) },
      { CASE(a_rejected_stats_request_is_an_invalid_argument) },
    },
  };
}

} // namespace couchbase::test
