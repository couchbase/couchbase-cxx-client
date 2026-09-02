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

#include "core/error_context/http.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_drop.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <string>

namespace couchbase::test
{
namespace
{
// The server reports every one of these as a status code and a sentence, so the request has to
// read the sentence to decide which of them it was.
template<typename Request>
auto
mapped_error(const Request& req, std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  return req.make_response({}, encoded).ctx.ec;
}

auto
collection_create() -> couchbase::core::operations::management::collection_create_request
{
  return { "default", "_default", "c1" };
}

auto
collection_drop() -> couchbase::core::operations::management::collection_drop_request
{
  return { "default", "_default", "c1" };
}

auto
collection_update() -> couchbase::core::operations::management::collection_update_request
{
  return { "default", "_default", "c1" };
}

auto
scope_create() -> couchbase::core::operations::management::scope_create_request
{
  return { "default", "myscope" };
}

auto
scope_drop() -> couchbase::core::operations::management::scope_drop_request
{
  return { "default", "myscope" };
}

void
collection_create_reports_a_missing_scope([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_create(),
                         404,
                         "Scope with name `myscope` is not found in bucket `default`"),
            couchbase::errc::common::scope_not_found,
            "a 404 naming the scope");
}

void
collection_create_reports_a_missing_bucket([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_create(), 404, "Bucket with name `nope` is not found"),
            couchbase::errc::common::bucket_not_found,
            "a 404 naming the bucket is not read as a missing scope");
}

void
collection_create_reports_a_collection_that_already_exists([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_create(),
                         400,
                         "Collection with name `c1` already exists in scope `_default`"),
            couchbase::errc::management::collection_exists,
            "a 400 naming the collection");
}

void
collection_create_reports_an_unrecognised_rejection_as_invalid_argument(
  [[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_create(), 400, "name - max length (251) exceeded"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose text matches none of the known reasons");
}

void
scope_drop_reports_a_missing_scope([[maybe_unused]] context& ctx)
{
  assert_eq(
    mapped_error(scope_drop(), 404, "Scope with name `myscope` is not found in bucket `default`"),
    couchbase::errc::common::scope_not_found,
    "a 404 naming the scope");
}

void
scope_drop_reports_a_missing_bucket([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(scope_drop(), 404, "Bucket with name `nope` is not found"),
            couchbase::errc::common::bucket_not_found,
            "a 404 naming the bucket is not read as a missing scope");
}

void
scope_create_reports_a_scope_that_already_exists([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(
              scope_create(), 400, "Scope with name `myscope` already exists in bucket `default`"),
            couchbase::errc::management::scope_exists,
            "a 400 naming the scope");
}

void
scope_create_reports_a_cluster_too_old_for_scopes([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(scope_create(), 400, "Not allowed on this version of cluster"),
            couchbase::errc::common::feature_not_available,
            "a 400 refusing the operation outright");
}

void
scope_create_reports_an_unrecognised_rejection_as_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(scope_create(), 400, "name - max length (251) exceeded"),
            couchbase::errc::common::invalid_argument,
            "a 400 whose text matches none of the known reasons");
}

void
collection_drop_reports_a_missing_collection([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(
              collection_drop(), 404, "Collection with name `c1` is not found in scope `_default`"),
            couchbase::errc::common::collection_not_found,
            "a 404 naming the collection");
}

void
collection_drop_reports_a_missing_scope([[maybe_unused]] context& ctx)
{
  assert_eq(
    mapped_error(collection_drop(), 404, "Scope with name `nope` is not found in bucket `default`"),
    couchbase::errc::common::scope_not_found,
    "a 404 naming the scope");
}

void
collection_drop_reports_a_missing_bucket([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_drop(), 404, "Bucket with name `default` is not found"),
            couchbase::errc::common::bucket_not_found,
            "a 404 naming the bucket");
}

void
collection_update_reports_a_missing_collection([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_update(),
                         404,
                         "Collection with name `c1` is not found in scope `_default`"),
            couchbase::errc::common::collection_not_found,
            "a 404 naming the collection");
}

void
collection_update_reports_a_missing_scope([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(
              collection_update(), 404, "Scope with name `nope` is not found in bucket `default`"),
            couchbase::errc::common::scope_not_found,
            "a 404 naming the scope");
}

void
collection_update_reports_a_missing_bucket([[maybe_unused]] context& ctx)
{
  assert_eq(mapped_error(collection_update(), 404, "Bucket with name `default` is not found"),
            couchbase::errc::common::bucket_not_found,
            "a 404 naming the bucket");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(collection_create_reports_a_missing_scope) },
      { CASE(collection_create_reports_a_missing_bucket) },
      { CASE(collection_create_reports_a_collection_that_already_exists) },
      { CASE(collection_create_reports_an_unrecognised_rejection_as_invalid_argument) },
      { CASE(scope_drop_reports_a_missing_scope) },
      { CASE(scope_drop_reports_a_missing_bucket) },
      { CASE(scope_create_reports_a_scope_that_already_exists) },
      { CASE(scope_create_reports_a_cluster_too_old_for_scopes) },
      { CASE(scope_create_reports_an_unrecognised_rejection_as_invalid_argument) },
      { CASE(collection_drop_reports_a_missing_collection) },
      { CASE(collection_drop_reports_a_missing_scope) },
      { CASE(collection_drop_reports_a_missing_bucket) },
      { CASE(collection_update_reports_a_missing_collection) },
      { CASE(collection_update_reports_a_missing_scope) },
      { CASE(collection_update_reports_a_missing_bucket) },
    },
  };
}

} // namespace couchbase::test
