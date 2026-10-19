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

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS
#include "core/operations/document_view.hxx"
#undef COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/http_context.hxx"

#include "core/design_document_namespace.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/management/view_index_get.hxx"

#include <string>

namespace couchbase::test
{
namespace
{
// A name holding '#', '?', '/', '%' and a space, and its escaped form.
const std::string hostile_name{ "a#b?c/d%e f" };
const std::string escaped_name{ "a%23b%3Fc%2Fd%25e%20f" };

void
a_design_document_name_is_one_path_segment([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::view_index_get_request req{};
  req.bucket_name = "travel-sample";
  req.document_name = hostile_name;
  req.ns = couchbase::core::design_document_namespace::development;

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // Catches a raw '#', which ends the path at design document "dev_a".
  assert_eq(http_req.path,
            "/travel-sample/_design/dev_" + escaped_name,
            "the design document name is escaped");
  assert_ne(http_req.path, "/travel-sample/_design/dev_" + hostile_name, "not the raw name");
}

void
a_view_request_escapes_every_name_in_its_path([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::document_view_request req{};
  req.bucket_name = hostile_name;
  req.document_name = hostile_name;
  req.view_name = hostile_name;
  req.ns = couchbase::core::design_document_namespace::production;

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // Catches a raw '#', which ends the path at bucket "a".
  assert_eq(http_req.path,
            "/" + escaped_name + "/_design/" + escaped_name + "/_view/" + escaped_name + "?",
            "bucket, design document and view names are escaped");
  assert_ne(http_req.path,
            "/" + hostile_name + "/_design/" + hostile_name + "/_view/" + hostile_name + "?",
            "not the raw names");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_design_document_name_is_one_path_segment) },
      { CASE(a_view_request_escapes_every_name_in_its_path) },
    },
  };
}

} // namespace couchbase::test
