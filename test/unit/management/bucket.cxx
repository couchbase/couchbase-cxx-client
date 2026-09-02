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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/http_context.hxx"

#include "core/io/http_message.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_update.hxx"

#include <couchbase/durability_level.hxx>

#include <map>
#include <string>

namespace couchbase::test
{
namespace
{
/**
 * Parse an application/x-www-form-urlencoded body into a key->value map. Values used in these
 * tests do not require percent-decoding, so the raw value is kept as-is.
 */
auto
parse_form_body(const std::string& body) -> std::map<std::string, std::string>
{
  std::map<std::string, std::string> values{};
  std::size_t pos{ 0 };
  while (pos < body.size()) {
    auto amp = body.find('&', pos);
    auto pair = body.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
    auto eq = pair.find('=');
    if (eq != std::string::npos) {
      values[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
    if (amp == std::string::npos) {
      break;
    }
    pos = amp + 1;
  }
  return values;
}

// Couchbase Server 8.1 and later reject an empty form parameter (MB-61655), so the body has to be
// non-empty and must not open with, close with, or run through a bare separator, whatever the
// values around it say.
void
assert_well_formed_form_body(const std::string& body)
{
  assert_false(body.empty(), "the encoder produced a body");
  assert_ne(body.front(), '&', "the body does not open with a separator");
  assert_eq(body.find("&&"), std::string::npos, "the body carries no empty parameter");
  assert_ne(body.back(), '&', "the body does not close with a separator");
}

void
bucket_update_encodes_the_settings_it_was_given([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::bucket_update_request req{};
  req.bucket.name = "my_bucket";
  req.bucket.ram_quota_mb = 256;
  req.bucket.num_replicas = 2;
  req.bucket.flush_enabled = true;
  req.bucket.minimum_durability_level = couchbase::durability_level::majority;

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_well_formed_form_body(http_req.body);

  auto values = parse_form_body(http_req.body);
  assert_eq(values["ramQuotaMB"], "256", "RAM quota");
  assert_eq(values["replicaNumber"], "2", "replica count");
  assert_eq(values["flushEnabled"], "1", "flush");
  assert_eq(values["durabilityMinLevel"], "majority", "minimum durability level");
  assert_eq(values.count("name"), 0U, "the bucket name is carried in the path, not the body");
}

void
bucket_create_encodes_the_settings_it_was_given([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::bucket_create_request req{};
  req.bucket.name = "my_bucket";
  req.bucket.ram_quota_mb = 512;
  req.bucket.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
  req.bucket.num_replicas = 1;

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_well_formed_form_body(http_req.body);

  auto values = parse_form_body(http_req.body);
  assert_eq(values["name"], "my_bucket", "bucket name");
  assert_eq(values["bucketType"], "couchbase", "bucket type");
  assert_eq(values["ramQuotaMB"], "512", "RAM quota");
  assert_eq(values["replicaNumber"], "1", "replica count");
}

void
bucket_create_defaults_the_ram_quota([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::bucket_create_request req{};
  req.bucket.name = "my_bucket";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");

  auto values = parse_form_body(http_req.body);
  assert_eq(values["ramQuotaMB"], "100", "the RAM quota a request that names none is given");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(bucket_update_encodes_the_settings_it_was_given) },
      { CASE(bucket_create_encodes_the_settings_it_was_given) },
      { CASE(bucket_create_defaults_the_ram_quota) },
    },
  };
}

} // namespace couchbase::test
