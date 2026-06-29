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

#include "test_helper.hxx"

#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/io/query_cache.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_update.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/durability_level.hxx>

#include <map>
#include <string>

namespace
{
auto
make_http_context() -> couchbase::core::http_context
{
  static couchbase::core::topology::configuration config{};
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  std::string canonical_hostname{};
  std::uint16_t canonical_port{};
  return {
    config, cluster_options, query_cache, hostname, port, canonical_hostname, canonical_port
  };
}

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
} // namespace

TEST_CASE("unit: bucket_update::encode_to form body", "[unit]")
{
  couchbase::core::io::http_request http_req;
  auto ctx = make_http_context();

  couchbase::core::operations::management::bucket_update_request req{};
  req.bucket.name = "my_bucket";
  req.bucket.ram_quota_mb = 256;
  req.bucket.num_replicas = 2;
  req.bucket.flush_enabled = true;
  req.bucket.minimum_durability_level = couchbase::durability_level::majority;

  auto ec = req.encode_to(http_req, ctx);
  REQUIRE_SUCCESS(ec);

  // The body must be a well-formed form-urlencoded string: no leading '&' (which Couchbase
  // Server 8.1+ via MB-61655 rejects as an empty parameter) and no empty tokens.
  REQUIRE_FALSE(http_req.body.empty());
  REQUIRE(http_req.body.front() != '&');
  REQUIRE(http_req.body.find("&&") == std::string::npos);
  REQUIRE(http_req.body.back() != '&');

  auto values = parse_form_body(http_req.body);
  CHECK(values["ramQuotaMB"] == "256");
  CHECK(values["replicaNumber"] == "2");
  CHECK(values["flushEnabled"] == "1");
  CHECK(values["durabilityMinLevel"] == "majority");
  // bucket name is carried in the path, not the body
  CHECK(values.count("name") == 0);
}

TEST_CASE("unit: bucket_create::encode_to form body", "[unit]")
{
  couchbase::core::io::http_request http_req;
  auto ctx = make_http_context();

  couchbase::core::operations::management::bucket_create_request req{};
  req.bucket.name = "my_bucket";
  req.bucket.ram_quota_mb = 512;
  req.bucket.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
  req.bucket.num_replicas = 1;

  auto ec = req.encode_to(http_req, ctx);
  REQUIRE_SUCCESS(ec);

  REQUIRE_FALSE(http_req.body.empty());
  REQUIRE(http_req.body.front() != '&');
  REQUIRE(http_req.body.find("&&") == std::string::npos);
  REQUIRE(http_req.body.back() != '&');

  auto values = parse_form_body(http_req.body);
  CHECK(values["name"] == "my_bucket");
  CHECK(values["bucketType"] == "couchbase");
  CHECK(values["ramQuotaMB"] == "512");
  CHECK(values["replicaNumber"] == "1");
}

TEST_CASE("unit: bucket_create::encode_to defaults ram quota to 100", "[unit]")
{
  couchbase::core::io::http_request http_req;
  auto ctx = make_http_context();

  couchbase::core::operations::management::bucket_create_request req{};
  req.bucket.name = "my_bucket";

  auto ec = req.encode_to(http_req, ctx);
  REQUIRE_SUCCESS(ec);

  auto values = parse_form_body(http_req.body);
  CHECK(values["ramQuotaMB"] == "100");
}
