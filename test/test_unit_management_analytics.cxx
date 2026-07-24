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

#include "test/utils/http_context.hxx"
#include "test_helper_integration.hxx"

#include "core/io/http_message.hxx"
#include "core/operations/management/analytics.hxx"
#include "core/utils/name_codec.hxx"

#include <tao/json/value.hpp>

namespace
{
auto
encoded_statement(const couchbase::core::io::http_request& http_req) -> std::string
{
  auto body = couchbase::core::utils::json::parse(http_req.body);
  REQUIRE(body.is_object());
  REQUIRE(body.get_object().at("statement").is_string());
  return body.get_object().at("statement").get_string();
}

// The six characters '\', 'u', '0', '0', '6', '0'. The Analytics parser expands \uXXXX before the
// lexer runs, so an unescaped backslash would turn this into a real backtick and close the
// identifier. Contains no literal backtick, which is exactly why doubling backticks does not stop
// it.
constexpr auto unicode_backtick = "\\u0060";
} // namespace

TEST_CASE("unit: analytics management rejects names that cannot be quoted", "[unit][security]")
{
  couchbase::core::io::http_request http_req{};
  http_req.type = couchbase::core::service_type::analytics;
  auto ctx = test::utils::make_http_context();

  // No escape sequence in the Analytics grammar produces a backtick, so a name containing one
  // cannot be represented as a delimited identifier and has to be refused before it reaches the
  // server.
  const std::string evil{ "a`b" };
  const auto invalid = couchbase::errc::common::invalid_argument;

  SECTION("analytics_dataverse_create_request")
  {
    couchbase::core::operations::management::analytics_dataverse_create_request req{};
    req.dataverse_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_dataverse_drop_request")
  {
    couchbase::core::operations::management::analytics_dataverse_drop_request req{};
    req.dataverse_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_dataset_create_request")
  {
    couchbase::core::operations::management::analytics_dataset_create_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.bucket_name = "bkt";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    req.dataverse_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.dataverse_name = "dv";
    req.dataset_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.dataset_name = "ds";
    req.bucket_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_dataset_drop_request")
  {
    couchbase::core::operations::management::analytics_dataset_drop_request req{};
    req.dataverse_name = evil;
    req.dataset_name = "ds";
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.dataverse_name = "dv";
    req.dataset_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_index_create_request")
  {
    couchbase::core::operations::management::analytics_index_create_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.index_name = "idx";
    req.fields = { { "field", "string" } };

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    req.dataverse_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.dataverse_name = "dv";
    req.dataset_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.dataset_name = "ds";
    req.index_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
    req.index_name = "idx";
    req.fields = { { evil, "string" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_index_drop_request")
  {
    couchbase::core::operations::management::analytics_index_drop_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.index_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_link_connect_request")
  {
    couchbase::core::operations::management::analytics_link_connect_request req{};
    req.dataverse_name = "dv";
    req.link_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }

  SECTION("analytics_link_disconnect_request")
  {
    couchbase::core::operations::management::analytics_link_disconnect_request req{};
    req.dataverse_name = "dv";
    req.link_name = evil;
    REQUIRE(req.encode_to(http_req, ctx) == invalid);
  }
}

TEST_CASE("unit: analytics management contains backslash escapes", "[unit][security]")
{
  couchbase::core::io::http_request http_req{};
  http_req.type = couchbase::core::service_type::analytics;
  auto ctx = test::utils::make_http_context();

  SECTION("a unicode escape cannot close the identifier")
  {
    couchbase::core::operations::management::analytics_dataverse_create_request req{};
    req.dataverse_name = std::string{ "evil" } + unicode_backtick + " IF NOT EXISTS";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    // the backslash is doubled, so the parser sees a literal "`" rather than a backtick
    REQUIRE(encoded_statement(http_req) == "CREATE DATAVERSE `evil\\\\u0060 IF NOT EXISTS`");
  }

  SECTION("a trailing backslash cannot escape the closing backtick")
  {
    couchbase::core::operations::management::analytics_dataverse_create_request req{};
    req.dataverse_name = "evil\\";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATAVERSE `evil\\\\`");
  }

  SECTION("backslashes in every identifier position are escaped")
  {
    couchbase::core::operations::management::analytics_dataset_create_request req{};
    req.dataverse_name = "d\\v";
    req.dataset_name = "d\\s";
    req.bucket_name = "b\\kt";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATASET `d\\\\v`.`d\\\\s` ON `b\\\\kt`");
  }

  SECTION("backslashes in field names are escaped")
  {
    couchbase::core::operations::management::analytics_index_create_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.index_name = "idx";
    req.fields = { { "a\\b.c", "string" } };

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE INDEX `idx` ON `dv`.`ds` (`a\\\\b`.`c`:string)");
  }
}

TEST_CASE("unit: analytics management statement clauses", "[unit]")
{
  couchbase::core::io::http_request http_req{};
  http_req.type = couchbase::core::service_type::analytics;
  auto ctx = test::utils::make_http_context();

  SECTION("analytics_dataverse_create_request")
  {
    couchbase::core::operations::management::analytics_dataverse_create_request req{};
    req.dataverse_name = "dv";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATAVERSE `dv`");

    req.ignore_if_exists = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATAVERSE `dv` IF NOT EXISTS");
  }

  SECTION("analytics_dataverse_drop_request")
  {
    couchbase::core::operations::management::analytics_dataverse_drop_request req{};
    req.dataverse_name = "dv";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP DATAVERSE `dv`");

    req.ignore_if_does_not_exist = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP DATAVERSE `dv` IF EXISTS");
  }

  SECTION("analytics_dataset_create_request")
  {
    couchbase::core::operations::management::analytics_dataset_create_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.bucket_name = "bkt";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATASET `dv`.`ds` ON `bkt`");

    req.ignore_if_exists = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATASET IF NOT EXISTS `dv`.`ds` ON `bkt`");

    req.condition = "type = 'airline'";
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) ==
            "CREATE DATASET IF NOT EXISTS `dv`.`ds` ON `bkt` WHERE type = 'airline'");

    // a compound dataverse name is split into a dot-qualified pair of delimited identifiers
    req.ignore_if_exists = false;
    req.condition.reset();
    req.dataverse_name = "bkt/scope";
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE DATASET `bkt`.`scope`.`ds` ON `bkt`");
  }

  SECTION("analytics_dataset_drop_request")
  {
    couchbase::core::operations::management::analytics_dataset_drop_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP DATASET `dv`.`ds`");

    req.ignore_if_does_not_exist = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP DATASET `dv`.`ds` IF EXISTS");
  }

  SECTION("analytics_index_create_request")
  {
    couchbase::core::operations::management::analytics_index_create_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.index_name = "idx";
    req.fields = { { "field", "string" } };

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CREATE INDEX `idx` ON `dv`.`ds` (`field`:string)");

    req.ignore_if_exists = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) ==
            "CREATE INDEX `idx` IF NOT EXISTS ON `dv`.`ds` (`field`:string)");

    // fields are held in a std::map, so they are emitted in sorted key order
    req.ignore_if_exists = false;
    req.fields = { { "b", "int" }, { "a.nested", "string" } };
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) ==
            "CREATE INDEX `idx` ON `dv`.`ds` (`a`.`nested`:string,`b`:int)");
  }

  SECTION("analytics_index_drop_request")
  {
    couchbase::core::operations::management::analytics_index_drop_request req{};
    req.dataverse_name = "dv";
    req.dataset_name = "ds";
    req.index_name = "idx";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP INDEX `dv`.`ds`.`idx`");

    req.ignore_if_does_not_exist = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DROP INDEX `dv`.`ds`.`idx` IF EXISTS");
  }

  SECTION("analytics_link_connect_request")
  {
    couchbase::core::operations::management::analytics_link_connect_request req{};
    req.dataverse_name = "dv";
    req.link_name = "lnk";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CONNECT LINK `dv`.`lnk`");

    req.force = true;
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "CONNECT LINK `dv`.`lnk` WITH {\"force\": true}");
  }

  SECTION("analytics_link_disconnect_request")
  {
    couchbase::core::operations::management::analytics_link_disconnect_request req{};
    req.dataverse_name = "dv";
    req.link_name = "lnk";

    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    REQUIRE(encoded_statement(http_req) == "DISCONNECT LINK `dv`.`lnk`");
  }
}

TEST_CASE("unit: analytics_index_create_request validates the field type", "[unit]")
{
  couchbase::core::io::http_request http_req{};
  http_req.type = couchbase::core::service_type::analytics;
  auto ctx = test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "Default";
  req.dataset_name = "dataset";
  req.index_name = "index";

  const auto invalid = couchbase::errc::common::invalid_argument;

  SECTION("field_type must be ASCII alphanumeric")
  {
    // the type is an identifier the server resolves by name, so it is whitelisted rather than
    // quoted: the whitelist excludes every character that could be structural
    req.fields = { { "field", "string; DROP TABLE" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);

    req.fields = { { "field", "" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);

    // the whitelist is a plain ASCII range check, so bytes above 0x7f are rejected regardless of
    // whether char is signed on this platform, and regardless of the host locale
    req.fields = { { "field", "strin\xc3\xa9" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);

    req.fields = { { "field", "\x80" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);

    // no type the Analytics grammar accepts contains an underscore
    req.fields = { { "field", "year_month_duration" } };
    REQUIRE(req.encode_to(http_req, ctx) == invalid);

    for (const auto& type :
         { "BIGINT", "INT", "DOUBLE", "STRING", "DATE", "TIME", "DATETIME", "string", "int" }) {
      req.fields = { { "field", type } };
      REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    }
  }

  SECTION("a name the server will refuse is encoded here rather than rejected")
  {
    // Quoting renders the content of a name inert, so quotability is the only property checked and
    // an unusable name is left for the server to refuse — matching every other encoder in this
    // file. Each case below yields a statement carrying an empty identifier, which the server
    // rejects rather than misreads.
    req.fields = { { "field", "string" } };
    req.index_name = "";
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    req.index_name = "index";
    req.dataset_name = "";
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    req.dataset_name = "dataset";
    req.dataverse_name = "";
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    req.dataverse_name = "Default";
    req.fields = {};
    REQUIRE_SUCCESS(req.encode_to(http_req, ctx));

    for (const auto& name : { "", ".field", "field.", "field..sub" }) {
      req.fields = { { name, "string" } };
      REQUIRE_SUCCESS(req.encode_to(http_req, ctx));
    }
  }
}

TEST_CASE("unit: analytics name codec", "[unit]")
{
  using couchbase::core::utils::analytics;

  SECTION("all_quotable accepts anything without a backtick")
  {
    REQUIRE(analytics::all_quotable({ "" }));
    REQUIRE(analytics::all_quotable({ "name" }));
    REQUIRE(analytics::all_quotable({ "a\\b" }));
    REQUIRE(analytics::all_quotable({ unicode_backtick }));
    REQUIRE(analytics::all_quotable({ "a", "b", "c" }));

    REQUIRE_FALSE(analytics::all_quotable({ "a`b" }));
    REQUIRE_FALSE(analytics::all_quotable({ "`" }));
    REQUIRE_FALSE(analytics::all_quotable({ "a", "b`", "c" }));
  }

  SECTION("quote_identifier quotes and escapes the backslash")
  {
    REQUIRE(analytics::quote_identifier("") == "``");
    REQUIRE(analytics::quote_identifier("name") == "`name`");
    REQUIRE(analytics::quote_identifier("a\\b") == "`a\\\\b`");
    REQUIRE(analytics::quote_identifier("\\") == "`\\\\`");
    // a unicode escape is defused, because its backslash is escaped
    REQUIRE(analytics::quote_identifier(unicode_backtick) == "`\\\\u0060`");
    // '/' and '.' are not separators here, so they stay verbatim
    REQUIRE(analytics::quote_identifier("a/b") == "`a/b`");
    REQUIRE(analytics::quote_identifier("a.b") == "`a.b`");
    // A backtick has no escape, so this input is refused by all_quotable() and never reaches
    // quote_identifier() in practice. Pin the backstop anyway: doubling yields two adjacent
    // identifier tokens, i.e. an unparseable statement, rather than the lone backtick that would
    // end the identifier and let the remainder be parsed as SQL++. It is not an escape.
    REQUIRE_FALSE(analytics::all_quotable({ "a`b" }));
    REQUIRE(analytics::quote_identifier("a`b") == "`a``b`");
  }

  SECTION("quote_dataverse_name splits on slash and escapes the backslash")
  {
    REQUIRE(analytics::quote_dataverse_name("Default") == "`Default`");
    REQUIRE(analytics::quote_dataverse_name("bucket/scope") == "`bucket`.`scope`");
    REQUIRE(analytics::quote_dataverse_name("a\\b") == "`a\\\\b`");
    REQUIRE(analytics::quote_dataverse_name(unicode_backtick) == "`\\\\u0060`");
    // a backslash on either side of the separator stays inside its own part
    REQUIRE(analytics::quote_dataverse_name("a\\/b") == "`a\\\\`.`b`");
    REQUIRE(analytics::quote_dataverse_name("a/\\b") == "`a`.`\\\\b`");
    // degenerate inputs stay quoted, so they reach the server as invalid rather than as injection
    REQUIRE(analytics::quote_dataverse_name("") == "``");
    REQUIRE(analytics::quote_dataverse_name("/") == "``.``");
    REQUIRE(analytics::quote_dataverse_name("/a") == "``.`a`");
    REQUIRE(analytics::quote_dataverse_name("a/") == "`a`.``");
    REQUIRE(analytics::quote_dataverse_name("a//b") == "`a`.``.`b`");
  }

  SECTION("quote_field_path quotes every segment of a dotted path")
  {
    REQUIRE(analytics::quote_field_path("") == "``");
    REQUIRE(analytics::quote_field_path("name") == "`name`");
    REQUIRE(analytics::quote_field_path("address.city") == "`address`.`city`");
    REQUIRE(analytics::quote_field_path("a.b.c") == "`a`.`b`.`c`");
    REQUIRE(analytics::quote_field_path("a\\b.c") == "`a\\\\b`.`c`");
  }
}
