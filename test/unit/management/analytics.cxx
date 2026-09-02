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
#include "core/operations/management/analytics.hxx"
#include "core/utils/json.hxx"
#include "core/utils/name_codec.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

#include <string>

namespace couchbase::test
{
namespace
{
auto
analytics_http_request() -> couchbase::core::io::http_request
{
  couchbase::core::io::http_request http_req{};
  http_req.type = couchbase::core::service_type::analytics;
  return http_req;
}

auto
encoded_statement(const couchbase::core::io::http_request& http_req) -> std::string
{
  auto body = couchbase::core::utils::json::parse(http_req.body);
  assert_true(body.is_object(), "the encoded body is a JSON object");
  assert_true(body.get_object().at("statement").is_string(), "the statement is a JSON string");
  return body.get_object().at("statement").get_string();
}

// The six characters '\', 'u', '0', '0', '6', '0'. The Analytics parser expands \uXXXX before the
// lexer runs, so an unescaped backslash would turn this into a real backtick and close the
// identifier. Contains no literal backtick, which is exactly why doubling backticks does not stop
// it.
constexpr auto unicode_backtick = "\\u0060";

// No escape sequence in the Analytics grammar produces a backtick, so a name containing one cannot
// be represented as a delimited identifier and has to be refused before it reaches the server.
const std::string evil{ "a`b" };
constexpr auto invalid = couchbase::errc::common::invalid_argument;

void
analytics_dataverse_create_refuses_a_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_create_request req{};
  req.dataverse_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataverse name");
}

void
analytics_dataverse_drop_refuses_a_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_drop_request req{};
  req.dataverse_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataverse name");
}

void
analytics_dataset_create_refuses_any_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataset_create_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.bucket_name = "bkt";

  assert_success(req.encode_to(http_req, http_ctx), "names free of backticks are accepted");

  req.dataverse_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataverse name");
  req.dataverse_name = "dv";
  req.dataset_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataset name");
  req.dataset_name = "ds";
  req.bucket_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the bucket name");
}

void
analytics_dataset_drop_refuses_any_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataset_drop_request req{};
  req.dataverse_name = evil;
  req.dataset_name = "ds";
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataverse name");
  req.dataverse_name = "dv";
  req.dataset_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataset name");
}

void
analytics_index_create_refuses_any_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.index_name = "idx";
  req.fields = { { "field", "string" } };

  assert_success(req.encode_to(http_req, http_ctx), "names free of backticks are accepted");

  req.dataverse_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataverse name");
  req.dataverse_name = "dv";
  req.dataset_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the dataset name");
  req.dataset_name = "ds";
  req.index_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the index name");
  req.index_name = "idx";
  req.fields = { { evil, "string" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "a field name");
}

void
analytics_index_drop_refuses_a_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_drop_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.index_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the index name");
}

void
analytics_link_connect_refuses_a_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_link_connect_request req{};
  req.dataverse_name = "dv";
  req.link_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the link name");
}

void
analytics_link_disconnect_refuses_a_name_holding_a_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_link_disconnect_request req{};
  req.dataverse_name = "dv";
  req.link_name = evil;
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "the link name");
}

void
a_unicode_escape_in_a_name_cannot_close_the_identifier([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_create_request req{};
  req.dataverse_name = std::string{ "evil" } + unicode_backtick + " IF NOT EXISTS";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // the backslash is doubled, so the parser sees a literal "`" rather than a backtick
  assert_eq(encoded_statement(http_req),
            "CREATE DATAVERSE `evil\\\\u0060 IF NOT EXISTS`",
            "the rendered statement");
}

void
a_trailing_backslash_cannot_escape_the_closing_backtick([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_create_request req{};
  req.dataverse_name = "evil\\";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "CREATE DATAVERSE `evil\\\\`", "the rendered statement");
}

void
backslashes_in_every_identifier_position_are_escaped([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataset_create_request req{};
  req.dataverse_name = "d\\v";
  req.dataset_name = "d\\s";
  req.bucket_name = "b\\kt";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE DATASET `d\\\\v`.`d\\\\s` ON `b\\\\kt`",
            "the rendered statement");
}

void
backslashes_in_a_field_name_are_escaped([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.index_name = "idx";
  req.fields = { { "a\\b.c", "string" } };

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE INDEX `idx` ON `dv`.`ds` (`a\\\\b`.`c`:string)",
            "the rendered statement");
}

void
analytics_dataverse_create_renders_create_dataverse([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_create_request req{};
  req.dataverse_name = "dv";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "CREATE DATAVERSE `dv`", "the rendered statement");

  req.ignore_if_exists = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(
    encoded_statement(http_req), "CREATE DATAVERSE `dv` IF NOT EXISTS", "the IF NOT EXISTS clause");
}

void
analytics_dataverse_drop_renders_drop_dataverse([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataverse_drop_request req{};
  req.dataverse_name = "dv";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "DROP DATAVERSE `dv`", "the rendered statement");

  req.ignore_if_does_not_exist = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "DROP DATAVERSE `dv` IF EXISTS", "the IF EXISTS clause");
}

void
analytics_dataset_create_renders_create_dataset([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataset_create_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.bucket_name = "bkt";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(
    encoded_statement(http_req), "CREATE DATASET `dv`.`ds` ON `bkt`", "the rendered statement");

  req.ignore_if_exists = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE DATASET IF NOT EXISTS `dv`.`ds` ON `bkt`",
            "the IF NOT EXISTS clause");

  req.condition = "type = 'airline'";
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE DATASET IF NOT EXISTS `dv`.`ds` ON `bkt` WHERE type = 'airline'",
            "the WHERE clause");

  // a compound dataverse name is split into a dot-qualified pair of delimited identifiers
  req.ignore_if_exists = false;
  req.condition.reset();
  req.dataverse_name = "bkt/scope";
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE DATASET `bkt`.`scope`.`ds` ON `bkt`",
            "a compound dataverse name");
}

void
analytics_dataset_drop_renders_drop_dataset([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_dataset_drop_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "DROP DATASET `dv`.`ds`", "the rendered statement");

  req.ignore_if_does_not_exist = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(
    encoded_statement(http_req), "DROP DATASET `dv`.`ds` IF EXISTS", "the IF EXISTS clause");
}

void
analytics_index_create_renders_create_index([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.index_name = "idx";
  req.fields = { { "field", "string" } };

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE INDEX `idx` ON `dv`.`ds` (`field`:string)",
            "the rendered statement");

  req.ignore_if_exists = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE INDEX `idx` IF NOT EXISTS ON `dv`.`ds` (`field`:string)",
            "the IF NOT EXISTS clause");

  // fields are held in a std::map, so they are emitted in sorted key order
  req.ignore_if_exists = false;
  req.fields = { { "b", "int" }, { "a.nested", "string" } };
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CREATE INDEX `idx` ON `dv`.`ds` (`a`.`nested`:string,`b`:int)",
            "the field list");
}

void
analytics_index_drop_renders_drop_index([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_drop_request req{};
  req.dataverse_name = "dv";
  req.dataset_name = "ds";
  req.index_name = "idx";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "DROP INDEX `dv`.`ds`.`idx`", "the rendered statement");

  req.ignore_if_does_not_exist = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(
    encoded_statement(http_req), "DROP INDEX `dv`.`ds`.`idx` IF EXISTS", "the IF EXISTS clause");
}

void
analytics_link_connect_renders_connect_link([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_link_connect_request req{};
  req.dataverse_name = "dv";
  req.link_name = "lnk";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "CONNECT LINK `dv`.`lnk`", "the rendered statement");

  req.force = true;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req),
            "CONNECT LINK `dv`.`lnk` WITH {\"force\": true}",
            "the force option");
}

void
analytics_link_disconnect_renders_disconnect_link([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_link_disconnect_request req{};
  req.dataverse_name = "dv";
  req.link_name = "lnk";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_eq(encoded_statement(http_req), "DISCONNECT LINK `dv`.`lnk`", "the rendered statement");
}

void
analytics_index_create_requires_an_ascii_alphanumeric_field_type([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "Default";
  req.dataset_name = "dataset";
  req.index_name = "index";

  // the type is an identifier the server resolves by name, so it is whitelisted rather than
  // quoted: the whitelist excludes every character that could be structural
  req.fields = { { "field", "string; DROP TABLE" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "a type carrying a statement separator");

  req.fields = { { "field", "" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "an empty type");

  // the whitelist is a plain ASCII range check, so bytes above 0x7f are rejected regardless of
  // whether char is signed on this platform, and regardless of the host locale
  req.fields = { { "field", "strin\xc3\xa9" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "a type holding a multi-byte character");

  req.fields = { { "field", "\x80" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "a type holding a lone high byte");

  // no type the Analytics grammar accepts contains an underscore
  req.fields = { { "field", "year_month_duration" } };
  assert_eq(req.encode_to(http_req, http_ctx), invalid, "a type holding an underscore");

  for (const auto& type :
       { "BIGINT", "INT", "DOUBLE", "STRING", "DATE", "TIME", "DATETIME", "string", "int" }) {
    req.fields = { { "field", type } };
    assert_success(req.encode_to(http_req, http_ctx), "an accepted type");
  }
}

void
analytics_index_create_leaves_an_unusable_name_to_the_server([[maybe_unused]] context& ctx)
{
  auto http_req = analytics_http_request();
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::analytics_index_create_request req{};
  req.dataverse_name = "Default";
  req.dataset_name = "dataset";

  // Quoting renders the content of a name inert, so quotability is the only property checked and
  // an unusable name is left for the server to refuse — matching every other encoder in this
  // file. Each case below yields a statement carrying an empty identifier, which the server
  // rejects rather than misreads.
  req.fields = { { "field", "string" } };
  req.index_name = "";
  assert_success(req.encode_to(http_req, http_ctx), "an empty index name");

  req.index_name = "index";
  req.dataset_name = "";
  assert_success(req.encode_to(http_req, http_ctx), "an empty dataset name");

  req.dataset_name = "dataset";
  req.dataverse_name = "";
  assert_success(req.encode_to(http_req, http_ctx), "an empty dataverse name");

  req.dataverse_name = "Default";
  req.fields = {};
  assert_success(req.encode_to(http_req, http_ctx), "no fields at all");

  for (const auto& name : { "", ".field", "field.", "field..sub" }) {
    req.fields = { { name, "string" } };
    assert_success(req.encode_to(http_req, http_ctx), "a degenerate field path");
  }
}

void
all_quotable_accepts_any_name_without_a_backtick([[maybe_unused]] context& ctx)
{
  using couchbase::core::utils::analytics;

  assert_true(analytics::all_quotable({ "" }), "an empty name");
  assert_true(analytics::all_quotable({ "name" }), "an ordinary name");
  assert_true(analytics::all_quotable({ "a\\b" }), "a name holding a backslash");
  assert_true(analytics::all_quotable({ unicode_backtick }), "a name holding a unicode escape");
  assert_true(analytics::all_quotable({ "a", "b", "c" }), "several names");

  assert_false(analytics::all_quotable({ "a`b" }), "a name holding a backtick");
  assert_false(analytics::all_quotable({ "`" }), "a name that is a backtick");
  assert_false(analytics::all_quotable({ "a", "b`", "c" }), "one name of several");
}

void
quote_identifier_quotes_the_name_and_escapes_its_backslashes([[maybe_unused]] context& ctx)
{
  using couchbase::core::utils::analytics;

  assert_eq(analytics::quote_identifier(""), "``", "an empty name");
  assert_eq(analytics::quote_identifier("name"), "`name`", "an ordinary name");
  assert_eq(analytics::quote_identifier("a\\b"), "`a\\\\b`", "a backslash");
  assert_eq(analytics::quote_identifier("\\"), "`\\\\`", "a name that is a backslash");
  // a unicode escape is defused, because its backslash is escaped
  assert_eq(analytics::quote_identifier(unicode_backtick), "`\\\\u0060`", "a unicode escape");
  // '/' and '.' are not separators here, so they stay verbatim
  assert_eq(analytics::quote_identifier("a/b"), "`a/b`", "a slash");
  assert_eq(analytics::quote_identifier("a.b"), "`a.b`", "a dot");
  // A backtick has no escape, so this input is refused by all_quotable() and never reaches
  // quote_identifier() in practice. Pin the backstop anyway: doubling yields two adjacent
  // identifier tokens, i.e. an unparseable statement, rather than the lone backtick that would
  // end the identifier and let the remainder be parsed as SQL++. It is not an escape.
  assert_false(analytics::all_quotable({ "a`b" }), "a backtick is refused upstream");
  assert_eq(analytics::quote_identifier("a`b"), "`a``b`", "a doubled backtick parses as nothing");
}

void
quote_dataverse_name_splits_on_slash_and_escapes_backslashes([[maybe_unused]] context& ctx)
{
  using couchbase::core::utils::analytics;

  assert_eq(analytics::quote_dataverse_name("Default"), "`Default`", "a single part");
  assert_eq(analytics::quote_dataverse_name("bucket/scope"), "`bucket`.`scope`", "two parts");
  assert_eq(analytics::quote_dataverse_name("a\\b"), "`a\\\\b`", "a backslash");
  assert_eq(analytics::quote_dataverse_name(unicode_backtick), "`\\\\u0060`", "a unicode escape");
  // a backslash on either side of the separator stays inside its own part
  assert_eq(
    analytics::quote_dataverse_name("a\\/b"), "`a\\\\`.`b`", "a backslash before the slash");
  assert_eq(analytics::quote_dataverse_name("a/\\b"), "`a`.`\\\\b`", "a backslash after the slash");
  // degenerate inputs stay quoted, so they reach the server as invalid rather than as injection
  assert_eq(analytics::quote_dataverse_name(""), "``", "an empty name");
  assert_eq(analytics::quote_dataverse_name("/"), "``.``", "a lone separator");
  assert_eq(analytics::quote_dataverse_name("/a"), "``.`a`", "an empty leading part");
  assert_eq(analytics::quote_dataverse_name("a/"), "`a`.``", "an empty trailing part");
  assert_eq(analytics::quote_dataverse_name("a//b"), "`a`.``.`b`", "an empty inner part");
}

void
quote_field_path_quotes_every_segment_of_a_dotted_path([[maybe_unused]] context& ctx)
{
  using couchbase::core::utils::analytics;

  assert_eq(analytics::quote_field_path(""), "``", "an empty path");
  assert_eq(analytics::quote_field_path("name"), "`name`", "a single segment");
  assert_eq(analytics::quote_field_path("address.city"), "`address`.`city`", "two segments");
  assert_eq(analytics::quote_field_path("a.b.c"), "`a`.`b`.`c`", "three segments");
  assert_eq(analytics::quote_field_path("a\\b.c"), "`a\\\\b`.`c`", "a backslash inside a segment");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(analytics_dataverse_create_refuses_a_name_holding_a_backtick) },
      { CASE(analytics_dataverse_drop_refuses_a_name_holding_a_backtick) },
      { CASE(analytics_dataset_create_refuses_any_name_holding_a_backtick) },
      { CASE(analytics_dataset_drop_refuses_any_name_holding_a_backtick) },
      { CASE(analytics_index_create_refuses_any_name_holding_a_backtick) },
      { CASE(analytics_index_drop_refuses_a_name_holding_a_backtick) },
      { CASE(analytics_link_connect_refuses_a_name_holding_a_backtick) },
      { CASE(analytics_link_disconnect_refuses_a_name_holding_a_backtick) },
      { CASE(a_unicode_escape_in_a_name_cannot_close_the_identifier) },
      { CASE(a_trailing_backslash_cannot_escape_the_closing_backtick) },
      { CASE(backslashes_in_every_identifier_position_are_escaped) },
      { CASE(backslashes_in_a_field_name_are_escaped) },
      { CASE(analytics_dataverse_create_renders_create_dataverse) },
      { CASE(analytics_dataverse_drop_renders_drop_dataverse) },
      { CASE(analytics_dataset_create_renders_create_dataset) },
      { CASE(analytics_dataset_drop_renders_drop_dataset) },
      { CASE(analytics_index_create_renders_create_index) },
      { CASE(analytics_index_drop_renders_drop_index) },
      { CASE(analytics_link_connect_renders_connect_link) },
      { CASE(analytics_link_disconnect_renders_disconnect_link) },
      { CASE(analytics_index_create_requires_an_ascii_alphanumeric_field_type) },
      { CASE(analytics_index_create_leaves_an_unusable_name_to_the_server) },
      { CASE(all_quotable_accepts_any_name_without_a_backtick) },
      { CASE(quote_identifier_quotes_the_name_and_escapes_its_backslashes) },
      { CASE(quote_dataverse_name_splits_on_slash_and_escapes_backslashes) },
      { CASE(quote_field_path_quotes_every_segment_of_a_dotted_path) },
    },
  };
}

} // namespace couchbase::test
