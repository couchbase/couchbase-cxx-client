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
#include "core/operations/management/change_password.hxx"
#include "core/operations/management/group_upsert.hxx"
#include "core/operations/management/user_upsert.hxx"
#include "core/utils/url_codec.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <map>
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

// Splits a form body on '&' and form-decodes each value. An unescaped '&' or '=' in a value yields
// an extra or altered field.
auto
parse_form_body(const std::string& body) -> std::map<std::string, std::string>
{
  using couchbase::core::utils::string_codec::url_decode;

  std::map<std::string, std::string> values{};
  std::size_t pos{ 0 };
  while (pos <= body.size()) {
    auto amp = body.find('&', pos);
    auto pair = body.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
    auto eq = pair.find('=');
    std::string value = eq == std::string::npos ? std::string{} : pair.substr(eq + 1);
    for (auto& c : value) {
      if (c == '+') {
        c = ' ';
      }
    }
    values[pair.substr(0, eq)] = url_decode(value);
    if (amp == std::string::npos) {
      break;
    }
    pos = amp + 1;
  }
  return values;
}

void
a_user_password_cannot_add_form_fields([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::user_upsert_request req{};
  req.user.username = "alice";
  req.user.password = "Pw123456&roles=admin";
  req.user.roles = { { "data_reader", "default" } };

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // Catches '&' and '=' in the password reaching the body raw, which stores role admin.
  assert_ne(http_req.body.find("password=Pw123456%26roles%3dadmin"),
            std::string::npos,
            "'&' and '=' in the password are escaped");
  assert_eq(
    http_req.body.find("&roles=admin"), std::string::npos, "no roles field from the password");

  auto values = parse_form_body(http_req.body);
  assert_eq(values.size(), 2U, "only password and roles are sent");
  assert_eq(values["password"], "Pw123456&roles=admin", "the password decodes back");
  assert_eq(values["roles"], "data_reader[default]", "the roles are the ones passed");
}

void
a_plus_in_a_user_password_is_not_a_space([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::user_upsert_request req{};
  req.user.username = "alice";
  req.user.password = "Pa55+word";

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // Catches a raw '+', which the server form-decodes to a space.
  assert_eq(http_req.body, "password=Pa55%2bword", "'+' is escaped");
  assert_ne(http_req.body, "password=Pa55+word", "not the raw '+'");
  assert_eq(
    parse_form_body("password=Pa55+word")["password"], "Pa55 word", "a raw '+' means a space");
}

void
a_user_upsert_form_encodes_every_field([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::user_upsert_request req{};
  req.user.username = "alice";
  req.user.display_name = "Alice Smith";
  req.user.groups = { "g&1", "g2" };
  req.user.roles = {
    { "data_reader", "default", "p%x", "c1" },
    { "admin" },
  };

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  assert_ne(http_req.body.find("name=Alice+Smith"), std::string::npos, "a space is written as '+'");
  // Catches a '%' in a name escaped twice; the server rejects data_reader[default:p%25x:c1].
  assert_ne(http_req.body.find("roles=data_reader%5bdefault%3ap%25x%3ac1%5d%2cadmin"),
            std::string::npos,
            "the role spec is escaped once");

  auto values = parse_form_body(http_req.body);
  assert_eq(values.size(), 3U, "name, groups and roles are sent");
  assert_eq(values["name"], "Alice Smith", "display name");
  assert_eq(values["groups"], "g&1,g2", "groups");
  assert_eq(values["roles"], "data_reader[default:p%x:c1],admin", "roles");
}

void
a_group_upsert_form_encodes_every_field([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();

  couchbase::core::operations::management::group_upsert_request req{};
  req.group.name = "readers";
  req.group.description = "readers&x=y";
  req.group.ldap_group_reference = "cn=readers,ou=Groups";
  req.group.roles = { { "data_reader", "default" } };

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  // Catches '&' and '=' in the description reaching the body raw, which adds field x.
  assert_ne(http_req.body.find("description=readers%26x%3dy"),
            std::string::npos,
            "'&' and '=' in the description are escaped");
  assert_eq(http_req.body.find("&x=y"), std::string::npos, "no x field from the description");

  auto values = parse_form_body(http_req.body);
  assert_eq(values.size(), 3U, "description, ldap_group_ref and roles are sent");
  assert_eq(values["description"], "readers&x=y", "description");
  assert_eq(values["ldap_group_ref"], "cn=readers,ou=Groups", "LDAP group reference");
  assert_eq(values["roles"], "data_reader[default]", "roles");
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
      { CASE(a_user_password_cannot_add_form_fields) },
      { CASE(a_plus_in_a_user_password_is_not_a_space) },
      { CASE(a_user_upsert_form_encodes_every_field) },
      { CASE(a_group_upsert_form_encodes_every_field) },
    },
  };
}

} // namespace couchbase::test
