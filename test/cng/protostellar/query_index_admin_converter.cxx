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

// Unit tests for the query-index-admin <-> couchbase.admin.query.v1 translation (CXXCBC-901).
// Pure, no server.
//
// The encode side carries the only statement text this transport lets a caller supply. The gateway
// builds the CREATE INDEX statement from the request and joins the field list with commas
// verbatim, so an index key that does not stay inside its own quoting adds terms to the statement.
// The adversarial cases below reconstruct that join and count the tokens the query parser would
// find in it.

#include "framework/test_runner.hxx"

#include "core/protostellar/query_index_admin_converter.hxx"

#include <cstddef>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace qi = ::couchbase::core::protostellar::query_index_admin;
namespace om = ::couchbase::core::operations::management;
namespace v1 = ::couchbase::admin::query::v1;

// Where the escaped identifier starting at `pos` ends, or npos if it never closes.
//
// Transcribed from the query parser's own lexer rule for an escaped identifier
// (parser/n1ql/n1ql.nex): a backtick opens it, the body is a sequence of doubled backticks,
// backslash escapes, and plain characters, and a lone backtick closes it. Written out here rather
// than reusing the converter's is_escaped_identifier(), so that a converter that mis-scans cannot
// agree with itself.
[[nodiscard]] auto
identifier_end(const std::string& text, std::size_t pos) -> std::size_t
{
  if (pos >= text.size() || text[pos] != '`') {
    return std::string::npos;
  }
  for (auto i = pos + 1; i < text.size();) {
    if (text[i] == '\\') {
      if (i + 1 >= text.size()) {
        return std::string::npos; // a trailing backslash has nothing to escape
      }
      i += 2;
    } else if (text[i] == '`') {
      if (i + 1 >= text.size() || text[i + 1] != '`') {
        return i + 1;
      }
      i += 2;
    } else {
      i += 1;
    }
  }
  return std::string::npos;
}

// How many identifiers the parser would read out of the field list the gateway builds. Anything
// other than one token per key means a key escaped its own quoting.
[[nodiscard]] auto
count_identifiers(const std::string& field_list) -> std::size_t
{
  std::size_t count = 0;
  std::size_t pos = 0;
  while (pos < field_list.size()) {
    const auto end = identifier_end(field_list, pos);
    if (end == std::string::npos) {
      return count; // loose text: fewer tokens than keys, which is the failure being detected
    }
    ++count;
    pos = end;
    if (pos < field_list.size()) {
      if (field_list[pos] != ',') {
        return count; // a token followed by something other than the separator
      }
      ++pos;
    }
  }
  return count;
}

// The name an escaped identifier denotes, by the rule the parser applies to it
// (parser/n1ql/util.go): a backslash escapes the character after it. Only the two escapes the
// converter emits are handled -- everything else is copied, which is what the parser does too.
[[nodiscard]] auto
identifier_name(const std::string& token) -> std::string
{
  std::string name;
  for (std::size_t i = 1; i + 1 < token.size(); ++i) {
    if (token[i] == '\\' && i + 2 < token.size()) {
      ++i;
    }
    name.push_back(token[i]);
  }
  return name;
}

// Joins the encoded keys the way the gateway does before splicing them into the statement
// (gocbcorex cbqueryx: strings.Join(opts.Fields, ",")).
[[nodiscard]] auto
gateway_field_list(const v1::CreateIndexRequest& proto) -> std::string
{
  std::string joined;
  for (const auto& field : proto.fields()) {
    if (!joined.empty()) {
      joined.push_back(',');
    }
    joined += field;
  }
  return joined;
}

[[nodiscard]] auto
create_request(std::vector<std::string> keys) -> om::query_index_create_request
{
  om::query_index_create_request request{};
  request.bucket_name = "travel";
  request.index_name = "ix";
  request.keys = std::move(keys);
  return request;
}

// Keys chosen to break out of the quoting the encode applies, plus the ordinary ones they must not
// break. Every case below runs the whole list.
[[nodiscard]] auto
hostile_keys() -> std::vector<std::string>
{
  return {
    "field",
    "two words",                                     // only works because the key is quoted
    "`field`",                                       // the form GetAllIndexes reports
    "`a\\`b`",                                       // already one token, with an escaped backtick
    "`a`,`b`",                                       // pre-quoted, but closes after `a`
    "a`,`b",                                         // closes the quoting the encode opens
    "a`",                                            //
    "`a",                                            //
    "`",                                             //
    "``",                                            //
    "```",                                           //
    "a\\",                                           // trailing backslash: escapes the closer
    "a\\\\",                                         //
    "`a\\`",                                         // pre-quoted, but the closer is escaped
    "a) , (1=1) OR (1",                              //
    "x`) USING GSI WITH {\"defer_build\":true} -- ", //
    "name`,`type`) WITH {\"num_replica\":9} -- ",    //
    "a\nb",
    "a,b",
    "",
  };
}

void
index_keys_are_escaped_into_one_token_each([[maybe_unused]] context& ctx)
{
  for (const auto& key : hostile_keys()) {
    const auto encoded = qi::encode_index_key(key);
    const auto message = fmt::format("key <{}> encodes to <{}>", key, encoded);
    assert_eq(identifier_end(encoded, 0),
              encoded.size(),
              fmt::format("{} as a single identifier", message));
    if (encoded != key) {
      // Not passed through, so the escape must be reversible: the identifier denotes the key the
      // caller asked for, with nothing added and nothing lost.
      assert_eq(identifier_name(encoded), key, fmt::format("{} denoting the key itself", message));
    }
  }
}

void
a_hostile_key_cannot_add_terms_to_the_field_list([[maybe_unused]] context& ctx)
{
  const auto keys = hostile_keys();
  const auto proto = qi::encode_create(create_request(keys));
  assert_eq(static_cast<std::size_t>(proto.fields_size()), keys.size(), "every key is encoded");

  const auto field_list = gateway_field_list(proto);
  assert_eq(count_identifiers(field_list),
            keys.size(),
            fmt::format("the field list has exactly one identifier per key: <{}>", field_list));
}

void
an_ordinary_key_is_quoted_and_a_quoted_key_is_left_alone([[maybe_unused]] context& ctx)
{
  const auto proto = qi::encode_create(create_request({ "field", "two words", "`already`" }));
  assert_eq(static_cast<std::size_t>(proto.fields_size()), std::size_t{ 3 }, "three keys");
  assert_eq(proto.fields(0), std::string{ "`field`" }, "a bare key is quoted");
  assert_eq(proto.fields(1), std::string{ "`two words`" }, "a key with a space is quoted");
  assert_eq(proto.fields(2), std::string{ "`already`" }, "a quoted key is not quoted twice");
}

void
encode_create_fills_the_secondary_request([[maybe_unused]] context& ctx)
{
  auto request = create_request({ "country" });
  request.scope_name = "inventory";
  request.collection_name = "airport";
  request.num_replicas = 2;
  request.deferred = true;
  request.ignore_if_exists = true;

  const auto proto = qi::encode_create(request);
  assert_eq(proto.bucket_name(), std::string{ "travel" }, "bucket encoded");
  assert_eq(proto.scope_name(), std::string{ "inventory" }, "scope encoded");
  assert_eq(proto.collection_name(), std::string{ "airport" }, "collection encoded");
  assert_eq(proto.name(), std::string{ "ix" }, "index name encoded");
  assert_eq(proto.num_replicas(), 2, "num_replicas encoded");
  assert_true(proto.deferred(), "deferred encoded");
  assert_true(proto.ignore_if_exists(), "ignore_if_exists encoded");

  // An index name is not escaped here: it travels as its own field and the gateway escapes it.
  // Escaping it a second time would create an index whose name carries the backticks.
  auto hostile = create_request({ "country" });
  hostile.index_name = "ix`) --";
  assert_eq(qi::encode_create(hostile).name(),
            std::string{ "ix`) --" },
            "the index name is carried verbatim, for the gateway to escape");
}

void
an_unset_keyspace_part_is_left_unset([[maybe_unused]] context& ctx)
{
  // A present-but-empty scope names a scope called "", which the gateway rejects; an absent one
  // means the whole bucket. The core request spells both as an empty string.
  const auto proto = qi::encode_create(create_request({ "country" }));
  assert_false(proto.has_scope_name(), "an empty scope is unset, not empty");
  assert_false(proto.has_collection_name(), "an empty collection is unset, not empty");

  om::query_index_build_deferred_request build{};
  build.bucket_name = "travel";
  build.scope_name = std::string{};
  const auto build_proto = qi::encode_build_deferred(build);
  assert_eq(build_proto.bucket_name(), std::string{ "travel" }, "bucket encoded");
  assert_false(build_proto.has_scope_name(), "an empty optional scope is unset too");
}

void
create_picks_the_rpc_from_is_primary([[maybe_unused]] context& ctx)
{
  auto request = create_request({});
  request.is_primary = true;
  request.index_name = "";
  request.num_replicas = 1;
  request.deferred = true;

  const auto proto = qi::encode_create_primary(request);
  assert_false(proto.has_name(), "an unnamed primary index leaves the name to the server");
  assert_eq(proto.num_replicas(), 1, "num_replicas encoded");
  assert_true(proto.deferred(), "deferred encoded");

  request.index_name = "named_primary";
  assert_eq(qi::encode_create_primary(request).name(),
            std::string{ "named_primary" },
            "a named primary index carries its name");
}

void
drop_picks_the_rpc_from_is_primary([[maybe_unused]] context& ctx)
{
  om::query_index_drop_request request{};
  request.bucket_name = "travel";
  request.scope_name = "inventory";
  request.collection_name = "airport";
  request.index_name = "ix";
  request.ignore_if_does_not_exist = true;

  const auto secondary = qi::encode_drop(request);
  assert_eq(secondary.name(), std::string{ "ix" }, "secondary drop names the index");
  assert_eq(secondary.collection_name(), std::string{ "airport" }, "secondary drop is qualified");
  assert_true(secondary.ignore_if_missing(), "ignore_if_does_not_exist encoded");

  request.is_primary = true;
  request.index_name = "";
  const auto primary = qi::encode_drop_primary(request);
  assert_false(primary.has_name(), "an unnamed primary drop leaves the name to the server");
  assert_true(primary.ignore_if_missing(), "ignore_if_does_not_exist encoded");
}

void
a_conditional_secondary_index_cannot_be_encoded([[maybe_unused]] context& ctx)
{
  auto request = create_request({ "country" });
  assert_true(qi::can_encode(request), "a plain secondary index is encodable");

  request.condition = "country = \"US\"";
  assert_false(qi::can_encode(request), "CreateIndexRequest has no condition field");

  // The condition is not silently dropped for a primary index either -- there is nothing to drop,
  // since a primary index indexes every key.
  request.is_primary = true;
  assert_true(qi::can_encode(request), "a primary index has no condition to lose");
}

void
decode_index_maps_fields([[maybe_unused]] context& ctx)
{
  v1::GetAllIndexesResponse_Index proto;
  proto.set_name("ix1");
  proto.set_is_primary(false);
  proto.set_type(v1::INDEX_TYPE_GSI);
  proto.set_state(v1::INDEX_STATE_ONLINE);
  proto.set_bucket_name("travel");
  proto.set_scope_name("inventory");
  proto.set_collection_name("airport");
  proto.add_fields("country");
  proto.add_fields("city");
  proto.set_condition("(`country` = \"US\")");

  const auto index = qi::decode_index(proto);
  assert_eq(index.name, std::string{ "ix1" }, "name decoded");
  assert_false(index.is_primary, "is_primary decoded");
  assert_eq(index.type, std::string{ "gsi" }, "type enum -> string");
  assert_eq(index.state, std::string{ "online" }, "state enum -> string");
  assert_eq(index.bucket_name, std::string{ "travel" }, "bucket decoded");
  assert_true(index.scope_name.has_value() && index.scope_name.value() == "inventory",
              "scope decoded");
  assert_true(index.collection_name.has_value() && index.collection_name.value() == "airport",
              "collection decoded");
  assert_eq(index.index_key.size(), std::size_t{ 2 }, "index keys decoded");
  assert_eq(index.index_key.at(0), std::string{ "country" }, "first key decoded");
  assert_true(index.condition.has_value(), "condition decoded");
}

// Both enums fall back to "unknown" rather than to whichever value happens to sit first. The
// schema has no value outside the two below today, so this is about what a schema bump decodes to:
// naming an unrecognised type "gsi" is a claim about the index rather than an admission.
void
an_unrecognised_enum_decodes_as_unknown([[maybe_unused]] context& ctx)
{
  const auto out_of_range = static_cast<v1::IndexType>(9999);
  assert_eq(qi::index_type_to_string(out_of_range), std::string{ "unknown" }, "unknown index type");
  assert_eq(qi::index_state_to_string(static_cast<v1::IndexState>(9999)),
            std::string{ "unknown" },
            "unknown index state");
  assert_eq(qi::index_type_to_string(v1::INDEX_TYPE_GSI), std::string{ "gsi" }, "gsi still maps");
  assert_eq(
    qi::index_type_to_string(v1::INDEX_TYPE_VIEW), std::string{ "view" }, "view still maps");
}

void
decode_primary_index_has_no_keys([[maybe_unused]] context& ctx)
{
  v1::GetAllIndexesResponse_Index proto;
  proto.set_name("#primary");
  proto.set_is_primary(true);
  proto.set_type(v1::INDEX_TYPE_GSI);
  proto.set_state(v1::INDEX_STATE_DEFERRED);
  proto.set_bucket_name("travel");

  const auto index = qi::decode_index(proto);
  assert_true(index.is_primary, "primary decoded");
  assert_eq(index.state, std::string{ "deferred" }, "deferred state decoded");
  assert_true(index.index_key.empty(), "primary has no keys");
  assert_false(index.scope_name.has_value(), "unset scope stays nullopt");
  assert_false(index.condition.has_value(), "unset condition stays nullopt");
}

// condition and partition are `optional string`, so "the server did not report one" and "the
// server reported an empty one" are two answers and only presence separates them. Reading
// emptiness instead collapses the second onto the first.
void
an_explicitly_empty_condition_is_not_an_absent_one([[maybe_unused]] context& ctx)
{
  v1::GetAllIndexesResponse_Index proto;
  proto.set_bucket_name("travel");
  proto.set_condition("");
  proto.set_partition("");

  const auto index = qi::decode_index(proto);
  assert_true(index.condition.has_value(),
              "a condition the server sent empty is still a condition");
  assert_eq(index.condition.value_or("absent"), std::string{}, "and it decodes to what was sent");
  assert_true(index.partition.has_value(),
              "a partition the server sent empty is still a partition");
  assert_eq(index.partition.value_or("absent"), std::string{}, "and it decodes to what was sent");

  v1::GetAllIndexesResponse_Index unset;
  unset.set_bucket_name("travel");
  const auto without = qi::decode_index(unset);
  assert_false(without.condition.has_value(), "an unsent condition stays nullopt");
  assert_false(without.partition.has_value(), "an unsent partition stays nullopt");
}

// GetAllIndexes reports keys in the quoted form, so a key read from one index and passed to
// create_index() for another must name the same field.
void
a_decoded_key_re_encodes_to_itself([[maybe_unused]] context& ctx)
{
  v1::GetAllIndexesResponse_Index proto;
  proto.set_bucket_name("travel");
  proto.add_fields("`field`");
  proto.add_fields("`two words`");

  const auto index = qi::decode_index(proto);
  for (const auto& key : index.index_key) {
    assert_eq(qi::encode_index_key(key), key, "a key read back from the server survives re-encode");
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_query_index_admin_converter",
    {
      { "index_keys_are_escaped_into_one_token_each", index_keys_are_escaped_into_one_token_each },
      { "a_hostile_key_cannot_add_terms_to_the_field_list",
        a_hostile_key_cannot_add_terms_to_the_field_list },
      { "an_ordinary_key_is_quoted_and_a_quoted_key_is_left_alone",
        an_ordinary_key_is_quoted_and_a_quoted_key_is_left_alone },
      { "encode_create_fills_the_secondary_request", encode_create_fills_the_secondary_request },
      { "an_unset_keyspace_part_is_left_unset", an_unset_keyspace_part_is_left_unset },
      { "create_picks_the_rpc_from_is_primary", create_picks_the_rpc_from_is_primary },
      { "drop_picks_the_rpc_from_is_primary", drop_picks_the_rpc_from_is_primary },
      { "a_conditional_secondary_index_cannot_be_encoded",
        a_conditional_secondary_index_cannot_be_encoded },
      { "decode_index_maps_fields", decode_index_maps_fields },
      { "an_unrecognised_enum_decodes_as_unknown", an_unrecognised_enum_decodes_as_unknown },
      { "decode_primary_index_has_no_keys", decode_primary_index_has_no_keys },
      { "an_explicitly_empty_condition_is_not_an_absent_one",
        an_explicitly_empty_condition_is_not_an_absent_one },
      { "a_decoded_key_re_encodes_to_itself", a_decoded_key_re_encodes_to_itself },
    },
  };
}

} // namespace couchbase::test
