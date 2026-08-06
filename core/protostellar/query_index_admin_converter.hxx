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

#pragma once

// Translates core query index management requests to and from couchbase.admin.query.v1 (unary
// admin RPCs). The proto IndexType / IndexState enums have no `unknown` sentinel; they map to the
// lowercase strings the rest of the SDK uses.
//
// The gateway builds the N1QL statement from the request rather than taking one, so the encode
// side owns everything the statement's shape depends on: which RPC a primary index goes to, and
// how an index key is escaped.

#include "core/operations/management/query_index_build_deferred.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all.hxx"
#include "couchbase/management/query_index.hxx"

#include <couchbase/admin/query/v1/query.pb.h>

#include <cstddef>
#include <string>

namespace couchbase::core::protostellar::query_index_admin
{
namespace v1 = ::couchbase::admin::query::v1;
namespace om = ::couchbase::core::operations::management;

// Whether the key is already one complete escaped identifier: an opening backtick, a body in which
// every backslash escapes the character after it and every backtick is escaped or doubled, and the
// closing backtick at the very end of the key.
//
// Ending exactly at the end is the whole question. A key whose identifier closes early -- "`a`,`b`"
// -- leaves the remainder as loose statement text, and a key ending in a lone backslash -- "`a\" --
// escapes its own closing backtick and swallows what follows it.
[[nodiscard]] inline auto
is_escaped_identifier(const std::string& key) -> bool
{
  if (key.size() < 2 || key.front() != '`' || key.back() != '`') {
    return false;
  }
  const auto last = key.size() - 1;
  for (std::size_t i = 1; i < last;) {
    if (key[i] == '\\') {
      if (i + 1 >= last) {
        return false;
      }
      i += 2;
      continue;
    }
    if (key[i] == '`') {
      if (i + 1 < last && key[i + 1] == '`') {
        i += 2;
        continue;
      }
      return false;
    }
    i += 1;
  }
  return true;
}

// Escapes one index key into exactly one N1QL identifier token.
//
// This is the transport's only client-supplied statement text. The gateway builds the statement
// itself and joins the field list with commas verbatim (gocbcorex cbqueryx), so whatever arrives in
// `fields` is spliced between the parentheses of CREATE INDEX unchanged. A key that closed its own
// quoting would add terms to the index -- or change the statement -- rather than name a field.
//
// The escape is the one the query parser reads back: a backslash escapes the character after it
// (parser/n1ql/util.go), so a backtick and a backslash each arrive prefixed by one. Doubling the
// backtick instead would parse, but reads back as two backticks in the name rather than one, and
// leaving the backslash bare makes the parser reject the whole statement. Anything else inside the
// quotes -- spaces, commas, parentheses, newlines -- is part of the identifier and needs no escape,
// which is why "two words" is a key and not a syntax error.
//
// A key that is already one complete identifier is passed through, because that is the form
// GetAllIndexes reports keys in and the classic query path accepts. "Already" is verified rather
// than assumed: is_escaped_identifier() is what separates a caller's pre-quoted key from a key
// carrying quotes that end early.
[[nodiscard]] inline auto
encode_index_key(const std::string& key) -> std::string
{
  if (is_escaped_identifier(key)) {
    return key;
  }
  std::string escaped;
  escaped.reserve(key.size() + 2);
  escaped.push_back('`');
  for (const auto c : key) {
    if (c == '\\' || c == '`') {
      escaped.push_back('\\');
    }
    escaped.push_back(c);
  }
  escaped.push_back('`');
  return escaped;
}

// Copies the keyspace onto a request whose scope/collection fields are `optional string`. An empty
// name is left unset: the gateway reads an unset scope as "the whole bucket", and a present-but-
// empty one as a scope literally named "", which it rejects.
template<typename Proto>
void
apply_keyspace(const std::string& bucket_name,
               const std::string& scope_name,
               const std::string& collection_name,
               Proto& proto)
{
  proto.set_bucket_name(bucket_name);
  if (!scope_name.empty()) {
    proto.set_scope_name(scope_name);
  }
  if (!collection_name.empty()) {
    proto.set_collection_name(collection_name);
  }
}

// couchbase2 CreateIndexRequest has no condition field, so a secondary index with a WHERE clause
// cannot be expressed at all -- encoding one would create an index over the whole collection under
// the name the caller asked for. A primary index has no condition to lose.
[[nodiscard]] inline auto
can_encode(const om::query_index_create_request& request) -> bool
{
  return request.is_primary || !request.condition.has_value();
}

[[nodiscard]] inline auto
encode_get_all(const om::query_index_get_all_request& request) -> v1::GetAllIndexesRequest
{
  v1::GetAllIndexesRequest proto;
  apply_keyspace(request.bucket_name, request.scope_name, request.collection_name, proto);
  return proto;
}

[[nodiscard]] inline auto
encode_create_primary(const om::query_index_create_request& request)
  -> v1::CreatePrimaryIndexRequest
{
  v1::CreatePrimaryIndexRequest proto;
  apply_keyspace(request.bucket_name, request.scope_name, request.collection_name, proto);
  // An unset name asks the server for its own default (#primary); an empty one is not a name.
  if (!request.index_name.empty()) {
    proto.set_name(request.index_name);
  }
  if (request.num_replicas.has_value()) {
    proto.set_num_replicas(*request.num_replicas);
  }
  if (request.deferred.has_value()) {
    proto.set_deferred(*request.deferred);
  }
  proto.set_ignore_if_exists(request.ignore_if_exists);
  return proto;
}

[[nodiscard]] inline auto
encode_create(const om::query_index_create_request& request) -> v1::CreateIndexRequest
{
  v1::CreateIndexRequest proto;
  apply_keyspace(request.bucket_name, request.scope_name, request.collection_name, proto);
  proto.set_name(request.index_name);
  for (const auto& key : request.keys) {
    proto.add_fields(encode_index_key(key));
  }
  if (request.num_replicas.has_value()) {
    proto.set_num_replicas(*request.num_replicas);
  }
  if (request.deferred.has_value()) {
    proto.set_deferred(*request.deferred);
  }
  proto.set_ignore_if_exists(request.ignore_if_exists);
  return proto;
}

[[nodiscard]] inline auto
encode_drop_primary(const om::query_index_drop_request& request) -> v1::DropPrimaryIndexRequest
{
  v1::DropPrimaryIndexRequest proto;
  apply_keyspace(request.bucket_name, request.scope_name, request.collection_name, proto);
  if (!request.index_name.empty()) {
    proto.set_name(request.index_name);
  }
  proto.set_ignore_if_missing(request.ignore_if_does_not_exist);
  return proto;
}

[[nodiscard]] inline auto
encode_drop(const om::query_index_drop_request& request) -> v1::DropIndexRequest
{
  v1::DropIndexRequest proto;
  apply_keyspace(request.bucket_name, request.scope_name, request.collection_name, proto);
  proto.set_name(request.index_name);
  proto.set_ignore_if_missing(request.ignore_if_does_not_exist);
  return proto;
}

[[nodiscard]] inline auto
encode_build_deferred(const om::query_index_build_deferred_request& request)
  -> v1::BuildDeferredIndexesRequest
{
  v1::BuildDeferredIndexesRequest proto;
  apply_keyspace(request.bucket_name,
                 request.scope_name.value_or(std::string{}),
                 request.collection_name.value_or(std::string{}),
                 proto);
  return proto;
}

[[nodiscard]] inline auto
index_type_to_string(v1::IndexType type) -> std::string
{
  switch (type) {
    case v1::INDEX_TYPE_VIEW:
      return "view";
    case v1::INDEX_TYPE_GSI:
      return "gsi";
    default:
      return "unknown";
  }
}

[[nodiscard]] inline auto
index_state_to_string(v1::IndexState state) -> std::string
{
  switch (state) {
    case v1::INDEX_STATE_DEFERRED:
      return "deferred";
    case v1::INDEX_STATE_BUILDING:
      return "building";
    case v1::INDEX_STATE_PENDING:
      return "pending";
    case v1::INDEX_STATE_ONLINE:
      return "online";
    case v1::INDEX_STATE_OFFLINE:
      return "offline";
    case v1::INDEX_STATE_ABRIDGED:
      return "abridged";
    case v1::INDEX_STATE_SCHEDULED:
      return "scheduled";
    default:
      return "unknown";
  }
}

[[nodiscard]] inline auto
decode_index(const v1::GetAllIndexesResponse_Index& proto) -> couchbase::management::query_index
{
  couchbase::management::query_index index;
  index.is_primary = proto.is_primary();
  index.name = proto.name();
  index.type = index_type_to_string(proto.type());
  index.state = index_state_to_string(proto.state());
  index.bucket_name = proto.bucket_name();
  if (!proto.scope_name().empty()) {
    index.scope_name = proto.scope_name();
  }
  if (!proto.collection_name().empty()) {
    index.collection_name = proto.collection_name();
  }
  for (const auto& field : proto.fields()) {
    index.index_key.push_back(field);
  }
  // Presence, not emptiness: condition and partition are `optional string`, so an index that has
  // no WHERE clause is a different answer from one whose clause the server reports as empty.
  // scope_name and collection_name above carry no presence, so there an empty name is the only
  // form "not collection-qualified" can arrive in.
  if (proto.has_condition()) {
    index.condition = proto.condition();
  }
  if (proto.has_partition()) {
    index.partition = proto.partition();
  }
  return index;
}

} // namespace couchbase::core::protostellar::query_index_admin
