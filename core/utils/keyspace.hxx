/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <spdlog/fmt/bundled/core.h>

#include <string>
#include <string_view>

namespace couchbase::core::utils
{
inline auto
escape_query_identifier(std::string_view identifier) -> std::string
{
  std::string out;
  out.reserve(identifier.size() + 2);
  out.push_back('`');
  for (const char ch : identifier) {
    if (ch == '`' || ch == '\\') {
      out.push_back('\\');
    }
    out.push_back(ch);
  }
  out.push_back('`');
  return out;
}

inline auto
is_unquoted_query_identifier_start(char ch) -> bool
{
  return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '$';
}

inline auto
is_unquoted_query_identifier_part(char ch) -> bool
{
  return is_unquoted_query_identifier_start(ch) || (ch >= '0' && ch <= '9');
}

inline auto
is_sqlpp_space(char ch) -> bool
{
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f';
}

inline auto
consume_backtick_identifier(std::string_view key, std::size_t& pos) -> bool
{
  if (pos >= key.size() || key[pos] != '`') {
    return false;
  }
  std::size_t i = pos + 1;
  while (i < key.size()) {
    if (key[i] == '\\') {
      if (i + 1 >= key.size()) {
        return false;
      }
      i += 2;
      continue;
    }
    if (key[i] == '`') {
      if (i + 1 < key.size() && key[i + 1] == '`') {
        i += 2;
        continue;
      }
      if (i == pos + 1) {
        return false;
      }
      pos = i + 1;
      return true;
    }
    i += 1;
  }
  return false;
}

inline auto
equals_ignore_case(std::string_view a, std::string_view b) -> bool
{
  if (a.size() != b.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    const char ca = (a[i] >= 'a' && a[i] <= 'z') ? static_cast<char>(a[i] - ('a' - 'A')) : a[i];
    const char cb = (b[i] >= 'a' && b[i] <= 'z') ? static_cast<char>(b[i] - ('a' - 'A')) : b[i];
    if (ca != cb) {
      return false;
    }
  }
  return true;
}

inline auto
is_sqlpp_binary_keyword_op(std::string_view word) -> bool
{
  return equals_ignore_case(word, "AND") || equals_ignore_case(word, "OR") ||
         equals_ignore_case(word, "XOR") || equals_ignore_case(word, "IS") ||
         equals_ignore_case(word, "LIKE") || equals_ignore_case(word, "IN");
}

inline auto
is_sqlpp_disallowed_top_level_keyword(std::string_view word) -> bool
{
  return equals_ignore_case(word, "WHERE") || equals_ignore_case(word, "USING") ||
         equals_ignore_case(word, "WITH") || equals_ignore_case(word, "PARTITION") ||
         equals_ignore_case(word, "FROM") || equals_ignore_case(word, "SELECT") ||
         equals_ignore_case(word, "CREATE") || equals_ignore_case(word, "DROP") ||
         equals_ignore_case(word, "ALTER") || equals_ignore_case(word, "BUILD") ||
         equals_ignore_case(word, "INDEX") || equals_ignore_case(word, "ON") ||
         equals_ignore_case(word, "INSERT") || equals_ignore_case(word, "UPSERT") ||
         equals_ignore_case(word, "UPDATE") || equals_ignore_case(word, "DELETE") ||
         equals_ignore_case(word, "MERGE") || equals_ignore_case(word, "LIMIT") ||
         equals_ignore_case(word, "OFFSET") || equals_ignore_case(word, "ORDER") ||
         equals_ignore_case(word, "GROUP") || equals_ignore_case(word, "LET") ||
         equals_ignore_case(word, "LETTING") || is_sqlpp_binary_keyword_op(word);
}

inline auto
parse_query_index_expr(std::string_view key,
                       std::size_t& pos,
                       bool allow_colon,
                       bool& has_backtick_segment,
                       int depth = 0) -> bool
{
  if (depth > 32) {
    return false;
  }
  while (true) {
    while (pos < key.size() && is_sqlpp_space(key[pos])) {
      pos += 1;
    }
    while (pos < key.size()) {
      if (key[pos] == '+' || key[pos] == '-') {
        if (key[pos] == '-' && pos + 1 < key.size() && key[pos + 1] == '-') {
          return false;
        }
        pos += 1;
        while (pos < key.size() && is_sqlpp_space(key[pos])) {
          pos += 1;
        }
        continue;
      }
      if (is_unquoted_query_identifier_start(key[pos])) {
        std::size_t word_end = pos + 1;
        while (word_end < key.size() && is_unquoted_query_identifier_part(key[word_end])) {
          word_end += 1;
        }
        if (equals_ignore_case(key.substr(pos, word_end - pos), "NOT")) {
          pos = word_end;
          while (pos < key.size() && is_sqlpp_space(key[pos])) {
            pos += 1;
          }
          continue;
        }
      }
      break;
    }
    if (pos >= key.size()) {
      return false;
    }
    if (key[pos] == '(') {
      pos += 1;
      if (!parse_query_index_expr(key, pos, false, has_backtick_segment, depth + 1)) {
        return false;
      }
      while (pos < key.size() && is_sqlpp_space(key[pos])) {
        pos += 1;
      }
      if (pos >= key.size() || key[pos] != ')') {
        return false;
      }
      pos += 1;
    } else if (key[pos] == '`') {
      if (!consume_backtick_identifier(key, pos)) {
        return false;
      }
      has_backtick_segment = true;
    } else if (is_unquoted_query_identifier_start(key[pos])) {
      const std::size_t start = pos;
      pos += 1;
      while (pos < key.size() && is_unquoted_query_identifier_part(key[pos])) {
        pos += 1;
      }
      if (is_sqlpp_disallowed_top_level_keyword(key.substr(start, pos - start))) {
        return false;
      }
    } else if ((key[pos] >= '0' && key[pos] <= '9') ||
               (pos + 1 < key.size() && key[pos] == '.' && key[pos + 1] >= '0' &&
                key[pos + 1] <= '9')) {
      while (pos < key.size() && key[pos] >= '0' && key[pos] <= '9') {
        pos += 1;
      }
      if (pos + 1 < key.size() && key[pos] == '.' && key[pos + 1] >= '0' && key[pos + 1] <= '9') {
        pos += 1;
        while (pos < key.size() && key[pos] >= '0' && key[pos] <= '9') {
          pos += 1;
        }
      }
      if (pos < key.size() && (key[pos] == 'e' || key[pos] == 'E')) {
        pos += 1;
        if (pos < key.size() && (key[pos] == '+' || key[pos] == '-')) {
          pos += 1;
        }
        if (pos >= key.size() || key[pos] < '0' || key[pos] > '9') {
          return false;
        }
        while (pos < key.size() && key[pos] >= '0' && key[pos] <= '9') {
          pos += 1;
        }
      }
      if (pos < key.size() && is_unquoted_query_identifier_start(key[pos])) {
        return false;
      }
    } else {
      return false;
    }

    while (true) {
      std::size_t ws_pos = pos;
      while (ws_pos < key.size() && is_sqlpp_space(key[ws_pos])) {
        ws_pos += 1;
      }
      if (ws_pos < key.size() && key[ws_pos] == '.') {
        pos = ws_pos + 1;
        while (pos < key.size() && is_sqlpp_space(key[pos])) {
          pos += 1;
        }
        if (pos >= key.size()) {
          return false;
        }
        if (key[pos] == '`') {
          if (!consume_backtick_identifier(key, pos)) {
            return false;
          }
          has_backtick_segment = true;
        } else if (is_unquoted_query_identifier_start(key[pos])) {
          pos += 1;
          while (pos < key.size() && is_unquoted_query_identifier_part(key[pos])) {
            pos += 1;
          }
        } else {
          return false;
        }
        continue;
      }
      if (ws_pos < key.size() && key[ws_pos] == '[') {
        pos = ws_pos + 1;
        if (!parse_query_index_expr(key, pos, true, has_backtick_segment, depth + 1)) {
          return false;
        }
        while (pos < key.size() && is_sqlpp_space(key[pos])) {
          pos += 1;
        }
        if (pos >= key.size() || key[pos] != ']') {
          return false;
        }
        pos += 1;
        continue;
      }
      pos = ws_pos;
      break;
    }

    if (pos < key.size()) {
      const char op = key[pos];
      if (op == '+' || op == '*' || op == '%' || (allow_colon && op == ':')) {
        pos += 1;
        continue;
      }
      if (op == '-' && (pos + 1 >= key.size() || key[pos + 1] != '-')) {
        pos += 1;
        continue;
      }
      if (op == '/' && (pos + 1 >= key.size() || (key[pos + 1] != '*' && key[pos + 1] != '/'))) {
        pos += 1;
        continue;
      }
      if (op == '|' && pos + 1 < key.size() && key[pos + 1] == '|') {
        pos += 2;
        continue;
      }
      if ((op == '=' && pos + 1 < key.size() && key[pos + 1] == '=') ||
          (op == '!' && pos + 1 < key.size() && key[pos + 1] == '=') ||
          (op == '<' && pos + 1 < key.size() && (key[pos + 1] == '=' || key[pos + 1] == '>')) ||
          (op == '>' && pos + 1 < key.size() && key[pos + 1] == '=')) {
        pos += 2;
        continue;
      }
      if (op == '=' || op == '<' || op == '>') {
        pos += 1;
        continue;
      }
      if (is_unquoted_query_identifier_start(op)) {
        std::size_t word_end = pos + 1;
        while (word_end < key.size() && is_unquoted_query_identifier_part(key[word_end])) {
          word_end += 1;
        }
        if (is_sqlpp_binary_keyword_op(key.substr(pos, word_end - pos))) {
          pos = word_end;
          continue;
        }
      }
    }
    return true;
  }
}

// True when `key` starts with a backtick and parses as a single backtick-quoted
// identifier, a dotted/subscripted field path, or an arithmetic/comparison/boolean
// expression over identifiers and numeric literals, with no statement keywords or
// SQL++ comment markers.
//
// SQL++ constructs outside this grammar -- string literals, function calls,
// array-star `[*]`, `CASE`, `BETWEEN`, multi-word operators -- return false so
// `query_index_create_request::encode_to` quotes the whole key as one literal
// field name.
inline auto
is_escaped_query_identifier(std::string_view key) -> bool
{
  if (key.size() <= 2 || key.front() != '`') {
    return false;
  }
  std::size_t pos = 0;
  bool has_backtick_segment = false;
  if (!parse_query_index_expr(key, pos, false, has_backtick_segment)) {
    return false;
  }
  while (pos < key.size() && is_sqlpp_space(key[pos])) {
    pos += 1;
  }
  return pos == key.size() && has_backtick_segment;
}

template<typename Request>
static auto
check_query_management_request(const Request& req) -> bool
{
  // if there is a query_context, then bucket and scope will be ignored
  if (req.query_ctx.has_value()) {
    return !req.collection_name.empty();
  }

  // otherwise, both scope and collection must be specified, if one is
  // and bucket _must_ be there as well.
  return !req.bucket_name.empty() && ((req.scope_name.empty() && req.collection_name.empty()) ||
                                      (!req.scope_name.empty() && !req.collection_name.empty()));
}

template<typename Request>
static auto
build_keyspace(const Request& req) -> std::string
{
  // keyspace is just the collection if we have a query_context.
  if (req.query_ctx.has_value()) {
    return fmt::format(
      "{}.{}", req.query_ctx.value(), escape_query_identifier(req.collection_name));
  }
  // otherwise, build from the bucket, scope and collection names in request...
  if (req.scope_name.empty() && req.collection_name.empty()) {
    return fmt::format("{}:{}", req.namespace_id, escape_query_identifier(req.bucket_name));
  }
  return fmt::format("{}:{}.{}.{}",
                     req.namespace_id,
                     escape_query_identifier(req.bucket_name),
                     escape_query_identifier(req.scope_name),
                     escape_query_identifier(req.collection_name));
}

} // namespace couchbase::core::utils
