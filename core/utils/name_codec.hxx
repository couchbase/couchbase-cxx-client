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

#include <algorithm>
#include <initializer_list>
#include <string>
#include <string_view>

namespace couchbase::core::utils
{
/**
 * Renders names as Analytics SQL++ delimited identifiers.
 *
 * Two distinct operations are involved, and this file keeps them apart deliberately:
 *
 *   - QUOTING wraps a name in backticks so the parser reads it as one identifier token instead of
 *     as keywords and punctuation. Analytics calls the result a delimited identifier.
 *   - ESCAPING rewrites individual characters inside those backticks so that none of them can end
 *     the token early or be reinterpreted by the parser.
 *
 * Both are needed: quoting alone is not safe, because a character inside the quotes can still
 * terminate them.
 *
 * The grammar for a delimited identifier is (asterix-lang-sqlpp SQLPP.jj, token QUOTED_STRING):
 *
 *     "`" ( "\\\\" | "\\\"" | "\\/" | "\\b" | "\\f" | "\\n" | "\\r" | "\\t" | ~["`","\\"] )* "`"
 *
 * so the only escape mechanism is a backslash sequence, and the only two characters with any
 * special meaning inside the quotes are the backslash and the backtick. Three consequences shape
 * this code:
 *
 *   - THE BACKSLASH MUST BE ESCAPED, as "\\\\". It is the escape introducer, and before the lexer
 *     ever runs, the character stream expands a \\uXXXX sequence into the character it denotes
 *     (JavaCharStream::readChar). That expansion fires only when the backslash run preceding the
 *     'u' is of odd length, so doubling every backslash makes every run even and puts the
 *     expansion permanently out of reach. Leaving backslashes alone instead would let the six
 *     characters \\u0060 become a real backtick and close the identifier.
 *
 *   - A BACKTICK CANNOT BE ESCAPED AT ALL, so a name containing one has no representation as a
 *     delimited identifier. Note that doubling it is NOT an escape here: unlike a string literal,
 *     whose grammar rule concatenates adjacent tokens and so does give "``" the meaning of one
 *     literal quote, an identifier is a single token with no such rule, and "`a``b`" is simply two
 *     adjacent tokens. Requests reject these names up front via all_quotable(); see the note on
 *     append_escaped() for what happens if one gets through anyway.
 *
 *   - NOTHING ELSE NEEDS ESCAPING. Every other byte, including a raw quote, slash, newline, tab,
 *     NUL and anything above 0x7f, is matched by ~["`","\\"] and passes through unchanged. In
 *     particular a double quote must NOT be escaped as "\\\"": the server only honours that escape
 *     when the escaped character is the same as the opening delimiter, so inside backticks it
 *     would silently drop the quote and corrupt the name.
 */
struct analytics {
  /**
   * True when every name can be quoted, i.e. none of them contains a backtick.
   *
   * Requests call this before building a statement and fail with errc::common::invalid_argument
   * when it returns false. It reports only whether the whole set is usable, not which name was at
   * fault: the encoders return a single error code and have no channel for a richer message.
   */
  [[nodiscard]] static auto all_quotable(std::initializer_list<std::string_view> names) -> bool
  {
    return std::all_of(names.begin(), names.end(), [](const auto name) {
      return name.find('`') == std::string_view::npos;
    });
  }

  /**
   * Quotes a name as a single delimited identifier, escaping its body.
   *
   * Preconditions: all_quotable({ name }). Separators are not interpreted, so a '.' or '/' in the
   * name becomes part of the identifier rather than splitting it.
   */
  [[nodiscard]] static auto quote_identifier(const std::string& name) -> std::string
  {
    std::string result;
    result.reserve(name.size() + 2);
    result.push_back('`');
    for (const auto symbol : name) {
      append_escaped(result, symbol);
    }
    result.push_back('`');
    return result;
  }

  /**
   * Quotes a dotted field path ("address.city") by quoting each segment separately and rejoining
   * them with an unquoted '.', so that the parser sees a path of identifiers rather than one
   * identifier containing dots.
   *
   * Preconditions: all_quotable({ path }).
   */
  [[nodiscard]] static auto quote_field_path(const std::string& path) -> std::string
  {
    return quote_parts(path, '.', '.');
  }

  /**
   * Quotes a dataverse name, which the SDK spells as a '/'-separated compound ("bucket/scope"),
   * into the dot-qualified form the statement needs: "bucket/scope" becomes `bucket`.`scope`.
   *
   * Preconditions: all_quotable({ name }).
   *
   * This is a pure encoder: it does not substitute a default for an empty name. Requests that treat
   * the dataverse as optional already default it to "Default" in their own declarations, and the
   * two that do not (analytics_dataverse_create, analytics_dataverse_drop) take the dataverse as a
   * required argument naming the object to create or drop, where silently retargeting an empty name
   * onto "Default" would be destructive.
   */
  [[nodiscard]] static auto quote_dataverse_name(const std::string& name) -> std::string
  {
    return quote_parts(name, '/', '.');
  }

private:
  /**
   * Splits on separator, quotes each part, and rejoins with joiner. The separator itself is never
   * escaped, because it is consumed here rather than passed to quote_identifier(); an empty part
   * yields an empty identifier, which the server rejects rather than misinterprets.
   */
  [[nodiscard]] static auto quote_parts(const std::string& name, char separator, char joiner)
    -> std::string
  {
    std::string result;
    result.reserve(name.size() + 2);
    for (std::size_t start = 0;;) {
      const auto pos = name.find(separator, start);
      // Standing in for "this is not the first part", which holds only because quote_identifier()
      // never returns an empty string. Were that to change, a leading separator would lose its
      // empty first part and silently retarget the name instead of producing one the server
      // rejects.
      if (!result.empty()) {
        result.push_back(joiner);
      }
      result.append(quote_identifier(
        name.substr(start, pos == std::string::npos ? std::string::npos : pos - start)));
      if (pos == std::string::npos) {
        return result;
      }
      start = pos + 1;
    }
  }

  /**
   * Appends one character of an identifier body.
   *
   * The backslash is escaped, which is the whole of what escaping means here. The backtick has no
   * escape, so it cannot be handled correctly at all; emitting it doubled is a deliberate last
   * resort for a name that reached this point without passing all_quotable(). It produces two
   * adjacent identifier tokens and therefore an unparseable statement, which fails closed, rather
   * than a lone backtick, which would end the identifier and let the rest of the name be parsed as
   * SQL++. It is a backstop against a future caller forgetting the precondition, not an escape.
   */
  static void append_escaped(std::string& result, char symbol)
  {
    switch (symbol) {
      case '\\':
        result.append("\\\\");
        break;
      case '`':
        result.append("``");
        break;
      default:
        result.push_back(symbol);
        break;
    }
  }
};
} // namespace couchbase::core::utils
