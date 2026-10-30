/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/serializer_traits.hxx>
#include <couchbase/search_query.hxx>

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace couchbase
{
namespace codec
{
class tao_json_serializer;
} // namespace codec

/**
 * A custom score query wraps another query and passes its results through a user-defined
 * JavaScript scoring function, which assigns each result its score in place of the score given by
 * the wrapped query.
 *
 * The function is invoked as `function(doc, params)`, where `doc.score` is the score assigned by
 * the wrapped query, `doc.fields` holds the document fields requested with @ref fields(), and
 * `params` holds the values given with @ref parameters(). It must return a number.
 *
 * @snippet{trimleft} unit/management/search.cxx search-custom-score
 *
 * @note This feature is disabled on the server by default.
 *
 * @since 1.6.0
 * @committed
 */
class custom_score_query : public search_query
{
public:
  /**
   * Create a custom score query.
   *
   * @tparam SearchQuery any subclass of @ref search_query
   * @param query the query whose results are passed through the scoring function
   * @param source the JavaScript source of the scoring function. Cannot be empty.
   *
   * @since 1.6.0
   * @committed
   */
  template<typename SearchQuery,
           std::enable_if_t<std::is_base_of_v<search_query, SearchQuery>, bool> = true>
  custom_score_query(SearchQuery query, std::string source)
    : query_{ std::make_shared<SearchQuery>(std::move(query)) }
    , source_{ std::move(source) }
  {
  }

  /**
   * Set the document fields made available to the scoring function through `doc.fields`. The
   * function cannot see any field that is not listed here. Use `"*"` to make all fields of the
   * document available. Any fields set previously are replaced.
   *
   * @param fields the names of the fields
   * @return this query for chaining purposes.
   *
   * @since 1.6.0
   * @committed
   */
  auto fields(std::vector<std::string> fields) -> custom_score_query&
  {
    fields_ = std::move(fields);
    return *this;
  }

  /**
   * Set the parameters made available to the scoring function through `params`. Any parameters set
   * previously are replaced.
   *
   * @tparam Parameters types for the parameter pairs
   * @param parameters the sequence of name-value pairs. Each value will be encoded into JSON, and
   * may be any value the serializer can encode, including nested objects and arrays.
   * @return this query for chaining purposes.
   *
   * @since 1.6.0
   * @committed
   */
  template<typename Serializer = codec::tao_json_serializer,
           typename... Parameters,
           std::enable_if_t<codec::is_serializer_v<Serializer>, bool> = true>
  auto parameters(const Parameters&... parameters) -> custom_score_query&
  {
    parameters_.clear();
    if constexpr (sizeof...(parameters) > 0) {
      encode_parameters<Serializer>(parameters...);
    }
    return *this;
  }

  /**
   * Add a parameter to those made available to the scoring function through `params`, replacing
   * any parameter of the same name.
   *
   * @tparam Value type for the parameter's value.
   * @param name the parameter's name
   * @param value the parameter's value. It will be encoded into JSON.
   * @return this query for chaining purposes.
   *
   * @since 1.6.0
   * @committed
   */
  template<typename Serializer = codec::tao_json_serializer,
           typename Value,
           std::enable_if_t<codec::is_serializer_v<Serializer>, bool> = true>
  auto add_parameter(const std::string& name, const Value& value) -> custom_score_query&
  {
    encode_parameters<Serializer>(std::make_pair(name, value));
    return *this;
  }

  /**
   * @return encoded representation of the query.
   *
   * @since 1.6.0
   * @internal
   */
  [[nodiscard]] auto encode() const -> encoded_search_query override;

private:
  // The server ignores boost for this query.
  using search_query::boost;

  template<typename Serializer, typename Name, typename Parameter, typename... Rest>
  void encode_parameters(const std::pair<Name, Parameter>& parameter, const Rest&... rest)
  {
    parameters_[parameter.first] =
      std::move(Serializer::template serialize<Parameter>(parameter.second));
    if constexpr (sizeof...(rest) > 0) {
      encode_parameters<Serializer>(rest...);
    }
  }

  std::shared_ptr<search_query> query_;
  std::string source_;
  std::vector<std::string> fields_{};
  std::map<std::string, codec::binary, std::less<>> parameters_{};
};
} // namespace couchbase
