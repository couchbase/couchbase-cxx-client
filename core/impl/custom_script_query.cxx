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

#include "encoded_search_query.hxx"

#include "core/utils/json.hxx"

#include <couchbase/custom_filter_query.hxx>
#include <couchbase/custom_score_query.hxx>
#include <couchbase/error_codes.hxx>

#include <tao/json/contrib/traits.hpp>
#include <tao/json/type.hpp>
#include <tao/json/value.hpp>

#include <exception>

namespace couchbase
{
namespace
{
// custom_score and custom_filter share one shape and differ only in the key that holds it.
auto
encode_custom_script_query(const std::string& type,
                           const search_query& query,
                           const std::string& source,
                           const std::vector<std::string>& fields,
                           const std::map<std::string, codec::binary, std::less<>>& parameters,
                           bool has_boost) -> encoded_search_query
{
  // The server rejects a function without a source. Boost is hidden on these queries, so it can
  // only have been set through the base class, and the server would drop it without saying so.
  if (source.empty() || has_boost) {
    return { errc::common::invalid_argument };
  }

  auto encoded = query.encode();
  if (encoded.ec) {
    return { encoded.ec };
  }

  tao::json::value body = {
    { "query", encoded.query },
    { "source", source },
  };
  if (!fields.empty()) {
    body["fields"] = fields;
  }
  if (!parameters.empty()) {
    tao::json::value params = tao::json::empty_object;
    for (const auto& [name, value] : parameters) {
      try {
        params[name] = core::utils::json::parse_binary(value);
      } catch (const std::exception&) {
        return { errc::common::invalid_argument };
      }
    }
    body["params"] = params;
  }

  encoded_search_query built;
  built.query = { { type, body } };
  return built;
}
} // namespace

auto
custom_score_query::encode() const -> encoded_search_query
{
  return encode_custom_script_query(
    "custom_score", *query_, source_, fields_, parameters_, boost_.has_value());
}

auto
custom_filter_query::encode() const -> encoded_search_query
{
  return encode_custom_script_query(
    "custom_filter", *query_, source_, fields_, parameters_, boost_.has_value());
}
} // namespace couchbase
