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

#include "document_analytics.hxx"
#include "analytics_response_parsing.hxx"
#include "core/cluster_options.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <gsl/assert>
#include <tao/json/value.hpp>

namespace couchbase::core::operations
{
void
encode_analytics_options(tao::json::value& body, const analytics_request& request)
{
  for (const auto& [name, value] : request.named_parameters) {
    Expects(name.empty() == false);
    std::string key = name;
    if (key[0] != '$') {
      key.insert(key.begin(), '$');
    }
    body[key] = utils::json::parse(value);
  }
  if (!request.positional_parameters.empty()) {
    std::vector<tao::json::value> parameters;
    parameters.reserve(request.positional_parameters.size());
    for (const auto& value : request.positional_parameters) {
      parameters.emplace_back(utils::json::parse(value));
    }
    body["args"] = std::move(parameters);
  }
  if (request.readonly) {
    body["readonly"] = true;
  }
  if (request.scan_consistency) {
    switch (request.scan_consistency.value()) {
      case couchbase::core::analytics_scan_consistency::not_bounded:
        body["scan_consistency"] = "not_bounded";
        break;
      case couchbase::core::analytics_scan_consistency::request_plus:
        body["scan_consistency"] = "request_plus";
        break;
    }
  }
  if (request.scope_qualifier) {
    body["query_context"] = request.scope_qualifier.value();
  } else if (request.scope_name && request.bucket_name) {
    body["query_context"] =
      fmt::format("default:`{}`.`{}`", *request.bucket_name, *request.scope_name);
  }
  for (const auto& [name, value] : request.raw) {
    body[name] = utils::json::parse(value);
  }
}

auto
analytics_request::encode_to(analytics_request::encoded_request_type& encoded,
                             http_context& context) -> std::error_code
{
  tao::json::value body{ { "statement", statement },
                         { "client_context_id", encoded.client_context_id },
                         { "timeout", fmt::format("{}ms", encoded.timeout.count()) } };

  encode_analytics_options(body, *this);

  encoded.type = type;
  encoded.headers["content-type"] = "application/json";
  if (priority) {
    encoded.headers["analytics-priority"] = "-1";
  }
  encoded.method = "POST";
  encoded.path = "/query/service";
  body_str = utils::json::generate(body);
  encoded.body = body_str;
  if (context.options.show_queries) {
    CB_LOG_INFO("ANALYTICS: client_context_id=\"{}\", {}",
                encoded.client_context_id,
                utils::json::generate(body["statement"]));
  } else {
    CB_LOG_DEBUG("ANALYTICS: client_context_id=\"{}\", {}",
                 encoded.client_context_id,
                 utils::json::generate(body["statement"]));
  }
  if (row_callback) {
    encoded.streaming.emplace(couchbase::core::io::streaming_settings{
      "/results/^",
      4,
      std::move(row_callback.value()),
    });
  }
  return {};
}

auto
analytics_request::make_response(error_context::analytics&& ctx,
                                 const encoded_response_type& encoded) const -> analytics_response
{
  analytics_response response{ std::move(ctx) };
  response.ctx.statement = statement;
  response.ctx.parameters = body_str;
  if (!response.ctx.ec) {
    tao::json::value payload;
    try {
      payload = utils::json::parse(encoded.body.data());
    } catch (const tao::pegtl::parse_error&) {
      response.ctx.ec = errc::common::parsing_failure;
      return response;
    }
    response.meta = parse_analytics_meta(payload);
    if (response.ctx.client_context_id != response.meta.client_context_id &&
        !response.meta.client_context_id.empty()) {
      CB_LOG_WARNING(R"(unexpected clientContextID returned by service: "{}", expected "{}")",
                     response.meta.client_context_id,
                     response.ctx.client_context_id);
    }

    if (const auto* r = payload.find("results"); r != nullptr) {
      response.rows.reserve(r->get_array().size());
      for (const auto& row : r->get_array()) {
        response.rows.emplace_back(couchbase::core::utils::json::generate(row));
      }
    }

    if (response.meta.status != analytics_response::analytics_status::success) {
      if (!response.meta.errors.empty()) {
        response.ctx.first_error_code = response.meta.errors.front().code;
        response.ctx.first_error_message = response.meta.errors.front().message;
      }
      response.ctx.ec = map_analytics_error(response.meta);
    }
  }
  return response;
}
} // namespace couchbase::core::operations
