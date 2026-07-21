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

#include "document_query.hxx"
#include "query_response_parsing.hxx"

#include "core/cluster_options.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <gsl/assert>
#include <tao/json/value.hpp>

namespace couchbase::core::operations
{
void
encode_query_options(tao::json::value& body, const query_request& request)
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
  if (request.profile.has_value()) {
    switch (request.profile.value()) {
      case couchbase::query_profile::phases:
        body["profile"] = "phases";
        break;
      case couchbase::query_profile::timings:
        body["profile"] = "timings";
        break;
      case couchbase::query_profile::off:
        body["profile"] = "off";
        break;
    }
  }
  if (request.use_replica.has_value()) {
    // The capability gate is enforced by the caller (encode_to for buffered, cluster::query_stream
    // for streaming); here we only emit the encoded field.
    body["use_replica"] = request.use_replica.value() ? "on" : "off";
  }
  if (request.max_parallelism) {
    body["max_parallelism"] = std::to_string(request.max_parallelism.value());
  }
  if (request.pipeline_cap) {
    body["pipeline_cap"] = std::to_string(request.pipeline_cap.value());
  }
  if (request.pipeline_batch) {
    body["pipeline_batch"] = std::to_string(request.pipeline_batch.value());
  }
  if (request.scan_cap) {
    body["scan_cap"] = std::to_string(request.scan_cap.value());
  }
  if (!request.metrics) {
    body["metrics"] = false;
  }
  if (request.readonly) {
    body["readonly"] = true;
  }
  if (request.flex_index) {
    body["use_fts"] = true;
  }
  if (request.preserve_expiry) {
    body["preserve_expiry"] = true;
  }
  bool check_scan_wait = false;
  if (request.scan_consistency) {
    switch (request.scan_consistency.value()) {
      case query_scan_consistency::not_bounded:
        body["scan_consistency"] = "not_bounded";
        break;
      case query_scan_consistency::request_plus:
        check_scan_wait = true;
        body["scan_consistency"] = "request_plus";
        break;
    }
  } else if (!request.mutation_state.empty()) {
    check_scan_wait = true;
    body["scan_consistency"] = "at_plus";
    tao::json::value scan_vectors = tao::json::empty_object;
    for (const auto& token : request.mutation_state) {
      auto* bucket = scan_vectors.find(token.bucket_name());
      if (bucket == nullptr) {
        scan_vectors[token.bucket_name()] = tao::json::empty_object;
        bucket = scan_vectors.find(token.bucket_name());
      }
      auto& bucket_obj = bucket->get_object();
      bucket_obj[std::to_string(token.partition_id())] =
        std::vector<tao::json::value>{ token.sequence_number(),
                                       std::to_string(token.partition_uuid()) };
    }
    body["scan_vectors"] = scan_vectors;
  }
  if (check_scan_wait && request.scan_wait) {
    body["scan_wait"] = fmt::format("{}ms", request.scan_wait.value().count());
  }
  if (request.query_context) {
    body["query_context"] = request.query_context.value();
  }
  for (const auto& [name, value] : request.raw) {
    body[name] = utils::json::parse(value);
  }
}

auto
query_request::encode_to(query_request::encoded_request_type& encoded, http_context& context)
  -> std::error_code
{
  ctx_.emplace(context);
  tao::json::value body{
    { "client_context_id", encoded.client_context_id },
  };
  if (adhoc) {
    body["statement"] = statement;
  } else {
    if (auto entry = ctx_->cache.get(statement)) {
      body["prepared"] = entry->name;
      if (entry->plan) {
        body["encoded_plan"] = entry->plan.value();
      }
    } else {
      body["statement"] = "PREPARE " + statement;
      if (context.config.capabilities.supports_enhanced_prepared_statements()) {
        body["auto_execute"] = true;
      } else {
        extract_encoded_plan_ = true;
      }
    }
  }
  auto timeout_for_service = encoded.timeout;
  if (timeout_for_service > std::chrono::milliseconds(5'000)) {
    /* if allocated timeout is large enough, tell query engine, that it is 500ms smaller, to make
     * sure we will always get response */
    timeout_for_service -= std::chrono::milliseconds(500);
  }
  body["timeout"] = fmt::format("{}ms", timeout_for_service.count());

  // use_replica is only honored when the cluster advertises read-from-replica support; reject the
  // request up front otherwise. The field itself is encoded by encode_query_options below.
  if (use_replica.has_value() && !context.config.capabilities.supports_read_from_replica()) {
    return errc::common::feature_not_available;
  }

  encode_query_options(body, *this);

  encoded.type = type;
  encoded.headers["connection"] = "keep-alive";
  encoded.headers["content-type"] = "application/json";
  encoded.method = "POST";
  encoded.path = "/query/service";
  body_str = utils::json::generate(body);
  encoded.body = body_str;

  tao::json::value stmt = body["statement"];
  tao::json::value prep = body["prepared"];
  if (!stmt.is_string()) {
    stmt = statement;
  }
  if (!prep.is_string()) {
    prep = false;
  }
  body.erase("statement");
  body.erase("prepared");
  if (ctx_->options.show_queries) {
    CB_LOG_INFO("QUERY: client_context_id=\"{}\", prep={}, {}, options={}",
                encoded.client_context_id,
                utils::json::generate(prep),
                utils::json::generate(stmt),
                utils::json::generate(body));
  } else {
    CB_LOG_DEBUG("QUERY: client_context_id=\"{}\", prep={}, {}, options={}",
                 encoded.client_context_id,
                 utils::json::generate(prep),
                 utils::json::generate(stmt),
                 utils::json::generate(body));
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
query_request::make_response(error_context::query&& ctx, const encoded_response_type& encoded)
  -> query_response
{
  query_response response{ std::move(ctx) };
  response.ctx.statement = statement;
  response.ctx.parameters = body_str;
  response.served_by_node = fmt::format("{}:{}", response.ctx.hostname, response.ctx.port);
  if (!response.ctx.ec) {
    if (encoded.body.data().empty()) {
      switch (encoded.status_code) {
        case 503:
          response.ctx.ec = errc::common::service_not_available;
          break;

        case 500:
        default:
          response.ctx.ec = errc::common::internal_server_failure;
          break;
      }
      return response;
    }
    tao::json::value payload;
    try {
      payload = utils::json::parse(encoded.body.data());
    } catch (const tao::pegtl::parse_error&) {
      response.ctx.ec = errc::common::parsing_failure;
      return response;
    }
    response.meta = parse_query_meta(payload);

    if (response.ctx.client_context_id != response.meta.client_context_id &&
        !response.meta.client_context_id.empty()) {
      CB_LOG_WARNING(R"(unexpected clientContextID returned by service: "{}", expected "{}")",
                     response.meta.client_context_id,
                     response.ctx.client_context_id);
    }

    if (const auto* c = payload.find("prepared"); c != nullptr) {
      response.prepared = c->get_string();
    }

    if (const auto* r = payload.find("results"); r != nullptr) {
      response.rows.reserve(r->get_array().size());
      for (const auto& row : r->get_array()) {
        response.rows.emplace_back(couchbase::core::utils::json::generate(row));
      }
    }

    if (response.meta.status == "success") {
      if (response.prepared) {
        if (ctx_.has_value()) {
          ctx_->cache.put(statement, response.prepared.value());
        }
      } else if (extract_encoded_plan_) {
        extract_encoded_plan_ = false;
        if (response.rows.size() == 1) {
          tao::json::value row{};
          try {
            row = utils::json::parse(response.rows[0]);
          } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = errc::common::parsing_failure;
            return response;
          }
          auto* plan = row.find("encoded_plan");
          auto* name = row.find("name");
          if (plan != nullptr && name != nullptr) {
            if (ctx_) {
              ctx_->cache.put(statement, name->get_string(), plan->get_string());
            }
            throw couchbase::core::priv::retry_http_request{};
          }
          response.ctx.ec = errc::query::prepared_statement_failure;

        } else {
          response.ctx.ec = errc::query::prepared_statement_failure;
        }
      }
    } else {
      if (response.meta.errors && !response.meta.errors->empty()) {
        response.ctx.first_error_code = response.meta.errors->front().code;
        response.ctx.first_error_message = response.meta.errors->front().message;

        // Prepared-statement codes that require cache eviction + retry must be handled
        // before delegating to the pure map_query_error classifier.
        switch (response.ctx.first_error_code) {
          case 4040: /* IKey: "plan.build_prepared.no_such_name" */
          case 4050: /* IKey: "plan.build_prepared.unrecognized_prepared" */
          case 4070: /* IKey: "plan.build_prepared.decoding" */
            if (ctx_.has_value()) {
              ctx_->cache.erase(statement);
            }
            throw couchbase::core::priv::retry_http_request{};
          default:
            break;
        }

        response.ctx.ec = map_query_error(response.meta);
      }
      if (!response.ctx.ec) {
        CB_LOG_TRACE("Unexpected error returned by query engine: client_context_id=\"{}\", body={}",
                     response.ctx.client_context_id,
                     encoded.body.data());
        response.ctx.ec = errc::common::internal_server_failure;
      }
    }
  }
  return response;
}
} // namespace couchbase::core::operations
