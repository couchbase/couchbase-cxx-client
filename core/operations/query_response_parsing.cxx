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

#include "query_response_parsing.hxx"

#include "core/operations/management/error_utils.hxx"
#include "core/utils/contains_string.hxx"
#include "core/utils/duration_parser.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

namespace couchbase::core::operations
{

auto
parse_query_meta(const tao::json::value& payload) -> query_response::query_meta_data
{
  query_response::query_meta_data meta{};

  if (const auto* i = payload.find("requestID"); i != nullptr) {
    meta.request_id = i->get_string();
  }
  if (const auto* i = payload.find("clientContextID"); i != nullptr) {
    meta.client_context_id = i->get_string();
  }
  if (const auto* s = payload.find("status"); s != nullptr) {
    meta.status = s->get_string();
  }
  if (const auto* s = payload.find("signature"); s != nullptr) {
    meta.signature = couchbase::core::utils::json::generate(*s);
  }
  if (const auto* p = payload.find("profile"); p != nullptr) {
    meta.profile = couchbase::core::utils::json::generate(*p);
  }

  if (const auto* m = payload.find("metrics"); m != nullptr) {
    query_response::query_metrics metrics{};
    metrics.result_count = m->at("resultCount").get_unsigned();
    metrics.result_size = m->at("resultSize").get_unsigned();
    metrics.elapsed_time = utils::parse_duration(m->at("elapsedTime").get_string());
    metrics.execution_time = utils::parse_duration(m->at("executionTime").get_string());
    metrics.sort_count = m->template optional<std::uint64_t>("sortCount").value_or(0);
    metrics.mutation_count = m->template optional<std::uint64_t>("mutationCount").value_or(0);
    metrics.error_count = m->template optional<std::uint64_t>("errorCount").value_or(0);
    metrics.warning_count = m->template optional<std::uint64_t>("warningCount").value_or(0);
    meta.metrics.emplace(metrics);
  }

  if (const auto* e = payload.find("errors"); e != nullptr) {
    std::vector<query_response::query_problem> problems{};
    for (const auto& err : e->get_array()) {
      query_response::query_problem problem;
      problem.code = err.at("code").get_unsigned();
      problem.message = err.at("msg").get_string();
      if (const auto* reason = err.find("reason"); reason != nullptr && reason->is_object()) {
        problem.reason = reason->optional<std::uint64_t>("code");
        problem.retry = reason->optional<bool>("retry");
      }
      problems.emplace_back(problem);
    }
    meta.errors.emplace(problems);
  }

  if (const auto* w = payload.find("warnings"); w != nullptr) {
    std::vector<query_response::query_problem> problems{};
    for (const auto& warn : w->get_array()) {
      query_response::query_problem problem;
      problem.code = warn.at("code").get_unsigned();
      problem.message = warn.at("msg").get_string();
      problems.emplace_back(problem);
    }
    meta.warnings.emplace(problems);
  }

  return meta;
}

auto
map_query_error(const query_response::query_meta_data& meta) -> std::error_code
{
  if (meta.status == "success") {
    return {};
  }

  if (!meta.errors || meta.errors->empty()) {
    /* Unreachable from make_response, which only calls map_query_error after confirming
     * errors is non-empty; present for the streaming-path caller, which has no such guard. */
    return errc::common::internal_server_failure;
  }

  const auto& first = meta.errors->front();
  const auto first_code = first.code;
  const auto& first_message = first.message;

  std::error_code ec{};

  switch (first_code) {
    case 1065: /* IKey: "service.io.request.unrecognized_parameter" */
      if ((first_message.find("Unrecognized parameter in request") != std::string::npos) &&
          (first_message.find("preserve_expiry") != std::string::npos)) {
        ec = errc::common::feature_not_available;
      } else {
        ec = errc::common::invalid_argument;
      }
      break;
    case 1080: /* IKey: "timeout" */
      ec = errc::common::unambiguous_timeout;
      break;
    case 3000: /* IKey: "parse.syntax_error" */
      ec = errc::common::parsing_failure;
      break;
    case 4040: /* IKey: "plan.build_prepared.no_such_name" */
    case 4050: /* IKey: "plan.build_prepared.unrecognized_prepared" */
    case 4060: /* IKey: "plan.build_prepared.no_such_name" */
    case 4070: /* IKey: "plan.build_prepared.decoding" */
    case 4080: /* IKey: "plan.build_prepared.name_encoded_plan_mismatch" */
    case 4090: /* IKey: "plan.build_prepared.name_not_in_encoded_plan" */
      /* NOTE: make_response also does cache.erase + throw retry_http_request for codes
       * 4040/4050/4070. That stateful logic stays in make_response. This pure classifier
       * returns prepared_statement_failure so callers (e.g. streaming path) get a
       * non-empty error_code. */
      ec = errc::query::prepared_statement_failure;
      break;
    case 4300: /* IKey: "plan.new_index_already_exists" */
      ec = errc::common::index_exists;
      break;
    case 5000: /* IKey: "Internal Error" */
      if (utils::contains_string(first_message, "index", true) &&
          utils::contains_string(first_message, "already exist", true)) {
        ec = errc::common::index_exists;
      } else if (utils::contains_string(first_message, "Index does not exist") ||
                 (utils::contains_string(first_message, "index", true) &&
                  utils::contains_string(first_message, "not found", true))) {
        ec = errc::common::index_not_found;
      } else if (first_message.find("Bucket Not Found") != std::string::npos) {
        ec = errc::common::bucket_not_found;
      }
      break;
    case 12009: /* IKey: "datastore.couchbase.DML_error" */
      if (first_message.find("CAS mismatch") != std::string::npos) {
        ec = errc::common::cas_mismatch;
      } else {
        switch (first.reason.value_or(0)) {
          case 12033:
            ec = errc::common::cas_mismatch;
            break;
          case 17014:
            ec = errc::key_value::document_not_found;
            break;
          case 17012:
            ec = errc::key_value::document_exists;
            break;
          default:
            ec = errc::query::dml_failure;
            break;
        }
      }
      break;
    case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
    case 12016: /* IKey: "datastore.couchbase.index_not_found" */
      ec = errc::common::index_not_found;
      break;
    case 13014: /* IKey: "datastore.couchbase.insufficient_credentials" */
      ec = errc::common::authentication_failure;
      break;
    default:
      if ((first_code >= 12000 && first_code < 13000) ||
          (first_code >= 14000 && first_code < 15000)) {
        ec = errc::query::index_failure;
      } else if (first_code >= 4000 && first_code < 5000) {
        ec = errc::query::planning_failure;
      }
      break;
  }

  if (!ec) {
    auto common_ec = management::extract_common_query_error_code(first_code, first_message);
    if (common_ec) {
      ec = common_ec.value();
    }
  }

  if (!ec) {
    ec = errc::common::internal_server_failure;
  }

  return ec;
}

} // namespace couchbase::core::operations
