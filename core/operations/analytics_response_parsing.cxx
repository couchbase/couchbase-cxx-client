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

#include "analytics_response_parsing.hxx"

#include "core/utils/duration_parser.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

namespace couchbase::core::operations
{

auto
parse_analytics_meta(const tao::json::value& payload) -> analytics_response::analytics_meta_data
{
  analytics_response::analytics_meta_data meta{};

  if (const auto* i = payload.find("requestID"); i != nullptr) {
    meta.request_id = i->get_string();
  }
  if (const auto* i = payload.find("clientContextID"); i != nullptr) {
    meta.client_context_id = i->get_string();
  }

  if (const auto* s = payload.find("status"); s != nullptr && s->is_string()) {
    const auto& status = s->get_string();
    if (status == "running") {
      meta.status = analytics_response::analytics_status::running;
    } else if (status == "success") {
      meta.status = analytics_response::analytics_status::success;
    } else if (status == "errors") {
      meta.status = analytics_response::analytics_status::errors;
    } else if (status == "completed") {
      meta.status = analytics_response::analytics_status::completed;
    } else if (status == "stopped") {
      meta.status = analytics_response::analytics_status::stopped;
    } else if (status == "timedout") {
      meta.status = analytics_response::analytics_status::timedout;
    } else if (status == "closed") {
      meta.status = analytics_response::analytics_status::closed;
    } else if (status == "fatal") {
      meta.status = analytics_response::analytics_status::fatal;
    } else if (status == "aborted") {
      meta.status = analytics_response::analytics_status::aborted;
    } else {
      meta.status = analytics_response::analytics_status::unknown;
    }
  } else {
    meta.status = analytics_response::analytics_status::unknown;
  }

  if (const auto* s = payload.find("signature"); s != nullptr) {
    meta.signature = couchbase::core::utils::json::generate(*s);
  }

  if (const auto* m = payload.find("metrics"); m != nullptr) {
    meta.metrics.result_count = m->at("resultCount").get_unsigned();
    meta.metrics.result_size = m->at("resultSize").get_unsigned();
    meta.metrics.elapsed_time = utils::parse_duration(m->at("elapsedTime").get_string());
    meta.metrics.execution_time = utils::parse_duration(m->at("executionTime").get_string());
    meta.metrics.processed_objects = m->at("processedObjects").get_unsigned();
    meta.metrics.error_count = m->optional<std::uint64_t>("errorCount").value_or(0);
    meta.metrics.warning_count = m->optional<std::uint64_t>("warningCount").value_or(0);
  }

  if (const auto* e = payload.find("errors"); e != nullptr) {
    for (const auto& err : e->get_array()) {
      analytics_response::analytics_problem problem;
      problem.code = err.at("code").get_unsigned();
      problem.message = err.at("msg").get_string();
      meta.errors.emplace_back(problem);
    }
  }

  if (const auto* w = payload.find("warnings"); w != nullptr) {
    for (const auto& warn : w->get_array()) {
      analytics_response::analytics_problem problem;
      problem.code = warn.at("code").get_unsigned();
      problem.message = warn.at("msg").get_string();
      meta.warnings.emplace_back(problem);
    }
  }

  return meta;
}

auto
map_analytics_error(const analytics_response::analytics_meta_data& meta) -> std::error_code
{
  if (meta.status == analytics_response::analytics_status::success) {
    return {};
  }

  if (meta.errors.empty()) {
    /* Reachable from make_response, which calls this for any non-success status even when the
     * payload carries no "errors" array, and from the streaming-path caller, which has no such
     * guarantee either. Classify as a generic internal failure. */
    return errc::common::internal_server_failure;
  }

  const auto first_error_code = meta.errors.front().code;

  std::error_code ec{};
  switch (first_error_code) {
    case 21002: /* Request timed out and will be cancelled */
      ec = errc::common::unambiguous_timeout;
      break;
    case 23007: /* Job queue is full with [string] jobs */
      ec = errc::analytics::job_queue_full;
      break;
    case 24044: /* Cannot find dataset [string] because there is no dataverse declared, nor an
                   alias with name [string]! */
    case 24045: /* Cannot find dataset [string] in dataverse [string] nor an alias with name
                   [string]! */
    case 24025: /* Cannot find dataset with name [string] in dataverse [string] */
      ec = errc::analytics::dataset_not_found;
      break;
    case 24034: /* Cannot find dataverse with name [string] */
      ec = errc::analytics::dataverse_not_found;
      break;
    case 24040: /* A dataset with name [string] already exists in dataverse [string] */
      ec = errc::analytics::dataset_exists;
      break;
    case 24039: /* A dataverse with this name [string] already exists. */
      ec = errc::analytics::dataverse_exists;
      break;
    case 24006: /* Link [string] does not exist | Link [string] does not exist */
      ec = errc::analytics::link_not_found;
      break;
    default:
      if (first_error_code >= 24000 && first_error_code < 25000) {
        ec = errc::analytics::compilation_failure;
      }
      break;
  }

  if (!ec) {
    ec = errc::common::internal_server_failure;
  }

  return ec;
}

} // namespace couchbase::core::operations
