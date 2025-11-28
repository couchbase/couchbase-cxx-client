/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025. Couchbase, Inc.
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

#include <couchbase/analytics_options.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/tracing/request_span.hxx>

#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <memory>
#include <string>

namespace couchbase::core::impl
{
class observability_recorder
{
public:
  static auto create(std::string op_name,
                     std::shared_ptr<couchbase::tracing::request_span> parent_span,
                     std::weak_ptr<tracing::tracer_wrapper> tracer,
                     std::weak_ptr<metrics::meter_wrapper> meter)
    -> std::unique_ptr<observability_recorder>;

  [[nodiscard]] auto operation_span() -> const std::shared_ptr<couchbase::tracing::request_span>&;

  [[nodiscard]] auto create_request_encoding_span() const
    -> std::shared_ptr<couchbase::tracing::request_span>;
  [[nodiscard]] auto record_suboperation(std::string subop_name) const
    -> std::unique_ptr<observability_recorder>;

  void with_service(const std::string& service);
  void with_collection_name(const std::string& collection_name);
  void with_scope_name(const std::string& scope_name);
  void with_bucket_name(const std::string& bucket_name);
  void with_durability(couchbase::durability_level durability);
  void with_query_statement(const std::string& statement,
                            const query_options::built& query_options);
  void with_query_statement(const std::string& statement,
                            const analytics_options::built& analytics_options);

  void finish(std::error_code ec);
  void finish(std::size_t retry_attempts, std::error_code ec);

  observability_recorder(std::string op_name,
                         std::shared_ptr<couchbase::tracing::request_span> parent_span,
                         std::weak_ptr<tracing::tracer_wrapper> tracer,
                         std::weak_ptr<metrics::meter_wrapper> meter);

private:
  std::string op_name_;
  std::weak_ptr<tracing::tracer_wrapper> tracer_;
  std::weak_ptr<metrics::meter_wrapper> meter_;
  std::shared_ptr<couchbase::tracing::request_span> span_;
  std::chrono::time_point<std::chrono::steady_clock> start_time_;
  metrics::metric_attributes metric_attributes_{};
};
} // namespace couchbase::core::impl
