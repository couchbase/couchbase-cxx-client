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

#include "observability_recorder.hxx"
#include "core/tracing/attribute_helpers.hxx"

namespace couchbase::core::impl
{
auto
observability_recorder::create(std::string op_name,
                               std::shared_ptr<couchbase::tracing::request_span> parent_span,
                               std::weak_ptr<tracing::tracer_wrapper> tracer,
                               std::weak_ptr<metrics::meter_wrapper> meter)
  -> std::unique_ptr<observability_recorder>
{
  auto rec = std::make_unique<observability_recorder>(
    std::move(op_name), std::move(parent_span), std::move(tracer), std::move(meter));
  if (rec->span_->uses_tags()) {
    rec->span_->add_tag(tracing::attributes::op::operation_name, rec->op_name_);
  }
  rec->metric_attributes_.operation = rec->op_name_;
  return rec;
}

auto
observability_recorder::operation_span() -> const std::shared_ptr<couchbase::tracing::request_span>&
{
  return span_;
}

void
observability_recorder::finish(const std::error_code ec)
{
  metric_attributes_.ec = ec;
  meter_.lock()->record_value(std::move(metric_attributes_), start_time_);
  span_->end();
}

void
observability_recorder::finish(const std::size_t retry_attempts, const std::error_code ec)
{
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::retry_count, retry_attempts);
  }
  finish(ec);
}

auto
observability_recorder::create_request_encoding_span() const
  -> std::shared_ptr<couchbase::tracing::request_span>
{
  return tracer_.lock()->create_span(tracing::operation::step_request_encoding, span_);
}

auto
observability_recorder::record_suboperation(std::string subop_name) const
  -> std::unique_ptr<observability_recorder>
{
  return create(std::move(subop_name), span_, tracer_, meter_);
}

void
observability_recorder::with_service(const std::string& service)
{
  metric_attributes_.service = service;
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::service, service);
  }
}

void
observability_recorder::with_collection_name(const std::string& collection_name)

{
  metric_attributes_.collection_name = collection_name;
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::collection_name, collection_name);
  }
}

void
observability_recorder::with_scope_name(const std::string& scope_name)
{
  metric_attributes_.scope_name = scope_name;
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::scope_name, scope_name);
  }
}

void
observability_recorder::with_bucket_name(const std::string& bucket_name)
{
  metric_attributes_.bucket_name = bucket_name;
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::bucket_name, bucket_name);
  }
}

void
observability_recorder::with_durability(const couchbase::durability_level durability)

{
  if (span_->uses_tags()) {
    tracing::set_durability_level_attribute(span_, durability);
  }
}

void
observability_recorder::with_query_statement(const std::string& statement,
                                             const query_options::built& query_options)

{
  if (query_options.positional_parameters.empty() && query_options.named_parameters.empty()) {
    return;
  }
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::query_statement, statement);
  }
}

void
observability_recorder::with_query_statement(const std::string& statement,
                                             const analytics_options::built& analytics_options)

{
  if (analytics_options.positional_parameters.empty() &&
      analytics_options.named_parameters.empty()) {
    return;
  }
  if (span_->uses_tags()) {
    span_->add_tag(tracing::attributes::op::query_statement, statement);
  }
}

observability_recorder::observability_recorder(
  std::string op_name,
  std::shared_ptr<couchbase::tracing::request_span> parent_span,
  std::weak_ptr<tracing::tracer_wrapper> tracer,
  std::weak_ptr<metrics::meter_wrapper> meter)
  : op_name_{ std::move(op_name) }
  , tracer_{ std::move(tracer) }
  , meter_{ std::move(meter) }
  , span_{ tracer_.lock()->create_span(op_name_, std::move(parent_span)) }
  , start_time_{ std::chrono::steady_clock::now() }
{
}
} // namespace couchbase::core::impl
