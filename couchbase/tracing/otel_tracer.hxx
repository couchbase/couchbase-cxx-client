/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include <couchbase/tracing/request_tracer.hxx>

#include <opentelemetry/trace/tracer.h>

namespace couchbase::tracing
{
class otel_request_span : public couchbase::tracing::request_span
{
public:
  explicit otel_request_span(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span)
    : span_(std::move(span))
  {
  }
  void add_tag(const std::string& name, const std::string& value) override
  {
    span_->SetAttribute(name, value);
  }
  void add_tag(const std::string& name, uint64_t value) override
  {
    span_->SetAttribute(name, value);
  }
  void end() override
  {
    span_->End();
  }
  auto wrapped_span() -> opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
  {
    return span_;
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
};

class otel_request_tracer : public couchbase::tracing::request_tracer
{
public:
  explicit otel_request_tracer(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer)
    : tracer_(std::move(tracer))
  {
  }

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    const auto wrapped_parent = std::dynamic_pointer_cast<otel_request_span>(parent);
    opentelemetry::trace::StartSpanOptions opts;
    opts.kind = opentelemetry::trace::SpanKind::kClient;
    if (wrapped_parent) {
      opts.parent = wrapped_parent->wrapped_span()->GetContext();
    }
    return std::make_shared<otel_request_span>(tracer_->StartSpan(name, opts));
  }

  auto wrap_span(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span)
    -> std::shared_ptr<couchbase::tracing::otel_request_span>
  {
    return std::make_shared<couchbase::tracing::otel_request_span>(std::move(span));
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
};
} // namespace couchbase::tracing
