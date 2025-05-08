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

#include "otel_tracer.hxx"

#include "../meta/version.hxx"

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

namespace couchbase::core::tracing
{
namespace
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

  void add_tag(const std::string& name, std::uint64_t value) override
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

} // namespace

class otel_request_tracer_impl
{
  friend otel_request_tracer;

public:
  otel_request_tracer_impl()
    : tracer_{
      opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("couchbase_cxx_sdk",
                                                                     meta::sdk_semver()),
    }
  {
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
};

otel_request_tracer::otel_request_tracer()
  : impl_{ std::make_unique<otel_request_tracer_impl>() }
{
}

otel_request_tracer::~otel_request_tracer() = default;

auto
otel_request_tracer::start_span(std::string name,
                                std::shared_ptr<couchbase::tracing::request_span> parent)
  -> std::shared_ptr<couchbase::tracing::request_span>
{
  auto wrapped_parent = std::dynamic_pointer_cast<otel_request_span>(parent);
  if (wrapped_parent) {
    opentelemetry::trace::StartSpanOptions opts;
    opts.parent = wrapped_parent->wrapped_span()->GetContext();
    return std::make_shared<otel_request_span>(impl_->tracer_->StartSpan(name, opts));
  }
  return std::make_shared<otel_request_span>(impl_->tracer_->StartSpan(name));
}

} // namespace couchbase::core::tracing
