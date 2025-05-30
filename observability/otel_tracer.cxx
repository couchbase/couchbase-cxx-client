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

#include "core/meta/version.hxx"

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#endif
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace couchbase::core::tracing
{

auto
otel_request_span::wrapped_span() -> std::shared_ptr<opentelemetry::trace::Span>
{
  return span_;
}

void
otel_request_span::end()
{
  span_->End();
}

void
otel_request_span::add_tag(const std::string& name, std::uint64_t value)
{
  span_->SetAttribute(name, value);
}

void
otel_request_span::add_tag(const std::string& name, const std::string& value)
{
  span_->SetAttribute(name, value);
}

otel_request_span::otel_request_span(std::shared_ptr<opentelemetry::trace::Span> span)
  : span_(std::move(span))
{
}

otel_request_span::~otel_request_span()
{
  span_->End();
}

auto
otel_request_span::wrap(std::shared_ptr<opentelemetry::trace::Span> span)
  -> std::shared_ptr<otel_request_span>
{
  return std::make_shared<otel_request_span>(span);
}

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

  explicit otel_request_tracer_impl(std::shared_ptr<opentelemetry::trace::Tracer> tracer)
    : tracer_{ std::move(tracer) }
  {
  }

private:
  std::shared_ptr<opentelemetry::trace::Tracer> tracer_;
};

otel_request_tracer::otel_request_tracer()
  : impl_{ std::make_unique<otel_request_tracer_impl>() }
{
}

otel_request_tracer::otel_request_tracer(std::shared_ptr<opentelemetry::trace::Tracer> tracer)
  : impl_{ std::make_unique<otel_request_tracer_impl>(std::move(tracer)) }
{
}

auto
otel_request_tracer::wrap(std::shared_ptr<opentelemetry::trace::Tracer> tracer)
  -> std::shared_ptr<otel_request_tracer>
{
  return std::make_shared<couchbase::core::tracing::otel_request_tracer>(std::move(tracer));
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
    return otel_request_span::wrap(impl_->tracer_->StartSpan(name, opts));
  }
  return otel_request_span::wrap(impl_->tracer_->StartSpan(name));
}

} // namespace couchbase::core::tracing
