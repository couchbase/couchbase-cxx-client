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

#include <opentelemetry/version.h>

OPENTELEMETRY_BEGIN_NAMESPACE
namespace trace
{
class Tracer;
class Span;
} // namespace trace
OPENTELEMETRY_END_NAMESPACE

namespace couchbase::core::tracing
{

class otel_request_span : public couchbase::tracing::request_span
{
public:
  static auto wrap(std::shared_ptr<opentelemetry::trace::Span> span)
    -> std::shared_ptr<otel_request_span>;

  explicit otel_request_span(std::shared_ptr<opentelemetry::trace::Span> span);
  ~otel_request_span() override;
  otel_request_span(const otel_request_span&) = delete;
  otel_request_span(otel_request_span&&) = delete;
  auto operator=(const otel_request_span&) -> otel_request_span& = delete;
  auto operator=(otel_request_span&&) -> otel_request_span& = delete;

  void add_tag(const std::string& name, const std::string& value) override;
  void add_tag(const std::string& name, std::uint64_t value) override;
  void end() override;
  auto wrapped_span() -> std::shared_ptr<opentelemetry::trace::Span>;

private:
  std::shared_ptr<opentelemetry::trace::Span> span_;
};

class otel_request_tracer_impl;

class otel_request_tracer : public couchbase::tracing::request_tracer
{
public:
  static auto wrap(std::shared_ptr<opentelemetry::trace::Tracer> tracer)
    -> std::shared_ptr<otel_request_tracer>;

  otel_request_tracer();
  explicit otel_request_tracer(std::shared_ptr<opentelemetry::trace::Tracer> tracer);
  otel_request_tracer(const otel_request_tracer&) = delete;
  otel_request_tracer(otel_request_tracer&&) noexcept = default;
  auto operator=(const otel_request_tracer&) = delete;
  auto operator=(otel_request_tracer&&) -> otel_request_tracer& = default;
  ~otel_request_tracer() override;

  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override;

private:
  std::unique_ptr<otel_request_tracer_impl> impl_;
};
} // namespace couchbase::core::tracing
