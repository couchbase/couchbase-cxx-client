/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY

#include "observability.config.pb.h"

#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>

#include <memory>

namespace fit_cxx::observability::otel
{
auto
create_tracer_provider(const protocol::observability::TracingConfig& cfg)
  -> std::unique_ptr<opentelemetry::sdk::trace::TracerProvider>;

auto
create_meter_provider(const protocol::observability::MetricsConfig& cfg)
  -> std::unique_ptr<opentelemetry::sdk::metrics::MeterProvider>;
} // namespace fit_cxx::observability::otel

#endif // COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
