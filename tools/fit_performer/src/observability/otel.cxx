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

#include "otel.hxx"

#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY

#include "observability.config.pb.h"
#include "observability.top.pb.h"

#include <google/protobuf/map.h>
#include <spdlog/spdlog.h>

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/view_registry_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/sampler.h>
#include <opentelemetry/sdk/trace/samplers/always_off_factory.h>
#include <opentelemetry/sdk/trace/samplers/always_on_factory.h>
#include <opentelemetry/sdk/trace/samplers/trace_id_ratio_factory.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_context_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <chrono>
#include <memory>
#include <string>
#include <utility>

namespace fit_cxx::observability::otel
{
namespace
{
auto
create_otel_resource(
  const google::protobuf::Map<std::string, protocol::observability::Attribute>& resources)
{
  auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes{};
  for (const auto& [k, v] : resources) {
    switch (v.value_case()) {
      case protocol::observability::Attribute::ValueCase::kValueString:
        resource_attributes.SetAttribute(k, v.value_string());
        break;
      case protocol::observability::Attribute::ValueCase::kValueLong:
        resource_attributes.SetAttribute(k, v.value_long());
        break;
      case protocol::observability::Attribute::ValueCase::kValueBoolean:
        resource_attributes.SetAttribute(k, v.value_boolean());
        break;
      default:
        spdlog::warn("Unexpected OpenTelemetry resource attribute value: {}", v.DebugString());
        break;
    }
  }
  return opentelemetry::sdk::resource::Resource::Create(resource_attributes);
}
} // namespace

auto
create_tracer_provider(const protocol::observability::TracingConfig& cfg)
  -> std::unique_ptr<opentelemetry::sdk::trace::TracerProvider>
{
  opentelemetry::exporter::otlp::OtlpHttpExporterOptions exporter_opts{};
  exporter_opts.url = cfg.endpoint_hostname();

  auto exporter = opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(exporter_opts);

  std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor;
  if (cfg.batching()) {
    opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_opts{};
    processor_opts.schedule_delay_millis = std::chrono::milliseconds(cfg.export_every_millis());
    processor = opentelemetry::sdk::trace::BatchSpanProcessorFactory::Create(std::move(exporter),
                                                                             processor_opts);
  } else {
    processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  }

  auto resource = create_otel_resource(cfg.resources());

  const float epsilon{ 0.00001f };
  std::unique_ptr<opentelemetry::sdk::trace::Sampler> sampler;
  if (static_cast<double>(cfg.sampling_percentage()) < static_cast<double>(epsilon)) {
    sampler = opentelemetry::sdk::trace::AlwaysOffSamplerFactory::Create();
  } else if (static_cast<double>(cfg.sampling_percentage()) > 1.0 - static_cast<double>(epsilon)) {
    sampler = opentelemetry::sdk::trace::AlwaysOnSamplerFactory::Create();
  } else {
    sampler =
      opentelemetry::sdk::trace::TraceIdRatioBasedSamplerFactory::Create(cfg.sampling_percentage());
  }

  auto tracer_context =
    opentelemetry::sdk::trace::TracerContextFactory::Create({}, resource, std::move(sampler));
  tracer_context->AddProcessor(std::move(processor));

  return opentelemetry::sdk::trace::TracerProviderFactory::Create(std::move(tracer_context));
}

auto
create_meter_provider(const protocol::observability::MetricsConfig& cfg)
  -> std::unique_ptr<opentelemetry::sdk::metrics::MeterProvider>
{
  opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions exporter_opts{};
  exporter_opts.url = cfg.endpoint_hostname();

  auto exporter =
    opentelemetry::exporter::otlp::OtlpHttpMetricExporterFactory::Create(exporter_opts);

  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_opts{};
  reader_opts.export_interval_millis = std::chrono::milliseconds(cfg.export_every_millis());
  reader_opts.export_timeout_millis = std::chrono::milliseconds(cfg.export_every_millis() / 2);
  auto reader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
    std::move(exporter), reader_opts);

  auto resource = create_otel_resource(cfg.resources());
  auto view_registry = opentelemetry::sdk::metrics::ViewRegistryFactory::Create();

  auto meter_context =
    opentelemetry::sdk::metrics::MeterContextFactory::Create(std::move(view_registry), resource);
  meter_context->AddMetricReader(std::move(reader));

  return opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(meter_context));
}
} // namespace fit_cxx::observability::otel

#endif // COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
