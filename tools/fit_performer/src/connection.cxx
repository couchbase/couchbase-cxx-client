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

#include "connection.hxx"

#include "observability.top.grpc.pb.h"
#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
#include "observability/otel.hxx"
#endif

#include "core/meta/features.hxx"

#include <couchbase/tracing/request_tracer.hxx>
#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
#include <couchbase/tracing/otel_tracer.hxx>
#ifdef COUCHBASE_CXX_CLIENT_OTEL_METER_USES_GA_METRICS_API
#include <couchbase/metrics/otel_meter.hxx>
#endif
#endif

#include <chrono>
#include <memory>
#include <utility>

namespace fit_cxx
{
void
Connection::configure_observability(const protocol::observability::Config& config,
                                    couchbase::cluster_options& opts)
{
  if (config.use_noop_tracer()) {
    opts.tracing().tracer(nullptr);
    opts.tracing().enable(false);
  }

  if (config.has_threshold_logging_tracer()) {
    const auto& t = config.threshold_logging_tracer();
    if (t.has_emit_interval_millis()) {
      opts.tracing().threshold_emit_interval(std::chrono::milliseconds(t.emit_interval_millis()));
    }
    if (t.has_kv_threshold_millis()) {
      opts.tracing().key_value_threshold(std::chrono::milliseconds(t.kv_threshold_millis()));
    }
    if (t.has_query_threshold_millis()) {
      opts.tracing().query_threshold(std::chrono::milliseconds(t.query_threshold_millis()));
    }
    if (t.has_views_threshold_millis()) {
      opts.tracing().view_threshold(std::chrono::milliseconds(t.views_threshold_millis()));
    }
    if (t.has_search_threshold_millis()) {
      opts.tracing().search_threshold(std::chrono::milliseconds(t.search_threshold_millis()));
    }
    if (t.has_analytics_threshold_millis()) {
      opts.tracing().analytics_threshold(std::chrono::milliseconds(t.analytics_threshold_millis()));
    }
    if (t.has_transactions_threshold_millis()) {
      spdlog::warn("transactions_threshold_millis not supported - ignoring");
    }
    if (t.has_sample_size()) {
      opts.tracing().threshold_sample_size(static_cast<std::size_t>(t.sample_size()));
    }
    if (t.has_enabled()) {
      opts.tracing().enable(t.enabled());
      opts.tracing().tracer(nullptr);
    }
  }

  if (config.has_logging_meter()) {
    const auto& m = config.logging_meter();
    if (m.has_emit_interval_millis()) {
      opts.metrics().emit_interval(std::chrono::milliseconds(m.emit_interval_millis()));
    }
    if (m.has_enabled()) {
      opts.metrics().enable(m.enabled());
      opts.metrics().meter(nullptr);
    }
  }

  if (config.has_orphan_response()) {
    const auto& o = config.orphan_response();
    if (o.has_emit_interval_millis()) {
      opts.tracing().orphaned_emit_interval(std::chrono::milliseconds(o.emit_interval_millis()));
    }
    if (o.has_sample_size()) {
      opts.tracing().orphaned_sample_size(static_cast<std::size_t>(o.sample_size()));
    }
  }

#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
  if (config.has_tracing()) {
    otel_tracer_provider_ = observability::otel::create_tracer_provider(config.tracing());
    auto otel_tracer = otel_tracer_provider_->GetTracer("com.couchbase.client/cxx");
    tracer_ = std::make_shared<couchbase::tracing::otel_request_tracer>(std::move(otel_tracer));
    opts.tracing().tracer(tracer_);
  }

#ifdef COUCHBASE_CXX_CLIENT_OTEL_METER_USES_GA_METRICS_API
  if (config.has_metrics()) {
    otel_meter_provider_ = observability::otel::create_meter_provider(config.metrics());
    auto otel_meter = otel_meter_provider_->GetMeter("com.couchbase.client/cxx");
    meter_ = std::make_shared<couchbase::metrics::otel_meter>(std::move(otel_meter));
    opts.metrics().meter(meter_);
  }
#endif
#endif
}

auto
Connection::tracer() -> const std::shared_ptr<couchbase::tracing::request_tracer>&
{
  return tracer_;
}

auto
Connection::create_certificate_authenticator(
  const protocol::shared::Authenticator_CertificateAuthenticator& auth)
  -> couchbase::certificate_authenticator
{
  const auto cert_path = create_temporary_file("cert", auth.cert());
  const auto key_path = create_temporary_file("key", auth.key());

  spdlog::info("Creating certificate authenticator (cert_path={}, key_path={})",
               cert_path.string(),
               key_path.string());

  return couchbase::certificate_authenticator{
    cert_path.string(),
    key_path.string(),
  };
}

auto
Connection::create_temporary_file(const std::string& id, const std::string& content)
  -> std::filesystem::path
{
  const auto temp_dir = std::filesystem::temp_directory_path();
  const auto temp_file_path =
    temp_dir / fmt::format("fit_cxx_temp_file_{}_{}",
                           id,
                           std::chrono::steady_clock::now().time_since_epoch().count());
  ;
  std::ofstream out(temp_file_path);
  out << content << std::endl;
  out.close();
  temp_files_.emplace_back(temp_file_path);
  return temp_file_path;
}

void
Connection::clear_temporary_files()
{
  if (!temp_files_.empty()) {
    spdlog::trace("removing temporary files");
  }
  for (const auto& file_path : temp_files_) {
    std::error_code ec;
    std::filesystem::remove(file_path, ec);
    if (ec) {
      spdlog::warn("failed to remove temporary file {}: {}", file_path.string(), ec.message());
    }
  }
  temp_files_.clear();
}
} // namespace fit_cxx
