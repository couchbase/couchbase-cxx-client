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

#include <couchbase/metrics/meter.hxx>

#include <opentelemetry/context/context.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/sync_instruments.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <variant>

namespace couchbase::metrics
{
template<typename T>
class otel_value_recorder : public couchbase::metrics::value_recorder
{
public:
  explicit otel_value_recorder(
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<T>> histogram,
    const std::map<std::string, std::string>& tags)
    : histogram_{ std::move(histogram) }
    , tags_{ tags }
  {
    tags_.erase("__unit");
  }

  void record_value(std::int64_t value) override
  {
    if constexpr (std::is_same_v<T, double>) {
      auto value_in_seconds = static_cast<double>(value) / 1'000'000.0;
      histogram_->Record(
        value_in_seconds, opentelemetry::common::KeyValueIterableView{ tags_ }, context_);
    } else {
      value = std::max<int64_t>(value, 0);
      auto uvalue = static_cast<std::uint64_t>(value);
      histogram_->Record(uvalue, opentelemetry::common::KeyValueIterableView{ tags_ }, context_);
    }
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<T>> histogram_;
  std::map<std::string, std::string> tags_{};
  opentelemetry::context::Context context_{};
  std::mutex mutex_;
};

class otel_meter : public couchbase::metrics::meter
{
public:
  explicit otel_meter(opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> meter)
    : meter_{ std::move(meter) }
  {
  }

  auto get_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags)
    -> std::shared_ptr<value_recorder> override
  {
    bool in_seconds{ false };
    if (tags.count("__unit") > 0) {
      if (tags.at("__unit") == "s") {
        in_seconds = true;
      }
    }

    {
      // Check if we already have the histogram
      std::shared_lock lock(mutex_);
      if (in_seconds) {
        if (const auto it = double_histograms_.find(name); it != double_histograms_.end()) {
          return std::make_shared<otel_value_recorder<double>>(it->second, tags);
        }
      } else {
        if (const auto it = uint_histograms_.find(name); it != uint_histograms_.end()) {
          return std::make_shared<otel_value_recorder<std::uint64_t>>(it->second, tags);
        }
      }
    }

    {
      // We have to check if we already have the histogram again, before creating it, in case
      // another thread created it while we were waiting for  the exclusive lock
      std::scoped_lock lock(mutex_);
      if (in_seconds) {
        if (const auto it = double_histograms_.find(name); it != double_histograms_.end()) {
          return std::make_shared<otel_value_recorder<double>>(it->second, tags);
        }
        // Not found, we have to create it
        auto histogram = meter_->CreateDoubleHistogram(name, "", "s");
        double_histograms_.emplace(name, std::move(histogram));
        return std::make_shared<otel_value_recorder<double>>(double_histograms_.at(name), tags);
      } else {
        if (const auto it = uint_histograms_.find(name); it != uint_histograms_.end()) {
          return std::make_shared<otel_value_recorder<std::uint64_t>>(it->second, tags);
        }
        // Not found, we have to create it
        auto histogram = meter_->CreateUInt64Histogram(name);
        uint_histograms_.emplace(name, std::move(histogram));
        return std::make_shared<otel_value_recorder<std::uint64_t>>(uint_histograms_.at(name),
                                                                    tags);
      }
    }
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> meter_;
  std::shared_mutex mutex_;
  std::map<std::string, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>>
    double_histograms_;
  std::map<std::string,
           opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>>
    uint_histograms_;
};
} // namespace couchbase::metrics
