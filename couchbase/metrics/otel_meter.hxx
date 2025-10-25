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
#include <mutex>
#include <utility>

namespace couchbase::metrics
{

class otel_sync_histogram
{
public:
  explicit otel_sync_histogram(
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>
      histogram_counter)
    : histogram_counter_{ std::move(histogram_counter) }
  {
  }

  void record(std::uint64_t value,
              const opentelemetry::common::KeyValueIterable& tags,
              opentelemetry::context::Context& ctx)
  {
    histogram_counter_->Record(value, tags, ctx);
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>
    histogram_counter_;
  std::mutex mutex_;
};

class otel_value_recorder : public couchbase::metrics::value_recorder
{
public:
  explicit otel_value_recorder(
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>
      histogram_counter,
    const std::map<std::string, std::string>& tags)
    : histogram_counter_{ std::move(histogram_counter) }
    , tags_(tags)
  {
  }
  void record_value(std::int64_t value) override
  {
    value = std::max<int64_t>(value, 0);
    auto uvalue = static_cast<std::uint64_t>(value);
    histogram_counter_->Record(
      uvalue, opentelemetry::common::KeyValueIterableView<decltype(tags_)>{ tags_ }, context_);
  }

  [[nodiscard]] auto tags() const -> const std::map<std::string, std::string>&
  {
    return tags_;
  }

  auto histogram_counter()
    -> opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>
  {
    return histogram_counter_;
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<std::uint64_t>>
    histogram_counter_;
  const std::map<std::string, std::string> tags_;
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
    // first look up the histogram, in case we already have it...
    std::scoped_lock<std::mutex> lock(mutex_);
    auto it = recorders_.equal_range(name);
    if (it.first == it.second) {
      // this name isn't associated with any histogram, so make one and return it.
      // Note we'd like to make one with more buckets than default, given the range of
      // response times we'd like to display (queries vs kv for instance), but otel
      // api doesn't seem to allow this.
      return recorders_
        .insert({ name,
                  std::make_shared<otel_value_recorder>(
                    meter_->CreateUInt64Histogram(name, "", "us"), tags) })
        ->second;
    }
    // so it is already, lets see if we already have one with those tags, or need
    // to make a new one (using the histogram we already have).
    for (auto itr = it.first; itr != it.second; itr++) {
      if (tags == itr->second->tags()) {
        return itr->second;
      }
    }
    // if you are here, we need to add one with these tags and the histogram associated with the
    // name.
    return recorders_
      .insert(
        { name,
          std::make_shared<otel_value_recorder>(it.first->second->histogram_counter(), tags) })
      ->second;
  }

private:
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> meter_;
  std::mutex mutex_;
  std::multimap<std::string, std::shared_ptr<otel_value_recorder>> recorders_;
};
} // namespace couchbase::metrics
