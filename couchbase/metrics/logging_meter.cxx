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

#include <couchbase/metrics/logging_meter.hxx>

#include <couchbase/logger/logger.hxx>
#include <couchbase/metrics/noop_meter.hxx>
#include <couchbase/utils/json.hxx>

#include <gsl/assert>
#include <hdr_histogram.h>
#include <memory>
#include <utility>

namespace couchbase::metrics
{
class logging_value_recorder : public value_recorder
{
  private:
    std::string name_;
    std::map<std::string, std::string> tags_;
    hdr_histogram* histogram_{ nullptr };

    void initialize_histogram()
    {
        histogram_ = nullptr;
        hdr_init(/* minimum - 1 ns*/ 1,
                 /* maximum - 30 s*/ 30'000'000'000LL,
                 /* significant figures */ 3,
                 /* pointer */ &histogram_);
        Expects(histogram_ != nullptr);
    }

  public:
    logging_value_recorder(std::string name, std::map<std::string, std::string> tags)
      : value_recorder()
      , name_(std::move(name))
      , tags_(std::move(tags))
    {
        initialize_histogram();
    }

    logging_value_recorder(const logging_value_recorder& other)
      : value_recorder()
      , name_(other.name_)
      , tags_(other.tags_)
    {
        initialize_histogram();
    }

    logging_value_recorder(logging_value_recorder&& other) noexcept
      : value_recorder()
      , name_(std::move(other.name_))
      , tags_(std::move(other.tags_))
    {
        initialize_histogram();
    }

    logging_value_recorder& operator=(const logging_value_recorder& other)
    {
        if (this == &other) {
            return *this;
        }
        name_ = other.name_;
        tags_ = other.tags_;
        initialize_histogram();
        return *this;
    }

    logging_value_recorder& operator=(logging_value_recorder&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        name_ = std::move(other.name_);
        tags_ = std::move(other.tags_);
        initialize_histogram();
        return *this;
    }

    ~logging_value_recorder() override
    {
        if (histogram_ != nullptr) {
            hdr_close(histogram_);
            histogram_ = nullptr;
        }
    }

    void record_value(std::int64_t value) override
    {
        if (histogram_ == nullptr) {
            return;
        }
        hdr_record_value_atomic(histogram_, value);
    }

    [[nodiscard]] tao::json::value emit() const
    {
        auto total_count = histogram_->total_count;
        auto val_50_0 = hdr_value_at_percentile(histogram_, 50.0);
        auto val_90_0 = hdr_value_at_percentile(histogram_, 90.0);
        auto val_99_0 = hdr_value_at_percentile(histogram_, 99.0);
        auto val_99_9 = hdr_value_at_percentile(histogram_, 99.9);
        auto val_100_0 = hdr_value_at_percentile(histogram_, 100.0);

        hdr_reset(histogram_);

        return {
            { "total_count", total_count },
            { "percentiles_us",
              {
                { "50.0", val_50_0 },
                { "90.0", val_90_0 },
                { "99.0", val_99_0 },
                { "99.9", val_99_9 },
                { "100.0", val_100_0 },
              } },
        };
    }
};

void
logging_meter::log_report() const
{
    tao::json::value report{
        {
          "meta",
          {

            { "emit_interval_s", std::chrono::duration_cast<std::chrono::seconds>(options_.emit_interval).count() },
#if BACKEND_DEBUG_BUILD
            { "emit_interval_ms", options_.emit_interval.count() },
#endif
          },
        },
    };
    for (const auto& [service, operations] : recorders_) {
        for (const auto& [operation, recorder] : operations) {
            report["operations"][service][operation] = recorder->emit();
        }
    }
    if (report.find("operations") != nullptr) {
        LOG_INFO("Metrics: {}", utils::json::generate(report));
    }
}

std::shared_ptr<value_recorder>
logging_meter::get_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags)
{
    static std::shared_ptr<noop_value_recorder> noop_recorder{ std::make_shared<noop_value_recorder>() };

    if (static std::string meter_name = "db.couchbase.operations"; name != meter_name) {
        return noop_recorder;
    }

    static std::string service_tag = "db.couchbase.service";
    auto service = tags.find(service_tag);
    if (service == tags.end()) {
        return noop_recorder;
    }

    static std::string operation_tag = "db.operation";
    auto operation = tags.find(operation_tag);
    if (operation == tags.end()) {
        return noop_recorder;
    }

    std::scoped_lock lock(recorders_mutex_);
    auto& service_recorders = recorders_[service->second];

    auto recorder = service_recorders.find(operation->second);
    if (recorder != service_recorders.end()) {
        return recorder->second;
    }

    service_recorders.try_emplace(operation->second, std::make_shared<logging_value_recorder>(operation->second, tags));
    recorder = service_recorders.find(operation->second);
    return recorder->second;
}
} // namespace couchbase::metrics
