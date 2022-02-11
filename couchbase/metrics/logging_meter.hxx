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

#include <couchbase/metrics/logging_meter_options.hxx>
#include <couchbase/metrics/meter.hxx>

#include <asio/steady_timer.hpp>

#include <mutex>

namespace couchbase::metrics
{
class logging_value_recorder;

class logging_meter : public meter
{
  private:
    asio::steady_timer emit_report_;
    logging_meter_options options_;
    std::mutex recorders_mutex_{};
    std::map<std::string, std::map<std::string, std::shared_ptr<logging_value_recorder>>> recorders_{};

    void log_report() const;

    void rearm_reporter()
    {
        emit_report_.expires_after(options_.emit_interval);
        emit_report_.async_wait([this](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            log_report();
            rearm_reporter();
        });
    }

  public:
    logging_meter(asio::io_context& ctx, logging_meter_options options)
      : emit_report_(ctx)
      , options_(options)
    {
    }

    ~logging_meter() override
    {
        emit_report_.cancel();
        log_report();
    }

    void start()
    {
        rearm_reporter();
    }

    std::shared_ptr<value_recorder> get_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags) override;
};

} // namespace couchbase::metrics
