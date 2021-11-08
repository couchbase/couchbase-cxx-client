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

#include <chrono>
#include <queue>

#include <tao/json/forward.hpp>

#include <asio/steady_timer.hpp>

#include <couchbase/platform/uuid.h>

#include <couchbase/meta/version.hxx>

#include <couchbase/logger/logger.hxx>

#include <couchbase/tracing/threshold_logging_tracer.hxx>

#include <couchbase/utils/json.hxx>

namespace couchbase::tracing
{
struct reported_span {
    std::chrono::milliseconds duration;
    tao::json::value payload;

    bool operator<(const reported_span& other) const
    {
        return duration < other.duration;
    }
};

class threshold_logging_span : public request_span
{
  private:
    std::chrono::system_clock::time_point start_{ std::chrono::system_clock::now() };
    std::string id_{ uuid::to_string(uuid::random()) };
    std::map<std::string, std::uint64_t> integer_tags_{};
    std::map<std::string, std::string> string_tags_{
        { attributes::system, "couchbase" },
        { attributes::span_kind, "client" },
        { attributes::component, couchbase::meta::sdk_id() },
    };
    std::chrono::milliseconds duration_{ 0 };
    std::uint64_t last_server_duration_us_{ 0 };
    std::uint64_t total_server_duration_us_{ 0 };

    std::string name_;
    threshold_logging_tracer* tracer_;

  public:
    threshold_logging_span(const std::string& name, threshold_logging_tracer* tracer)
      : request_span(name)
      , tracer_{ tracer }
    {
    }

    void add_tag(const std::string& name, std::uint64_t value) override
    {
        if (name == tracing::attributes::server_duration) {
            last_server_duration_us_ = value;
            total_server_duration_us_ += value;
        }
        integer_tags_.try_emplace(name, value);
    }

    void add_tag(const std::string& name, const std::string& value) override
    {
        string_tags_.try_emplace(name, value);
    }

    void end() override;

    [[nodiscard]] const auto& string_tags() const
    {
        return string_tags_;
    }

    [[nodiscard]] std::chrono::milliseconds duration() const
    {
        return duration_;
    }

    [[nodiscard]] std::uint64_t last_server_duration_us() const
    {
        return last_server_duration_us_;
    }

    [[nodiscard]] std::uint64_t total_server_duration_us() const
    {
        return total_server_duration_us_;
    }

    [[nodiscard]] bool orphan() const
    {
        return string_tags_.find(tracing::attributes::orphan) != string_tags_.end();
    }

    [[nodiscard]] bool is_key_value() const
    {
        auto service_tag = string_tags_.find(tracing::attributes::service);
        if (service_tag == string_tags_.end()) {
            return false;
        }
        return service_tag->second == tracing::service::key_value;
    }

    [[nodiscard]] std::optional<service_type> service() const
    {
        auto service_tag = string_tags_.find(tracing::attributes::service);
        if (service_tag == string_tags_.end()) {
            return {};
        }
        const auto& service_name = service_tag->second;
        if (service_name == tracing::service::key_value) {
            return service_type::key_value;
        }
        if (service_name == tracing::service::query) {
            return service_type::query;
        }
        if (service_name == tracing::service::view) {
            return service_type::view;
        }
        if (service_name == tracing::service::search) {
            return service_type::search;
        }
        if (service_name == tracing::service::analytics) {
            return service_type::analytics;
        }
        if (service_name == tracing::service::management) {
            return service_type::management;
        }
        return {};
    }
};

template<typename T>
class fixed_queue : private std::priority_queue<T>
{
  private:
    std::size_t capacity_{};

  public:
    explicit fixed_queue(std::size_t capacity)
      : std::priority_queue<T>()
      , capacity_(capacity)
    {
    }

    using std::priority_queue<T>::pop;
    using std::priority_queue<T>::size;
    using std::priority_queue<T>::empty;
    using std::priority_queue<T>::top;

    void emplace(const T&& item)
    {
        std::priority_queue<T>::emplace(item);
        if (size() > capacity_) {
            pop();
        }
    }
};

using fixed_span_queue = fixed_queue<reported_span>;

reported_span
convert(threshold_logging_span* span)
{
    tao::json::value entry{ { "operation_name", span->name() },
                            { "total_duration_us", std::chrono::duration_cast<std::chrono::microseconds>(span->duration()).count() } };
    if (span->is_key_value()) {
        entry["last_server_duration_us"] = span->last_server_duration_us();
        entry["total_server_duration_us"] = span->total_server_duration_us();
    }

    const auto& tags = span->string_tags();
    auto pair = tags.find(attributes::operation_id);
    if (pair != tags.end()) {
        entry["last_operation_id"] = pair->second;
    }

    pair = tags.find(attributes::local_id);
    if (pair != tags.end()) {
        entry["last_local_id"] = pair->second;
    }

    pair = tags.find(attributes::local_socket);
    if (pair != tags.end()) {
        entry["last_local_socket"] = pair->second;
    }

    pair = tags.find(attributes::remote_socket);
    if (pair != tags.end()) {
        entry["last_remote_socket"] = pair->second;
    }

    return { span->duration(), std::move(entry) };
}

class threshold_logging_tracer_impl
{
  public:
    threshold_logging_tracer_impl(const threshold_logging_options& options, asio::io_context& ctx)
      : options_(options)
      , emit_orphan_report_(ctx)
      , emit_threshold_report_(ctx)
      , orphan_queue_{ options.orphaned_sample_size }
    {
        threshold_queues_.try_emplace(service_type::key_value, options.threshold_sample_size);
        threshold_queues_.try_emplace(service_type::query, options.threshold_sample_size);
        threshold_queues_.try_emplace(service_type::view, options.threshold_sample_size);
        threshold_queues_.try_emplace(service_type::search, options.threshold_sample_size);
        threshold_queues_.try_emplace(service_type::analytics, options.threshold_sample_size);
        threshold_queues_.try_emplace(service_type::management, options.threshold_sample_size);
    }

    ~threshold_logging_tracer_impl()
    {
        emit_orphan_report_.cancel();
        emit_threshold_report_.cancel();

        log_orphan_report();
        log_threshold_report();
    }

    void start()
    {
        rearm_orphan_reporter();
        rearm_threshold_reporter();
    }

    void add_orphan(threshold_logging_span* span)
    {
        orphan_queue_.emplace(convert(span));
    }

    void check_threshold(threshold_logging_span* span)
    {
        auto service = span->service();
        if (!service.has_value()) {
            return;
        }
        if (span->duration() > options_.threshold_for_service(service.value())) {
            auto queue = threshold_queues_.find(service.value());
            if (queue != threshold_queues_.end()) {
                queue->second.emplace(convert(span));
            }
        }
    }

  private:
    void rearm_orphan_reporter()
    {
        emit_orphan_report_.expires_after(options_.orphaned_emit_interval);
        emit_orphan_report_.async_wait([this](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            log_orphan_report();
            rearm_orphan_reporter();
        });
    }

    void rearm_threshold_reporter()
    {
        emit_threshold_report_.expires_after(options_.threshold_emit_interval);
        emit_threshold_report_.async_wait([this](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            log_threshold_report();
            rearm_threshold_reporter();
        });
    }

    void log_orphan_report()
    {
        if (orphan_queue_.empty()) {
            return;
        }
        auto queue = fixed_span_queue(options_.orphaned_sample_size);
        std::swap(orphan_queue_, queue);
        tao::json::value report
        {
            { "count", queue.size() },
#if BACKEND_DEBUG_BUILD
              { "emit_interval_ms", options_.orphaned_emit_interval.count() }, { "sample_size", options_.orphaned_sample_size },
#endif
        };
        tao::json::value entries = tao::json::empty_array;
        while (!queue.empty()) {
            entries.emplace_back(queue.top().payload);
            queue.pop();
        }
        report["top"] = entries;
        LOG_WARNING("Orphan responses observed: {}", utils::json::generate(report));
    }

    void log_threshold_report()
    {
        for (auto& [service, threshold_queue] : threshold_queues_) {
            if (threshold_queue.empty()) {
                continue;
            }
            auto queue = fixed_span_queue(options_.threshold_sample_size);
            std::swap(threshold_queue, queue);
            tao::json::value report
            {
                { "count", queue.size() }, { "service", fmt::format("{}", service) },
#if BACKEND_DEBUG_BUILD
                  { "emit_interval_ms", options_.threshold_emit_interval.count() }, { "sample_size", options_.threshold_sample_size },
                  { "threshold_ms",
                    std::chrono::duration_cast<std::chrono::microseconds>(options_.threshold_for_service(service)).count() },
#endif
            };
            tao::json::value entries = tao::json::empty_array;
            while (!queue.empty()) {
                entries.emplace_back(queue.top().payload);
                queue.pop();
            }
            report["top"] = entries;
            LOG_WARNING("Operations over threshold: {}", utils::json::generate(report));
        }
    }

    const threshold_logging_options& options_;

    asio::steady_timer emit_orphan_report_;
    asio::steady_timer emit_threshold_report_;
    fixed_span_queue orphan_queue_;
    std::map<service_type, fixed_span_queue> threshold_queues_{};
};

request_span*
threshold_logging_tracer::start_span(std::string name, request_span*)
{
    return new threshold_logging_span(name, this);
}

void
threshold_logging_tracer::report(threshold_logging_span* span)
{
    if (span->orphan()) {
        impl_->add_orphan(span);
    } else {
        impl_->check_threshold(span);
    }
}

threshold_logging_tracer::threshold_logging_tracer(asio::io_context& ctx, threshold_logging_options options)
  : options_{ options }
{
    impl_ = std::make_shared<threshold_logging_tracer_impl>(options_, ctx);
}

void
threshold_logging_tracer::start()
{
    impl_->start();
}

void
threshold_logging_span::end()
{
    duration_ = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start_);
    tracer_->report(this);
    delete this;
}

} // namespace couchbase::tracing
