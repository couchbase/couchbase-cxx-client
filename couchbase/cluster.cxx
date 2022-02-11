/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/cluster.hxx>

namespace couchbase
{
class ping_collector : public std::enable_shared_from_this<ping_collector>
{
    diag::ping_result res_;
    utils::movable_function<void(diag::ping_result)> handler_;
    std::atomic_int expected_{ 0 };
    std::mutex mutex_{};

  public:
    ping_collector(std::string report_id, utils::movable_function<void(diag::ping_result)>&& handler)
      : res_{ std::move(report_id), couchbase::meta::sdk_id() }
      , handler_(std::move(handler))
    {
    }

    ~ping_collector()
    {
        invoke_handler();
    }

    [[nodiscard]] diag::ping_result& result()
    {
        return res_;
    }

    auto build_reporter()
    {
        expected_++;
        return [self = this->shared_from_this()](diag::endpoint_ping_info&& info) {
            std::scoped_lock lock(self->mutex_);
            self->res_.services[info.type].emplace_back(std::move(info));
            if (--self->expected_ == 0) {
                self->invoke_handler();
            }
        };
    }

    void invoke_handler()
    {
        if (handler_ != nullptr) {
            handler_(std::move(res_));
            handler_ = nullptr;
        }
    }
};

void
cluster::do_ping(std::optional<std::string> report_id,
                 std::optional<std::string> bucket_name,
                 std::set<service_type> services,
                 utils::movable_function<void(diag::ping_result)> handler)
{
    if (!report_id) {
        report_id = std::make_optional(uuid::to_string(uuid::random()));
    }
    if (stopped_) {
        return handler({ report_id.value(), couchbase::meta::sdk_id() });
    }
    if (services.empty()) {
        services = { service_type::key_value, service_type::view, service_type::query, service_type::search, service_type::analytics };
    }
    asio::post(asio::bind_executor(ctx_, [this, report_id, bucket_name, services, handler = std::move(handler)]() mutable {
        auto collector = std::make_shared<ping_collector>(report_id.value(), std::move(handler));
        if (bucket_name) {
            if (services.find(service_type::key_value) != services.end()) {
                for_each_bucket([&collector](auto& bucket) { bucket->ping(collector); });
            }
        } else {
            if (services.find(service_type::key_value) != services.end()) {
                if (session_) {
                    session_->ping(collector->build_reporter());
                }
                for_each_bucket([&collector](auto& bucket) { bucket->ping(collector); });
            }
            session_manager_->ping(services, collector, origin_.credentials());
        }
    }));
}

std::shared_ptr<bucket>
cluster::find_bucket_by_name(const std::string& name)
{
    std::scoped_lock lock(buckets_mutex_);

    auto bucket = buckets_.find(name);
    if (bucket == buckets_.end()) {
        return {};
    }
    return bucket->second;
}
} // namespace couchbase
