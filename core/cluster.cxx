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

#include "cluster.hxx"

#include "core/mcbp/completion_token.hxx"
#include "core/mcbp/queue_request.hxx"
#include "ping_collector.hxx"
#include "ping_reporter.hxx"

namespace couchbase::core
{

class ping_collector_impl
  : public std::enable_shared_from_this<ping_collector_impl>
  , public diag::ping_reporter
  , public diag::ping_collector
{
    diag::ping_result res_;
    utils::movable_function<void(diag::ping_result)> handler_;
    std::atomic_int expected_{ 0 };
    std::mutex mutex_{};

  public:
    ping_collector_impl(std::string report_id, utils::movable_function<void(diag::ping_result)>&& handler)
      : res_{ std::move(report_id), meta::sdk_id() }
      , handler_(std::move(handler))
    {
    }

    ~ping_collector_impl()
    {
        invoke_handler();
    }

    [[nodiscard]] diag::ping_result& result()
    {
        return res_;
    }

    void report(diag::endpoint_ping_info&& info) override
    {
        std::scoped_lock lock(mutex_);
        res_.services[info.type].emplace_back(std::move(info));
        if (--expected_ == 0) {
            invoke_handler();
        }
    }

    auto build_reporter() -> std::shared_ptr<diag::ping_reporter> override
    {
        ++expected_;
        return shared_from_this();
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
        return handler({ report_id.value(), meta::sdk_id() });
    }
    if (services.empty()) {
        services = {
            service_type::key_value, service_type::view,       service_type::query,    service_type::search,
            service_type::analytics, service_type::management, service_type::eventing,
        };
    }
    asio::post(
      asio::bind_executor(ctx_, [cluster = shared_from_this(), report_id, bucket_name, services, handler = std::move(handler)]() mutable {
          auto collector = std::make_shared<ping_collector_impl>(report_id.value(), std::move(handler));
          if (bucket_name) {
              if (services.find(service_type::key_value) != services.end()) {
                  if (auto bucket = cluster->find_bucket_by_name(bucket_name.value()); bucket) {
                      return bucket->ping(collector);
                  }
                  cluster->open_bucket(bucket_name.value(), [collector, cluster, bucket_name](std::error_code ec) {
                      if (!ec) {
                          if (auto bucket = cluster->find_bucket_by_name(bucket_name.value()); bucket) {
                              return bucket->ping(collector);
                          }
                      }
                  });
              }
          } else {
              if (services.find(service_type::key_value) != services.end()) {
                  if (cluster->session_) {
                      cluster->session_->ping(collector->build_reporter());
                  }
                  cluster->for_each_bucket([&collector](auto& bucket) { bucket->ping(collector); });
              }
              cluster->session_manager_->ping(services, collector, cluster->origin_.credentials());
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

auto
cluster::direct_dispatch(const std::string& bucket_name, std::shared_ptr<couchbase::core::mcbp::queue_request> req) -> std::error_code
{
    if (stopped_) {
        return errc::network::cluster_closed;
    }
    if (bucket_name.empty()) {
        return errc::common::invalid_argument;
    }
    if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
        return bucket->direct_dispatch(std::move(req));
    }

    open_bucket(bucket_name, [self = shared_from_this(), req = std::move(req), bucket_name](std::error_code ec) mutable {
        if (ec) {
            return req->cancel(ec);
        }
        self->direct_dispatch(bucket_name, std::move(req));
    });
    return {};
}

auto
cluster::direct_re_queue(const std::string& bucket_name, std::shared_ptr<mcbp::queue_request> req, bool is_retry) -> std::error_code
{
    if (stopped_) {
        return errc::network::cluster_closed;
    }
    if (bucket_name.empty()) {
        return errc::common::invalid_argument;
    }
    if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
        return bucket->direct_re_queue(std::move(req), is_retry);
    }

    open_bucket(bucket_name, [self = shared_from_this(), bucket_name, req = std::move(req), is_retry](std::error_code ec) mutable {
        if (ec) {
            return req->cancel(ec);
        }
        self->direct_re_queue(bucket_name, std::move(req), is_retry);
    });
    return {};
}

} // namespace couchbase::core
