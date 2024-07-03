/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "query_component.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_strategy.hxx>

#include "core/free_form_http_request.hxx"
#include "core/http_component.hxx"
#include "core/logger/logger.hxx"
#include "core/row_streamer.hxx"
#include "core/service_type.hxx"
#include "core/utils/json.hxx"
#include "query_result.hxx"

#include <fmt/format.h>
#include <tao/json/value.hpp>

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

namespace couchbase::core::columnar
{
class query_component_impl : public std::enable_shared_from_this<query_component_impl>
{
public:
  query_component_impl(asio::io_context& io,
                       http_component http,
                       std::shared_ptr<retry_strategy> default_retry_strategy)
    : io_{ io }
    , http_{ std::move(http) }
    , default_retry_strategy_{ std::move(default_retry_strategy) }
  {
  }

  auto execute_query(query_options options, query_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto req = build_query_request(options);
    auto op = http_.do_http_request(
      std::move(req),
      [self = shared_from_this(), cb = std::move(callback)](auto resp, auto ec) mutable {
        if (ec) {
          cb({}, ec);
          return;
        }
        auto streamer = std::make_shared<row_streamer>(self->io_, resp.body(), "/results/^");
        return streamer->start([self, streamer, resp = std::move(resp), cb = std::move(cb)](
                                 auto metadata_header, auto ec) mutable {
          if (ec) {
            cb({}, ec);
            return;
          }
          auto query_ec =
            self->parse_query_error(resp.status_code(), utils::json::parse(metadata_header));
          if (query_ec) {
            cb({}, query_ec);
            return;
          }
          cb(query_result{ std::move(*streamer) }, {});
        });
      });
    return op;
  }

private:
  static auto parse_query_error(const std::uint32_t& http_status_code,
                                const tao::json::value& /* metadata_header */) -> std::error_code
  {
    // TODO(dimitris): Handle errors
    if (http_status_code != 200) {
      return errc::common::internal_server_failure;
    }
    return {};
  }

  static auto build_query_payload(const query_options& options) -> tao::json::value
  {
    tao::json::value payload{ { "statement", options.statement } };
    if (options.database_name.has_value() && options.scope_name.has_value()) {
      payload["query_context"] =
        fmt::format("default:`{}`.`{}`", options.database_name.value(), options.scope_name.value());
    }
    if (!options.positional_parameters.empty()) {
      std::vector<tao::json::value> params_json;
      params_json.reserve(options.positional_parameters.size());
      for (const auto& val : options.positional_parameters) {
        params_json.emplace_back(utils::json::parse(val));
      }
    }
    for (const auto& [name, val] : options.named_parameters) {
      std::string key = name;
      if (key[0] != '$') {
        key.insert(key.begin(), '$');
      }
      payload[key] = utils::json::parse(val);
    }
    if (options.read_only.has_value()) {
      payload["readonly"] = options.read_only.value();
    }
    if (options.scan_consistency.has_value()) {
      switch (options.scan_consistency.value()) {
        case query_scan_consistency::not_bounded:
          payload["scan_consistency"] = "not_bounded";
          break;
        case query_scan_consistency::request_plus:
          payload["scan_consistency"] = "request_plus";
          break;
      }
    }
    for (const auto& [key, val] : options.raw) {
      payload[key] = utils::json::parse(val);
    }
    if (options.timeout.has_value()) {
      std::chrono::milliseconds timeout = options.timeout.value() + std::chrono::seconds(5);
      payload["timeout"] = fmt::format("{}ms", timeout.count());
    }

    return payload;
  }

  static auto build_query_request(const query_options& options) -> http_request
  {
    http_request req{ service_type::analytics, "POST" };
    req.path = "/analytics/service";
    req.body = utils::json::generate(build_query_payload(options));
    if (options.timeout.has_value()) {
      req.timeout = options.timeout.value();
    }
    req.headers["connection"] = "keep-alive";
    req.headers["content-type"] = "application/json";
    if (options.priority.has_value() && options.priority.value()) {
      req.headers["analytics-priority"] = "-1";
    }
    if (options.read_only.has_value()) {
      req.is_read_only = options.read_only.value();
    }
    return req;
  }

  asio::io_context& io_;
  http_component http_;
  std::shared_ptr<retry_strategy> default_retry_strategy_;
};

query_component::query_component(asio::io_context& io,
                                 core::http_component http,
                                 std::shared_ptr<retry_strategy> default_retry_strategy)
  : impl_{
    std::make_shared<query_component_impl>(io, std::move(http), std::move(default_retry_strategy))
  }
{
}

auto
query_component::execute_query(query_options options, query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->execute_query(std::move(options), std::move(callback));
}

} // namespace couchbase::core::columnar
