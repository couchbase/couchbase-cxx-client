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
#include "core/row_streamer.hxx"
#include "core/service_type.hxx"
#include "core/utils/json.hxx"
#include "error.hxx"
#include "error_codes.hxx"
#include "query_result.hxx"

#include <fmt/format.h>
#include <gsl/util>
#include <tao/json/value.hpp>
#include <tl/expected.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <system_error>
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

  auto execute_query(const query_options& options, query_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, error>
  {
    auto req = build_query_request(options);
    auto op = http_.do_http_request(
      req, [self = shared_from_this(), cb = std::move(callback)](auto resp, auto ec) mutable {
        if (ec) {
          if (ec == couchbase::errc::common::request_canceled) {
            cb({},
               { couchbase::core::columnar::errc::generic, "The query operation was canceled." });
            return;
          }
          cb({}, { maybe_convert_error_code(ec) });
          return;
        }
        auto streamer = std::make_shared<row_streamer>(self->io_, resp.body(), "/results/^");
        return streamer->start([self, streamer, resp = std::move(resp), cb = std::move(cb)](
                                 auto metadata_header, auto ec) mutable {
          if (ec) {
            cb({}, { maybe_convert_error_code(ec) });
            return;
          }
          auto error_parse_res =
            parse_error(resp.status_code(), utils::json::parse(metadata_header));
          if (error_parse_res.err.ec) {
            cb({}, { error_parse_res.err });
            return;
          }
          cb(query_result{ std::move(*streamer) }, {});
        });
      });

    if (!op.has_value()) {
      return tl::unexpected<error>({ op.error() });
    }
    return { op.value() };
  }

private:
  struct error_parse_result {
    error err{};
    bool retriable{ false };
  };

  static auto parse_error(const std::uint32_t& http_status_code,
                          const tao::json::value& metadata_header) -> error_parse_result
  {
    const auto* errors_json = metadata_header.find("errors");
    if (errors_json == nullptr) {
      return {};
    }
    if (!errors_json->is_array()) {
      return { { errc::generic,
                 "Could not parse errors from server response - expected JSON array" } };
    }
    if (errors_json->get_array().empty()) {
      return {};
    }

    error_parse_result res;
    res.retriable = true;
    res.err.ctx["http_status"] = std::to_string(http_status_code);
    res.err.ctx["errors"] = std::vector<tao::json::value>{};

    if (http_status_code == 401) {
      res.err.ec = errc::invalid_credential;
    } else {
      res.err.ec = errc::query_error;
    }

    std::int32_t first_error_code{ 0 };
    std::string first_error_msg{};

    for (auto error_json : errors_json->get_array()) {
      auto* retr = error_json.find("retriable");
      if (retr == nullptr) {
        // An error is assumed to not be retriable if the field is missing
        res.retriable = false;
      } else if (!retr->is_boolean()) {
        return { { errc::generic,
                   "Could not parse error from server response - 'retriable' was not boolean" } };
      } else {
        // Operation is retriable iff all errors are retriable
        res.retriable = res.retriable && retr->get_boolean();
      }

      auto* msg = error_json.find("msg");
      if (msg == nullptr) {
        return { { errc::generic,
                   "Could not parse error from server response - could not find 'msg' field" } };
      }
      if (!msg->is_string()) {
        return { { errc::generic,
                   "Could not parse error from server response - 'msg' field was not string" } };
      }

      auto* c = error_json.find("code");
      if (c == nullptr) {
        return { { errc::generic,
                   "Could not parse error from server response - could not find 'code' field" } };
      }
      if (!(c->is_unsigned() || c->is_signed())) {
        return {
          { errc::generic,
            "Could not parse error from server response - 'code' field was not an integer" }
        };
      }

      std::int32_t code = c->is_signed() ? gsl::narrow_cast<std::int32_t>(c->get_signed())
                                         : gsl::narrow_cast<std::int32_t>(c->get_unsigned());

      tao::json::value error = {
        { "code", code },
        { "msg", msg->get_string() },
      };
      res.err.ctx["errors"].get_array().emplace_back(std::move(error));

      if (first_error_code == 0) {
        first_error_code = code;
        first_error_msg = msg->get_string();
      }

      switch (code) {
        case 20000:
          res.err.ec = errc::invalid_credential;
          break;
        case 21002:
          res.err.ec = errc::timeout;
          break;
        default:
          break;
      }
    }

    if (res.err.ec == errc::query_error) {
      res.err.properties = query_error_properties{ first_error_code, first_error_msg };
    }

    return res;
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
      const std::chrono::milliseconds timeout = options.timeout.value() + std::chrono::seconds(5);
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
query_component::execute_query(const query_options& options, query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, error>
{
  return impl_->execute_query(options, std::move(callback));
}

} // namespace couchbase::core::columnar
