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

#include "backoff_calculator.hxx"
#include "core/free_form_http_request.hxx"
#include "core/http_component.hxx"
#include "core/logger/logger.hxx"
#include "core/pending_operation_connection_info.hxx"
#include "core/platform/uuid.h"
#include "core/row_streamer.hxx"
#include "core/service_type.hxx"
#include "core/utils/json.hxx"
#include "error.hxx"
#include "error_codes.hxx"
#include "query_result.hxx"
#include "retry_info.hxx"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <fmt/format.h>
#include <gsl/util>
#include <tao/json/value.hpp>
#include <tl/expected.hpp>

#include <cstdint>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core::columnar
{
class pending_query_operation
  : public std::enable_shared_from_this<pending_query_operation>
  , public pending_operation
{
public:
  pending_query_operation(const query_options& options,
                          asio::io_context& io,
                          http_component& http,
                          std::chrono::milliseconds default_timeout)
    : client_context_id_{ uuid::to_string(uuid::random()) }
    , http_req_{ build_query_request(options) }
    , timeout_{ options.timeout.value_or(default_timeout) }
    , io_{ io }
    , deadline_{ io_ }
    , retry_timer_{ io_ }
    , http_{ http }
  {
  }

  auto invoke_callback(query_result res, error err)
  {
    const std::scoped_lock lock{ callback_mutex_ };
    if (auto cb = std::move(callback_); cb) {
      cb(std::move(res), std::move(err));
    }
  }

  auto dispatch() -> error
  {
    auto op =
      http_.do_http_request(http_req_, [self = shared_from_this()](auto resp, auto ec) mutable {
        std::shared_ptr<pending_operation> op;
        {
          const std::scoped_lock lock{ self->pending_op_mutex_ };
          std::swap(op, self->pending_op_);
        }
        // op can be null if the pending_query_operation was cancelled.
        if (op) {
          auto op_info = std::dynamic_pointer_cast<pending_operation_connection_info>(op);
          self->retry_info_.last_dispatched_from = op_info->dispatched_from();
          self->retry_info_.last_dispatched_to = op_info->dispatched_to();
          self->retry_info_.last_dispatched_to_host = op_info->dispatched_to_host();
        }

        if (ec) {
          if (ec == couchbase::errc::common::request_canceled) {
            self->invoke_callback(
              {},
              { couchbase::core::columnar::errc::generic, "The query operation was canceled." });
            return;
          }
          self->invoke_callback({}, { maybe_convert_error_code(ec) });
          return;
        }
        auto streamer = std::make_shared<row_streamer>(self->io_, resp.body(), "/results/^");
        return streamer->start(
          [self, streamer, resp = std::move(resp)](auto metadata_header, auto ec) mutable {
            if (ec) {
              self->invoke_callback({}, { maybe_convert_error_code(ec) });
              return;
            }
            auto error_parse_res =
              self->parse_error(resp.status_code(), utils::json::parse(metadata_header));

            if (error_parse_res.retriable) {
              return self->maybe_retry();
            }

            if (error_parse_res.err) {
              self->invoke_callback({}, { error_parse_res.err });
              return;
            }
            self->invoke_callback(query_result{ std::move(*streamer) }, {});
          });
      });

    if (op.has_value()) {
      const std::scoped_lock lock{ pending_op_mutex_ };
      pending_op_ = op.value();
      return {};
    }
    return error{ op.error() };
  }

  auto start(query_callback&& callback) -> error
  {
    callback_ = std::move(callback);

    deadline_.expires_after(timeout_);
    deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      CB_LOG_DEBUG(R"(Columnar Query request timed out: retry_attempts={})",
                   self->retry_info_.retry_attempts);
      self->trigger_timeout();
    });

    return dispatch();
  }

  void cancel() override
  {
    cancelled_ = true;
    retry_timer_.cancel();
    std::shared_ptr<pending_operation> op;
    {
      const std::scoped_lock lock{ pending_op_mutex_ };
      std::swap(op, pending_op_);
    }
    if (op) {
      op->cancel();
    }
    // This will only call the callback if it has not already been called.
    invoke_callback(
      {}, { couchbase::core::columnar::errc::generic, "The query operation was canceled." });
  }

private:
  void maybe_retry()
  {
    if (cancelled_) {
      return;
    }
    auto backoff = backoff_calculator_(retry_info_.retry_attempts);
    if (std::chrono::steady_clock::now() + backoff >= deadline_.expiry()) {
      // Retrying will exceed the deadline, time out immediately instead.
      return trigger_timeout();
    }
    retry_timer_.expires_after(backoff);
    retry_timer_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->retry_info_.retry_attempts++;
      self->http_req_.internal.undesired_endpoint = self->retry_info_.last_dispatched_to;
      auto err = self->dispatch();
      if (err) {
        self->invoke_callback({}, std::move(err));
      }
    });
  }

  void trigger_timeout()
  {
    error err{ errc::timeout };
    enhance_error(err);
    invoke_callback({}, err);
    cancel();
  }

  auto build_query_payload(const query_options& options) -> tao::json::value
  {
    tao::json::value payload{ { "statement", options.statement },
                              { "client_context_id", client_context_id_ } };
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
      payload["args"] = std::move(params_json);
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
    const std::chrono::milliseconds server_timeout = timeout_ + std::chrono::seconds(5);
    payload["timeout"] = fmt::format("{}ms", server_timeout.count());

    return payload;
  }

  auto build_query_request(const query_options& options) -> http_request
  {
    http_request req{ service_type::analytics, "POST" };
    req.path = "/api/v1/request";
    req.body = utils::json::generate(build_query_payload(options));
    if (options.timeout.has_value()) {
      req.timeout = options.timeout.value();
    }

    req.client_context_id = client_context_id_;
    req.headers["connection"] = "keep-alive";
    req.headers["content-type"] = "application/json";
    if (options.priority.has_value() && options.priority.value()) {
      req.headers["analytics-priority"] = "-1";
    }
    if (options.read_only.has_value()) {
      req.is_read_only = options.read_only.value();
    }
    CB_LOG_DEBUG("QUERY REQUEST: client_context_id={}, body={}.", client_context_id_, req.body);
    return req;
  }

  struct error_parse_result {
    error err{};
    bool retriable{ false };
  };

  void enhance_error(error& err)
  {
    err.ctx["retry_attempts"] = retry_info_.retry_attempts;
    err.ctx["last_dispatched_to"] = retry_info_.last_dispatched_to;
    err.ctx["last_dispatched_from"] = retry_info_.last_dispatched_from;
  }

  auto parse_error(const std::uint32_t& http_status_code,
                   const tao::json::value& metadata_header) -> error_parse_result
  {
    const auto* errors_json = metadata_header.find("errors");
    if (errors_json == nullptr) {
      return {};
    }
    CB_LOG_DEBUG("QUERY ERROR (client_context_id={}): {}.",
                 client_context_id_,
                 utils::json::generate(errors_json));
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
    enhance_error(res.err);

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

  std::string client_context_id_;
  http_request http_req_;
  std::chrono::milliseconds timeout_;
  asio::io_context& io_;
  asio::steady_timer deadline_;
  asio::steady_timer retry_timer_;
  http_component& http_;
  query_callback callback_{};
  std::mutex callback_mutex_{};
  std::shared_ptr<pending_operation> pending_op_{};
  std::mutex pending_op_mutex_{};
  std::atomic_bool cancelled_{ false };
  backoff_calculator backoff_calculator_{ default_backoff_calculator };
  retry_info retry_info_{};
};

class query_component_impl : public std::enable_shared_from_this<query_component_impl>
{
public:
  query_component_impl(asio::io_context& io,
                       http_component http,
                       std::chrono::milliseconds default_timeout)
    : io_{ io }
    , http_{ std::move(http) }
    , default_timeout_{ default_timeout }
  {
  }

  auto execute_query(const query_options& options, query_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, error>
  {
    auto op = std::make_shared<pending_query_operation>(options, io_, http_, default_timeout_);
    auto err = op->start(std::move(callback));
    if (err) {
      return tl::unexpected<error>(err);
    }
    return op;
  }

private:
  asio::io_context& io_;
  http_component http_;
  std::chrono::milliseconds default_timeout_;
};

query_component::query_component(asio::io_context& io,
                                 core::http_component http,
                                 std::chrono::milliseconds default_timeout)
  : impl_{ std::make_shared<query_component_impl>(io, std::move(http), default_timeout) }
{
}

auto
query_component::execute_query(const query_options& options, query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, error>
{
  return impl_->execute_query(options, std::move(callback));
}

} // namespace couchbase::core::columnar
