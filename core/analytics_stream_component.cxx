/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "analytics_stream_component.hxx"

#include "free_form_http_request.hxx"
#include "http_component.hxx"
#include "impl/bootstrap_error.hxx"
#include "operations/document_analytics.hxx"
#include "platform/uuid.h"
#include "service_type.hxx"
#include "utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <spdlog/fmt/bundled/core.h>
#include <tao/json/value.hpp>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core
{
namespace
{
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
auto
to_error_code(const error_union& err) -> std::error_code
{
  if (std::holds_alternative<std::error_code>(err)) {
    return std::get<std::error_code>(err);
  }
  if (std::holds_alternative<impl::bootstrap_error>(err)) {
    return std::get<impl::bootstrap_error>(err).ec;
  }
  return {};
}
#else
auto
to_error_code(std::error_code err) -> std::error_code
{
  return err;
}
#endif

auto
build_streaming_analytics_body(const operations::analytics_request& request,
                               const std::string& client_context_id,
                               std::chrono::milliseconds timeout) -> std::string
{
  tao::json::value body{};

  // The remaining options are identical to the buffered analytics path; share one encoder so the
  // two stay byte-for-byte consistent. This runs first so the control fields set below always win
  // over any same-named `raw` entry a caller may have supplied.
  operations::encode_analytics_options(body, request);

  body["client_context_id"] = client_context_id;
  body["statement"] = request.statement;
  // The server-side timeout is sent unmodified here — unlike the query path, which shaves 500ms so
  // it observes the engine's timeout before its own. This mirrors the buffered analytics path
  // (document_analytics), which likewise does not tighten the engine deadline; keep them
  // consistent.
  body["timeout"] = fmt::format("{}ms", timeout.count());

  return utils::json::generate(body);
}
} // namespace

class analytics_stream_component_impl
  : public std::enable_shared_from_this<analytics_stream_component_impl>
{
public:
  analytics_stream_component_impl(asio::io_context& io,
                                  http_component http,
                                  std::chrono::milliseconds default_timeout,
                                  row_streamer_options streaming_options)
    : io_{ io }
    , http_{ std::move(http) }
    , default_timeout_{ default_timeout }
    , streaming_options_{ streaming_options }
  {
  }

  void execute(operations::analytics_request request,
               analytics_stream_component::handler_type&& handler)
  {
    const std::string client_context_id =
      request.client_context_id.value_or(uuid::to_string(uuid::random()));
    const std::chrono::milliseconds timeout = request.timeout.value_or(default_timeout_);

    http_request http_req{ service_type::analytics, "POST" };
    http_req.path = "/query/service";
    try {
      http_req.body = build_streaming_analytics_body(request, client_context_id, timeout);
    } catch (const std::exception&) {
      // A caller-supplied raw/parameter value that is not valid JSON fails encoding; surface it as
      // invalid_argument rather than letting the exception escape the public API.
      return handler({}, errc::common::invalid_argument);
    }
    http_req.timeout = timeout;
    http_req.client_context_id = client_context_id;
    http_req.is_read_only = request.readonly;
    http_req.headers["connection"] = "keep-alive";
    http_req.headers["content-type"] = "application/json";
    if (request.priority) {
      http_req.headers["analytics-priority"] = "-1";
    }
    if (request.parent_span) {
      http_req.parent_span = request.parent_span;
    }

    auto callback_state = std::make_shared<callback_state_type>(std::move(handler));

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    using dispatch_error = error_union;
#else
    using dispatch_error = std::error_code;
#endif

    const bool read_only = request.readonly;
    auto op = http_.do_http_request(
      http_req,
      [self = shared_from_this(), callback_state, timeout, read_only](http_response resp,
                                                                      dispatch_error err) mutable {
        if (auto ec = to_error_code(err)) {
          self->invoke(callback_state, {}, ec);
          return;
        }
        // Use the cluster's streaming tuning, but bound the server between reads with this
        // request's timeout: if it stalls mid-body the stream times out rather than hanging next()
        // forever (the request timeout is otherwise released once headers arrive).
        auto options = self->streaming_options_;
        options.idle_timeout = timeout;
        options.is_read_only = read_only;
        auto stream = std::make_shared<analytics_stream>(self->io_, resp.body(), options);
        stream->start([self, callback_state, stream](std::error_code early_error) mutable {
          if (early_error) {
            // No consumer will ever take ownership of this stream, so tear the underlying body
            // (socket + timers) down explicitly rather than leaking it until the last handle drops.
            stream->cancel();
            self->invoke(callback_state, {}, early_error);
            return;
          }
          self->invoke(callback_state, std::move(*stream), {});
        });
      });

    if (!op.has_value()) {
      invoke(callback_state, {}, to_error_code(op.error()));
    }
  }

private:
  struct callback_state_type {
    explicit callback_state_type(analytics_stream_component::handler_type h)
      : handler{ std::move(h) }
    {
    }

    analytics_stream_component::handler_type handler;
    std::mutex mutex{};
    bool invoked{ false };
  };

  void invoke(const std::shared_ptr<callback_state_type>& state,
              analytics_stream stream,
              std::error_code ec)
  {
    analytics_stream_component::handler_type handler;
    {
      const std::scoped_lock lock{ state->mutex };
      if (state->invoked) {
        return;
      }
      state->invoked = true;
      handler = std::move(state->handler);
    }
    if (handler) {
      handler(std::move(stream), ec);
    }
  }

  asio::io_context& io_;
  http_component http_;
  std::chrono::milliseconds default_timeout_;
  row_streamer_options streaming_options_;
};

analytics_stream_component::analytics_stream_component(asio::io_context& io,
                                                       http_component http,
                                                       std::chrono::milliseconds default_timeout,
                                                       row_streamer_options streaming_options)
  : impl_{ std::make_shared<analytics_stream_component_impl>(io,
                                                             std::move(http),
                                                             default_timeout,
                                                             streaming_options) }
{
}

void
analytics_stream_component::execute(operations::analytics_request request,
                                    handler_type&& handler) const
{
  impl_->execute(std::move(request), std::move(handler));
}
} // namespace couchbase::core
