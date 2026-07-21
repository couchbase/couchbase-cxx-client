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

#include "analytics_stream.hxx"

#include <couchbase/error_codes.hxx>

#include "core/operations/analytics_response_parsing.hxx"
#include "free_form_http_request.hxx"
#include "logger/logger.hxx"
#include "row_streamer.hxx"
#include "utils/json.hxx"
#include "utils/movable_function.hxx"

#include <tao/json/value.hpp>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core
{
namespace
{
constexpr auto ANALYTICS_RESULTS_POINTER = "/results/^";
} // namespace

class analytics_stream_impl : public std::enable_shared_from_this<analytics_stream_impl>
{
public:
  analytics_stream_impl(asio::io_context& io, http_response_body body, row_streamer_options options)
    : streamer_{ io, std::move(body), ANALYTICS_RESULTS_POINTER, options }
  {
  }

  void start(utils::movable_function<void(std::error_code)>&& on_ready)
  {
    streamer_.start([self = shared_from_this(), on_ready = std::move(on_ready)](
                      std::string preamble, std::error_code ec) mutable {
      if (ec) {
        return on_ready(ec);
      }
      operations::analytics_response::analytics_meta_data meta{};
      try {
        meta = operations::parse_analytics_meta(utils::json::parse(preamble));
      } catch (const std::exception&) {
        return on_ready(errc::common::parsing_failure);
      }
      {
        const std::scoped_lock lock{ self->mutex_ };
        self->signature_ = meta.signature;
      }
      // An upfront error is rare for the streaming path (status/errors usually arrive in the
      // trailer). The preamble status is typically "running", so only surface an early error when
      // the preamble already carries error entries.
      std::error_code early_error{};
      if (!meta.errors.empty()) {
        early_error = operations::map_analytics_error(meta);
      }
      on_ready(early_error);
    });
  }

  void next_row(
    utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler)
  {
    streamer_.next_row([self = shared_from_this(),
                        handler = std::move(handler)](std::string row, std::error_code ec) mutable {
      if (!row.empty()) {
        return handler(std::move(row), {});
      }
      // Empty row marks the end of the stream.
      if (ec) {
        return handler(std::nullopt, ec);
      }
      auto raw_meta = self->streamer_.metadata();
      if (!raw_meta.has_value()) {
        // A clean end without reconstructed metadata violates the row_streamer invariant; surface
        // it rather than silently reporting success with no metadata.
        CB_LOG_WARNING("analytics_stream: end reached but row_streamer returned no metadata");
        return handler(std::nullopt, errc::common::internal_server_failure);
      }
      std::error_code terminal_ec{};
      try {
        auto meta = operations::parse_analytics_meta(utils::json::parse(raw_meta.value()));
        terminal_ec = operations::map_analytics_error(meta);
        const std::scoped_lock lock{ self->mutex_ };
        self->meta_data_ = std::move(meta);
      } catch (const std::exception&) {
        terminal_ec = errc::common::parsing_failure;
      }
      handler(std::nullopt, terminal_ec);
    });
  }

  [[nodiscard]] auto signature() const -> std::optional<std::string>
  {
    const std::scoped_lock lock{ mutex_ };
    return signature_;
  }

  [[nodiscard]] auto meta_data() const
    -> std::optional<operations::analytics_response::analytics_meta_data>
  {
    const std::scoped_lock lock{ mutex_ };
    return meta_data_;
  }

  void cancel()
  {
    streamer_.cancel();
  }

private:
  row_streamer streamer_;
  mutable std::mutex mutex_{};
  std::optional<std::string> signature_{};
  std::optional<operations::analytics_response::analytics_meta_data> meta_data_{};
};

analytics_stream::analytics_stream(asio::io_context& io,
                                   http_response_body body,
                                   row_streamer_options options)
  : impl_{ std::make_shared<analytics_stream_impl>(io, std::move(body), options) }
{
}

void
analytics_stream::start(utils::movable_function<void(std::error_code)>&& on_ready)
{
  impl_->start(std::move(on_ready));
}

void
analytics_stream::next_row(
  utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler)
{
  impl_->next_row(std::move(handler));
}

auto
analytics_stream::signature() const -> std::optional<std::string>
{
  return impl_->signature();
}

auto
analytics_stream::meta_data() const
  -> std::optional<operations::analytics_response::analytics_meta_data>
{
  return impl_->meta_data();
}

void
analytics_stream::cancel()
{
  impl_->cancel();
}
} // namespace couchbase::core
