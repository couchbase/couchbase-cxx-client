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

// Normalize a bounded-lexer failure (malformed / oversized JSON) to errc::common::parsing_failure,
// matching the buffered analytics_query() path and row_streamer's own truncated-body terminal, so
// the public streaming error contract is consistent.
auto
normalize_stream_error(std::error_code ec) -> std::error_code
{
  if (ec.category() == couchbase::core::impl::streaming_json_lexer_category()) {
    return errc::common::parsing_failure;
  }
  return ec;
}
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
        return on_ready(normalize_stream_error(ec));
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
    // Terminal-sticky: once the stream has ended, re-deliver the stored terminal on every
    // subsequent pull instead of issuing another receive that would park forever on the
    // drained-but-open row channel. The public handle guards this too, but a core::-direct consumer
    // (e.g. a wrapper) relies on this being safe.
    std::optional<std::error_code> sticky_terminal;
    {
      const std::scoped_lock lock{ mutex_ };
      if (terminal_reached_) {
        sticky_terminal = terminal_ec_;
      }
    }
    if (sticky_terminal) {
      // Deliver outside the lock: the handler typically issues the next pull, which re-enters
      // next_row and would re-lock the non-recursive mutex_ and deadlock if invoked while held.
      return handler(std::nullopt, *sticky_terminal);
    }
    streamer_.next_row([self = shared_from_this(),
                        handler = std::move(handler)](std::string row, std::error_code ec) mutable {
      if (!row.empty()) {
        return handler(std::move(row), {});
      }
      // Empty row marks the end of the stream. Classify the terminal, remember it (so later pulls
      // are served the sticky terminal above), then deliver.
      std::error_code terminal_ec{};
      if (ec) {
        // Normalize a mid-document lexer failure to parsing_failure, matching the buffered path.
        terminal_ec = normalize_stream_error(ec);
      } else if (auto raw_meta = self->streamer_.metadata(); !raw_meta.has_value()) {
        // A clean end without reconstructed metadata violates the row_streamer invariant; surface
        // it rather than silently reporting success with no metadata.
        CB_LOG_WARNING("analytics_stream: end reached but row_streamer returned no metadata");
        terminal_ec = errc::common::internal_server_failure;
      } else {
        try {
          auto meta = operations::parse_analytics_meta(utils::json::parse(raw_meta.value()));
          terminal_ec = operations::map_analytics_error(meta);
          const std::scoped_lock lock{ self->mutex_ };
          self->meta_data_ = std::move(meta);
        } catch (const std::exception&) {
          terminal_ec = errc::common::parsing_failure;
        }
      }
      {
        const std::scoped_lock lock{ self->mutex_ };
        self->terminal_reached_ = true;
        self->terminal_ec_ = terminal_ec;
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
  // Terminal-sticky state (guarded by mutex_): once next_row reports the terminal, it is remembered
  // so every later pull re-delivers it instead of parking on the drained channel.
  bool terminal_reached_{ false };
  std::error_code terminal_ec_{};
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
