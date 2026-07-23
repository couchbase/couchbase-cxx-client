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

#include "query_stream.hxx"

#include <couchbase/error_codes.hxx>

#include "core/operations/query_response_parsing.hxx"
#include "free_form_http_request.hxx"
#include "logger/logger.hxx"
#include "row_streamer.hxx"
#include "utils/json.hxx"
#include "utils/movable_function.hxx"

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <tao/json/value.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core
{
namespace
{
constexpr auto N1QL_RESULTS_POINTER = "/results/^";

// The bounded streaming lexer surfaces malformed- or oversized-JSON failures as
// errc::streaming_json_lexer::* codes. The buffered query() path normalizes any body-parse failure
// to errc::common::parsing_failure (document_query.cxx), and so does row_streamer's own
// truncated-body terminal; normalize the lexer codes here too so the public streaming error
// contract matches the buffered one (a consumer matching parsing_failure never silently misses a
// malformed response).
auto
normalize_stream_error(std::error_code ec) -> std::error_code
{
  if (ec.category() == couchbase::core::impl::streaming_json_lexer_category()) {
    return errc::common::parsing_failure;
  }
  return ec;
}
} // namespace

// Abstract backing for a query_stream. Two implementations: one that lazily pulls rows off a live
// socket (streaming_query_stream_impl), and one that replays an already-buffered response without
// re-parsing it (buffered_query_stream_impl), used for the prepared-statement path.
class query_stream_impl
{
public:
  query_stream_impl() = default;
  query_stream_impl(const query_stream_impl&) = delete;
  query_stream_impl(query_stream_impl&&) = delete;
  auto operator=(const query_stream_impl&) -> query_stream_impl& = delete;
  auto operator=(query_stream_impl&&) -> query_stream_impl& = delete;
  virtual ~query_stream_impl() = default;

  virtual void start(utils::movable_function<void(std::error_code)>&& on_ready) = 0;
  virtual void next_row(
    utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler) = 0;
  [[nodiscard]] virtual auto signature() const -> std::optional<std::string> = 0;
  [[nodiscard]] virtual auto meta_data() const
    -> std::optional<operations::query_response::query_meta_data> = 0;
  virtual void cancel() = 0;
};

namespace
{
class streaming_query_stream_impl
  : public query_stream_impl
  , public std::enable_shared_from_this<streaming_query_stream_impl>
{
public:
  streaming_query_stream_impl(asio::io_context& io,
                              http_response_body body,
                              row_streamer_options options)
    : streamer_{ io, std::move(body), N1QL_RESULTS_POINTER, options }
  {
  }

  void start(utils::movable_function<void(std::error_code)>&& on_ready) override
  {
    streamer_.start([self = shared_from_this(), on_ready = std::move(on_ready)](
                      std::string preamble, std::error_code ec) mutable {
      if (ec) {
        return on_ready(normalize_stream_error(ec));
      }
      operations::query_response::query_meta_data meta{};
      try {
        meta = operations::parse_query_meta(utils::json::parse(preamble));
      } catch (const std::exception&) {
        return on_ready(errc::common::parsing_failure);
      }
      {
        const std::scoped_lock lock{ self->mutex_ };
        self->signature_ = meta.signature;
      }
      // An upfront error is rare for the streaming path (status/errors usually arrive in the
      // trailer). Mirror the analytics preamble check and gate on error entries rather than status:
      // a benign non-success preamble status (e.g. "running") would otherwise be handed to
      // map_query_error, whose no-errors fallback returns internal_server_failure — a spurious
      // error. Checking errors also surfaces a genuine preamble error now rather than one next()
      // call later.
      std::error_code early_error{};
      if (meta.errors && !meta.errors->empty()) {
        early_error = operations::map_query_error(meta);
      }
      on_ready(early_error);
    });
  }

  void next_row(
    utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler) override
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
        CB_LOG_WARNING("query_stream: end reached but row_streamer returned no metadata");
        terminal_ec = errc::common::internal_server_failure;
      } else {
        try {
          auto meta = operations::parse_query_meta(utils::json::parse(raw_meta.value()));
          terminal_ec = operations::map_query_error(meta);
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

  [[nodiscard]] auto signature() const -> std::optional<std::string> override
  {
    const std::scoped_lock lock{ mutex_ };
    return signature_;
  }

  [[nodiscard]] auto meta_data() const
    -> std::optional<operations::query_response::query_meta_data> override
  {
    const std::scoped_lock lock{ mutex_ };
    return meta_data_;
  }

  void cancel() override
  {
    streamer_.cancel();
  }

private:
  row_streamer streamer_;
  mutable std::mutex mutex_{};
  std::optional<std::string> signature_{};
  std::optional<operations::query_response::query_meta_data> meta_data_{};
  // Terminal-sticky state (guarded by mutex_): once next_row reports the terminal, it is remembered
  // so every later pull re-delivers it instead of parking on the drained channel.
  bool terminal_reached_{ false };
  std::error_code terminal_ec_{};
};

// Replays an already-buffered query_response as a stream without any JSON re-parsing: the rows are
// already row-JSON strings and the metadata is already parsed. Used for the prepared-statement
// (adhoc == false) path, which must run buffered but presents the uniform streaming handle.
class buffered_query_stream_impl
  : public query_stream_impl
  , public std::enable_shared_from_this<buffered_query_stream_impl>
{
public:
  buffered_query_stream_impl(asio::io_context& io,
                             std::vector<std::string> rows,
                             operations::query_response::query_meta_data meta)
    : io_{ io }
    , rows_{ std::move(rows) }
    , meta_{ std::move(meta) }
  {
  }

  void start(utils::movable_function<void(std::error_code)>&& on_ready) override
  {
    // The whole response is already in hand; there is no preamble to parse. Any query error is
    // delivered at the terminal (mirroring the streaming path's trailing-error semantics).
    asio::post(io_, [self = shared_from_this(), on_ready = std::move(on_ready)]() mutable {
      on_ready({});
    });
  }

  void next_row(
    utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler) override
  {
    // Post the delivery so a consumer that pulls the next row from within its handler does not
    // recurse synchronously through a large buffered result and overflow the stack.
    asio::post(io_, [self = shared_from_this(), handler = std::move(handler)]() mutable {
      if (self->cancelled_) {
        return handler(std::nullopt, errc::common::request_canceled);
      }
      if (self->index_ < self->rows_.size()) {
        // Each buffered row is consumed at most once, so hand it to the handler by move rather than
        // copying what can be a large prepared-statement result out of the vector.
        return handler(std::move(self->rows_[self->index_++]), {});
      }
      return handler(std::nullopt, operations::map_query_error(self->meta_));
    });
  }

  [[nodiscard]] auto signature() const -> std::optional<std::string> override
  {
    return meta_.signature;
  }

  [[nodiscard]] auto meta_data() const
    -> std::optional<operations::query_response::query_meta_data> override
  {
    return meta_;
  }

  void cancel() override
  {
    cancelled_ = true;
  }

private:
  asio::io_context& io_;
  std::vector<std::string> rows_;
  operations::query_response::query_meta_data meta_;
  std::size_t index_{ 0 };
  // cancel() may be called from the consumer thread while the posted delivery reads it on the io
  // thread, so it must be atomic. index_/rows_ are only touched on the io thread (posted handlers).
  std::atomic_bool cancelled_{ false };
};
} // namespace

query_stream::query_stream(asio::io_context& io,
                           http_response_body body,
                           row_streamer_options options)
  : impl_{ std::make_shared<streaming_query_stream_impl>(io, std::move(body), options) }
{
}

query_stream::query_stream(asio::io_context& io,
                           std::vector<std::string> rows,
                           operations::query_response::query_meta_data meta)
  : impl_{ std::make_shared<buffered_query_stream_impl>(io, std::move(rows), std::move(meta)) }
{
}

void
query_stream::start(utils::movable_function<void(std::error_code)>&& on_ready)
{
  impl_->start(std::move(on_ready));
}

void
query_stream::next_row(
  utils::movable_function<void(std::optional<std::string>, std::error_code)>&& handler)
{
  impl_->next_row(std::move(handler));
}

auto
query_stream::signature() const -> std::optional<std::string>
{
  return impl_->signature();
}

auto
query_stream::meta_data() const -> std::optional<operations::query_response::query_meta_data>
{
  return impl_->meta_data();
}

void
query_stream::cancel()
{
  impl_->cancel();
}
} // namespace couchbase::core
