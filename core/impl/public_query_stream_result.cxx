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

#include <couchbase/error_codes.hxx>
#include <couchbase/query_meta_data.hxx>
#include <couchbase/query_metrics.hxx>
#include <couchbase/query_status.hxx>
#include <couchbase/query_stream_result.hxx>
#include <couchbase/query_warning.hxx>

#include "core/operations/document_query.hxx"
#include "core/utils/binary.hxx"

#include "internal_query_stream_result.hxx"
#include "observability_recorder.hxx"
#include "query.hxx"

#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase
{
namespace
{
auto
to_binary(const std::string& s) -> codec::binary
{
  return core::utils::to_binary(s);
}

auto
build_query_meta_data(core::operations::query_response::query_meta_data meta) -> query_meta_data
{
  std::vector<query_warning> warnings;
  if (meta.warnings) {
    warnings.reserve(meta.warnings->size());
    for (auto& w : *meta.warnings) {
      warnings.emplace_back(w.code, std::move(w.message), w.reason, w.retry);
    }
  }

  std::optional<query_metrics> metrics;
  if (meta.metrics) {
    metrics = query_metrics{
      meta.metrics->elapsed_time, meta.metrics->execution_time, meta.metrics->result_count,
      meta.metrics->result_size,  meta.metrics->sort_count,     meta.metrics->mutation_count,
      meta.metrics->error_count,  meta.metrics->warning_count,
    };
  }

  std::optional<codec::binary> signature;
  if (meta.signature) {
    signature = to_binary(*meta.signature);
  }

  std::optional<codec::binary> profile;
  if (meta.profile) {
    profile = to_binary(*meta.profile);
  }

  return query_meta_data{
    std::move(meta.request_id),
    std::move(meta.client_context_id),
    core::impl::map_query_status(std::move(meta.status)),
    std::move(warnings),
    metrics,
    std::move(signature),
    std::move(profile),
  };
}
} // namespace

// ---------------------------------------------------------------------------
// internal_query_stream_result implementation
// ---------------------------------------------------------------------------

internal_query_stream_result::internal_query_stream_result(
  core::query_stream stream,
  std::unique_ptr<core::impl::observability_recorder> obs_rec)
  : stream_{ std::move(stream) }
  , obs_rec_{ std::move(obs_rec) }
{
}

internal_query_stream_result::~internal_query_stream_result()
{
  cancel();
}

void
internal_query_stream_result::cancel()
{
  // Resolve any parked meta_data() futures with a cancellation error so they don't throw
  // broken_promise when the stream is torn down before reaching a natural terminal. Idempotent
  // via terminal_reached_, so a real terminal that already ran is preserved.
  resolve_meta_data(errc::common::request_canceled);
  stream_.cancel();
}

auto
internal_query_stream_result::signature() const -> std::optional<codec::binary>
{
  auto raw = stream_.signature();
  if (!raw) {
    return {};
  }
  return to_binary(*raw);
}

void
internal_query_stream_result::resolve_meta_data(std::error_code ec)
{
  std::pair<error, query_meta_data> value;
  if (ec) {
    value.first = error(ec, "query stream ended with an error");
  }
  auto core_meta = stream_.meta_data();
  if (core_meta) {
    value.second = build_query_meta_data(std::move(*core_meta));
  }

  std::vector<std::promise<std::pair<error, query_meta_data>>> to_satisfy;
  {
    const std::scoped_lock lk{ meta_mutex_ };
    if (terminal_reached_) {
      return;
    }
    terminal_reached_ = true;
    terminal_value_ = value;
    to_satisfy = std::move(pending_meta_promises_);
    pending_meta_promises_.clear();
  }
  for (auto& promise : to_satisfy) {
    promise.set_value(value);
  }
  // Reached exactly once (the terminal_reached_ guard above returns on every later call), so the
  // operation is finished here: record the latency metric and end the operation span. ec is the
  // terminal error (falsy on a clean drain, request_canceled on teardown before a natural end).
  if (obs_rec_) {
    obs_rec_->finish(ec);
  }
}

auto
internal_query_stream_result::meta_data() -> std::future<std::pair<error, query_meta_data>>
{
  const std::scoped_lock lk{ meta_mutex_ };
  std::promise<std::pair<error, query_meta_data>> promise;
  auto future = promise.get_future();
  if (terminal_reached_) {
    promise.set_value(terminal_value_);
  } else {
    pending_meta_promises_.push_back(std::move(promise));
  }
  return future;
}

void
internal_query_stream_result::next(query_row_handler&& handler)
{
  {
    // Idempotent terminal: once the stream has ended, re-deliver the terminal (clean end or the
    // terminal error) instead of issuing a pull that would block forever on the drained channel.
    const std::scoped_lock lk{ meta_mutex_ };
    if (terminal_reached_) {
      return handler(terminal_value_.first, std::nullopt);
    }
  }
  bool expected = false;
  if (!pull_in_flight_.compare_exchange_strong(expected, true)) {
    // Enforce the single-outstanding-next() contract instead of racing the single-consumer channel.
    return handler(
      error{ errc::common::invalid_argument, "a previous next() call is still in flight" },
      std::nullopt);
  }
  // Capture a strong self so the object outlives an in-flight pull; dropping the public handle
  // mid-pull would otherwise destroy this object before the channel completion runs (use-after-
  // free). The stream is always owned via shared_ptr (make_shared in dispatch_query_stream).
  stream_.next_row([handler = std::move(handler), self = shared_from_this()](
                     std::optional<std::string> row, std::error_code ec) mutable {
    if (row.has_value()) {
      // Real row (ec is falsy). Release the pull gate before invoking the handler so it may
      // immediately issue the next pull.
      self->pull_in_flight_.store(false);
      return handler({}, query_row{ to_binary(*row) });
    }
    // Terminal (nullopt row): publish the terminal state and intentionally leave pull_in_flight_
    // set. Every later next() is served by the terminal_reached_ guard (idempotent re-delivery) and
    // must never issue another pull onto the drained channel, so the gate is never reopened — this
    // also backstops a caller that violates the single-outstanding-next() contract from a racing
    // thread before terminal_reached_ becomes visible.
    self->resolve_meta_data(ec);
    if (ec) {
      return handler(error(ec, "query stream ended with an error"), {});
    }
    return handler({}, {});
  });
}

// ---------------------------------------------------------------------------
// query_stream_result public implementation
// ---------------------------------------------------------------------------

query_stream_result::query_stream_result(std::shared_ptr<internal_query_stream_result> internal)
  : internal_{ std::move(internal) }
{
}

void
query_stream_result::next(query_row_handler&& handler) const
{
  if (!internal_) {
    // Empty handle behaves as an immediately-exhausted stream.
    handler(error{}, std::nullopt);
    return;
  }
  return internal_->next(std::move(handler));
}

auto
query_stream_result::next() const -> std::future<std::pair<error, std::optional<query_row>>>
{
  auto barrier = std::make_shared<std::promise<std::pair<error, std::optional<query_row>>>>();
  if (!internal_) {
    // Empty handle behaves as an immediately-exhausted stream.
    barrier->set_value({ error{}, std::nullopt });
    return barrier->get_future();
  }
  internal_->next([barrier](const auto& err, const auto& row) mutable {
    barrier->set_value({ err, row });
  });
  return barrier->get_future();
}

auto
query_stream_result::signature() const -> std::optional<codec::binary>
{
  if (!internal_) {
    return {};
  }
  return internal_->signature();
}

auto
query_stream_result::meta_data() const -> std::future<std::pair<error, query_meta_data>>
{
  if (!internal_) {
    // Empty handle resolves immediately with empty metadata.
    std::promise<std::pair<error, query_meta_data>> p;
    p.set_value({ error{}, query_meta_data{} });
    return p.get_future();
  }
  return internal_->meta_data();
}

void
query_stream_result::cancel() const
{
  if (internal_) {
    return internal_->cancel();
  }
}

// ---------------------------------------------------------------------------
// iterator implementation
// ---------------------------------------------------------------------------

query_stream_result::iterator::iterator(std::shared_ptr<internal_query_stream_result> internal)
  : internal_{ std::move(internal) }
{
  if (internal_) {
    fetch_item();
  } else {
    // An empty result handle has no rows: the iterator starts already at end.
    terminal_ = true;
  }
}

void
query_stream_result::iterator::fetch_item()
{
  auto barrier = std::make_shared<std::promise<std::pair<error, std::optional<query_row>>>>();
  internal_->next([barrier](const error& err, std::optional<query_row> row) mutable {
    barrier->set_value({ err, std::move(row) });
  });
  auto result = barrier->get_future().get();
  if (result.first) {
    // Terminal error: deliver it as one visible element (still != end()) so a range-for loop can
    // observe it; the next increment then advances to true end.
    item_ = { result.first, {} };
    terminal_ = false;
    error_pending_ = true;
  } else if (!result.second.has_value()) {
    // Clean end-of-stream.
    item_ = {};
    terminal_ = true;
    error_pending_ = false;
  } else {
    item_ = { {}, *result.second };
    terminal_ = false;
    error_pending_ = false;
  }
}

auto
query_stream_result::iterator::operator*() const -> std::pair<error, query_row>
{
  return item_;
}

auto
query_stream_result::iterator::operator++() -> query_stream_result::iterator&
{
  if (error_pending_) {
    // The terminal error element has been consumed; advance to end().
    error_pending_ = false;
    item_ = {};
    terminal_ = true;
    return *this;
  }
  fetch_item();
  return *this;
}

auto
query_stream_result::begin() const -> query_stream_result::iterator
{
  // The iterator constructor treats a null internal handle as an already-drained stream, so an
  // empty result handle yields begin() == end().
  return query_stream_result::iterator{ internal_ };
}

auto
query_stream_result::end() const -> query_stream_result::end_sentinel
{
  return {};
}
} // namespace couchbase
