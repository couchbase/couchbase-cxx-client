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

#pragma once

#include <couchbase/analytics_meta_data.hxx>
#include <couchbase/analytics_row.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/error.hxx>

#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <utility>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
class internal_analytics_stream_result;
#endif

/**
 * The signature for the handler of the @ref analytics_stream_result#next() operation.
 *
 * @since 1.4.0
 * @volatile
 */
using analytics_row_handler = std::function<void(error, std::optional<analytics_row>)>;

/**
 * A streaming result handle for analytics queries.
 *
 * Rows are fetched one at a time via @ref next(). The stream must be fully drained
 * (or @ref cancel() called) before @ref meta_data() resolves. Only a single @ref next()
 * call may be outstanding at a time.
 *
 * @note The future-returning overloads (`next()`, `meta_data()`) and the eager @ref iterator
 * block the calling thread until the result is ready. They must not be called from within an SDK
 * completion handler (i.e. the library's I/O thread) — doing so blocks that thread against itself
 * and the stream can never advance. From a completion handler, use the callback @ref next()
 * overload instead.
 *
 * @note Copies of a result handle share one underlying stream: they alias the same rows, the same
 * single-outstanding-next() budget, and the same @ref cancel(). Copying does not fork the stream;
 * a @ref next() on any copy consumes the next row for all of them. Use a single handle per stream.
 *
 * @since 1.4.0
 * @volatile
 */
class analytics_stream_result
{
public:
  /**
   * Constructs an empty (no-op) result handle.
   *
   * @since 1.4.0
   * @internal
   */
  analytics_stream_result() = default;

  /**
   * Constructs an analytics stream result from an internal result.
   *
   * @param internal the internal result handle
   *
   * @since 1.4.0
   * @internal
   */
  explicit analytics_stream_result(std::shared_ptr<internal_analytics_stream_result> internal);

  /**
   * Fetches the next row asynchronously, invoking the handler when ready.
   *
   * The handler receives ({}, row) for a real row, ({}, {}) at clean end-of-stream,
   * or (error, {}) if the stream ended with an error.
   *
   * Only one outstanding call is allowed at a time.
   *
   * @param handler callable that implements @ref analytics_row_handler
   *
   * @since 1.4.0
   * @volatile
   */
  void next(analytics_row_handler&& handler) const;

  /**
   * Fetches the next row, returning a future.
   *
   * Only one outstanding call is allowed at a time.
   *
   * @return future object that carries the result of the operation
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto next() const -> std::future<std::pair<error, std::optional<analytics_row>>>;

  /**
   * Returns the analytics signature captured from the response metadata, if present.
   *
   * @return optional binary JSON signature
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto signature() const -> std::optional<codec::binary>;

  /**
   * Returns the analytics metadata.
   *
   * The returned future resolves only after the stream has been fully drained
   * (all rows consumed or the stream cancelled). May be called more than once;
   * each call returns its own future and all of them resolve together once the
   * metadata becomes available.
   *
   * @return future carrying (error, analytics_meta_data)
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto meta_data() const -> std::future<std::pair<error, analytics_meta_data>>;

  /**
   * Cancels the stream and closes the underlying HTTP connection.
   *
   * Simply dropping the last handle before the stream is fully drained also tears the stream down,
   * but if a pull is in flight the underlying connection is not released until that pull settles
   * (up to the inter-read idle timeout). Call cancel() explicitly for prompt, deterministic
   * teardown of the connection and its timers.
   *
   * @since 1.4.0
   * @volatile
   */
  void cancel() const;

  /**
   * Sentinel returned by @ref end(). An @ref iterator compares equal to it once the stream has been
   * fully drained (after any terminal error element). Using a distinct end type means iterators are
   * only ever compared against the end sentinel, never against one another.
   *
   * @since 1.4.0
   * @volatile
   */
  struct end_sentinel {
  };

  /**
   * A single-pass input iterator that synchronously fetches rows one at a time.
   *
   * Dereferencing yields `std::pair<error, analytics_row>`: for a data row the error is falsy; if
   * the stream ends with an error, the iterator visits exactly one final element whose error is
   * truthy (and whose row is empty) before comparing equal to @ref end(). A clean end-of-stream
   * yields no such element. This guarantees a `for (auto [err, row] : result)` loop can observe a
   * terminal error rather than silently stopping.
   *
   * This is a minimal single-pass iterator intended for range-based for and manual
   * `while (it != result.end())` loops. It compares only against @ref end() (never against another
   * iterator), and provides neither `operator->` nor post-increment, so it is deliberately not a
   * `LegacyInputIterator` and does not work with `<algorithm>` or `std::distance`. @ref begin()
   * must be called once.
   *
   * @since 1.4.0
   * @volatile
   */
  class iterator
  {
  public:
    auto operator*() const -> std::pair<error, analytics_row>;
    auto operator++() -> iterator&;

    /**
     * Constructs an iterator over an internal result; prefer @ref begin().
     *
     * @since 1.4.0
     * @internal
     */
    explicit iterator(std::shared_ptr<internal_analytics_stream_result> internal);

    /**
     * Compares an iterator against the end sentinel. Provided in both argument orders (and as
     * both @c == and @c !=) so the range-for idiom, `while (it != result.end())` loops, and wrapper
     * code all work regardless of which operand is written first. Iterators are intentionally never
     * comparable to one another: a single-pass stream has one position, so iterator-to-iterator
     * equality would be meaningless — the type only ever compares against @ref end().
     */
    friend auto operator==(const iterator& it, end_sentinel /* end */) -> bool
    {
      return it.terminal_;
    }
    friend auto operator==(end_sentinel end, const iterator& it) -> bool
    {
      return it == end;
    }
    friend auto operator!=(const iterator& it, end_sentinel end) -> bool
    {
      return !(it == end);
    }
    friend auto operator!=(end_sentinel end, const iterator& it) -> bool
    {
      return !(it == end);
    }

    // Intentionally no std::iterator_traits typedefs (notably no iterator_category). This is a
    // deliberately minimal single-pass iterator for range-based for and `while (it !=
    // result.end())` loops only (see the class doc); it omits post-increment and operator->.
    // Declaring an input_iterator_tag while omitting those would falsely advertise
    // LegacyInputIterator / std::input_iterator conformance and could break generic code that
    // trusts iterator_traits. Range-based for needs none of these typedefs.

  private:
    void fetch_item();

    std::shared_ptr<internal_analytics_stream_result> internal_{};
    std::pair<error, analytics_row> item_{};
    bool terminal_{ false };
    // Set when fetch_item() lands on a terminal error: the error element is delivered once (the
    // iterator is not yet equal to end()), and the next increment advances to true end.
    bool error_pending_{ false };
  };

  /**
   * Returns an iterator to the beginning.
   *
   * @return iterator to the first row
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto begin() const -> iterator;

  /**
   * Returns the end sentinel.
   *
   * @return sentinel that an @ref iterator compares equal to once the stream is drained
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto end() const -> end_sentinel;

private:
  std::shared_ptr<internal_analytics_stream_result> internal_{};
};
} // namespace couchbase
