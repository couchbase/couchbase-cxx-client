/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <cstddef>

namespace couchbase
{
/**
 * Replica to read from, where @ref replica_index::first is the first replica of
 * the vbucket. The active copy cannot be selected.
 *
 * @since 1.5.0
 * @volatile
 */
enum class replica_index {
  first = 0,
  second = 1,
  third = 2,
};

/**
 * Options for @ref get_replica_strategy::from_index().
 *
 * @since 1.5.0
 * @volatile
 */
struct get_replica_strategy_from_index_options {
  /**
   * Immutable value object representing consistent options.
   *
   * @since 1.5.0
   * @internal
   */
  struct built {
    bool wrap;
    bool revalidate_on_retry;
  };

  /**
   * Resolve an index the current topology has no replica for, or one the
   * vbucket map does not place on a node the topology lists, to the next
   * replica that is placed, instead of failing with
   * @ref errc::key_value::replica_index_out_of_bounds or
   * @ref errc::key_value::replica_index_currently_unavailable.
   *
   * Resolution follows the vbucket map, not node health: a replica the map
   * places on a node is selected even when that node cannot be reached, and the
   * operation retries against it until its timeout rather than moving on to
   * another replica.
   *
   * @param value whether to wrap around the available replicas
   * @return this options builder for chaining purposes.
   *
   * @since 1.5.0
   * @volatile
   */
  auto wrap(bool value) -> get_replica_strategy_from_index_options&
  {
    wrap_ = value;
    return *this;
  }

  /**
   * Re-resolve the requested index against the current topology on every retry
   * attempt, so that a replica chain changed by a rebalance is honoured. When
   * disabled, the replica resolved on the first attempt is kept and the
   * operation waits for it until the timeout expires.
   *
   * This option is specific to this SDK. RFC-0053 defines only @ref wrap() for
   * a from-index strategy, so code written against it is not portable to the
   * other Couchbase SDKs, and the default leaves the RFC behaviour in place.
   *
   * @param value whether to re-resolve the index on retry
   * @return this options builder for chaining purposes.
   *
   * @since 1.5.0
   * @uncommitted
   */
  auto revalidate_on_retry(bool value) -> get_replica_strategy_from_index_options&
  {
    revalidate_on_retry_ = value;
    return *this;
  }

  /**
   * Validates options and returns them as an immutable value.
   *
   * @return consistent options as an immutable value
   *
   * @since 1.5.0
   * @internal
   */
  [[nodiscard]] auto build() const -> built
  {
    return { wrap_, revalidate_on_retry_ };
  }

private:
  bool wrap_{ false };
  bool revalidate_on_retry_{ true };
};

/**
 * Selects the replica that @ref collection#get_replica() reads from.
 *
 * @since 1.5.0
 * @volatile
 */
class get_replica_strategy
{
public:
  /**
   * Read from the replica at the given index.
   *
   * @param index the replica to read from
   * @param options the custom options
   * @return the strategy to pass to @ref collection#get_replica()
   *
   * @since 1.5.0
   * @volatile
   */
  [[nodiscard]] static auto from_index(replica_index index,
                                       const get_replica_strategy_from_index_options& options = {})
    -> get_replica_strategy
  {
    return get_replica_strategy{ static_cast<std::size_t>(index), options.build() };
  }

  /**
   * Immutable value object representing a consistent strategy.
   *
   * @since 1.5.0
   * @internal
   */
  struct built {
    std::size_t replica_index;
    bool wrap;
    bool revalidate_on_retry;
  };

  /**
   * Returns the strategy as an immutable value.
   *
   * @return the strategy as an immutable value
   *
   * @since 1.5.0
   * @internal
   */
  [[nodiscard]] auto build() const -> built
  {
    return built_;
  }

private:
  get_replica_strategy(std::size_t index,
                       const get_replica_strategy_from_index_options::built& options)
    : built_{ index, options.wrap, options.revalidate_on_retry }
  {
  }

  built built_;
};
} // namespace couchbase
