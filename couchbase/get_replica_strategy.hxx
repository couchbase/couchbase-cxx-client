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
 * @committed
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
 * @committed
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
  };

  /**
   * Resolve an index that cannot be read as requested to the next one that can,
   * instead of failing with @ref errc::key_value::replica_index_out_of_bounds or
   * @ref errc::key_value::replica_index_currently_unavailable.
   *
   * The search walks the bucket's configured replica count, starting at the
   * requested index modulo that count. An index beyond the vbucket map's
   * current row therefore still reaches the replicas the row does list.
   *
   * Two failures remain. A bucket configured for no replicas reports
   * @ref errc::key_value::replica_index_out_of_bounds. A full lap that finds
   * nothing readable reports
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
   * @committed
   */
  auto wrap(bool value) -> get_replica_strategy_from_index_options&
  {
    wrap_ = value;
    return *this;
  }

  /**
   * Returns the options as an immutable value.
   *
   * @return the options as an immutable value
   *
   * @since 1.5.0
   * @internal
   */
  [[nodiscard]] auto build() const -> built
  {
    return { wrap_ };
  }

private:
  bool wrap_{ false };
};

/**
 * Selects the replica that @ref collection#get_replica() reads from.
 *
 * @since 1.5.0
 * @committed
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
   * @committed
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
    : built_{ index, options.wrap }
  {
  }

  built built_;
};
} // namespace couchbase
