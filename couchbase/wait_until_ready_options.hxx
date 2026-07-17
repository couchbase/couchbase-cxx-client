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

#include <couchbase/cluster_state.hxx>
#include <couchbase/error.hxx>
#include <couchbase/service_type.hxx>

#include <functional>
#include <set>
#include <utility>

namespace couchbase
{
/**
 * Options for @ref cluster::wait_until_ready() and @ref bucket::wait_until_ready().
 *
 * @since 1.4.0
 * @committed
 */
struct wait_until_ready_options {
  /**
   * Sets the state the cluster (or bucket) has to reach before the wait completes.
   *
   * @note @c cluster_state::offline is not a valid target and is rejected with @c
   * errc::common::invalid_argument.
   *
   * @param desired_state the target state.
   * @return reference to this object, for use in chaining.
   *
   * @since 1.4.0
   * @committed
   */
  auto desired_state(cluster_state desired_state) -> wait_until_ready_options&
  {
    desired_state_ = desired_state;
    return *this;
  }

  /**
   * Customizes the set of services to consider. When empty, whichever services the topology reports
   * are considered.
   *
   * @param service_types the services to consider.
   * @return reference to this object, for use in chaining.
   *
   * @since 1.4.0
   * @committed
   */
  auto service_types(std::set<service_type> service_types) -> wait_until_ready_options&
  {
    service_types_ = std::move(service_types);
    return *this;
  }

  /**
   * Immutable value object representing consistent options.
   *
   * @since 1.4.0
   * @internal
   */
  struct built {
    cluster_state desired_state;
    std::set<service_type> service_types;
  };

  /**
   * Validates the options and returns them as an immutable value.
   *
   * @return consistent options as an immutable value
   *
   * @exception std::system_error with code errc::common::invalid_argument if the options are not
   * valid
   *
   * @since 1.4.0
   * @internal
   */
  [[nodiscard]] auto build() const -> built
  {
    return { desired_state_, service_types_ };
  }

private:
  cluster_state desired_state_{ cluster_state::online };
  std::set<service_type> service_types_{};
};

/**
 * The signature for the handler of the @c cluster::wait_until_ready() and @c
 * bucket::wait_until_ready() operations.
 *
 * @since 1.4.0
 * @committed
 */
using wait_until_ready_handler = std::function<void(error)>;
} // namespace couchbase
