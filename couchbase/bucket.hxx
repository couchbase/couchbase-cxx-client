/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/collection.hxx>
#include <couchbase/collection_manager.hxx>
#include <couchbase/ping_options.hxx>
#include <couchbase/scope.hxx>
#include <couchbase/wait_until_ready_options.hxx>

#include <chrono>
#include <memory>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace core
{
class cluster;
} // namespace core
class cluster;
class bucket_impl;
namespace crypto
{
class manager;
} // namespace crypto
#endif

/**
 * Provides access to Couchbase bucket
 *
 * @since 1.0.0
 * @committed
 */
class bucket
{
public:
  /**
   * Opens default {@link scope}.
   *
   * @return the {@link scope} once opened.
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto default_scope() const -> scope;

  /**
   * Opens the default collection for this bucket using the default scope.
   *
   * @return the opened default {@link collection}.
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto default_collection() const -> collection;

  /**
   * Opens the {@link scope} with the given name.
   *
   * @param scope_name the name of the scope.
   * @return the {@link scope} once opened.
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto scope(std::string_view scope_name) const -> scope;

  /**
   * Performs application-level ping requests against services in the Couchbase cluster.
   *
   * @note This operation performs active I/O against services and endpoints to assess their health.
   * If you do not wish to performs I/O, consider using @ref cluster::diagnostics() instead.
   *
   * @param options custom options to change the default behavior.
   * @param handler the handler that implements @ref ping_handler.
   *
   * @since 1.0.0
   * @committed
   */
  void ping(const ping_options& options, ping_handler&& handler) const;

  /**
   * Performs application-level ping requests against services in the Couchbase cluster.
   *
   * @note This operation performs active I/O against services and endpoints to assess their health.
   * If you do not wish to performs I/O, consider using @ref cluster::diagnostics() instead.
   *
   * @param options custom options to change the default behavior.
   * @return future object that carries result of the operation.
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto ping(const ping_options& options = {}) const
    -> std::future<std::pair<error, ping_result>>;

  /**
   * Waits until the desired state of the bucket is reached, actively pinging its services until
   * they respond or the timeout elapses.
   *
   * When the desired state is @c cluster_state::online, this additionally waits until the bucket's
   * vbucket map is fully placed -- every vbucket has its active and all replica copies assigned to
   * a node. That is the readiness durable writes require and that a freshly created bucket does not
   * satisfy immediately. The @c cluster_state::degraded target is ping-only, since it deliberately
   * tolerates partial availability.
   *
   * @param timeout the maximum time to wait for readiness.
   * @param options custom options to change the default behavior.
   * @param handler the handler that implements @ref wait_until_ready_handler.
   *
   * @since 1.4.0
   * @committed
   */
  void wait_until_ready(std::chrono::milliseconds timeout,
                        const wait_until_ready_options& options,
                        wait_until_ready_handler&& handler) const;

  /**
   * Waits until the desired state of the bucket is reached.
   *
   * @see wait_until_ready(std::chrono::milliseconds, const wait_until_ready_options&,
   * wait_until_ready_handler&&) const
   *
   * @param timeout the maximum time to wait for readiness.
   * @param options custom options to change the default behavior.
   * @return future object that carries result of the operation.
   *
   * @since 1.4.0
   * @committed
   */
  [[nodiscard]] auto wait_until_ready(std::chrono::milliseconds timeout,
                                      const wait_until_ready_options& options = {}) const
    -> std::future<error>;

  /**
   * Provides access to the collection management services.
   *
   * @return a manager instance
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto collections() const -> collection_manager;

private:
  friend cluster;

  bucket(core::cluster core, std::string_view name, std::shared_ptr<crypto::manager>);

  std::shared_ptr<bucket_impl> impl_;
};
} // namespace couchbase
