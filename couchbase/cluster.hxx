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

#include <couchbase/bucket.hxx>
#include <couchbase/query_index_manager.hxx>

#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase::core
{
class cluster;
} // namespace couchbase::core
#endif

namespace couchbase
{
/**
 * The {@link cluster} is the main entry point when connecting to a Couchbase cluster.
 *
 * @since 1.0.0
 * @committed
 */
class cluster
{
  public:
    /**
     * Wraps low-level implementation of the SDK to provide common API.
     *
     * @param core pointer to the low-level SDK handle
     *
     * @since 1.0.0
     * @volatile
     */
    explicit cluster(std::shared_ptr<couchbase::core::cluster> core)
      : core_(std::move(core))
    {
    }

    /**
     * Opens a {@link bucket} with the given name.
     *
     * @param bucket_name the name of the bucket to open.
     * @return a {@link bucket} once opened.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket(std::string_view bucket_name) const -> bucket
    {
        return { core_, bucket_name };
    }

    /**
     * Provides access to the N1QL index management services.
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto query_indexes() const -> query_index_manager
    {
        return query_index_manager{ core_ };
    }

  private:
    std::shared_ptr<couchbase::core::cluster> core_;
};
} // namespace couchbase
