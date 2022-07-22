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

#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase
{
class cluster;
} // namespace couchbase
#endif

namespace couchbase
{
/**
 * The scope identifies a group of collections and allows high application density as a result.
 *
 * @since 1.0.0
 */
class scope
{
  public:
    /**
     * Constant for the name of the default scope in the bucket.
     *
     * @since 1.0.0
     * @committed
     */
    static constexpr auto default_name{ "_default" };

    /**
     * Returns name of the bucket where the scope is defined.
     *
     * @return name of the bucket
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket_name() const -> const std::string&
    {
        return bucket_name_;
    }

    /**
     * Returns name of the scope.
     *
     * @return name of the scope
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto name() const -> const std::string&
    {
        return name_;
    }

    /**
     * Opens a collection for this scope with an explicit name.
     *
     * @param collection_name the collection name.
     * @return the requested collection if successful.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto collection(std::string_view collection_name) const -> collection
    {
        return { core_, bucket_name_, name_, collection_name };
    }

  private:
    friend class bucket;

    scope(std::shared_ptr<couchbase::core::cluster> core, std::string_view bucket_name, std::string_view name)
      : core_(std::move(core))
      , bucket_name_(bucket_name)
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
    std::string bucket_name_;
    std::string name_;
};
} // namespace couchbase
