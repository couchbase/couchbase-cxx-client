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
#include <couchbase/scope.hxx>

#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase::core
{
class cluster;
} // namespace couchbase::core
#endif

namespace couchbase
{
class cluster;

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
    [[nodiscard]] auto default_scope() const -> scope
    {
        return { core_, name_, scope::default_name };
    }

    /**
     * Opens the default collection for this bucket using the default scope.
     *
     * @return the opened default {@link collection}.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto default_collection() const -> collection
    {
        return { core_, name_, scope::default_name, collection::default_name };
    }

    /**
     * Opens the {@link scope} with the given name.
     *
     * @param scope_name the name of the scope.
     * @return the {@link scope} once opened.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto scope(std::string_view scope_name) const -> scope
    {
        return { core_, name_, scope_name };
    }

  private:
    friend class cluster;

    bucket(std::shared_ptr<couchbase::core::cluster> core, std::string_view name)
      : core_(std::move(core))
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
    std::string name_;
};
} // namespace couchbase
