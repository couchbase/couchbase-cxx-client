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

#include <couchbase/common_options.hxx>
#include <couchbase/manager_error_context.hxx>

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace couchbase
{
/**
 * Options for query_index_manager#build_deferred_indexes().
 *
 * @since 1.0.0
 * @committed
 */
class build_query_index_options : public common_options<build_query_index_options>
{
  public:
    /**
     * Sets the scope name for this query management operation.
     *
     * Please note that if the scope name is set, the {@link #collection_name} must also be set.
     *
     * @since 1.0.0
     * @committed
     */
    auto scope_name(std::string scope_name) -> build_query_index_options&
    {
        scope_name_.emplace(std::move(scope_name));
        return self();
    }

    /**
     * Sets the collection name for this query management operation.
     *
     * Please note that if the collection name is set, the {@link #scope_name} must also be set.
     *
     * @since 1.0.0
     * @committed
     */
    auto collection_name(std::string collection_name) -> build_query_index_options&
    {
        collection_name_.emplace(std::move(collection_name));
        return self();
    }

    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built : public common_options<build_query_index_options>::built {
        const std::optional<std::string> scope_name{};
        const std::optional<std::string> collection_name{};
    };

    /**
     * Validates options and returns them as an immutable value.
     *
     * @return consistent options as an immutable value
     *
     * @exception std::system_error with code errc::common::invalid_argument if the options are not valid
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto build() const -> built
    {
        if (scope_name_.has_value() && !collection_name_.has_value()) {
            throw std::system_error(errc::common::invalid_argument, "if a scope_name is provided, a collection_name must also be provided");
        }
        if (!scope_name_.has_value() && collection_name_.has_value()) {
            throw std::system_error(errc::common::invalid_argument, "if a collection_name is provided, a scope_name must also be provided");
        }
        return { build_common_options(), scope_name_, collection_name_ };
    }

  private:
    std::optional<std::string> scope_name_{};
    std::optional<std::string> collection_name_{};
};

/**
 * The signature for the handler of the @ref query_index_manager#build_deferred_indexes() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using build_deferred_query_indexes_handler = std::function<void(couchbase::manager_error_context)>;

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace core
{
class cluster;
namespace impl
{

/**
 * @since 1.0.0
 * @internal
 */
void
initiate_build_deferred_indexes(std::shared_ptr<couchbase::core::cluster> resp1,
                                std::string bucket_name,
                                build_query_index_options::built options,
                                build_deferred_query_indexes_handler&& handler);

#endif
} // namespace impl
} // namespace core
} // namespace couchbase
