/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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
#include <couchbase/error_codes.hxx>
#include <couchbase/manager_error_context.hxx>

#include <functional>
#include <optional>
#include <string>

namespace couchbase
{
class drop_dataset_analytics_options : public common_options<drop_dataset_analytics_options>
{
  public:
    /**
     * Ignore error if the dataset does not exist.
     *
     * defaults to `false`
     *
     * @param ignore_if_not_exists
     * @return reference to this object, for use in chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto ignore_if_not_exists(bool ignore_if_not_exists) -> drop_dataset_analytics_options&
    {
        ignore_if_not_exists_ = ignore_if_not_exists;
        return self();
    }

    /**
     * The name of the dataverse the dataset should be dropped from
     *
     * @param dataverse_name
     * @return reference to this object, for use in chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto dataverse_name(std::string dataverse_name) -> drop_dataset_analytics_options&
    {
        dataverse_name_ = std::move(dataverse_name);
        return self();
    }

    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built : public common_options<drop_dataset_analytics_options>::built {
        bool ignore_if_not_exists{};
        std::optional<std::string> dataverse_name{};
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
        return { build_common_options(), ignore_if_not_exists_, dataverse_name_ };
    }

  private:
    bool ignore_if_not_exists_{ false };
    std::optional<std::string> dataverse_name_{};
};

/**
 * The signature for the handler of the @ref analytics_index_manager#drop_dataset() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using drop_dataset_analytics_handler = std::function<void(error)>;
} // namespace couchbase
