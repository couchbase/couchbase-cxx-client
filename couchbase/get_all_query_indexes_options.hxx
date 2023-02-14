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
#include <couchbase/error_codes.hxx>
#include <couchbase/management/query_index.hxx>
#include <couchbase/manager_error_context.hxx>

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace couchbase
{
class get_all_query_indexes_options : public common_options<get_all_query_indexes_options>
{
  public:
    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built : public common_options<get_all_query_indexes_options>::built {
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
        return { build_common_options() };
    }

  private:
};

/**
 * The signature for the handler of the @ref query_index_manager#get_all_indexes() operation
 *
 * @since 1.0.0
 * @uncommitted
 */

using get_all_indexes_handler = std::function<void(couchbase::manager_error_context, std::vector<couchbase::management::query::index>)>;

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace core
{
class cluster;
class query_context;
namespace impl
{

/**
 * @since 1.0.0
 * @internal
 */
void
initiate_get_all_query_indexes(std::shared_ptr<couchbase::core::cluster> core,
                               std::string bucket_name,
                               couchbase::get_all_query_indexes_options::built options,
                               get_all_indexes_handler&& handler);

void
initiate_get_all_query_indexes(std::shared_ptr<couchbase::core::cluster> core,
                               std::string bucket_name,
                               couchbase::get_all_query_indexes_options::built options,
                               query_context query_ctx,
                               std::string collection_name,
                               get_all_indexes_handler&& handler);

#endif
} // namespace impl
} // namespace core
} // namespace couchbase