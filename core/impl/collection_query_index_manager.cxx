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

#include <core/query_context.hxx>
#include <couchbase/collection_query_index_manager.hxx>

namespace couchbase
{

void
collection_query_index_manager::get_all_indexes(const get_all_query_indexes_options& options, get_all_query_indexes_handler&& handler) const
{
    return core::impl::initiate_get_all_query_indexes(core_,
                                                      "",
                                                      options.build(),
                                                      core::query_context(bucket_name_, scope_name_),
                                                      collection_name_,
                                                      std::forward<get_all_query_indexes_handler>(handler));
}

void
collection_query_index_manager::create_index(std::string index_name,
                                             std::vector<std::string> fields,
                                             const create_query_index_options& options,
                                             create_query_index_handler&& handler) const
{
    return core::impl::initiate_create_query_index(core_,
                                                   "",
                                                   std::move(index_name),
                                                   std::move(fields),
                                                   options.build(),
                                                   { bucket_name_, scope_name_ },
                                                   collection_name_,
                                                   std::forward<create_query_index_handler>(handler));
}

void
collection_query_index_manager::create_primary_index(const create_primary_query_index_options& options,
                                                     create_query_index_handler&& handler) const
{
    return core::impl::initiate_create_primary_query_index(
      core_, "", options.build(), { bucket_name_, scope_name_ }, collection_name_, std::move(handler));
}

void
collection_query_index_manager::drop_primary_index(const drop_primary_query_index_options& options,
                                                   drop_query_index_handler&& handler) const
{
    return core::impl::initiate_drop_primary_query_index(
      core_, "", options.build(), { bucket_name_, scope_name_ }, collection_name_, std::move(handler));
}
void
collection_query_index_manager::drop_index(std::string index_name,
                                           const drop_query_index_options& options,
                                           drop_query_index_handler&& handler) const
{
    return core::impl::initiate_drop_query_index(
      core_, "", std::move(index_name), options.build(), { bucket_name_, scope_name_ }, collection_name_, std::move(handler));
}
void
collection_query_index_manager::build_deferred_indexes(const build_query_index_options& options,
                                                       build_deferred_query_indexes_handler&& handler) const
{
    return core::impl::initiate_build_deferred_indexes(core_,
                                                       "",
                                                       options.build(),
                                                       { bucket_name_, scope_name_ },
                                                       collection_name_,
                                                       std::forward<build_deferred_query_indexes_handler>(handler));
}
void
collection_query_index_manager::watch_indexes(std::vector<std::string> index_names,
                                              const watch_query_indexes_options& options,
                                              watch_query_indexes_handler&& handler) const
{
    return core::impl::initiate_watch_query_indexes(
      core_, "", std::move(index_names), options.build(), { bucket_name_, scope_name_ }, collection_name_, std::move(handler));
}
} // namespace couchbase
