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

#include <couchbase/get_replica_result.hxx>
#include <couchbase/key_value_error_context.hxx>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>

namespace couchbase
{
/**
 * Options for collection#get_any_replica().
 *
 * @since 1.0.0
 * @committed
 */
struct get_any_replica_options {
    /**
     * The time allowed for the operation to be terminated.
     *
     * @since 1.0.0
     * @committed
     */
    std::optional<std::chrono::milliseconds> timeout{};
};

/**
 * The error context for the @ref collection#get_any_replica() operation
 *
 * @since 1.0.0
 * @committed
 */
using get_any_replica_error_context = couchbase::key_value_error_context;

/**
 * The result for the @ref collection#get_any_replica() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using get_any_replica_result = get_replica_result;

/**
 * The signature for the handler of the @ref collection#get_any_replica() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using get_any_replica_handler = std::function<void(get_any_replica_error_context, get_any_replica_result)>;

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
initiate_get_any_replica_operation(std::shared_ptr<couchbase::core::cluster> core,
                                   const std::string& bucket_name,
                                   const std::string& scope_name,
                                   const std::string& collection_name,
                                   std::string document_key,
                                   const get_any_replica_options& options,
                                   get_any_replica_handler&& handler);
#endif
} // namespace impl
} // namespace core
} // namespace couchbase
