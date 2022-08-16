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

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/common_durability_options.hxx>
#include <couchbase/expiry.hxx>
#include <couchbase/key_value_error_context.hxx>
#include <couchbase/mutation_result.hxx>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace couchbase
{

/**
 * Options for @ref binary_collection#append().
 *
 * @since 1.0.0
 * @committed
 */
struct append_options : public common_durability_options<append_options> {
    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built : public common_durability_options<append_options>::built {
        const couchbase::cas cas;
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
        return { build_common_durability_options(), cas_ };
    }

    /**
     * Specifies a CAS value that will be taken into account on the server side for optimistic concurrency.
     *
     * The CAS value is an opaque identifier which is associated with a specific state of the document on the server. The
     * CAS value is received on read operations (or after mutations) and can be used during a subsequent mutation to
     * make sure that the document has not been modified in the meantime.
     *
     * If document on the server has been modified in the meantime the SDK will raise a {@link errc::common::cas_mismatch}. In
     * this case the caller is expected to re-do the whole "fetch-modify-update" cycle again. Please refer to the
     * SDK documentation for more information on CAS mismatches and subsequent retries.
     *
     * @param cas the opaque CAS identifier to use for this operation.
     * @return the {@link append_options} for chaining purposes.
     */
    auto cas(couchbase::cas cas) -> append_options&
    {

        cas_ = cas;
        return self();
    }

  private:
    couchbase::cas cas_{};
};

/**
 * The signature for the handler of the @ref binary_collection#append() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using append_handler = std::function<void(couchbase::key_value_error_context, mutation_result)>;

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
initiate_append_operation(std::shared_ptr<couchbase::core::cluster> core,
                          std::string bucket_name,
                          std::string scope_name,
                          std::string collection_name,
                          std::string document_key,
                          std::vector<std::byte> data,
                          append_options::built options,
                          append_handler&& handler);
#endif
} // namespace impl
} // namespace core
} // namespace couchbase
