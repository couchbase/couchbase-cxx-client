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

#include <couchbase/subdoc/fwd/command_bundle.hxx>

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/common_durability_options.hxx>
#include <couchbase/expiry.hxx>
#include <couchbase/lookup_in_result.hxx>
#include <couchbase/store_semantics.hxx>
#include <couchbase/subdocument_error_context.hxx>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace couchbase
{

/**
 * Options for @ref collection#lookup_in().
 *
 * @since 1.0.0
 * @committed
 */
struct lookup_in_options : public common_durability_options<lookup_in_options> {
    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built : public common_durability_options<lookup_in_options>::built {
        const bool access_deleted;
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
        return { build_common_durability_options(), access_deleted_ };
    }

    /**
     * For internal use only: allows access to deleted documents that are in 'tombstone' form.
     *
     * @since 1.0.0
     * @internal
     */
    auto access_deleted(bool value) -> lookup_in_options&
    {
        access_deleted_ = value;
        return self();
    }

  private:
    bool access_deleted_{ false };
};

/**
 * The signature for the handler of the @ref collection#lookup_in() operation
 *
 * @since 1.0.0
 * @uncommitted
 */
using lookup_in_handler = std::function<void(couchbase::subdocument_error_context, lookup_in_result)>;

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
initiate_lookup_in_operation(std::shared_ptr<couchbase::core::cluster> core,
                             std::string bucket_name,
                             std::string scope_name,
                             std::string collection_name,
                             std::string document_key,
                             const std::vector<couchbase::core::impl::subdoc::command>& specs,
                             lookup_in_options::built options,
                             lookup_in_handler&& handler);
#endif
} // namespace impl
} // namespace core
} // namespace couchbase
