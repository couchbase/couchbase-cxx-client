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

#include <couchbase/errors.hxx>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace couchbase
{
bool
is_valid_collection_element(std::string_view element);
} // namespace couchbase

namespace couchbase::api
{
/**
 * Represents address of the document in the cluster.
 *
 * The instance of @ref document_id uniquely identifies the document in the cluster.
 *
 * @since 1.0.0
 * @committed
 */
struct document_id {
    /**
     * Constant for the name of the default scope in the bucket.
     */
    static constexpr auto default_scope{ "_default" };
    /**
     * Constant for the name of the default collection in the bucket.
     */
    static constexpr auto default_collection{ "_default" };

    /**
     * Creates the identifier for the document in the default collection of the bucket.
     *
     * @param bucket_name the name of the bucket
     * @param document_key the identifier of the document
     * @param use_collections pass @c false to disable collections support
     *
     * @since 1.0.0
     * @committed
     */
    document_id(std::string bucket_name, std::string document_key, bool use_collections = true)
      : bucket_{ std::move(bucket_name) }
      , key_{ std::move(document_key) }
    {
        // reset scope & collection for legacy servers
        if (!use_collections) {
            scope_.clear();
            collection_.clear();
        }
    }

    /**
     * Creates the identifier for the document in the specified collection_name of the bucket
     *
     * @param bucket_name the name of the bucket
     * @param scope_name the name of the scope
     * @param collection_name the name of the collection
     * @param document_key the identifier of the document
     *
     * @throws std::system_error with invalid_argument if the @c scope_name or @c collection_name name is invalid
     *
     * @since 1.0.0
     * @committed
     */
    document_id(std::string bucket_name, std::string scope_name, std::string collection_name, std::string document_key)
      : bucket_{ std::move(bucket_name) }
      , scope_{ std::move(scope_name) }
      , collection_{ std::move(collection_name) }
      , key_{ std::move(document_key) }
    {
        if (!is_valid_collection_element(scope_)) {
            throw std::system_error(couchbase::error::common_errc::invalid_argument, "invalid scope_name name");
        }
        if (!is_valid_collection_element(collection_)) {
            throw std::system_error(couchbase::error::common_errc::invalid_argument, "invalid collection_name name");
        }
    }

    /**
     * Returns the name of the bucket.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& bucket() const
    {
        return bucket_;
    }

    /**
     * Returns the name of the scope
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& scope() const
    {
        return scope_;
    }

    /**
     * Returns the name of the collection
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& collection() const
    {
        return collection_;
    }

    /**
     * Returns the document identifier (key) in the collection
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& key() const
    {
        return key_;
    }

  private:
    std::string bucket_;
    std::string scope_{ default_scope };
    std::string collection_{ default_collection };
    std::string key_{};
};
} // namespace couchbase::api
