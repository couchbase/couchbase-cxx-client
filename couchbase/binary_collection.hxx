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

#include <couchbase/append_options.hxx>
#include <couchbase/prepend_options.hxx>

#include <future>
#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase::core
{
class cluster;
} // namespace couchbase::core
#endif

namespace couchbase
{
class collection;

/**
 * Allows to perform certain operations on non-JSON documents.
 *
 * @since 1.0.0
 * @committed
 */
class binary_collection
{
  public:
    /**
     * Returns name of the bucket where the collection is defined.
     *
     * @return name of the bucket
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket_name() const noexcept -> const std::string&
    {
        return bucket_name_;
    }

    /**
     * Returns name of the scope where the collection is defined.
     *
     * @return name of the scope
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto scope_name() const noexcept -> const std::string&
    {
        return scope_name_;
    }

    /**
     * Returns name of the collection.
     *
     * @return name of the collection
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto name() const noexcept -> const std::string&
    {
        return name_;
    }

    template<typename Handler>
    void append(std::string document_id, std::vector<std::byte> data, const append_options& options, Handler&& handler) const
    {
        return core::impl::initiate_append_operation(core_,
                                                     bucket_name_,
                                                     scope_name_,
                                                     name_,
                                                     std::move(document_id),
                                                     std::move(data),
                                                     options.build(),
                                                     std::forward<Handler>(handler));
    }

    [[nodiscard]] auto append(std::string document_id, std::vector<std::byte> data, const append_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        append(std::move(document_id), std::move(data), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Handler>
    void prepend(std::string document_id, std::vector<std::byte> data, const prepend_options& options, Handler&& handler) const
    {
        return core::impl::initiate_prepend_operation(core_,
                                                      bucket_name_,
                                                      scope_name_,
                                                      name_,
                                                      std::move(document_id),
                                                      std::move(data),
                                                      options.build(),
                                                      std::forward<Handler>(handler));
    }

    [[nodiscard]] auto prepend(std::string document_id, std::vector<std::byte> data, const prepend_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        prepend(std::move(document_id), std::move(data), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

  private:
    friend class collection;

    /**
     * @param core
     * @param bucket_name
     * @param scope_name
     * @param name
     *
     * @since 1.0.0
     * @internal
     */
    binary_collection(std::shared_ptr<couchbase::core::cluster> core,
                      std::string_view bucket_name,
                      std::string_view scope_name,
                      std::string_view name)
      : core_(std::move(core))
      , bucket_name_(bucket_name)
      , scope_name_(scope_name)
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
    std::string bucket_name_;
    std::string scope_name_;
    std::string name_;
};
} // namespace couchbase
