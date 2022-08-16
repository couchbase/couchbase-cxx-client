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
#include <couchbase/decrement_options.hxx>
#include <couchbase/increment_options.hxx>
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

    /**
     * Appends binary content to the document.
     *
     * @tparam Handler type of the handler that implements @ref append_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param data the document content to append.
     * @param options custom options to customize the append behavior.
     * @param handler callable that implements @ref append_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
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

    /**
     * Appends binary content to the document.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param data the document content to append.
     * @param options custom options to customize the append behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
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

    /**
     * Prepends binary content to the document.
     *
     * @tparam Handler type of the handler that implements @ref prepend_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param data the document content to prepend.
     * @param options custom options to customize the prepend behavior.
     * @param handler callable that implements @ref prepend_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
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

    /**
     * Prepends binary content to the document.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param data the document content to prepend.
     * @param options custom options to customize the prepend behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
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

    /**
     * Increments the counter document by one or the number defined in the options.
     *
     * @tparam Handler type of the handler that implements @ref increment_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the increment behavior.
     * @param handler callable that implements @ref increment_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void increment(std::string document_id, const increment_options& options, Handler&& handler) const
    {
        return core::impl::initiate_increment_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Increments the counter document by one or the number defined in the options.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the increment behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto increment(std::string document_id, const increment_options& options) const
      -> std::future<std::pair<key_value_error_context, counter_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, counter_result>>>();
        auto future = barrier->get_future();
        increment(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Decrements the counter document by one or the number defined in the options.
     *
     * @tparam Handler type of the handler that implements @ref decrement_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the decrement behavior.
     * @param handler callable that implements @ref decrement_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void decrement(std::string document_id, const decrement_options& options, Handler&& handler) const
    {
        return core::impl::initiate_decrement_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Decrements the counter document by one or the number defined in the options.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the decrement behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto decrement(std::string document_id, const decrement_options& options) const
      -> std::future<std::pair<key_value_error_context, counter_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, counter_result>>>();
        auto future = barrier->get_future();
        decrement(std::move(document_id), options, [barrier](auto ctx, auto result) {
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
