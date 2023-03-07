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

#include <couchbase/binary_collection.hxx>
#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/collection_query_index_manager.hxx>
#include <couchbase/exists_options.hxx>
#include <couchbase/expiry.hxx>
#include <couchbase/get_all_replicas_options.hxx>
#include <couchbase/get_and_lock_options.hxx>
#include <couchbase/get_and_touch_options.hxx>
#include <couchbase/get_any_replica_options.hxx>
#include <couchbase/get_options.hxx>
#include <couchbase/insert_options.hxx>
#include <couchbase/lookup_in_options.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_options.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/remove_options.hxx>
#include <couchbase/replace_options.hxx>
#include <couchbase/touch_options.hxx>
#include <couchbase/unlock_options.hxx>
#include <couchbase/upsert_options.hxx>

#include <fmt/format.h>
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
class bucket;
class scope;

/**
 * The {@link collection} provides access to all collection APIs.
 *
 * @since 1.0.0
 * @committed
 */
class collection
{
  public:
    /**
     * Constant for the name of the default collection in the bucket.
     *
     * @since 1.0.0
     * @committed
     */
    static constexpr auto default_name{ "_default" };

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
     * Provides access to the binary APIs, not used for JSON documents.
     *
     * @return the requested collection if successful.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto binary() const -> binary_collection
    {
        return { core_, bucket_name_, scope_name_, name_ };
    }

    /**
     * Fetches the full document from this collection.
     *
     * @tparam Handler callable type that implements @ref get_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options options to customize the get request.
     * @param handler the handler that implements @ref get_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get(std::string document_id, const get_options& options, Handler&& handler) const
    {
        return core::impl::initiate_get_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Fetches the full document from this collection.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options options to customize the get request.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get(std::string document_id, const get_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
        auto future = barrier->get_future();
        get(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Fetches a full document and resets its expiration time to the value provided.
     *
     * @tparam Handler callable type that implements @ref get_and_touch_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param duration the new expiration time for the document.
     * @param options custom options to change the default behavior.
     * @param handler the handler that implements @ref get_and_touch_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get_and_touch(std::string document_id,
                       std::chrono::seconds duration,
                       const get_and_touch_options& options,
                       Handler&& handler) const
    {
        return core::impl::initiate_get_and_touch_operation(core_,
                                                            bucket_name_,
                                                            scope_name_,
                                                            name_,
                                                            std::move(document_id),
                                                            core::impl::expiry_relative(duration),
                                                            options.build(),
                                                            std::forward<Handler>(handler));
    }

    /**
     * Fetches a full document and resets its expiration time to the value provided.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param duration the new expiration time for the document.
     * @param options custom options to change the default behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get_and_touch(std::string document_id,
                                     std::chrono::seconds duration,
                                     const get_and_touch_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
        auto future = barrier->get_future();
        get_and_touch(std::move(document_id), duration, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Fetches a full document and resets its expiration time to the absolute value provided.
     *
     * @tparam Handler callable type that implements @ref get_and_touch_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param time_point the new expiration time point for the document.
     * @param options custom options to change the default behavior.
     * @param handler the handler that implements @ref get_and_touch_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get_and_touch(std::string document_id,
                       std::chrono::system_clock::time_point time_point,
                       const get_and_touch_options& options,
                       Handler&& handler) const
    {
        return core::impl::initiate_get_and_touch_operation(core_,
                                                            bucket_name_,
                                                            scope_name_,
                                                            name_,
                                                            std::move(document_id),
                                                            core::impl::expiry_absolute(time_point),
                                                            options.build(),
                                                            std::forward<Handler>(handler));
    }

    /**
     * Fetches a full document and resets its expiration time to the absolute value provided.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param time_point the new expiration time point for the document.
     * @param options custom options to change the default behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get_and_touch(std::string document_id,
                                     std::chrono::system_clock::time_point time_point,
                                     const get_and_touch_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
        auto future = barrier->get_future();
        get_and_touch(std::move(document_id), time_point, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Updates the expiration a document given an id, without modifying or returning its value.
     *
     * @tparam Handler callable type that implements @ref touch_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param duration the new expiration time for the document.
     * @param options custom options to change the default behavior.
     * @param handler the handler that implements @ref touch_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void touch(std::string document_id, std::chrono::seconds duration, const touch_options& options, Handler&& handler) const
    {
        return core::impl::initiate_touch_operation(core_,
                                                    bucket_name_,
                                                    scope_name_,
                                                    name_,
                                                    std::move(document_id),
                                                    core::impl::expiry_relative(duration),
                                                    options.build(),
                                                    std::forward<Handler>(handler));
    }

    /**
     * Updates the expiration a document given an id, without modifying or returning its value.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param duration the new expiration time for the document.
     * @param options custom options to change the default behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto touch(std::string document_id, std::chrono::seconds duration, const touch_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, result>>>();
        auto future = barrier->get_future();
        touch(std::move(document_id), duration, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Updates the expiration a document given an id, without modifying or returning its value.
     *
     * @tparam Handler callable type that implements @ref touch_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param time_point the new expiration time point for the document.
     * @param options custom options to change the default behavior.
     * @param handler the handler that implements @ref touch_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void touch(std::string document_id,
               std::chrono::system_clock::time_point time_point,
               const touch_options& options,
               Handler&& handler) const
    {
        return core::impl::initiate_touch_operation(core_,
                                                    bucket_name_,
                                                    scope_name_,
                                                    name_,
                                                    std::move(document_id),
                                                    core::impl::expiry_absolute(time_point),
                                                    options.build(),
                                                    std::forward<Handler>(handler));
    }

    /**
     * Updates the expiration a document given an id, without modifying or returning its value.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param time_point the new expiration time point for the document.
     * @param options custom options to change the default behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto touch(std::string document_id,
                             std::chrono::system_clock::time_point time_point,
                             const touch_options& options = {}) const -> std::future<std::pair<key_value_error_context, result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, result>>>();
        auto future = barrier->get_future();
        touch(std::move(document_id), time_point, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Reads all available replicas, and returns the first found.
     *
     * @tparam Handler callable type that implements @ref get_any_replica_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options the custom options
     * @param handler the handler that implements @ref get_any_replica_handler
     *
     * @exception errc::key_value::document_irretrievable
     *    the situation where the SDK got all responses (most likely: key not found) but none of them were successful so it
     *    ended up not returning anything
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get_any_replica(std::string document_id, const get_any_replica_options& options, Handler&& handler) const
    {
        return core::impl::initiate_get_any_replica_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Reads all available replicas, and returns the first found.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options the custom options
     * @return the future object with first available result, might be the active or a replica.
     *
     * @exception errc::key_value::document_irretrievable
     *    the situation where the SDK got all responses (most likely: key not found) but none of them were successful so it
     *    ended up not returning anything
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get_any_replica(std::string document_id, const get_any_replica_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_replica_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_replica_result>>>();
        auto future = barrier->get_future();
        get_any_replica(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Reads from all available replicas and the active node and returns the results as a vector.
     *
     * @note Individual errors are ignored, so you can think of this API as a best effort
     * approach which explicitly emphasises availability over consistency.
     *
     * @tparam Handler callable type that implements @ref get_all_replicas_handler signature
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options the custom options
     * @param handler the handler that implements @ref get_all_replicas_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get_all_replicas(std::string document_id, const get_all_replicas_options& options, Handler&& handler) const
    {
        return core::impl::initiate_get_all_replicas_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Reads from all available replicas and the active node and returns the results as a vector.
     *
     * @note Individual errors are ignored, so you can think of this API as a best effort
     * approach which explicitly emphasises availability over consistency.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options the custom options
     * @return future object that carries result of the operation
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get_all_replicas(std::string document_id, const get_all_replicas_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_all_replicas_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_all_replicas_result>>>();
        auto future = barrier->get_future();
        get_all_replicas(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Upserts a full document which might or might not exist yet with custom options.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     * @tparam Handler type of the handler that implements @ref upsert_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to upsert.
     * @param options custom options to customize the upsert behavior.
     * @param handler callable that implements @ref upsert_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder = codec::default_json_transcoder, typename Document, typename Handler>
    void upsert(std::string document_id, Document document, const upsert_options& options, Handler&& handler) const
    {
        return core::impl::initiate_upsert_operation(core_,
                                                     bucket_name_,
                                                     scope_name_,
                                                     name_,
                                                     std::move(document_id),
                                                     Transcoder::encode(document),
                                                     options.build(),
                                                     std::forward<Handler>(handler));
    }

    /**
     * Upserts a full document which might or might not exist yet with custom options.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to upsert.
     * @param options custom options to customize the upsert behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder = codec::default_json_transcoder, typename Document>
    [[nodiscard]] auto upsert(std::string document_id, const Document& document, const upsert_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        upsert<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Inserts a full document which does not exist yet with custom options.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     * @tparam Handler type of the handler that implements @ref insert_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to insert.
     * @param options custom options to customize the insert behavior.
     * @param handler callable that implements @ref insert_handler
     *
     * @exception errc::key_value::document_exists the given document id is already present in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder = codec::default_json_transcoder, typename Document, typename Handler>
    void insert(std::string document_id, Document document, const insert_options& options, Handler&& handler) const
    {
        return core::impl::initiate_insert_operation(core_,
                                                     bucket_name_,
                                                     scope_name_,
                                                     name_,
                                                     std::move(document_id),
                                                     Transcoder::encode(document),
                                                     options.build(),
                                                     std::forward<Handler>(handler));
    }

    /**
     * Inserts a full document which does not exist yet with custom options.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to insert.
     * @param options custom options to customize the insert behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_exists the given document id is already present in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder = codec::default_json_transcoder, typename Document>
    [[nodiscard]] auto insert(std::string document_id, const Document& document, const insert_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        insert<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Replaces a full document which already exists.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     * @tparam Handler type of the handler that implements @ref replace_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to replace.
     * @param options custom options to customize the replace behavior.
     * @param handler callable that implements @ref replace_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder = codec::default_json_transcoder, typename Document, typename Handler>
    void replace(std::string document_id, Document document, const replace_options& options, Handler&& handler) const
    {
        return core::impl::initiate_replace_operation(core_,
                                                      bucket_name_,
                                                      scope_name_,
                                                      name_,
                                                      std::move(document_id),
                                                      Transcoder::encode(document),
                                                      options.build(),
                                                      std::forward<Handler>(handler));
    }

    /**
     * Replaces a full document which already exists.
     *
     * @tparam Transcoder type of the transcoder that will be used to encode the document
     * @tparam Document type of the document
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param document the document content to replace.
     * @param options custom options to customize the replace behavior.
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
    template<typename Transcoder = codec::default_json_transcoder, typename Document>
    [[nodiscard]] auto replace(std::string document_id, const Document& document, const replace_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        replace<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Removes a Document from a collection.
     *
     * @tparam Handler type of the handler that implements @ref remove_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the remove behavior.
     * @param handler callable that implements @ref remove_handler
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
    void remove(std::string document_id, const remove_options& options, Handler&& handler) const
    {
        return core::impl::initiate_remove_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Removes a Document from a collection.
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param options custom options to customize the remove behavior.
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
    [[nodiscard]] auto remove(std::string document_id, const remove_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        remove(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Performs mutations to document fragments
     *
     * @tparam Handler type of the handler that implements @ref mutate_in_handler
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param specs the spec which specifies the type of mutations to perform.
     * @param options custom options to customize the mutate_in behavior.
     * @param handler callable that implements @ref mutate_in_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::key_value::document_exists the given document id is already present in the collection and insert is was selected.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void mutate_in(std::string document_id, mutate_in_specs specs, const mutate_in_options& options, Handler&& handler) const
    {
        return core::impl::initiate_mutate_in_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), specs.specs(), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Performs mutations to document fragments
     *
     * @param document_id the document id which is used to uniquely identify it.
     * @param specs the spec which specifies the type of mutations to perform.
     * @param options custom options to customize the mutate_in behavior.
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::key_value::document_exists the given document id is already present in the collection and insert is was selected.
     * @exception errc::common::cas_mismatch if the document has been concurrently modified on the server.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto mutate_in(std::string document_id, mutate_in_specs specs, const mutate_in_options& options = {}) const
      -> std::future<std::pair<subdocument_error_context, mutate_in_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, mutate_in_result>>>();
        auto future = barrier->get_future();
        mutate_in(std::move(document_id), std::move(specs), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Performs lookups to document fragments with default options.
     *
     * @tparam Handler type of the handler that implements @ref lookup_in_handler
     *
     * @param document_id the outer document ID
     * @param specs an object that specifies the types of lookups to perform
     * @param options custom options to modify the lookup options
     * @param handler callable that implements @ref lookup_in_handler
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void lookup_in(std::string document_id, lookup_in_specs specs, const lookup_in_options& options, Handler&& handler) const
    {
        return core::impl::initiate_lookup_in_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), specs.specs(), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Performs lookups to document fragments with default options.
     *
     * @param document_id the outer document ID
     * @param specs an object that specifies the types of lookups to perform
     * @param options custom options to modify the lookup options
     * @return future object that carries result of the operation
     *
     * @exception errc::key_value::document_not_found the given document id is not found in the collection.
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto lookup_in(std::string document_id, lookup_in_specs specs, const lookup_in_options& options = {}) const
      -> std::future<std::pair<subdocument_error_context, lookup_in_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<subdocument_error_context, lookup_in_result>>>();
        auto future = barrier->get_future();
        lookup_in(std::move(document_id), std::move(specs), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Gets a document for a given id and places a pessimistic lock on it for mutations
     *
     * @tparam Handler type of the handler that implements @ref get_and_lock_handler
     *
     * @param document_id the id of the document
     * @param lock_duration the length of time the lock will be held on the document
     * @param options the options to customize
     * @param handler callable that implements @ref get_and_lock_handler
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void get_and_lock(std::string document_id,
                      std::chrono::seconds lock_duration,
                      const get_and_lock_options& options,
                      Handler&& handler) const
    {
        return core::impl::initiate_get_and_lock_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), lock_duration, options.build(), std::forward<Handler>(handler));
    }

    /**
     * Gets a document for a given id and places a pessimistic lock on it for mutations
     *
     * @param document_id the id of the document
     * @param lock_duration the length of time the lock will be held on the document
     * @param options the options to customize
     * @return future object that carries result of the operation
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto get_and_lock(std::string document_id,
                                    std::chrono::seconds lock_duration,
                                    const get_and_lock_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, get_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_result>>>();
        auto future = barrier->get_future();
        get_and_lock(std::move(document_id), lock_duration, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Unlocks a document if it has been locked previously, with default options.
     *
     * @tparam Handler type of the handler that implements @ref unlock_handler
     *
     * @param document_id the id of the document
     * @param cas the CAS value which is needed to unlock it
     * @param options the options to customize
     * @param handler callable that implements @ref unlock_handler
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
    void unlock(std::string document_id, couchbase::cas cas, const unlock_options& options, Handler&& handler) const
    {
        return core::impl::initiate_unlock_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), cas, options.build(), std::forward<Handler>(handler));
    }

    /**
     * Unlocks a document if it has been locked previously, with default options.
     *
     * @param document_id the id of the document
     * @param cas the CAS value which is needed to unlock it
     * @param options the options to customize
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
    [[nodiscard]] auto unlock(std::string document_id, couchbase::cas cas, const unlock_options& options = {}) const
      -> std::future<key_value_error_context>
    {
        auto barrier = std::make_shared<std::promise<key_value_error_context>>();
        auto future = barrier->get_future();
        unlock(std::move(document_id), cas, options, [barrier](auto ctx) { barrier->set_value({ std::move(ctx) }); });
        return future;
    }

    /**
     * Checks if the document exists on the server.
     *
     * @tparam Handler type of the handler that implements @ref exists_handler
     *
     * @param document_id the id of the document
     * @param options the options to customize
     * @param handler callable that implements @ref exists_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void exists(std::string document_id, const exists_options& options, Handler&& handler) const
    {
        return core::impl::initiate_exists_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    /**
     * Checks if the document exists on the server.
     *
     * @param document_id the id of the document
     * @param options the options to customize
     * @return future object that carries result of the operation
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto exists(std::string document_id, const exists_options& options = {}) const
      -> std::future<std::pair<key_value_error_context, exists_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, exists_result>>>();
        auto future = barrier->get_future();
        exists(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    [[nodiscard]] auto query_indexes() const -> collection_query_index_manager
    {
        return collection_query_index_manager(core_, bucket_name_, scope_name_, name_);
    }

  private:
    friend class bucket;
    friend class scope;

    /**
     * @param core
     * @param bucket_name
     * @param scope_name
     * @param name
     *
     * @since 1.0.0
     * @internal
     */
    collection(std::shared_ptr<couchbase::core::cluster> core,
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
