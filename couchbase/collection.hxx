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
#include <couchbase/codec/json_transcoder.hxx>
#include <couchbase/get_all_replicas_options.hxx>
#include <couchbase/get_any_replica_options.hxx>
#include <couchbase/get_options.hxx>
#include <couchbase/insert_options.hxx>
#include <couchbase/mutate_in_options.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/remove_options.hxx>
#include <couchbase/replace_options.hxx>
#include <couchbase/upsert_options.hxx>

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

    template<typename Handler>
    void get(std::string document_id, const get_options& options, Handler&& handler) const
    {
        return core::impl::initiate_get_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    [[nodiscard]] auto get(std::string document_id, const get_options& options) const
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
    [[nodiscard]] auto get_any_replica(std::string document_id, const get_any_replica_options& options) const
      -> std::future<std::pair<key_value_error_context, get_any_replica_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_any_replica_result>>>();
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
    [[nodiscard]] auto get_all_replicas(std::string document_id, const get_all_replicas_options& options) const
      -> std::future<std::pair<key_value_error_context, get_all_replicas_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, get_all_replicas_result>>>();
        auto future = barrier->get_future();
        get_all_replicas(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Transcoder = codec::json_transcoder, typename Document, typename Handler>
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

    template<typename Transcoder = codec::json_transcoder, typename Document>
    [[nodiscard]] auto upsert(std::string document_id, const Document& document, const upsert_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        upsert<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Transcoder = codec::json_transcoder, typename Document, typename Handler>
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

    template<typename Transcoder = codec::json_transcoder, typename Document>
    [[nodiscard]] auto insert(std::string document_id, const Document& document, const insert_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        insert<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Transcoder = codec::json_transcoder, typename Document, typename Handler>
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

    template<typename Transcoder = codec::json_transcoder, typename Document>
    [[nodiscard]] auto replace(std::string document_id, const Document& document, const replace_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        replace<Transcoder>(std::move(document_id), document, options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Handler>
    void remove(std::string document_id, const remove_options& options, Handler&& handler) const
    {
        return core::impl::initiate_remove_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), options.build(), std::forward<Handler>(handler));
    }

    [[nodiscard]] auto remove(std::string document_id, const remove_options& options) const
      -> std::future<std::pair<key_value_error_context, mutation_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutation_result>>>();
        auto future = barrier->get_future();
        remove(std::move(document_id), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    template<typename Handler>
    void mutate_in(std::string document_id, mutate_in_specs specs, const mutate_in_options& options, Handler&& handler) const
    {
        return core::impl::initiate_mutate_in_operation(
          core_, bucket_name_, scope_name_, name_, std::move(document_id), specs.specs(), options.build(), std::forward<Handler>(handler));
    }

    [[nodiscard]] auto mutate_in(std::string document_id, mutate_in_specs specs, const mutate_in_options& options) const
      -> std::future<std::pair<key_value_error_context, mutate_in_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<key_value_error_context, mutate_in_result>>>();
        auto future = barrier->get_future();
        mutate_in(std::move(document_id), std::move(specs), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
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
