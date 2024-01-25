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

#include <couchbase/allow_querying_search_index_options.hxx>
#include <couchbase/analyze_document_options.hxx>
#include <couchbase/disallow_querying_search_index_options.hxx>
#include <couchbase/drop_search_index_options.hxx>
#include <couchbase/freeze_plan_search_index_options.hxx>
#include <couchbase/get_all_search_indexes_options.hxx>
#include <couchbase/get_indexed_search_index_options.hxx>
#include <couchbase/get_search_index_options.hxx>
#include <couchbase/pause_ingest_search_index_options.hxx>
#include <couchbase/resume_ingest_search_index_options.hxx>
#include <couchbase/unfreeze_plan_search_index_options.hxx>
#include <couchbase/upsert_search_index_options.hxx>

#include <future>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase
{
namespace core
{
class cluster;
} // namespace core
class search_index_manager_impl;
} // namespace couchbase
#endif

namespace couchbase
{
class scope;

class scope_search_index_manager
{
  public:
    /**
     * Fetches a scope-level index from the server if it exists
     *
     * @param index_name the name of the index
     * @param options optional parameters
     * @param handler  handler that implements @ref get_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void get_index(std::string index_name, const get_search_index_options& options, get_search_index_handler&& handler) const;

    [[nodiscard]] auto get_index(std::string index_name, const get_search_index_options& options = {}) const
      -> std::future<std::pair<manager_error_context, management::search::index>>;

    /**
     * Fetches all scope-level indexes from the server
     *
     * @param options optional parameters
     * @param handler  handler that implements @ref get_all_search_indexes_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void get_all_indexes(const get_all_search_indexes_options& options, get_all_search_indexes_handler&& handler) const;

    [[nodiscard]] auto get_all_indexes(const get_all_search_indexes_options& options = {}) const
      -> std::future<std::pair<manager_error_context, std::vector<management::search::index>>>;

    /**
     * Creates, or updates a scope-level index
     *
     * @param search_index the index definition including name and settings
     * @param options optional parameters
     * @param handler  handler that implements @ref upsert_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void upsert_index(const management::search::index& search_index,
                      const upsert_search_index_options& options,
                      upsert_search_index_handler&& handler) const;

    [[nodiscard]] auto upsert_index(const management::search::index& search_index, const upsert_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Drops a scope-level index
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref drop_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void drop_index(std::string index_name, const drop_search_index_options& options, drop_search_index_handler&& handler) const;

    [[nodiscard]] auto drop_index(std::string index_name, const drop_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Retrieves the number of documents that have been indexed for a scope-level index
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref get_indexed_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void get_indexed_documents_count(std::string index_name,
                                     const get_indexed_search_index_options& options,
                                     get_indexed_search_index_handler&& handler) const;

    [[nodiscard]] auto get_indexed_documents_count(std::string index_name, const get_indexed_search_index_options& options = {}) const
      -> std::future<std::pair<manager_error_context, std::uint64_t>>;

    /**
     * Pauses updates and maintenance for a scope-level index.
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref pause_ingest_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void pause_ingest(std::string index_name,
                      const pause_ingest_search_index_options& options,
                      pause_ingest_search_index_handler&& handler) const;

    [[nodiscard]] auto pause_ingest(std::string index_name, const pause_ingest_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Resumes updates and maintenance for a scope-level index.
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref resume_ingest_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void resume_ingest(std::string index_name,
                       const resume_ingest_search_index_options& options,
                       resume_ingest_search_index_handler&& handler) const;

    [[nodiscard]] auto resume_ingest(std::string index_name, const resume_ingest_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Allows querying against a scope-level index
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref allow_querying_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void allow_querying(std::string index_name,
                        const allow_querying_search_index_options& options,
                        allow_querying_search_index_handler&& handler) const;

    [[nodiscard]] auto allow_querying(std::string index_name, const allow_querying_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Disallows querying against a scope-level index
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref disallow_querying_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void disallow_querying(std::string index_name,
                           const disallow_querying_search_index_options& options,
                           disallow_querying_search_index_handler&& handler) const;

    [[nodiscard]] auto disallow_querying(std::string index_name, const disallow_querying_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Freeze the assignment of scope-level index partitions to nodes.
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref  freeze_plan_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void freeze_plan(std::string index_name,
                     const freeze_plan_search_index_options& options,
                     freeze_plan_search_index_handler&& handler) const;

    [[nodiscard]] auto freeze_plan(std::string index_name, const freeze_plan_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Unfreeze the assignment of scope-level index partitions to nodes.
     *
     * @param index_name the name of the search index
     * @param options optional parameters
     * @param handler  handler that implements @ref  unfreeze_plan_search_index_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void unfreeze_plan(std::string index_name,
                       const unfreeze_plan_search_index_options& options,
                       unfreeze_plan_search_index_handler&& handler) const;

    [[nodiscard]] auto unfreeze_plan(std::string index_name, const unfreeze_plan_search_index_options& options = {}) const
      -> std::future<manager_error_context>;

    /**
     * Allows to see how a document is analyzed against a specific scope-level index.
     *
     * @param index_name the name of the search index
     * @param document the document to be analyzed
     * @param options optional parameters
     * @param handler  handler that implements @ref analyze_document_handler
     *
     * @since 1.0.0
     * @volatile
     */
    template<typename Document>
    void analyze_document(std::string index_name,
                          Document document,
                          const analyze_document_options& options,
                          analyze_document_handler&& handler) const
    {
        auto encoded = couchbase::codec::default_json_transcoder::encode(document); // Encode as JSON and convert to string
        auto decoded = std::string{ reinterpret_cast<const char*>(encoded.data.data()), encoded.data.size() };
        return analyze_document(std::move(index_name), decoded, options, std::move(handler));
    }

    template<typename Document>
    [[nodiscard]] auto analyze_document(std::string index_name, Document document, const analyze_document_options& options = {}) const
      -> std::future<std::pair<manager_error_context, std::vector<std::string>>>
    {
        auto encoded = couchbase::codec::default_json_transcoder::encode(document); // Encode as JSON and convert to string
        auto decoded = std::string{ reinterpret_cast<const char*>(encoded.data.data()), encoded.data.size() };

        return analyze_document(std::move(index_name), decoded, options);
    }

    /**
     * Allows to see how a document is analyzed against a specific scope-level index.
     *
     * @param index_name the name of the search index
     * @param document the document to be analyzed encoded in JSON
     * @param options optional parameters
     * @param handler  handler that implements @ref analyze_document_handler
     *
     * @since 1.0.0
     * @volatile
     */
    void analyze_document(std::string index_name,
                          std::string document,
                          const analyze_document_options& options,
                          analyze_document_handler&& handler) const;

    [[nodiscard]] auto analyze_document(std::string index_name, std::string document, const analyze_document_options& options) const
      -> std::future<std::pair<manager_error_context, std::vector<std::string>>>;

  private:
    friend class scope;

    explicit scope_search_index_manager(core::cluster core, std::string bucket_name, std::string scope_name);

    std::shared_ptr<search_index_manager_impl> impl_;
};
} // namespace couchbase
