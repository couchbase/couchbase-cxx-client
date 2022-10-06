/*
 *     Copyright 2021-Present Couchbase, Inc.
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
#include <couchbase/transactions/transaction_get_result.hxx>

#include "core/document_id.hxx"
#include "core/operations.hxx"
#include "core/utils/json.hxx"
#include "document_metadata.hxx"
#include "transaction_links.hxx"

#include <ostream>
#include <utility>

namespace couchbase::core::transactions
{
struct result;
/**
 * @brief Encapsulates results of an individual transaction operation
 *
 */
class transaction_get_result : public couchbase::transactions::transaction_get_result
{
  private:
    couchbase::cas cas_{};
    core::document_id document_id_{};
    transaction_links links_{};

    /** This is needed for provide {BACKUP-FIELDS}.  It is only needed from the get to the staged mutation, hence Optional. */
    std::optional<document_metadata> metadata_{};

  public:
    /**
    workaround for MSVC standard library deficiency
    @internal
    */
    transaction_get_result() = default;
    ~transaction_get_result() = default;

    /** @internal */
    transaction_get_result(const transaction_get_result& doc) = default;
    transaction_get_result(transaction_get_result&& doc) = default;

    /** @internal */
    transaction_get_result(const transaction_op_error_context& ctx)
      : couchbase::transactions::transaction_get_result(ctx)
    {
    }

    /** @internal */
    template<typename Content>
    transaction_get_result(core::document_id id,
                           Content content,
                           std::uint64_t cas,
                           transaction_links links,
                           std::optional<document_metadata> metadata)
      : couchbase::transactions::transaction_get_result(content)
      , cas_(cas)
      , document_id_(id)
      , links_(std::move(links))
      , metadata_(std::move(metadata))
    {
    }

    /** @internal */
    transaction_get_result(core::document_id id, const tao::json::value& json)
      : couchbase::transactions::transaction_get_result()
      , document_id_(id)
      , links_(json)
      , metadata_(json.optional<std::string>("scas").value_or(""))
    {
        if (const auto* cas = json.find("cas"); cas != nullptr && cas->is_number()) {
            cas_ = couchbase::cas(cas->as<std::uint64_t>());
        }
        if (const auto* cas = json.find("scas"); cas != nullptr && cas->is_string() && cas_.value() == 0U) {
            cas_ = couchbase::cas(stoull(cas->as<std::string>()));
        }
        if (const auto* doc = json.find("doc"); doc != nullptr) {
            value_ = core::utils::json::generate_binary(doc->get_object());
        }
    }

    transaction_get_result& operator=(const transaction_get_result& o)
    {
        if (this != &o) {
            document_id_ = o.document_id_;
            value_ = o.value_;
            cas_ = o.cas_;
            links_ = o.links_;
        }
        return *this;
    }

    /** @internal */
    template<typename Content>
    static transaction_get_result create_from(const transaction_get_result& document, Content content)
    {
        transaction_links links(document.links().atr_id(),
                                document.links().atr_bucket_name(),
                                document.links().atr_scope_name(),
                                document.links().atr_collection_name(),
                                document.links().staged_transaction_id(),
                                document.links().staged_attempt_id(),
                                document.links().staged_content(),
                                document.links().cas_pre_txn(),
                                document.links().revid_pre_txn(),
                                document.links().exptime_pre_txn(),
                                document.links().crc32_of_staging(),
                                document.links().op(),
                                document.links().forward_compat(),
                                document.links().is_deleted());

        return { document.id(), content, document.cas().value(), links, document.metadata() };
    }

    /** @internal */
    static transaction_get_result create_from(const core::document_id& id, const result& res);

    /** @internal */
    static transaction_get_result create_from(const core::operations::lookup_in_response& resp);

    /** @internal */
    template<typename Content>
    transaction_get_result& operator=(const transaction_get_result& other)
    {
        if (this != &other) {
            document_id_ = other.document_id_;
            value_ = other.value_;
            cas_ = other.cas_;
            links_ = other.links_;
        }
        return *this;
    }

    /**
     * @brief Get document id.
     *
     * @return the id of this document.
     */
    [[nodiscard]] const core::document_id& id() const
    {
        return document_id_;
    }

    [[nodiscard]] const std::string& bucket() const override
    {
        return document_id_.bucket();
    }

    [[nodiscard]] const std::string& key() const override
    {
        return document_id_.key();
    }

    [[nodiscard]] const std::string& scope() const override
    {
        return document_id_.scope();
    }

    [[nodiscard]] const std::string& collection() const override
    {
        return document_id_.collection();
    }

    /** @internal */
    [[nodiscard]] transaction_links links() const
    {
        return links_;
    }

    /**
     * @brief Set document CAS.
     *
     * @param cas desired CAS for document.
     */
    void cas(std::uint64_t cas)
    {
        cas_ = couchbase::cas(cas);
    }

    /**
     * @brief Get document metadata.
     *
     * @return metadata for this document.
     */
    [[nodiscard]] const std::optional<document_metadata>& metadata() const
    {
        return metadata_;
    }

    /**
     * @brief Get document CAS.
     *
     * @return the CAS for this document.
     */
    [[nodiscard]] couchbase::cas cas() const override
    {
        return cas_;
    }

    void ctx(transaction_op_error_context ctx)
    {
        ctx_ = std::move(ctx);
    }

    /** @internal */
    template<typename OStream>
    friend OStream& operator<<(OStream& os, const transaction_get_result document)
    {
        os << "transaction_get_result{id: " << document.id().key() << ", cas: " << document.cas_.value() << ", links_: " << document.links_
           << "}";
        return os;
    }
};
} // namespace couchbase::core::transactions
