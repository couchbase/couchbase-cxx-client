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

#include <couchbase/transactions/document_metadata.hxx>
#include <couchbase/transactions/transaction_links.hxx>

#include "core/document_id.hxx"
#include "core/operations.hxx"
#include "core/utils/json.hxx"

#include <ostream>
#include <utility>

namespace couchbase::transactions
{
struct result;
/**
 * @brief Encapsulates results of an individual transaction operation
 *
 */
class transaction_get_result
{
  private:
    std::vector<std::byte> value_{};
    core::document_id id_{};
    std::uint64_t cas_{ 0 };
    transaction_links links_{};

    /** This is needed for provide {BACKUP-FIELDS}.  It is only needed from the get to the staged mutation, hence Optional. */
    std::optional<document_metadata> metadata_{};

  public:
    /**
    workaround for MSVC standard library deficiency
    @internal
    */
    transaction_get_result() = default;

    /** @internal */
    transaction_get_result(const transaction_get_result& doc) = default;

    /** @internal */
    template<typename Content>
    transaction_get_result(core::document_id id,
                           Content content,
                           std::uint64_t cas,
                           transaction_links links,
                           std::optional<document_metadata> metadata)
      : value_(std::move(content))
      , id_(std::move(id))
      , cas_(cas)
      , links_(std::move(links))
      , metadata_(std::move(metadata))
    {
    }

    /** @internal */
    transaction_get_result(core::document_id id, const tao::json::value& json)
      : id_(std::move(id))
      , links_(json)
      , metadata_(json.optional<std::string>("scas").value_or(""))
    {
        if (const auto* cas = json.find("cas"); cas != nullptr && cas->is_number()) {
            cas_ = cas->as<std::uint64_t>();
        }
        if (const auto* cas = json.find("scas"); cas != nullptr && cas->is_string() && cas_ == 0U) {
            cas_ = stoull(cas->as<std::string>());
        }
        if (const auto* doc = json.find("doc"); doc != nullptr) {
            value_ = core::utils::json::generate_binary(doc->get_object());
        }
    }

    transaction_get_result& operator=(const transaction_get_result& o)
    {
        value_ = o.value_;
        id_ = o.id_;
        cas_ = o.cas_;
        links_ = o.links_;
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

        return { document.id(), content, document.cas(), links, document.metadata() };
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
            this->value_ = other.value_;
            this->id_ = other.id_;
            this->links_ = other.links_;
        }
        return *this;
    }

    /**
     * @brief Content of the document.
     *
     * The content of the document is stored as json.  That is represented internally as
     * a nlohmann::json object.  If the documents have a c++ class that represents them, it
     * can be returned here by adding a to_json and from_json helper.
     * @code{.cpp}
     * namespace my_namespace {
     *   struct my_doc
     *   {
     *     std::string name;
     *     std::uint32_t age;
     *   };
     *
     *   void from_json(const nlohmann::json& j, my_doc& d)
     *   {
     *      j.at("name").get_to(d.name);
     *      j.at("age").get_to(d.age);
     *   }
     *   void to_json(nlohmann::json& j, const my_doc& d)
     *   {
     *      j = nlohmann::json({"name", d.name}, {"age", d.age});
     *   }
     * @endcode
     *
     * Then, you can do:
     * @code{.cpp}
     * ...
     * txn.run([&](attempt_context& ctx) {
     *   auto txn_doc = ctx.get("mydocid");
     *   my_namespace::my_doc& mydoc = td.content<my_doc>();
     *   ...
     * });
     * @endcode
     *
     * See @ref examples/game_server, and for more detail https://github.com/nlohmann/json#arbitrary-types-conversions
     *
     * @return content of the document.
     */
    template<typename Content>
    [[nodiscard]] Content content() const
    {
        return codec::json_transcoder::decode<Content>(value_);
    }

    [[nodiscard]] const std::vector<std::byte>& content() const
    {
        return value_;
    }

    void content(const std::vector<std::byte>& content)
    {
        value_ = content;
    }

    /**
     * @brief Get document id.
     *
     * @return the id of this document.
     */
    const core::document_id& id() const
    {
        return id_;
    }

    /**
     * @brief Get document CAS.
     *
     * @return the CAS for this document.
     */
    [[nodiscard]] std::uint64_t cas() const
    {
        return cas_;
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
        cas_ = cas;
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

    /** @internal */
    template<typename OStream>
    friend OStream& operator<<(OStream& os, const transaction_get_result document)
    {
        os << "transaction_get_result{id: " << document.id_.key() << ", cas: " << document.cas_ << ", links_: " << document.links_ << "}";
        return os;
    }
};
} // namespace couchbase::transactions
