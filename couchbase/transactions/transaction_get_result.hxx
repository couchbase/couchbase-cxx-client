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

#include <string>
#include <vector>

#include <couchbase/cas.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/result.hxx>
#include <couchbase/transaction_op_error_context.hxx>

// forward declarations...
namespace couchbase::core::transactions
{
class transaction_get_result;
class transaction_links;
class document_metadata;
} // namespace couchbase::core::transactions

namespace couchbase::transactions
{

class transaction_get_result
{
    friend class couchbase::core::transactions::transaction_get_result;

  private:
    std::shared_ptr<couchbase::core::transactions::transaction_get_result> base_{};

    transaction_get_result(std::shared_ptr<couchbase::core::transactions::transaction_get_result> base)
      : base_(base)
    {
    }

  public:
    /** @private */

    transaction_get_result();
    /**
     * Content of the document.
     *
     * @return content of the document.
     */
    template<typename Content>
    [[nodiscard]] Content content() const
    {
        return codec::tao_json_serializer::deserialize<Content>(content());
    }

    /**
     * Content of the document as raw byte vector
     *
     * @return content
     */
    [[nodiscard]] const std::vector<std::byte>& content() const;

    /**
     * Copy content into document
     * @param content
     */
    void content(std::vector<std::byte> content);

    /**
     * Move content into document
     *
     * @param content
     */
    void content(std::vector<std::byte>&& content);

    /**
     * Get document id.
     *
     * @return the id of this document.
     */
    [[nodiscard]] const std::string key() const;

    /**
     * Get the name of the bucket this document is in.
     *
     * @return name of the bucket which contains the document.
     */
    [[nodiscard]] const std::string bucket() const;

    /**
     * Get the name of the scope this document is in.
     *
     * @return name of the scope which contains the document.
     */
    [[nodiscard]] const std::string scope() const;

    /**
     * Get the name of the collection this document is in.
     *
     * @return name of the collection which contains the document.
     */
    [[nodiscard]] const std::string collection() const;

    /**
     * Get the CAS fot this document
     *
     * @return the CAS of the document.
     */
    [[nodiscard]] const couchbase::cas cas() const;
};
} // namespace couchbase::transactions
