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
#include <couchbase/transaction_op_error_context.hxx>

namespace couchbase::transactions
{

class transaction_get_result
{
  protected:
    std::string bucket_{};
    std::string scope_{};
    std::string collection_{};
    std::string key_{};
    cas cas_{};
    std::vector<std::byte> content_{};

  public:
    transaction_get_result() = default;

    transaction_get_result(std::string bucket,
                           std::string scope,
                           std::string collection,
                           std::string key,
                           couchbase::cas cas,
                           std::vector<std::byte> content)
      : bucket_(std::move(bucket))
      , scope_(std::move(scope))
      , collection_(std::move(collection))
      , key_(std::move(key))
      , cas_(std::move(cas))
      , content_(std::move(content))
    {
    }

    /**
     * Content of the document.
     *
     * @return content of the document.
     */
    template<typename Content>
    [[nodiscard]] Content content() const
    {
        return codec::tao_json_serializer::deserialize<Content>(content_);
    }

    /**
     * Content of the document as raw byte vector
     *
     * @return content
     */
    [[nodiscard]] const std::vector<std::byte>& content() const
    {
        return content_;
    }
    /**
     * Copy content into document
     * @param content
     */
    void content(std::vector<std::byte> content)
    {
        content_ = std::move(content);
    }
    /**
     * Move content into document
     *
     * @param content
     */
    void content(std::vector<std::byte>&& content)
    {
        content_ = std::move(content);
    }

    /**
     * Get document id.
     *
     * @return the id of this document.
     */
    [[nodiscard]] const std::string& key() const
    {
        return key_;
    }

    /**
     * Get document CAS.
     *
     * @return the CAS for this document.
     */
    [[nodiscard]] const couchbase::cas& cas() const
    {
        return cas_;
    }
    /**
     * Get the name of the bucket this document is in.
     *
     * @return name of the bucket which contains the document.
     */
    [[nodiscard]] const std::string& bucket() const
    {
        return bucket_;
    }
    /**
     * Get the name of the scope this document is in.
     *
     * @return name of the scope which contains the document.
     */
    [[nodiscard]] const std::string& scope() const
    {
        return scope_;
    }
    /**
     * Get the name of the collection this document is in.
     *
     * @return name of the collection which contains the document.
     */
    [[nodiscard]] const std::string& collection() const
    {
        return collection_;
    }
};
} // namespace couchbase::transactions
