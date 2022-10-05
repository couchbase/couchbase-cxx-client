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
    std::vector<std::byte> value_{};
    transaction_op_error_context ctx_{};

    transaction_get_result() = default;
    transaction_get_result(std::vector<std::byte> content)
      : value_(content)
    {
    }

    transaction_get_result(const transaction_op_error_context& ctx)
      : ctx_(ctx)
    {
    }

    virtual ~transaction_get_result() = default;

  public:
    /**
     * Content of the document.
     *
     * @return content of the document.
     */
    template<typename Content>
    [[nodiscard]] Content content() const
    {
        return codec::tao_json_serializer::deserialize<Content>(value_);
    }

    /**
     * Content of the document as raw byte vector
     *
     * @return content
     */
    [[nodiscard]] const std::vector<std::byte>& content() const
    {
        return value_;
    }

    // TODO: move to core
    void content(const std::vector<std::byte>& content)
    {
        value_ = content;
    }

    [[nodiscard]] const transaction_op_error_context& ctx() const
    {
        return ctx_;
    }

    /**
     * @brief Get document id.
     *
     * @return the id of this document.
     */
    [[nodiscard]] virtual const std::string& key() const = 0;

    /**
     * @brief Get document CAS.
     *
     * @return the CAS for this document.
     */
    [[nodiscard]] virtual couchbase::cas cas() const = 0;

    [[nodiscard]] virtual const std::string& bucket() const = 0;
    [[nodiscard]] virtual const std::string& scope() const = 0;
    [[nodiscard]] virtual const std::string& collection() const = 0;
};
using transaction_get_result_ptr = std::shared_ptr<transaction_get_result>;
} // namespace couchbase::transactions
