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

namespace couchbase::transactions
{
class attempt_context
{
  public:
    virtual transaction_get_result_ptr get(std::shared_ptr<couchbase::collection> coll, const std::string& id) = 0;

    template<typename Content>
    transaction_get_result_ptr insert(std::shared_ptr<couchbase::collection> coll, const std::string& id, const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return insert_raw(content, id, content);
        } else {
            return insert_raw(coll, id, codec::tao_json_serializer::serialize(content));
        }
    }

    template<typename Content>
    transaction_get_result_ptr replace(const transaction_get_result_ptr doc, const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return replace_raw(doc, content);
        } else {
            return replace_raw(doc, codec::tao_json_serializer::serialize(content));
        }
    }

    virtual transaction_op_error_context remove(transaction_get_result_ptr doc) = 0;

    // TODO: when public api has query, we will be able to use non-core options to create the transaction_query_options.
    // virtual transaction_op_error_context query(const std::string& statement, const transaction_query_options& options, QueryCallback&&
    // cb) = 0;

    virtual ~attempt_context() = default;

  protected:
    virtual transaction_get_result_ptr replace_raw(const transaction_get_result_ptr doc, std::vector<std::byte> content) = 0;
    virtual transaction_get_result_ptr insert_raw(std::shared_ptr<couchbase::collection> coll,
                                                  const std::string& id,
                                                  std::vector<std::byte> content) = 0;
};
} // namespace couchbase::transactions
