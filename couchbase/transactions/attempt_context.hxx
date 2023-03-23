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
#include <couchbase/transactions/transaction_query_options.hxx>
#include <couchbase/transactions/transaction_query_result.hxx>

namespace couchbase::transactions
{
class attempt_context
{
  public:
    virtual std::pair<transaction_op_error_context, transaction_get_result> get(const couchbase::collection& coll,
                                                                                const std::string& id) = 0;

    template<typename Content>
    std::pair<transaction_op_error_context, transaction_get_result> insert(const couchbase::collection& coll,
                                                                           const std::string& id,
                                                                           const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return insert_raw(coll, id, content);
        } else {
            return insert_raw(coll, id, codec::tao_json_serializer::serialize(content));
        }
    }

    template<typename Content>
    std::pair<transaction_op_error_context, transaction_get_result> replace(const transaction_get_result& doc, const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return replace_raw(doc, content);
        } else {
            return replace_raw(doc, codec::tao_json_serializer::serialize(content));
        }
    }

    virtual transaction_op_error_context remove(const transaction_get_result& doc) = 0;

    std::pair<transaction_op_error_context, transaction_query_result> query(const std::string& statement,
                                                                            const transaction_query_options& options = {})
    {
        return do_public_query(statement, options, {});
    }

    std::pair<transaction_op_error_context, transaction_query_result> query(const scope& scope,
                                                                            const std::string& statement,
                                                                            const transaction_query_options& opts = {})
    {
        return do_public_query(statement, opts, fmt::format("{}.{}", scope.bucket_name(), scope.name()));
    }

    virtual ~attempt_context() = default;

  protected:
    virtual std::pair<transaction_op_error_context, transaction_get_result> replace_raw(const transaction_get_result& doc,
                                                                                        std::vector<std::byte> content) = 0;
    virtual std::pair<transaction_op_error_context, transaction_get_result> insert_raw(const couchbase::collection& coll,
                                                                                       const std::string& id,
                                                                                       std::vector<std::byte> content) = 0;
    virtual std::pair<transaction_op_error_context, transaction_query_result> do_public_query(const std::string& statement,
                                                                                              const transaction_query_options& options,
                                                                                              std::optional<std::string> query_context) = 0;
};
} // namespace couchbase::transactions
