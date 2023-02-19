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
using async_result_handler = std::function<void(transaction_op_error_context, transaction_get_result)>;
using async_query_handler = std::function<void(transaction_op_error_context, transaction_query_result)>;
using async_err_handler = std::function<void(transaction_op_error_context)>;
class async_attempt_context
{

  public:
    virtual void get(const collection& coll, std::string id, async_result_handler&& handler) = 0;
    virtual void remove(transaction_get_result doc, async_err_handler&& handler) = 0;

    template<typename Content>
    void insert(const collection& coll, std::string id, Content&& content, async_result_handler&& handler)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return insert_raw(std::forward<std::vector<std::byte>>(content), std::move(id), content, std::move(handler));
        } else {
            return insert_raw(coll, std::move(id), codec::tao_json_serializer::serialize(content), std::move(handler));
        }
    }

    template<typename Content>
    void replace(transaction_get_result doc, Content&& content, async_result_handler&& handler)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return replace_raw(std::move(doc), std::forward<std::vector<std::byte>>(content), std::move(handler));
        } else {
            return replace_raw(std::move(doc), codec::tao_json_serializer::serialize(content), std::move(handler));
        }
    }

    void query(const scope& scope, std::string statement, transaction_query_options opts, async_query_handler&& handler)
    {
        return query(std::move(statement), std::move(opts), fmt::format("{}.{}", scope.bucket_name(), scope.name()), std::move(handler));
    }

    void query(std::string statement, transaction_query_options opts, async_query_handler&& handler)
    {
        return query(statement, opts, {}, std::move(handler));
    };

    void query(std::string statement, async_query_handler&& handler)
    {
        return query(std::move(statement), {}, std::move(handler));
    }
    virtual ~async_attempt_context() = default;

  protected:
    virtual void insert_raw(const collection& coll, std::string id, std::vector<std::byte> content, async_result_handler&& handler) = 0;
    virtual void replace_raw(transaction_get_result doc, std::vector<std::byte> content, async_result_handler&& handler) = 0;
    virtual void query(std::string statement,
                       transaction_query_options opts,
                       std::optional<std::string> query_context,
                       async_query_handler&&) = 0;
};
} // namespace couchbase::transactions
