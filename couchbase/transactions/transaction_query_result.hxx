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

#include <couchbase/query_result.hxx>
#include <couchbase/transaction_op_error_context.hxx>

namespace couchbase::transactions
{
class transaction_query_result : public query_result
{
  public:
    transaction_query_result(query_meta_data meta_data, std::vector<codec::binary> rows, transaction_op_error_context ctx)
      : query_result(std::move(meta_data), std::move(rows))
      , ctx_(std::move(ctx))
    {
    }

    transaction_query_result(query_meta_data meta_data, std::vector<codec::binary> rows, query_error_context ctx)
      : query_result(std::move(meta_data), std::move(rows))
      , ctx_({}, std::move(ctx))
    {
    }

    transaction_query_result(transaction_op_error_context ctx)
      : query_result()
      , ctx_(std::move(ctx))
    {
    }

    [[nodiscard]] const transaction_op_error_context& ctx() const
    {
        return ctx_;
    }

  private:
    transaction_op_error_context ctx_{};
};

// convenient to have a shared ptr for results
using transaction_query_result_ptr = std::shared_ptr<transaction_query_result>;
} // namespace couchbase::transactions
