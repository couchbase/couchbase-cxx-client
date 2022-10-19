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

#include "core/operations/document_query.hxx"

// NOTE: when query is in public api, we will hold a query_options struct instead, and
// import that from the public api.

namespace couchbase::core::transactions
{
class transaction_context;

class transaction_query_options
{
  public:
    transaction_query_options()
    {
        // set defaults specific to query in transactions.
        query_req_.metrics = true;
    }
    transaction_query_options(const core::operations::query_request& req)
      : query_req_(req)
    {
    }

    transaction_query_options& raw(const std::string& key, const core::json_string& value)
    {
        query_req_.raw[key] = value;
        return *this;
    }

    transaction_query_options& ad_hoc(bool value)
    {
        query_req_.adhoc = value;
        return *this;
    }

    transaction_query_options& scan_consistency(query_scan_consistency scan_consistency)
    {
        query_req_.scan_consistency = scan_consistency;
        return *this;
    }

    transaction_query_options& profile(query_profile mode)
    {
        query_req_.profile = mode;
        return *this;
    }

    transaction_query_options& client_context_id(const std::string& id)
    {
        query_req_.client_context_id = id;
        return *this;
    }

    transaction_query_options& scan_wait(std::chrono::milliseconds scan_wait)
    {
        query_req_.scan_wait = scan_wait;
        return *this;
    }

    transaction_query_options& readonly(bool readonly)
    {
        query_req_.readonly = readonly;
        return *this;
    }

    transaction_query_options& scan_cap(std::uint64_t cap)
    {
        query_req_.scan_cap = cap;
        return *this;
    }

    transaction_query_options& pipeline_batch(std::uint64_t batch)
    {
        query_req_.pipeline_batch = batch;
        return *this;
    }

    transaction_query_options& pipeline_cap(std::uint64_t cap)
    {
        query_req_.pipeline_cap = cap;
        return *this;
    }

    transaction_query_options& positional_parameters(std::vector<core::json_string> params)
    {
        query_req_.positional_parameters = params;
        return *this;
    }

    transaction_query_options& named_parameters(std::map<std::string, json_string, std::less<>> params)
    {
        query_req_.named_parameters = params;
        return *this;
    }

    transaction_query_options& bucket_name(const std::string& bucket)
    {
        query_req_.bucket_name = bucket;
        return *this;
    }

    transaction_query_options& scope_name(const std::string& scope)
    {
        query_req_.scope_name = scope;
        return *this;
    }

    transaction_query_options& metrics(bool metrics)
    {
        query_req_.metrics = metrics;
        return *this;
    }

    transaction_query_options& max_parallelism(std::uint64_t max)
    {
        query_req_.max_parallelism = max;
        return *this;
    }

    [[nodiscard]] core::operations::query_request wrap_request(const transaction_context& txn_ctx) const;

  private:
    core::operations::query_request query_req_;
};
} // namespace couchbase::core::transactions
