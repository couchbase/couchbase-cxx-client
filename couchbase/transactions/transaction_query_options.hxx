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

// TODO: remove this when moving the wrap_request call
#include "core/operations/document_query.hxx"
#include <couchbase/query_options.hxx>

// NOTE: when query is in public api, we will hold a query_options struct instead, and
// import that from the public api.

namespace couchbase::transactions
{
class transaction_context;

class transaction_query_options
{
  public:
    transaction_query_options()
    {
        // set defaults specific to query in transactions.
        opts_.metrics(true);
    }
    /*transaction_query_options(const core::operations::query_request& req)
      : query_req_(req)
    {
    }*/

    template<typename Value>
    transaction_query_options& raw(const std::string& key, const Value& value)
    {
        opts_.raw(key, value);
        return *this;
    }

    transaction_query_options& ad_hoc(bool value)
    {
        opts_.adhoc(value);
        return *this;
    }

    transaction_query_options& scan_consistency(query_scan_consistency scan_consistency)
    {
        opts_.scan_consistency(scan_consistency);
        return *this;
    }

    transaction_query_options& profile(query_profile mode)
    {
        opts_.profile(mode);
        return *this;
    }

    transaction_query_options& client_context_id(const std::string& id)
    {
        opts_.client_context_id(id);
        return *this;
    }

    transaction_query_options& scan_wait(std::chrono::milliseconds scan_wait)
    {
        opts_.scan_wait(scan_wait);
        return *this;
    }

    transaction_query_options& readonly(bool readonly)
    {
        opts_.readonly(readonly);
        return *this;
    }

    transaction_query_options& scan_cap(std::uint64_t cap)
    {
        opts_.scan_cap(cap);
        return *this;
    }

    transaction_query_options& pipeline_batch(std::uint64_t batch)
    {
        opts_.pipeline_batch(batch);
        return *this;
    }

    transaction_query_options& pipeline_cap(std::uint64_t cap)
    {
        opts_.pipeline_cap(cap);
        return *this;
    }

    template<typename... Parameters>
    auto positional_parameters(const Parameters&... parameters)
    {
        opts_.positional_parameters(parameters...);
        return *this;
    }

    template<typename... Parameters>
    transaction_query_options& named_parameters(const Parameters&... parameters)
    {
        opts_.named_parameters(parameters...);
        return *this;
    }

    transaction_query_options& scope_qualifier(const std::string& scope)
    {
        opts_.scope_qualifier(scope);
        return *this;
    }

    transaction_query_options& metrics(bool metrics)
    {
        opts_.metrics(metrics);
        return *this;
    }

    transaction_query_options& max_parallelism(std::uint64_t max)
    {
        opts_.max_parallelism(max);
        return *this;
    }

    /** @internal */
    transaction_query_options& encoded_raw_options(std::map<std::string, codec::binary, std::less<>> options)
    {
        opts_.encoded_raw_options(options);
        return *this;
    }

    /** @internal */
    transaction_query_options& encoded_positional_parameters(std::vector<codec::binary> parameters)
    {
        opts_.encoded_positional_parameters(parameters);
        return *this;
    }

    /** @internal */
    transaction_query_options& encoded_named_parameters(std::map<std::string, codec::binary, std::less<>> parameters)
    {
        opts_.encoded_named_parameters(parameters);
        return *this;
    }

    /** @internal */
    const query_options& get_query_options() const
    {
        return opts_;
    }

  private:
    query_options opts_{};
};
} // namespace couchbase::transactions
