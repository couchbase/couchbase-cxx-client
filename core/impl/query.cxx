/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/error_codes.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/transactions/transaction_query_result.hxx>

#include "core/cluster.hxx"
#include "core/operations/document_query.hxx"

namespace couchbase::core::impl
{
static query_error_context
build_context(operations::query_response& resp)
{
    return {
        resp.ctx.ec,
        resp.ctx.last_dispatched_to,
        resp.ctx.last_dispatched_from,
        resp.ctx.retry_attempts,
        std::move(resp.ctx.retry_reasons),
        resp.ctx.first_error_code,
        std::move(resp.ctx.first_error_message),
        std::move(resp.ctx.client_context_id),
        std::move(resp.ctx.statement),
        std::move(resp.ctx.parameters),
        std::move(resp.ctx.method),
        std::move(resp.ctx.path),
        resp.ctx.http_status,
        std::move(resp.ctx.http_body),
        std::move(resp.ctx.hostname),
        resp.ctx.port,
    };
}

static query_status
map_status(std::string status)
{
    std::transform(status.cbegin(), status.cend(), status.begin(), [](unsigned char c) { return std::tolower(c); });
    if (status == "running") {
        return query_status::running;
    } else if (status == "success") {
        return query_status::success;
    } else if (status == "errors") {
        return query_status::errors;
    } else if (status == "completed") {
        return query_status::completed;
    } else if (status == "stopped") {
        return query_status::stopped;
    } else if (status == "timeout") {
        return query_status::timeout;
    } else if (status == "closed") {
        return query_status::closed;
    } else if (status == "fatal") {
        return query_status::fatal;
    } else if (status == "aborted") {
        return query_status::aborted;
    }
    return query_status::unknown;
}

static std::vector<codec::binary>
map_rows(operations::query_response& resp)
{
    std::vector<codec::binary> rows;
    rows.reserve(resp.rows.size());
    for (const auto& row : resp.rows) {
        rows.emplace_back(utils::to_binary(row));
    }
    return rows;
}

static std::vector<query_warning>
map_warnings(operations::query_response& resp)
{
    std::vector<query_warning> warnings;
    if (resp.meta.warnings) {
        warnings.reserve(resp.meta.warnings->size());
        for (auto& warning : resp.meta.warnings.value()) {
            warnings.emplace_back(query_warning{
              warning.code,
              std::move(warning.message),
              std::move(warning.reason),
              std::move(warning.retry),
            });
        }
    }
    return warnings;
}

static std::optional<query_metrics>
map_metrics(operations::query_response& resp)
{
    if (!resp.meta.metrics) {
        return {};
    }

    return query_metrics{
        resp.meta.metrics->elapsed_time, resp.meta.metrics->execution_time, resp.meta.metrics->result_count,
        resp.meta.metrics->result_size,  resp.meta.metrics->sort_count,     resp.meta.metrics->mutation_count,
        resp.meta.metrics->error_count,  resp.meta.metrics->warning_count,
    };
}

static std::optional<std::vector<std::byte>>
map_signature(operations::query_response& resp)
{
    if (!resp.meta.signature) {
        return {};
    }
    return utils::to_binary(resp.meta.signature.value());
}

static std::optional<std::vector<std::byte>>
map_profile(operations::query_response& resp)
{
    if (!resp.meta.profile) {
        return {};
    }
    return utils::to_binary(resp.meta.profile.value());
}

static query_result
build_result(operations::query_response& resp)
{
    return {
        query_meta_data{
          std::move(resp.meta.request_id),
          std::move(resp.meta.client_context_id),
          map_status(resp.meta.status),
          map_warnings(resp),
          map_metrics(resp),
          map_signature(resp),
          map_profile(resp),
        },
        map_rows(resp),
    };
}

static core::operations::query_request
build_query_request(std::string statement, query_options::built options)
{
    operations::query_request request{
        std::move(statement),
        options.adhoc,
        options.metrics,
        options.readonly,
        options.flex_index,
        options.preserve_expiry,
        options.max_parallelism,
        options.scan_cap,
        options.scan_wait,
        options.pipeline_batch,
        options.pipeline_cap,
        options.scan_consistency,
        std::move(options.mutation_state),
        std::move(options.client_context_id),
        {}, // we put the query_context in later, if one was specified.
        options.timeout,
        options.profile,
    };
    if (!options.raw.empty()) {
        for (auto& [name, value] : options.raw) {
            request.raw[name] = std::move(value);
        }
    }
    if (!options.positional_parameters.empty()) {
        for (auto& value : options.positional_parameters) {
            request.positional_parameters.emplace_back(std::move(value));
        }
    }
    if (!options.named_parameters.empty()) {
        for (auto& [name, value] : options.named_parameters) {
            request.named_parameters[name] = std::move(value);
        }
    }
    return request;
}

std::pair<couchbase::transaction_op_error_context, couchbase::transactions::transaction_query_result>
build_transaction_query_result(operations::query_response resp, std::error_code txn_ec /*defaults to 0*/)
{
    if (resp.ctx.ec) {
        if (resp.ctx.ec == errc::common::parsing_failure) {
            txn_ec = errc::transaction_op::parsing_failure;
        }
        if (!txn_ec) {
            // TODO: review what our default should be...
            // no override error code was passed in, so default to not_set
            txn_ec = errc::transaction_op::not_set;
        }
    }
    return {
        { txn_ec, build_context(resp) },
        { query_meta_data{
            std::move(resp.meta.request_id),
            std::move(resp.meta.client_context_id),
            map_status(resp.meta.status),
            map_warnings(resp),
            map_metrics(resp),
            map_signature(resp),
            map_profile(resp),
          },
          map_rows(resp) },
    };
}

core::operations::query_request
build_transaction_query_request(query_options::built opts)
{
    return build_query_request("", opts);
}

void
initiate_query_operation(std::shared_ptr<couchbase::core::cluster> core,
                         std::string statement,
                         std::optional<std::string> query_context,
                         query_options::built options,
                         query_handler&& handler)
{
    auto request = build_query_request(std::move(statement), options);
    if (query_context) {
        request.query_context = std::move(query_context);
    }

    core->execute(std::move(request), [core, handler = std::move(handler)](operations::query_response resp) mutable {
        auto r = std::move(resp);
        return handler(build_context(r), build_result(r));
    });
}
} // namespace couchbase::core::impl
