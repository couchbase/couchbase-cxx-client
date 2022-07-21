/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/error_context/key_value.hxx"
#include "core/io/mcbp_context.hxx"
#include "core/io/mcbp_traits.hxx"
#include "core/io/retry_context.hxx"
#include "core/protocol/client_request.hxx"
#include "core/protocol/cmd_get.hxx"
#include "core/timeout_defaults.hxx"
#include "core/tracing/request_tracer.hxx"

namespace couchbase::core::operations
{

struct get_response {
    key_value_error_context ctx;
    std::vector<std::byte> value{};
    couchbase::cas cas{};
    std::uint32_t flags{};
};

struct get_request {
    using response_type = get_response;
    using encoded_request_type = protocol::client_request<protocol::get_request_body>;
    using encoded_response_type = protocol::client_response<protocol::get_response_body>;

    document_id id;
    std::uint16_t partition{};
    std::uint32_t opaque{};
    std::optional<std::chrono::milliseconds> timeout{};
    io::retry_context<io::retry_strategy::best_effort> retries{ true };
    std::shared_ptr<tracing::request_span> parent_span{ nullptr };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&& context) const;

    [[nodiscard]] get_response make_response(key_value_error_context&& ctx, const encoded_response_type& encoded) const;
};
} // namespace couchbase::core::operations

namespace couchbase::core::io::mcbp_traits
{
template<>
struct supports_parent_span<couchbase::core::operations::get_request> : public std::true_type {
};
} // namespace couchbase::core::io::mcbp_traits