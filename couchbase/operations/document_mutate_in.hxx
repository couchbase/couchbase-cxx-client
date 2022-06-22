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

#include <couchbase/error_context/key_value.hxx>
#include <couchbase/io/mcbp_context.hxx>
#include <couchbase/io/mcbp_traits.hxx>
#include <couchbase/io/retry_context.hxx>
#include <couchbase/protocol/client_request.hxx>
#include <couchbase/protocol/cmd_mutate_in.hxx>
#include <couchbase/protocol/durability_level.hxx>
#include <couchbase/timeout_defaults.hxx>

namespace couchbase::operations
{

struct mutate_in_response {
    struct field {
        protocol::subdoc_opcode opcode;
        protocol::status status;
        std::string path;
        std::string value;
        std::size_t original_index;
        std::error_code ec{};
    };
    error_context::key_value ctx;
    couchbase::cas cas{};
    mutation_token token{};
    std::vector<field> fields{};
    std::optional<std::size_t> first_error_index{};
    bool deleted{ false };
};

struct mutate_in_request {
    using response_type = mutate_in_response;
    using encoded_request_type = protocol::client_request<protocol::mutate_in_request_body>;
    using encoded_response_type = protocol::client_response<protocol::mutate_in_response_body>;

    document_id id;
    uint16_t partition{};
    uint32_t opaque{};
    couchbase::cas cas{ 0 };
    bool access_deleted{ false };
    bool create_as_deleted{ false };
    std::optional<std::uint32_t> expiry{};
    protocol::mutate_in_request_body::store_semantics_type store_semantics{
        protocol::mutate_in_request_body::store_semantics_type::replace
    };
    protocol::mutate_in_request_body::mutate_in_specs specs{};
    protocol::durability_level durability_level{ protocol::durability_level::none };
    std::optional<std::chrono::milliseconds> timeout{};
    io::retry_context<io::retry_strategy::best_effort> retries{ false };
    bool preserve_expiry{ false };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&& context);

    [[nodiscard]] mutate_in_response make_response(error_context::key_value&& ctx, const encoded_response_type& encoded) const;
};

} // namespace couchbase::operations

namespace couchbase::io::mcbp_traits
{
template<>
struct supports_durability<couchbase::operations::mutate_in_request> : public std::true_type {
};
} // namespace couchbase::io::mcbp_traits
