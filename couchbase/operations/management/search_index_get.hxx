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

#include <couchbase/error_context/http.hxx>
#include <couchbase/io/http_context.hxx>
#include <couchbase/io/http_message.hxx>
#include <couchbase/management/search_index.hxx>
#include <couchbase/platform/uuid.h>
#include <couchbase/timeout_defaults.hxx>

namespace couchbase::operations::management
{
struct search_index_get_response {
    error_context::http ctx;
    std::string status{};
    couchbase::management::search::index index{};
    std::string error{};
};

struct search_index_get_request {
    using response_type = search_index_get_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::search;

    std::string index_name;

    std::optional<std::string> client_context_id{};
    std::optional<std::chrono::milliseconds> timeout{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context) const;

    [[nodiscard]] search_index_get_response make_response(error_context::http&& ctx, const encoded_response_type& encoded) const;
};

} // namespace couchbase::operations::management
