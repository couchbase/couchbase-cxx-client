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

#include <couchbase/platform/uuid.h>

#include <couchbase/io/http_context.hxx>
#include <couchbase/io/http_message.hxx>
#include <couchbase/timeout_defaults.hxx>

#include <couchbase/error_context/http.hxx>

#include <couchbase/operations/management/analytics_link.hxx>

namespace couchbase::operations::management
{
struct analytics_link_replace_response {
    struct problem {
        std::uint32_t code;
        std::string message;
    };

    error_context::http ctx;
    std::string status{};
    std::vector<problem> errors{};
};

template<typename analytics_link_type>
struct analytics_link_replace_request {
    using response_type = analytics_link_replace_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::analytics;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    analytics_link_type link{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */) const
    {
        if (std::error_code ec = link.validate()) {
            return ec;
        }
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        encoded.headers["accept"] = "application/json";
        encoded.method = "PUT";
        encoded.path = analytics_link::endpoint_from_link(link);
        encoded.body = link.encode();
        return {};
    }

    [[nodiscard]] management::analytics_link_replace_response make_response(error_context::http&& ctx,
                                                                            const encoded_response_type& encoded) const
    {
        management::analytics_link_replace_response response{ std::move(ctx) };
        if (!response.ctx.ec) {
            if (encoded.body.empty() && response.ctx.http_status == 200) {
                return response;
            }
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body);
            } catch (const tao::pegtl::parse_error&) {
                auto colon = encoded.body.find(':');
                if (colon == std::string::npos) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                auto code = static_cast<std::uint32_t>(std::stoul(encoded.body));
                auto msg = encoded.body.substr(colon + 1);
                response.errors.emplace_back(management::analytics_link_replace_response::problem{ code, msg });
            }
            if (payload) {
                response.status = payload.at("status").get_string();
                if (response.status != "success") {
                    if (auto* errors = payload.find("errors"); errors != nullptr && errors->is_array()) {
                        for (const auto& error : errors->get_array()) {
                            management::analytics_link_replace_response::problem err{
                                error.at("code").as<std::uint32_t>(),
                                error.at("msg").get_string(),
                            };
                            response.errors.emplace_back(err);
                        }
                    }
                }
            }
            bool link_not_found = false;
            bool dataverse_does_not_exist = false;
            for (const auto& err : response.errors) {
                switch (err.code) {
                    case 24006: /* Link [string] does not exist */
                        link_not_found = true;
                        break;
                    case 24034: /* Cannot find dataverse with name [string] */
                        dataverse_does_not_exist = true;
                        break;
                }
            }
            if (dataverse_does_not_exist) {
                response.ctx.ec = error::analytics_errc::dataverse_not_found;
            } else if (link_not_found) {
                response.ctx.ec = error::analytics_errc::link_not_found;
            } else {
                response.ctx.ec = error::common_errc::internal_server_failure;
            }
        }
        return response;
    }
};
} // namespace couchbase::operations::management
