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

#include <couchbase/operations/document_get_projected.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations
{

static std::optional<tao::json::value>
subdoc_lookup(tao::json::value& root, const std::string& path)
{
    std::string::size_type offset = 0;
    tao::json::value* cur = &root;

    while (offset < path.size()) {
        std::string::size_type idx = path.find_first_of(".[]", offset);

        if (idx == std::string::npos) {
            std::string key = path.substr(offset);
            if (auto* val = cur->find(key); val != nullptr) {
                return *val;
            }
            break;
        }

        if (path[idx] == '.' || path[idx] == '[') {
            std::string key = path.substr(offset, idx - offset);
            auto* val = cur->find(key);
            if (val == nullptr) {
                break;
            }
            cur = val;
        } else if (path[idx] == ']') {
            if (!cur->is_array()) {
                break;
            }
            std::string key = path.substr(offset, idx - offset);
            if (int array_index = std::stoi(key); array_index == -1) {
                cur = &cur->get_array().back();
            } else if (static_cast<std::size_t>(array_index) < cur->get_array().size()) {
                cur = &cur->get_array().back();
            } else {
                break;
            }
            if (idx < path.size() - 1) {
                return *cur;
            }
            idx += 1;
        }
        offset = idx + 1;
    }

    return {};
}

static void
subdoc_apply_projection(tao::json::value& root, const std::string& path, tao::json::value& value, bool preserve_array_indexes)
{
    std::string::size_type offset = 0;
    tao::json::value* cur = &root;

    while (offset < path.size()) {
        std::string::size_type idx = path.find_first_of(".[]", offset);

        if (idx == std::string::npos) {
            cur->operator[](path.substr(offset)) = value;
            break;
        }

        if (path[idx] == '.') {
            std::string key = path.substr(offset, idx - offset);
            tao::json::value* child = cur->find(key);
            if (child == nullptr) {
                cur->operator[](key) = tao::json::empty_object;
                child = cur->find(key);
            }
            cur = child;
        } else if (path[idx] == '[') {
            std::string key = path.substr(offset, idx - offset);
            tao::json::value* child = cur->find(key);
            if (child == nullptr) {
                cur->operator[](key) = tao::json::empty_array;
                child = cur->find(key);
            }
            cur = child;
        } else if (path[idx] == ']') {
            tao::json::value child;
            if (idx == path.size() - 1) {
                child = value;
            } else if (path[idx + 1] == '.') {
                child = tao::json::empty_object;
            } else if (path[idx + 1] == '[') {
                child = tao::json::empty_array;
            } else {
                Expects(false);
            }
            if (preserve_array_indexes) {
                int array_index = std::stoi(path.substr(offset, idx - offset));
                if (array_index >= 0) {
                    if (static_cast<std::size_t>(array_index) >= cur->get_array().size()) {
                        cur->get_array().resize(static_cast<std::size_t>(array_index + 1), tao::json::null);
                    }
                    cur->at(static_cast<std::size_t>(array_index)) = child;
                    cur = &cur->at(static_cast<std::size_t>(array_index));
                } else {
                    // index is negative, just append and let user decide what it means
                    cur->get_array().push_back(child);
                    cur = &cur->get_array().back();
                }
            } else {
                cur->get_array().push_back(child);
                cur = &cur->get_array().back();
            }
            ++idx;
        }
        offset = idx + 1;
    }
}

std::error_code
get_projected_request::encode_to(get_projected_request::encoded_request_type& encoded, mcbp_context&& /* context */)
{
    encoded.opaque(opaque);
    encoded.partition(partition);
    encoded.body().id(id);

    effective_projections = projections;
    std::size_t num_projections = effective_projections.size();
    if (with_expiry) {
        num_projections++;
    }
    if (num_projections > 16) {
        // too many subdoc operations, better fetch full document
        effective_projections.clear();
    }

    protocol::lookup_in_request_body::lookup_in_specs specs{};
    if (with_expiry) {
        specs.add_spec(protocol::subdoc_opcode::get, true, "$document.exptime");
    }
    if (effective_projections.empty()) {
        specs.add_spec(protocol::subdoc_opcode::get_doc, false, "");
    } else {
        for (const auto& path : effective_projections) {
            specs.add_spec(protocol::subdoc_opcode::get, false, path);
        }
    }
    encoded.body().specs(specs);
    return {};
}

get_projected_response
get_projected_request::make_response(error_context::key_value&& ctx, const encoded_response_type& encoded) const
{
    get_projected_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        response.cas = encoded.cas();
        if (with_expiry && !encoded.body().fields()[0].value.empty()) {
            response.expiry = gsl::narrow_cast<std::uint32_t>(std::stoul(encoded.body().fields()[0].value));
        }
        if (effective_projections.empty()) {
            // from full document
            if (projections.empty() && with_expiry) {
                // special case when user only wanted full+expiration
                response.value = encoded.body().fields()[1].value;
            } else {
                tao::json::value full_doc{};
                try {
                    full_doc = utils::json::parse(encoded.body().fields()[with_expiry ? 1 : 0].value);
                } catch (const tao::pegtl::parse_error&) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                tao::json::value new_doc;
                for (const auto& projection : projections) {
                    auto value_to_apply = subdoc_lookup(full_doc, projection);
                    if (value_to_apply) {
                        subdoc_apply_projection(new_doc, projection, *value_to_apply, preserve_array_indexes);
                    } else {
                        response.ctx.ec = error::key_value_errc::path_not_found;
                        return response;
                    }
                }
                response.value = utils::json::generate(new_doc);
            }
        } else {
            tao::json::value new_doc = tao::json::empty_object;
            std::size_t offset = with_expiry ? 1 : 0;
            for (const auto& projection : projections) {
                const auto& field = encoded.body().fields()[offset++];
                if (field.status == protocol::status::success && !field.value.empty()) {
                    tao::json::value value_to_apply{};
                    try {
                        value_to_apply = utils::json::parse(field.value);
                    } catch (const tao::pegtl::parse_error&) {
                        response.ctx.ec = error::common_errc::parsing_failure;
                        return response;
                    }
                    subdoc_apply_projection(new_doc, projection, value_to_apply, preserve_array_indexes);
                } else {
                    response.ctx.ec = error::key_value_errc::path_not_found;
                    return response;
                }
            }
            response.value = utils::json::generate(new_doc);
        }
    }
    return response;
}
} // namespace couchbase::operations
