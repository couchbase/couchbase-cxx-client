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

#include "document_search.hxx"

#include "core/cluster_options.hxx"
#include "core/logger/logger.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/contrib/traits.hpp>

namespace couchbase::core::operations
{
std::error_code
search_request::encode_to(search_request::encoded_request_type& encoded, http_context& context)
{
    auto body = tao::json::value{
        { "query", utils::json::parse(query) },
        { "ctl", { { "timeout", encoded.timeout.count() } } },
    };
    if (explain) {
        body["explain"] = *explain;
    }
    if (limit) {
        body["size"] = *limit;
    }
    if (skip) {
        body["from"] = *skip;
    }
    if (disable_scoring) {
        body["score"] = "none";
    }
    if (include_locations) {
        body["includeLocations"] = true;
    }
    if (highlight_style || !highlight_fields.empty()) {
        tao::json::value highlight;
        if (highlight_style) {
            switch (*highlight_style) {
                case couchbase::core::search_highlight_style::html:
                    highlight["style"] = "html";
                    break;
                case couchbase::core::search_highlight_style::ansi:
                    highlight["style"] = "ansi";
                    break;
            }
        }
        if (!highlight_fields.empty()) {
            highlight["fields"] = highlight_fields;
        }
        body["highlight"] = highlight;
    }
    if (!fields.empty()) {
        body["fields"] = fields;
    }
    if (!sort_specs.empty()) {
        body["sort"] = tao::json::empty_array;
        for (const auto& spec : sort_specs) {
            body["sort"].get_array().push_back(utils::json::parse(spec));
        }
    }
    if (!facets.empty()) {
        body["facets"] = tao::json::empty_object;
        for (const auto& [name, facet] : facets) {
            body["facets"][name] = utils::json::parse(facet);
        }
    }
    if (!mutation_state.empty()) {
        tao::json::value scan_vectors = tao::json::empty_object;
        for (const auto& token : mutation_state) {
            auto key = fmt::format("{}/{}", token.partition_id(), token.partition_uuid());
            const auto* old_val = scan_vectors.find(key);
            if (old_val == nullptr || (old_val->is_integer() && old_val->get_unsigned() < token.sequence_number())) {
                scan_vectors[key] = token.sequence_number();
            }
        }
        body["ctl"]["consistency"] = tao::json::value{
            { "level", "at_plus" },
            { "vectors", { { index_name, scan_vectors } } },
        };
    }
    if (!collections.empty()) {
        body["collections"] = collections;
    }

    for (const auto& [key, value] : raw) {
        body[key] = utils::json::parse(value);
    }

    encoded.type = type;
    encoded.headers["content-type"] = "application/json";
    encoded.method = "POST";
    encoded.path = fmt::format("/api/index/{}/query", index_name);
    body_str = utils::json::generate(body);
    encoded.body = body_str;
    if (context.options.show_queries) {
        CB_LOG_INFO("SEARCH: {}", utils::json::generate(body["query"]));
    } else {
        CB_LOG_DEBUG("SEARCH: {}", utils::json::generate(body["query"]));
    }
    if (row_callback) {
        encoded.streaming.emplace(couchbase::core::io::streaming_settings{
          "/hits/^",
          4,
          std::move(row_callback.value()),
        });
    }
    return {};
}

search_response
search_request::make_response(error_context::search&& ctx, const encoded_response_type& encoded) const
{
    search_response response{ std::move(ctx) };
    response.meta.client_context_id = response.ctx.client_context_id;
    response.ctx.index_name = index_name;
    response.ctx.query = query.str();
    response.ctx.parameters = body_str;
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = errc::common::parsing_failure;
                return response;
            }
            response.meta.metrics.took = std::chrono::nanoseconds(payload.at("took").get_unsigned());
            response.meta.metrics.max_score = payload.at("max_score").as<double>();
            response.meta.metrics.total_rows = payload.at("total_hits").get_unsigned();

            if (auto& status_prop = payload.at("status"); status_prop.is_string()) {
                response.status = status_prop.get_string();
                if (response.status == "ok") {
                    return response;
                }
            } else if (status_prop.is_object()) {
                response.meta.metrics.error_partition_count = status_prop.at("failed").get_unsigned();
                response.meta.metrics.success_partition_count = status_prop.at("successful").get_unsigned();
                if (const auto* errors = status_prop.find("errors"); errors != nullptr && errors->is_object()) {
                    for (const auto& [location, message] : errors->get_object()) {
                        response.meta.errors.try_emplace(location, message.get_string());
                    }
                }
            } else {
                response.ctx.ec = errc::common::internal_server_failure;
                return response;
            }

            if (const auto* rows = payload.find("hits"); rows != nullptr && rows->is_array()) {
                for (const auto& entry : rows->get_array()) {
                    search_response::search_row row{};
                    row.index = entry.at("index").get_string();
                    row.id = entry.at("id").get_string();
                    row.score = entry.at("score").as<double>();
                    if (const auto* locations_map = entry.find("locations"); locations_map != nullptr && locations_map->is_object()) {
                        for (const auto& [field, terms] : locations_map->get_object()) {
                            for (const auto& [term, locations] : terms.get_object()) {
                                for (const auto& loc : locations.get_array()) {
                                    search_response::search_location location{};
                                    location.field = field;
                                    location.term = term;
                                    location.position = loc.at("pos").get_unsigned();
                                    location.start_offset = loc.at("start").get_unsigned();
                                    location.end_offset = loc.at("end").get_unsigned();
                                    if (const auto* array_positions = loc.find("array_positions");
                                        array_positions != nullptr && array_positions->is_array()) {
                                        location.array_positions.emplace(array_positions->as<std::vector<std::uint64_t>>());
                                    }
                                    row.locations.emplace_back(location);
                                }
                            }
                        }
                    }

                    if (const auto* fragments_map = entry.find("fragments"); fragments_map != nullptr && fragments_map->is_object()) {
                        for (const auto& [field, fragments] : fragments_map->get_object()) {
                            row.fragments.try_emplace(field, fragments.as<std::vector<std::string>>());
                        }
                    }
                    if (const auto* response_fields = entry.find("fields"); response_fields != nullptr && response_fields->is_object()) {
                        row.fields = utils::json::generate(*response_fields);
                    }
                    if (const auto* explanation = entry.find("explanation"); explanation != nullptr && explanation->is_object()) {
                        row.explanation = utils::json::generate(*explanation);
                    }
                    response.rows.emplace_back(row);
                }
            }

            if (const auto* response_facets = payload.find("facets"); response_facets != nullptr && response_facets->is_object()) {
                for (const auto& [name, object] : response_facets->get_object()) {
                    search_response::search_facet facet;
                    facet.name = name;
                    facet.field = object.at("field").get_string();
                    facet.total = object.at("total").get_unsigned();
                    facet.missing = object.at("missing").get_unsigned();
                    facet.other = object.at("other").get_unsigned();

                    if (const auto* date_ranges = object.find("date_ranges"); date_ranges != nullptr && date_ranges->is_array()) {
                        for (const auto& date_range : date_ranges->get_array()) {
                            search_response::search_facet::date_range_facet date_range_facet;
                            date_range_facet.name = date_range.at("name").get_string();
                            date_range_facet.count = date_range.at("count").get_unsigned();
                            if (const auto* start = date_range.find("start"); start != nullptr && start->is_string()) {
                                date_range_facet.start = start->get_string();
                            }
                            if (const auto* end = date_range.find("end"); end != nullptr && end->is_string()) {
                                date_range_facet.end = end->get_string();
                            }
                            facet.date_ranges.emplace_back(date_range_facet);
                        }
                    }

                    if (const auto& numeric_ranges = object.find("numeric_ranges");
                        numeric_ranges != nullptr && numeric_ranges->is_array()) {
                        for (const auto& numeric_range : numeric_ranges->get_array()) {
                            search_response::search_facet::numeric_range_facet numeric_range_facet;
                            numeric_range_facet.name = numeric_range.at("name").get_string();
                            numeric_range_facet.count = numeric_range.at("count").get_unsigned();
                            if (const auto* min = numeric_range.find("min"); min != nullptr) {
                                if (min->is_double()) {
                                    numeric_range_facet.min = min->as<double>();
                                } else if (min->is_integer()) {
                                    numeric_range_facet.min = min->get_unsigned();
                                }
                            }
                            if (const auto* max = numeric_range.find("max"); max != nullptr) {
                                if (max->is_double()) {
                                    numeric_range_facet.max = max->as<double>();
                                } else if (max->is_integer()) {
                                    numeric_range_facet.max = max->get_unsigned();
                                }
                            }
                            facet.numeric_ranges.emplace_back(numeric_range_facet);
                        }
                    }

                    if (const auto* terms = object.find("terms"); terms != nullptr && terms->is_array()) {
                        for (const auto& term : terms->get_array()) {
                            search_response::search_facet::term_facet term_facet;
                            term_facet.term = term.at("term").get_string();
                            term_facet.count = term.at("count").get_unsigned();
                            facet.terms.emplace_back(term_facet);
                        }
                    }

                    response.facets.emplace_back(facet);
                }
            }
            return response;
        }
        if (encoded.status_code == 400) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = errc::common::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ctx.ec = errc::common::index_not_found;
                return response;
            }
            if (response.error.find("no planPIndexes for indexName") != std::string::npos) {
                response.ctx.ec = errc::search::index_not_ready;
                return response;
            }
            if (response.error.find("pindex_consistency mismatched partition") != std::string::npos) {
                response.ctx.ec = errc::search::consistency_mismatch;
                return response;
            }
            if (response.error.find("num_fts_indexes (active + pending)") != std::string::npos) {
                response.ctx.ec = errc::common::quota_limited;
                return response;
            }
        } else if (encoded.status_code == 429) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = errc::common::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();

            if (response.error.find("num_concurrent_requests") != std::string::npos ||
                response.error.find("num_queries_per_min") != std::string::npos ||
                response.error.find("ingress_mib_per_min") != std::string::npos ||
                response.error.find("egress_mib_per_min") != std::string::npos) {
                response.ctx.ec = errc::common::rate_limited;
                return response;
            }
        }
        response.ctx.ec = errc::common::internal_server_failure;
    }
    return response;
}
} // namespace couchbase::core::operations
