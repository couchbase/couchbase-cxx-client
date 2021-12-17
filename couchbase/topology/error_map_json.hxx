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

#include <couchbase/topology/error_map.hxx>

#include <gsl/narrow>
#include <tao/json/forward.hpp>

namespace tao::json
{
template<>
struct traits<couchbase::error_map> {
    template<template<typename...> class Traits>
    static couchbase::error_map as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::error_map result;
        result.id = couchbase::uuid::random();
        result.version = v.at("revision").template as<std::uint16_t>();
        result.revision = v.at("revision").template as<std::uint16_t>();
        for (const auto& [code, definition] : v.at("errors").get_object()) {
            couchbase::error_map::error_info ei;
            ei.code = gsl::narrow_cast<std::uint16_t>(std::stoul(code, nullptr, 16));
            const auto& info = definition.get_object();
            ei.name = info.at("name").get_string();
            ei.description = info.at("desc").get_string();
            for (const auto& attribute : info.at("attrs").get_array()) {
                if (const std::string& attr_val = attribute.get_string(); attr_val == "success") {
                    ei.attributes.insert(couchbase::error_map::attribute::success);
                } else if (attr_val == "item-only") {
                    ei.attributes.insert(couchbase::error_map::attribute::item_only);
                } else if (attr_val == "invalid-input") {
                    ei.attributes.insert(couchbase::error_map::attribute::invalid_input);
                } else if (attr_val == "fetch-config") {
                    ei.attributes.insert(couchbase::error_map::attribute::fetch_config);
                } else if (attr_val == "conn-state-invalidated") {
                    ei.attributes.insert(couchbase::error_map::attribute::conn_state_invalidated);
                } else if (attr_val == "auth") {
                    ei.attributes.insert(couchbase::error_map::attribute::auth);
                } else if (attr_val == "special-handling") {
                    ei.attributes.insert(couchbase::error_map::attribute::special_handling);
                } else if (attr_val == "support") {
                    ei.attributes.insert(couchbase::error_map::attribute::support);
                } else if (attr_val == "temp") {
                    ei.attributes.insert(couchbase::error_map::attribute::temp);
                } else if (attr_val == "internal") {
                    ei.attributes.insert(couchbase::error_map::attribute::internal);
                } else if (attr_val == "retry-now") {
                    ei.attributes.insert(couchbase::error_map::attribute::retry_now);
                } else if (attr_val == "retry-later") {
                    ei.attributes.insert(couchbase::error_map::attribute::retry_later);
                } else if (attr_val == "subdoc") {
                    ei.attributes.insert(couchbase::error_map::attribute::subdoc);
                } else if (attr_val == "dcp") {
                    ei.attributes.insert(couchbase::error_map::attribute::dcp);
                } else if (attr_val == "auto-retry") {
                    ei.attributes.insert(couchbase::error_map::attribute::auto_retry);
                } else if (attr_val == "item-locked") {
                    ei.attributes.insert(couchbase::error_map::attribute::item_locked);
                } else if (attr_val == "item-deleted") {
                    ei.attributes.insert(couchbase::error_map::attribute::item_deleted);
                } else if (attr_val == "rate-limit") {
                    ei.attributes.insert(couchbase::error_map::attribute::rate_limit);
                } else {
                    LOG_WARNING(R"(skipping unknown attribute "{}" in error map for code={} and name="{}")", attr_val, ei.code, ei.name);
                }
            }
            result.errors.emplace(ei.code, ei);
        }
        return result;
    }
};
} // namespace tao::json
