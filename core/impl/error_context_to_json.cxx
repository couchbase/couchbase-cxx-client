/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "error_context_to_json.hxx"

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/key_value_error_map_attribute.hxx>
#include <couchbase/fmt/key_value_status_code.hxx>
#include <couchbase/fmt/retry_reason.hxx>

#include <fmt/core.h>

namespace couchbase::core::impl {

auto
key_value_error_context_to_json(const key_value_error_context& ctx) -> tao::json::value
{
    tao::json::value json = {
        {
          "ec",
          tao::json::value{
            { "value", ctx.ec().value() },
            { "message", ctx.ec().message() },
          },
        },
        { "operation_id", ctx.operation_id() },
        { "id", ctx.id() },
        { "bucket", ctx.bucket() },
        { "scope", ctx.scope() },
        { "collection", ctx.collection() },
    };

    if (auto val = ctx.retry_attempts(); val > 0) {
        json["retry_attempts"] = val;
    }
    if (ctx.opaque() > 0) {
        json["opaque"] = ctx.opaque();
    }

    if (!ctx.cas().empty()) {
        json["cas"] = fmt::format("{}", ctx.cas());
    }

    if (const auto& reasons = ctx.retry_reasons(); !reasons.empty()) {
        tao::json::value reasons_json = tao::json::empty_array;
        for (const auto& reason : reasons) {
            reasons_json.emplace_back(fmt::format("{}", reason));
        }
        json["retry_reasons"] = reasons_json;
    }
    if (const auto& val = ctx.last_dispatched_from(); val.has_value()) {
        json["last_dispatched_from"] = val.value();
    }
    if (const auto& val = ctx.last_dispatched_to(); val.has_value()) {
        json["last_dispatched_to"] = val.value();
    }
    if (const auto& val = ctx.status_code(); val.has_value()) {
        json["status_code"] = fmt::format("{}", val.value());
    }
    if (const auto& val = ctx.extended_error_info(); val.has_value()) {
        json["extended_error_info"] = tao::json::value{
            { "context", val->context() },
            { "reference", val->reference() },
        };
    }
    if (const auto& val = ctx.error_map_info(); val.has_value()) {
        tao::json::value info{
            { "code", val->code() },
            { "name", val->name() },
            { "description", val->description() },
        };
        if (const auto& attributes = val->attributes(); !attributes.empty()) {
            tao::json::value attrs_json = tao::json::empty_array;
            for (const auto& attr : attributes) {
                attrs_json.emplace_back(fmt::format("{}", attr));
            }
            info["attributes"] = attrs_json;
        }
        json["error_map_info"] = info;
    }
    return json;
}

auto
query_error_context_to_json(const query_error_context& ctx) -> tao::json::value
{
    tao::json::value json = {
        {
          "ec",
          tao::json::value{
            { "value", ctx.ec().value() },
            { "message", ctx.ec().message() },
          },
        },
        { "operation_id", ctx.operation_id() },
        { "retry_attempts", ctx.retry_attempts() },
        { "client_context_id", ctx.client_context_id() },
        { "statement", ctx.statement() },
        { "method", ctx.method() },
        { "path", ctx.path() },
        { "http_status", ctx.http_status() },
        { "http_body", ctx.http_body() },
        { "hostname", ctx.hostname() },
        { "port", ctx.port() },
    };

    if (const auto& val = ctx.parameters(); val.has_value()) {
        json["parameters"] = val.value();
    }
    if (ctx.first_error_code() > 0) {
        json["first_error_code"] = ctx.first_error_code();
    }
    if (!ctx.first_error_message().empty()) {
        json["first_error_message"] = ctx.first_error_message();
    }

    if (const auto& reasons = ctx.retry_reasons(); !reasons.empty()) {
        tao::json::value reasons_json = tao::json::empty_array;
        for (const auto& reason : reasons) {
            reasons_json.emplace_back(fmt::format("{}", reason));
        }
        json["retry_reasons"] = reasons_json;
    }
    if (const auto& val = ctx.last_dispatched_from(); val.has_value()) {
        json["last_dispatched_from"] = val.value();
    }
    if (const auto& val = ctx.last_dispatched_to(); val.has_value()) {
        json["last_dispatched_to"] = val.value();
    }

    return json;
}

auto
manager_error_context_to_json(const manager_error_context& ctx) -> tao::json::value
{
    tao::json::value json = {
        {
          "ec",
          tao::json::value{
            { "value", ctx.ec().value() },
            { "message", ctx.ec().message() },
          },
        },
        { "content", ctx.content() },
        { "operation_id", ctx.operation_id() },
        { "retry_attempts", ctx.retry_attempts() },
        { "client_context_id", ctx.client_context_id() },
        { "path", ctx.path() },
        { "http_status", ctx.http_status() },
    };

    if (const auto& reasons = ctx.retry_reasons(); !reasons.empty()) {
        tao::json::value reasons_json = tao::json::empty_array;
        for (const auto& reason : reasons) {
            reasons_json.emplace_back(fmt::format("{}", reason));
        }
        json["retry_reasons"] = reasons_json;
    }
    if (const auto& val = ctx.last_dispatched_from(); val.has_value()) {
        json["last_dispatched_from"] = val.value();
    }
    if (const auto& val = ctx.last_dispatched_to(); val.has_value()) {
        json["last_dispatched_to"] = val.value();
    }

    return json;
}

} // namespace couchbase::core::impl
