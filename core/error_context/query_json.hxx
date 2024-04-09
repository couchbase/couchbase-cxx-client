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

#include <core/error_context/query.hxx>

#include <tao/json/forward.hpp>
#include <tao/json/value.hpp>

#include <couchbase/fmt/retry_reason.hxx>

namespace tao::json
{
template<>
struct traits<couchbase::core::error_context::query> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const couchbase::core::error_context::query& ctx)
    {
        v["ec"] =
          tao::json::value{
              { "value", ctx.ec.value() },
              { "message", ctx.ec.message() },
          },
        v["retry_attempts"] = ctx.retry_attempts;
        v["client_context_id"] = ctx.client_context_id;
        v["statement"] = ctx.statement;
        v["method"] = ctx.method;
        v["path"] = ctx.path;
        v["http_status"] = ctx.http_status;
        v["http_body"] = ctx.http_body;
        v["hostname"] = ctx.hostname;
        v["port"] = ctx.port;

        if (const auto& val = ctx.parameters; val.has_value()) {
            v["parameters"] = val.value();
        }
        if (ctx.first_error_code > 0) {
            v["first_error_code"] = ctx.first_error_code;
        }
        if (!ctx.first_error_message.empty()) {
            v["first_error_message"] = ctx.first_error_message;
        }
        if (const auto& reasons = ctx.retry_reasons; !reasons.empty()) {
            tao::json::value reasons_json = tao::json::empty_array;
            for (const auto& reason : reasons) {
                reasons_json.emplace_back(fmt::format("{}", reason));
            }
            v["retry_reasons"] = reasons_json;
        }
        if (const auto& val = ctx.last_dispatched_from; val.has_value()) {
            v["last_dispatched_from"] = val.value();
        }
        if (const auto& val = ctx.last_dispatched_to; val.has_value()) {
            v["last_dispatched_to"] = val.value();
        }
    }
};
} // namespace tao::json
