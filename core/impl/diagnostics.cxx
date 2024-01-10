/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-present Couchbase, Inc.
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

#include "diagnostics.hxx"

#include "core/diagnostics.hxx"
#include "core/service_type.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/diagnostics_result.hxx>
#include <couchbase/endpoint_ping_report.hxx>
#include <couchbase/ping_result.hxx>
#include <couchbase/service_type.hxx>

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace couchbase
{
namespace
{
std::string
service_type_as_string(service_type service_type)
{
    switch (service_type) {
        case service_type::key_value:
            return "kv";
        case service_type::query:
            return "query";
        case service_type::analytics:
            return "analytics";
        case service_type::search:
            return "search";
        case service_type::view:
            return "views";
        case service_type::management:
            return "mgmt";
        case service_type::eventing:
            return "eventing";
    }
    return "";
}

std::string
ping_state_as_string(ping_state state)
{
    switch (state) {
        case ping_state::ok:
            return "ok";
        case ping_state::timeout:
            return "timeout";
        case ping_state::error:
            return "error";
    }
    return "";
}

std::string
endpoint_state_as_string(endpoint_state state)
{
    switch (state) {
        case endpoint_state::connected:
            return "connected";
        case endpoint_state::connecting:
            return "connecting";
        case endpoint_state::disconnected:
            return "disconnected";
        case endpoint_state::disconnecting:
            return "disconnecting";
    }
    return "";
}

codec::tao_json_serializer::document_type
endpoint_ping_report_as_json(const endpoint_ping_report& report)
{
    codec::tao_json_serializer::document_type res{
        { "id", report.id() },       { "latency_us", report.latency().count() },        { "remote", report.remote() },
        { "local", report.local() }, { "state", ping_state_as_string(report.state()) },
    };
    if (report.error()) {
        res["error"] = report.error().value();
    }
    if (report.endpoint_namespace()) {
        res["namespace"] = report.endpoint_namespace().value();
    }
    return res;
}

codec::tao_json_serializer::document_type
endpoint_diagnostics_as_json(const endpoint_diagnostics& report)
{
    codec::tao_json_serializer::document_type res{
        { "id", report.id() },
        { "local", report.local() },
        { "remote", report.remote() },
        { "state", endpoint_state_as_string(report.state()) },
    };
    if (report.last_activity()) {
        res["last_activity_us"] = report.last_activity().value().count();
    }
    if (report.endpoint_namespace()) {
        res["namespace"] = report.endpoint_namespace().value();
    }
    if (report.details()) {
        res["details"] = report.details();
    }
    return res;
}
} // namespace

auto
ping_result::as_json() const -> codec::tao_json_serializer::document_type
{
    codec::tao_json_serializer::document_type endpoints{};
    for (const auto& [service_type, reports] : endpoints_) {
        std::vector<codec::tao_json_serializer::document_type> json_reports{};
        for (const auto& report : reports) {
            json_reports.emplace_back(endpoint_ping_report_as_json(report));
        }
        endpoints[service_type_as_string(service_type)] = json_reports;
    }
    return {
        { "version", version_ },
        { "id", id_ },
        { "sdk", sdk_ },
        { "services", endpoints },
    };
}

auto
diagnostics_result::as_json() const -> codec::tao_json_serializer::document_type
{
    codec::tao_json_serializer::document_type endpoints{};
    for (const auto& [service_type, reports] : endpoints_) {
        std::vector<codec::tao_json_serializer::document_type> json_reports{};
        for (const auto& report : reports) {
            json_reports.emplace_back(endpoint_diagnostics_as_json(report));
        }
        endpoints[service_type_as_string(service_type)] = json_reports;
    }
    return {
        { "id", id_ },
        { "sdk", sdk_ },
        { "version", version_ },
        { "services", endpoints },
    };
}
} // namespace couchbase

namespace couchbase::core::impl
{
namespace
{
couchbase::service_type
to_public_service_type(core::service_type service_type)
{
    switch (service_type) {
        case core::service_type::key_value:
            return couchbase::service_type::key_value;
        case core::service_type::query:
            return couchbase::service_type::query;
        case core::service_type::analytics:
            return couchbase::service_type::analytics;
        case core::service_type::search:
            return couchbase::service_type::search;
        case core::service_type::view:
            return couchbase::service_type::view;
        case core::service_type::management:
            return couchbase::service_type::management;
        case core::service_type::eventing:
            return couchbase::service_type::eventing;
    }
    return {};
}

couchbase::ping_state
to_public_ping_state(core::diag::ping_state ping_state)
{
    switch (ping_state) {
        case core::diag::ping_state::timeout:
            return couchbase::ping_state::timeout;
        case core::diag::ping_state::error:
            return couchbase::ping_state::error;
        case core::diag::ping_state::ok:
            return couchbase::ping_state::ok;
    }
    return {};
}

couchbase::endpoint_state
to_public_endpoint_state(core::diag::endpoint_state endpoint_state)
{
    switch (endpoint_state) {
        case core::diag::endpoint_state::connected:
            return couchbase::endpoint_state::connected;
        case diag::endpoint_state::disconnected:
            return couchbase::endpoint_state::disconnected;
        case diag::endpoint_state::connecting:
            return couchbase::endpoint_state::connecting;
        case diag::endpoint_state::disconnecting:
            return couchbase::endpoint_state::disconnecting;
    }
    return {};
}
} // namespace

std::set<core::service_type>
to_core_service_types(const std::set<couchbase::service_type>& service_types)
{
    std::set<core::service_type> res{};
    for (auto s : service_types) {
        switch (s) {
            case couchbase::service_type::key_value:
                res.emplace(core::service_type::key_value);
                break;
            case couchbase::service_type::query:
                res.emplace(core::service_type::query);
                break;
            case couchbase::service_type::analytics:
                res.emplace(core::service_type::analytics);
                break;
            case couchbase::service_type::search:
                res.emplace(core::service_type::search);
                break;
            case couchbase::service_type::view:
                res.emplace(core::service_type::view);
                break;
            case couchbase::service_type::management:
                res.emplace(core::service_type::management);
                break;
            case couchbase::service_type::eventing:
                res.emplace(core::service_type::eventing);
                break;
        }
    }
    return res;
}

couchbase::ping_result
build_result(const core::diag::ping_result& result)
{
    std::map<couchbase::service_type, std::vector<couchbase::endpoint_ping_report>> endpoints{};
    for (const auto& [core_service_type, core_endpoints] : result.services) {
        auto service_type = to_public_service_type(core_service_type);
        endpoints[service_type] = std::vector<couchbase::endpoint_ping_report>{};
        for (const auto& info : core_endpoints) {
            endpoints[service_type].emplace_back(
              service_type, info.id, info.local, info.remote, to_public_ping_state(info.state), info.error, info.bucket, info.latency);
        }
    }

    return { result.id, static_cast<std::uint16_t>(result.version), result.sdk, endpoints };
}

couchbase::diagnostics_result
build_result(const core::diag::diagnostics_result& result)
{
    std::map<couchbase::service_type, std::vector<couchbase::endpoint_diagnostics>> endpoints{};
    for (const auto& [core_service_type, core_endpoints] : result.services) {
        auto service_type = to_public_service_type(core_service_type);
        endpoints[service_type] = std::vector<couchbase::endpoint_diagnostics>{};
        for (const auto& info : core_endpoints) {
            endpoints[service_type].emplace_back(service_type,
                                                 info.id,
                                                 info.last_activity,
                                                 info.local,
                                                 info.remote,
                                                 info.bucket,
                                                 to_public_endpoint_state(info.state),
                                                 info.details);
        }
    }

    return { result.id, static_cast<std::uint16_t>(result.version), result.sdk, endpoints };
}
} // namespace couchbase::core::impl
