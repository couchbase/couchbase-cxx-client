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

#include <map>
#include <set>

#include <couchbase/platform/uuid.h>

namespace couchbase
{
struct error_map {
    enum class attribute {
        /**
         * The operation was successful for those situations where the error code is indicating successful (i.e. subdoc operations carried
         * out on a deleted document)
         */
        success,

        /**
         * This attribute means that the error is related to a constraint failure regarding the item itself, i.e. the item does not exist,
         * already exists, or its current value makes the current operation impossible. Retrying the operation when the item's value or
         * status has changed may succeed.
         */
        item_only,

        /**
         * This attribute means that a user's input was invalid because it violates the semantics of the operation, or exceeds some
         * predefined limit.
         */
        invalid_input,

        /**
         * The client's cluster map may be outdated and requires updating. The client should obtain a newer configuration.
         */
        fetch_config,

        /**
         * The current connection is no longer valid. The client must reconnect to the server. Note that the presence of other attributes
         * may indicate an alternate remedy to fixing the connection without a disconnect, but without special remedial action a disconnect
         * is needed.
         */
        conn_state_invalidated,

        /**
         * The operation failed because the client failed to authenticate or is not authorized to perform this operation. Note that this
         * error in itself does not mean the connection is invalid, unless conn-state-invalidated is also present.
         */
        auth,

        /**
         * This error code must be handled specially. If it is not handled, the connection must be dropped.
         */
        special_handling,

        /**
         * The operation is not supported, possibly because the of server version, bucket type, or current user.
         */
        support,

        /**
         * This error is transient. Note that this does not mean the error is retriable.
         */
        temp,

        /**
         * This is an internal error in the server.
         */
        internal,

        /**
         * The operation may be retried immediately.
         */
        retry_now,

        /**
         * The operation may be retried after some time.
         */
        retry_later,

        /**
         * The error is related to the subdocument subsystem.
         */
        subdoc,

        /**
         * The error is related to the DCP subsystem.
         */
        dcp,

        /**
         * Use retry specifications from the server.
         */
        auto_retry,

        /**
         * This attribute specifies that the requested item is currently locked.
         */
        item_locked,

        /**
         * This attribute means that the error is related to operating on a soft-deleted document.
         */
        item_deleted,
    };

    struct error_info {
        std::uint16_t code;
        std::string name;
        std::string description;
        std::set<attribute> attributes;

        [[nodiscard]] bool has_retry_attribute() const
        {
            return attributes.find(attribute::retry_now) != attributes.end() || attributes.find(attribute::retry_later) != attributes.end();
        }
    };

    uuid::uuid_t id;
    uint16_t version;
    uint16_t revision{};
    std::map<std::uint16_t, error_info> errors{};
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::error_map::attribute> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::error_map::attribute attr, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (attr) {
            case couchbase::error_map::attribute::success:
                name = "success";
                break;
            case couchbase::error_map::attribute::item_only:
                name = "item-only";
                break;
            case couchbase::error_map::attribute::invalid_input:
                name = "invalid-input";
                break;
            case couchbase::error_map::attribute::fetch_config:
                name = "fetch-config";
                break;
            case couchbase::error_map::attribute::conn_state_invalidated:
                name = "conn-state-invalidated";
                break;
            case couchbase::error_map::attribute::auth:
                name = "auth";
                break;
            case couchbase::error_map::attribute::special_handling:
                name = "special-handling";
                break;
            case couchbase::error_map::attribute::support:
                name = "support";
                break;
            case couchbase::error_map::attribute::temp:
                name = "temp";
                break;
            case couchbase::error_map::attribute::internal:
                name = "internal";
                break;
            case couchbase::error_map::attribute::retry_now:
                name = "retry-now";
                break;
            case couchbase::error_map::attribute::retry_later:
                name = "retry-later";
                break;
            case couchbase::error_map::attribute::subdoc:
                name = "subdoc";
                break;
            case couchbase::error_map::attribute::dcp:
                name = "dcp";
                break;
            case couchbase::error_map::attribute::auto_retry:
                name = "auto-retry";
                break;
            case couchbase::error_map::attribute::item_locked:
                name = "item-locked";
                break;
            case couchbase::error_map::attribute::item_deleted:
                name = "item-deleted";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
