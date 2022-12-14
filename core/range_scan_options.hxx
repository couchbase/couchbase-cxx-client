/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include "couchbase/cas.hxx"
#include "couchbase/retry_strategy.hxx"
#include "utils/movable_function.hxx"

#include <cinttypes>
#include <memory>
#include <optional>
#include <system_error>
#include <variant>
#include <vector>

namespace couchbase::core
{
struct scan_term {
    static constexpr std::byte minimum_marker{ 0x00 };
    static constexpr std::byte maximum_marker{ 0xff };

    std::vector<std::byte> id;
    bool exclusive{ false };
};

struct range_scan {
    range_scan() = default;
    range_scan(scan_term start, scan_term end);
    range_scan(std::string_view start, std::string_view end);
    range_scan(std::string_view start, bool exclusive_start, std::string_view end, bool exclusive_end);
    range_scan(std::vector<std::byte> start, std::vector<std::byte> end);
    range_scan(std::vector<std::byte> start, bool exclusive_start, std::vector<std::byte> end, bool exclusive_end);

    scan_term start_{ { scan_term::minimum_marker } };
    scan_term end_{ { scan_term::maximum_marker } };
};

struct sampling_scan {
    std::size_t limit{};
    std::optional<std::uint32_t> seed{};
};

struct range_snapshot_requirements {
    std::uint64_t vbucket_uuid{};
    std::uint64_t sequence_number{};
    bool sequence_number_exists{ false };
};

struct range_scan_create_options {
    std::string scope_name{};
    std::string collection_name{};
    std::variant<std::monostate, range_scan, sampling_scan> scan_type{};
    std::chrono::milliseconds timeout{};
    std::uint32_t collection_id{ 0 };
    std::optional<range_snapshot_requirements> snapshot_requirements{};
    bool ids_only{ false };
    std::shared_ptr<couchbase::retry_strategy> retry_strategy{ nullptr };

    struct {
        std::string user{};
    } internal{};
};

struct range_scan_create_result {
    std::vector<std::byte> scan_uuid;
    bool ids_only;
};

using range_scan_create_callback = utils::movable_function<void(range_scan_create_result, std::error_code)>;

struct range_scan_continue_options {
    static constexpr std::uint32_t default_batch_item_limit{ 50 };
    static constexpr std::uint32_t default_batch_byte_limit{ 15000 };
    static constexpr std::chrono::milliseconds default_batch_time_limit{ 0 };

    std::uint32_t batch_item_limit{ default_batch_item_limit };
    std::uint32_t batch_byte_limit{ default_batch_byte_limit };
    std::chrono::milliseconds batch_time_limit{ default_batch_time_limit };
    std::shared_ptr<couchbase::retry_strategy> retry_strategy{ nullptr };

    bool ids_only{ false }; // support servers before MB-54267. TODO: remove after server GA
    struct {
        std::string user{};
    } internal{};
};

struct range_scan_continue_result {
    bool more;
    bool complete;
    bool ids_only;
};

using range_scan_continue_callback = utils::movable_function<void(range_scan_continue_result, std::error_code)>;

struct range_scan_cancel_options {
    std::chrono::milliseconds timeout;
    std::shared_ptr<couchbase::retry_strategy> retry_strategy{ nullptr };

    struct {
        std::string user{};
    } internal{};
};

struct range_scan_item_body {
    std::uint32_t flags{};
    std::uint32_t expiry{};
    couchbase::cas cas{};
    std::uint64_t sequence_number{};
    std::byte datatype{};
    std::vector<std::byte> value{};

    [[nodiscard]] auto expiry_time() const -> std::chrono::system_clock::time_point;
};

struct range_scan_item {
    std::vector<std::byte> key{};
    std::optional<range_scan_item_body> body{};
};

using range_scan_item_callback = utils::movable_function<void(range_scan_item item)>;

struct range_scan_cancel_result {
};

using range_scan_cancel_callback = utils::movable_function<void(range_scan_cancel_result, std::error_code)>;
} // namespace couchbase::core
