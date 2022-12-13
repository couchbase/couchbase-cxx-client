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

#include "range_scan_options.hxx"

#include "core/utils/binary.hxx"

namespace couchbase::core
{
range_scan::range_scan(scan_term start, scan_term end)
  : start_{ std::move(start) }
  , end_{ std::move(end) }
{
}

range_scan::range_scan(std::string_view start, std::string_view end)
  : start_{ utils::to_binary(start) }
  , end_{ utils::to_binary(end) }
{
}

range_scan::range_scan(std::string_view start, bool exclusive_start, std::string_view end, bool exclusive_end)
  : start_{ utils::to_binary(start), exclusive_start }
  , end_{ utils::to_binary(end), exclusive_end }
{
}

range_scan::range_scan(std::vector<std::byte> start, std::vector<std::byte> end)
  : start_{ std::move(start) }
  , end_{ std::move(end) }
{
}

range_scan::range_scan(std::vector<std::byte> start, bool exclusive_start, std::vector<std::byte> end, bool exclusive_end)
  : start_{ std::move(start), exclusive_start }
  , end_{ std::move(end), exclusive_end }
{
}

auto
range_scan_item_body::expiry_time() const -> std::chrono::system_clock::time_point
{
    return std::chrono::system_clock::time_point(std::chrono::seconds{ expiry });
}
} // namespace couchbase::core
