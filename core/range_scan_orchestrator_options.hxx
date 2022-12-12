/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include "scan_options.hxx"

#include <couchbase/mutation_token.hxx>
#include <couchbase/retry_strategy.hxx>

#include <cinttypes>
#include <memory>
#include <optional>
#include <system_error>
#include <variant>
#include <vector>

namespace couchbase
{
class retry_strategy;
namespace tracing
{
class request_span;
} // namespace tracing
} // namespace couchbase

namespace couchbase::core
{

struct range_scan_orchestrator_options {
    bool ids_only{ false };
    std::optional<mutation_state> consistent_with{};
    scan_sort sort{ scan_sort::none };
    std::uint32_t batch_item_limit{ range_scan_continue_options::default_batch_item_limit };
    std::uint32_t batch_byte_limit{ range_scan_continue_options::default_batch_byte_limit };
    std::chrono::milliseconds batch_time_limit{ range_scan_continue_options::default_batch_time_limit };

    std::shared_ptr<couchbase::retry_strategy> retry_strategy{ nullptr };
    std::chrono::milliseconds timeout{};
    std::shared_ptr<couchbase::tracing::request_span> parent_span{};
};
} // namespace couchbase::core
