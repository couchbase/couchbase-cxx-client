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

#include "range_scan_options.hxx"
#include "range_scan_orchestrator_options.hxx"
#include "scan_result.hxx"

#include <tl/expected.hpp>

#include <memory>
#include <optional>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class agent;
class range_scan_orchestrator_impl;

class range_scan_orchestrator
{
  public:
    range_scan_orchestrator(asio::io_context& io,
                            agent kv_provider,
                            std::size_t num_vbuckets,
                            std::string scope_name,
                            std::string collection_name,
                            std::variant<std::monostate, range_scan, sampling_scan> scan_type,
                            range_scan_orchestrator_options options);

    auto scan() -> tl::expected<scan_result, std::error_code>;

  private:
    std::shared_ptr<range_scan_orchestrator_impl> impl_;
};

} // namespace couchbase::core
