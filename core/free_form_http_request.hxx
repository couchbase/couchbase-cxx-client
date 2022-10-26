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

#include "utils/movable_function.hxx"

#include <chrono>
#include <cinttypes>
#include <map>
#include <memory>
#include <string>
#include <system_error>
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
enum class http_service_type {
    management,
    capi,
    n1ql,
    search,
    analytics,
};

class http_request
{
  public:
    http_service_type service;
    std::string method{};
    std::string endpoint{};
    std::string path{};
    std::string username{};
    std::string password{};
    std::vector<std::byte> body{};
    std::map<std::string, std::string> headers{};
    std::string content_type{};
    bool is_idempotent{};
    std::string unique_id{};
    std::shared_ptr<couchbase::retry_strategy> retry_strategy{};
    std::chrono::milliseconds timeout{};
    std::shared_ptr<couchbase::tracing::request_span> parent_span{};

    struct {
        std::string user{};
    } internal{};
};

class http_response_impl;

class http_response
{
  public:
    http_response();

    auto endpoint() -> std::string;
    auto status_code() -> std::uint32_t;
    auto content_length() -> std::size_t;

    auto body() -> std::vector<std::byte>;
    auto close() -> std::error_code;

  private:
    std::shared_ptr<http_response_impl> impl_;
};

using free_form_http_request_callback = utils::movable_function<void(http_response response, std::error_code ec)>;

} // namespace couchbase::core
