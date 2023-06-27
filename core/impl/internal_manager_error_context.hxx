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

#pragma once

#include "core/error_context/http.hxx"

#include <cstdint>
#include <optional>
#include <string>

namespace couchbase
{
class internal_manager_error_context
{
  public:
    explicit internal_manager_error_context(std::error_code ec,
                                            std::optional<std::string> last_dispatched_to,
                                            std::optional<std::string> last_dispatched_from,
                                            std::size_t retry_attempts,
                                            std::set<retry_reason> retry_reasons,
                                            std::string client_context_id,
                                            std::uint32_t http_status,
                                            std::string content,
                                            std::string path);
    internal_manager_error_context(internal_manager_error_context&& other) noexcept;
    internal_manager_error_context& operator=(internal_manager_error_context&& other) noexcept;
    internal_manager_error_context(const internal_manager_error_context& other) = delete;
    internal_manager_error_context& operator=(const internal_manager_error_context& other) = delete;

    [[nodiscard]] auto path() const -> const std::string&;
    [[nodiscard]] auto content() const -> const std::string&;
    [[nodiscard]] auto client_context_id() const -> const std::string&;
    [[nodiscard]] auto http_status() const -> std::uint32_t;

    [[nodiscard]] auto ec() const -> std::error_code;
    [[nodiscard]] auto last_dispatched_to() const -> const std::optional<std::string>&;
    [[nodiscard]] auto last_dispatched_from() const -> const std::optional<std::string>&;
    [[nodiscard]] auto retry_attempts() const -> std::size_t;
    [[nodiscard]] auto retry_reasons() const -> const std::set<retry_reason>&;
    [[nodiscard]] auto retried_because_of(retry_reason reason) const -> bool;

  private:
    core::error_context::http ctx_;
};
} // namespace couchbase
