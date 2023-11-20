/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/error_context.hxx>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
class internal_manager_error_context;
#endif

/**
 * The error context returned with manager operations.
 *
 * @since 1.0.0
 * @committed
 */
class manager_error_context : public error_context
{
  public:
    /**
     * Creates empty error context
     *
     * @since 1.0.0
     * @committed
     */
    manager_error_context();
    explicit manager_error_context(internal_manager_error_context ctx);
    manager_error_context(manager_error_context&& other);
    manager_error_context& operator=(manager_error_context&& other);
    manager_error_context(const manager_error_context& other) = delete;
    manager_error_context& operator=(const manager_error_context& other) = delete;
    ~manager_error_context() override;

    [[nodiscard]] auto ec() const -> std::error_code override;

    [[nodiscard]] auto last_dispatched_to() const -> const std::optional<std::string>& override;

    [[nodiscard]] auto last_dispatched_from() const -> const std::optional<std::string>& override;

    [[nodiscard]] auto retry_attempts() const -> std::size_t override;

    [[nodiscard]] auto retry_reasons() const -> const std::set<retry_reason>& override;

    [[nodiscard]] auto retried_because_of(retry_reason reason) const -> bool override;

    /**
     * Returns request path.
     *
     * @return request path
     *
     * @since 1.0.0
     * @uncommitted
     */
    [[nodiscard]] auto path() const -> const std::string&;

    /**
     * Returns response body.
     *
     * @return response body
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto content() const -> const std::string&;

    /**
     * Returns the unique
     *
     * @return collection name
     *
     * @since 1.0.0
     * @uncommitted
     */
    [[nodiscard]] auto client_context_id() const -> const std::string&;

    /**
     * Returns HTTP status of response
     *
     * @return HTTP status of response
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto http_status() const -> std::uint32_t;

  private:
    std::unique_ptr<internal_manager_error_context> internal_;
};
} // namespace couchbase
