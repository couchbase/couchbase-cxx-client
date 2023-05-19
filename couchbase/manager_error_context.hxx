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
#include <optional>
#include <string>

namespace couchbase
{
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
    manager_error_context() = default;

    /**
     * Creates and initializes error context with given parameters.
     *
     * @param ec
     * @param last_dispatched_to
     * @param last_dispatched_from
     * @param retry_attempts
     * @param retry_reasons
     * @param client_context_id
     * @param http_status
     * @param content
     * @param path
     *
     * @since 1.0.0
     * @internal
     */
    manager_error_context(std::error_code ec,
                          std::optional<std::string> last_dispatched_to,
                          std::optional<std::string> last_dispatched_from,
                          std::size_t retry_attempts,
                          std::set<retry_reason> retry_reasons,
                          std::string client_context_id,
                          std::uint32_t http_status,
                          std::string content,
                          std::string path)
      : error_context{ {}, ec, std::move(last_dispatched_to), std::move(last_dispatched_from), retry_attempts, std::move(retry_reasons) }
      , client_context_id_{ std::move(client_context_id) }
      , http_status_{ http_status }
      , content_{ std::move(content) }
      , path_{ std::move(path) }
    {
    }

    /**
     * Returns request path.
     *
     * @return request path
     *
     * @since 1.0.0
     * @uncommitted
     */
    [[nodiscard]] auto path() const -> const std::string&
    {
        return path_;
    }

    /**
     * Returns response body.
     *
     * @return response body
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto content() const -> const std::string&
    {
        return content_;
    }

    /**
     * Returns the unique
     *
     * @return collection name
     *
     * @since 1.0.0
     * @uncommitted
     */
    [[nodiscard]] auto client_context_id() const -> const std::string&
    {
        return client_context_id_;
    }

    /**
     * Returns HTTP status of response
     *
     * @return HTTP status of response
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto http_status() const -> std::uint32_t
    {
        return http_status_;
    }

  private:
    std::string client_context_id_{};
    std::uint32_t http_status_{};
    std::string content_{};
    std::string path_{};
};
} // namespace couchbase
