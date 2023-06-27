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

#include "internal_manager_error_context.hxx"
#include <couchbase/error_context.hxx>

#include <optional>

namespace couchbase
{
internal_manager_error_context::internal_manager_error_context(internal_manager_error_context&& other) noexcept = default;

internal_manager_error_context&
internal_manager_error_context::operator=(internal_manager_error_context&& other) noexcept = default;

internal_manager_error_context::internal_manager_error_context(std::error_code ec,
                                                               std::optional<std::string> last_dispatched_to,
                                                               std::optional<std::string> last_dispatched_from,
                                                               std::size_t retry_attempts,
                                                               std::set<retry_reason> retry_reasons,
                                                               std::string client_context_id,
                                                               std::uint32_t http_status,
                                                               std::string content,
                                                               std::string path)
  : ctx_{ ec,
          std::move(client_context_id),
          {},
          std::move(path),
          http_status,
          { std::move(content) },
          {},
          {},
          std::move(last_dispatched_to),
          std::move(last_dispatched_from),
          retry_attempts,
          std::move(retry_reasons) }
{
}

auto
internal_manager_error_context::path() const -> const std::string&
{
    return ctx_.path;
}

auto
internal_manager_error_context::content() const -> const std::string&
{
    return ctx_.http_body;
}

auto
internal_manager_error_context::client_context_id() const -> const std::string&
{
    return ctx_.client_context_id;
}

auto
internal_manager_error_context::http_status() const -> std::uint32_t
{
    return ctx_.http_status;
}

auto
internal_manager_error_context::ec() const -> std::error_code
{
    return ctx_.ec;
}

auto
internal_manager_error_context::last_dispatched_to() const -> const std::optional<std::string>&
{
    return ctx_.last_dispatched_to;
}

auto
internal_manager_error_context::last_dispatched_from() const -> const std::optional<std::string>&
{
    return ctx_.last_dispatched_from;
}

auto
internal_manager_error_context::retry_attempts() const -> std::size_t
{
    return ctx_.retry_attempts;
}

auto
internal_manager_error_context::retry_reasons() const -> const std::set<retry_reason>&
{
    return ctx_.retry_reasons;
}

auto
internal_manager_error_context::retried_because_of(retry_reason reason) const -> bool
{
    return ctx_.retry_reasons.count(reason) > 0;
}
} // namespace couchbase
