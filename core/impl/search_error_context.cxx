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

#include "internal_search_error_context.hxx"

#include <couchbase/search_error_context.hxx>

namespace couchbase
{
search_error_context::search_error_context()
  : internal_{ nullptr }
{
}

search_error_context::search_error_context(internal_search_error_context ctx)
  : internal_{ std::make_unique<internal_search_error_context>(std::move(ctx)) }
{
}

search_error_context::search_error_context(search_error_context&& other) = default;

search_error_context&
search_error_context::operator=(search_error_context&& other) = default;

search_error_context::~search_error_context() = default;

auto
search_error_context::ec() const -> std::error_code
{
    return internal_->ec();
}

auto
search_error_context::last_dispatched_to() const -> const std::optional<std::string>&
{
    return internal_->last_dispatched_to();
}

auto
search_error_context::last_dispatched_from() const -> const std::optional<std::string>&
{
    return internal_->last_dispatched_from();
}

auto
search_error_context::retry_attempts() const -> std::size_t
{
    return internal_->retry_attempts();
}

auto
search_error_context::retry_reasons() const -> const std::set<retry_reason>&
{
    return internal_->retry_reasons();
}

auto
search_error_context::retried_because_of(retry_reason reason) const -> bool
{
    return internal_->retried_because_of(reason);
}

auto
search_error_context::index_name() const -> const std::string&
{
    return internal_->index_name();
}

auto
search_error_context::client_context_id() const -> const std::string&
{
    return internal_->client_context_id();
}

auto
search_error_context::query() const -> const std::string&
{
    return internal_->query();
}

auto
search_error_context::parameters() const -> const std::optional<std::string>&
{
    return internal_->parameters();
}

auto
search_error_context::method() const -> const std::string&
{
    return internal_->method();
}

auto
search_error_context::path() const -> const std::string&
{
    return internal_->path();
}

auto
search_error_context::http_status() const -> std::uint32_t
{
    return internal_->http_status();
}

auto
search_error_context::http_body() const -> const std::string&
{
    return internal_->http_body();
}

auto
search_error_context::hostname() const -> const std::string&
{
    return internal_->hostname();
}

auto
search_error_context::port() const -> std::uint16_t
{
    return internal_->port();
}
auto
search_error_context::error() const -> const std::string&
{
    return internal_->error();
}

auto
search_error_context::status() const -> const std::string&
{
    return internal_->status();
}
} // namespace couchbase
