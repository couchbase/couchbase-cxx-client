/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include <couchbase/error.hxx>
#include <couchbase/error_context.hxx>

#include <core/error_context/analytics_json.hxx>
#include <core/error_context/http_json.hxx>
#include <core/error_context/query_json.hxx>
#include <core/error_context/search_json.hxx>

#include <memory>
#include <optional>
#include <system_error>
#include <utility>

namespace couchbase
{
error::error(std::error_code ec, std::string message, couchbase::error_context ctx)
  : ec_{ ec }
  , message_{ std::move(message) }
  , ctx_{ std::move(ctx) }
{
}

error::error(std::error_code ec, std::string message, couchbase::error_context ctx, couchbase::error cause)
  : ec_{ ec }
  , message_{ std::move(message) }
  , ctx_{ std::move(ctx) }
  , cause_{ std::make_shared<error>(std::move(cause)) }
{
}

auto
error::ec() const -> std::error_code
{
    return ec_;
}

auto
error::message() const -> const std::string&
{
    return message_;
}

auto
error::ctx() const -> const error_context&
{
    return ctx_;
}

auto
error::cause() const -> std::optional<error>
{
    if (!cause_) {
        return {};
    }

    return *cause_;
}

error::operator bool() const
{
    return ec_.value() != 0;
}

error
make_error(const core::error_context::query& core_ctx)
{
    return { core_ctx.ec, core_ctx.ec.message(), couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const core::error_context::search& core_ctx)
{
    return { core_ctx.ec, core_ctx.ec.message(), couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const core::error_context::analytics& core_ctx)
{
    return { core_ctx.ec, core_ctx.ec.message(), couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const core::error_context::http& core_ctx)
{
    return { core_ctx.ec, core_ctx.ec.message(), couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(core::error_context::http core_ctx, std::optional<std::error_code> ec)
{
    if (ec.has_value()) {
        core_ctx.ec = ec.value();
    }
    return make_error(core_ctx);
}
} // namespace couchbase
