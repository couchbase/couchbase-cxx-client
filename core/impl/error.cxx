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
#include <couchbase/transaction_error_context.hxx>

#include "core/error_context/analytics_json.hxx"
#include "core/error_context/http_json.hxx"
#include "core/error_context/key_value_json.hxx"
#include "core/error_context/query_json.hxx"
#include "core/error_context/query_public_json.hxx"
#include "core/error_context/search_json.hxx"
#include "core/error_context/subdocument_json.hxx"
#include "error.hxx"

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

auto
error::operator==(const couchbase::error& other) const -> bool
{
    return ec() == other.ec() && message() == other.message();
}

namespace core::impl
{
error
make_error(const core::error_context::query& core_ctx)
{
    return { core_ctx.ec, "", couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const query_error_context& core_ctx)
{
    tao::json::value ctx(core_ctx);
    return { core_ctx.ec(), {}, couchbase::error_context(ctx) };
}

error
make_error(const core::error_context::search& core_ctx)
{
    return { core_ctx.ec, "", couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const core::error_context::analytics& core_ctx)
{
    return { core_ctx.ec, "", couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const core::error_context::http& core_ctx)
{
    return { core_ctx.ec, "", couchbase::error_context{ internal_error_context(core_ctx) } };
}

error
make_error(const couchbase::key_value_error_context& core_ctx)
{
    tao::json::value ctx(core_ctx);
    return { core_ctx.ec(), "", couchbase::error_context(ctx) };
}

error
make_error(const couchbase::subdocument_error_context& core_ctx)
{
    tao::json::value ctx(core_ctx);
    return { core_ctx.ec(), "", couchbase::error_context(ctx) };
}

error
make_error(const couchbase::transaction_error_context& ctx)
{
    return { ctx.ec(), "", {}, { ctx.cause() } };
}

error
make_error(const couchbase::transaction_op_error_context& ctx)
{
    if (std::holds_alternative<key_value_error_context>(ctx.cause())) {
        return { ctx.ec(), "", {}, make_error(std::get<key_value_error_context>(ctx.cause())) };
    }
    if (std::holds_alternative<query_error_context>(ctx.cause())) {
        return { ctx.ec(), "", {}, make_error(std::get<query_error_context>(ctx.cause())) };
    }
    return ctx.ec();
}

couchbase::transactions::final_error
map_final_error(const couchbase::core::transactions::final_error& final_err)
{
    switch (final_err) {
        case couchbase::core::transactions::final_error::FAILED_POST_COMMIT:
            return couchbase::transactions::final_error::FAILED_POST_COMMIT;
        case couchbase::core::transactions::final_error::FAILED:
            return couchbase::transactions::final_error::FAILED;
        case couchbase::core::transactions::final_error::AMBIGUOUS:
            return couchbase::transactions::final_error::AMBIGUOUS;
        case couchbase::core::transactions::final_error::EXPIRED:
            return couchbase::transactions::final_error::EXPIRED;
    }
}

couchbase::transactions::error_class
map_error_class(const couchbase::core::transactions::error_class& error_class)
{
    switch (error_class) {
        case couchbase::core::transactions::error_class::FAIL_HARD:
            return couchbase::transactions::error_class::FAIL_HARD;
        case couchbase::core::transactions::error_class::FAIL_OTHER:
            return couchbase::transactions::error_class::FAIL_OTHER;
        case couchbase::core::transactions::error_class::FAIL_TRANSIENT:
            return couchbase::transactions::error_class::FAIL_TRANSIENT;
        case couchbase::core::transactions::error_class::FAIL_AMBIGUOUS:
            return couchbase::transactions::error_class::FAIL_AMBIGUOUS;
        case couchbase::core::transactions::error_class::FAIL_DOC_ALREADY_EXISTS:
            return couchbase::transactions::error_class::FAIL_DOC_ALREADY_EXISTS;
        case couchbase::core::transactions::error_class::FAIL_DOC_NOT_FOUND:
            return couchbase::transactions::error_class::FAIL_DOC_NOT_FOUND;
        case couchbase::core::transactions::error_class::FAIL_PATH_NOT_FOUND:
            return couchbase::transactions::error_class::FAIL_PATH_NOT_FOUND;
        case couchbase::core::transactions::error_class::FAIL_CAS_MISMATCH:
            return couchbase::transactions::error_class::FAIL_CAS_MISMATCH;
        case couchbase::core::transactions::error_class::FAIL_WRITE_WRITE_CONFLICT:
            return couchbase::transactions::error_class::FAIL_WRITE_WRITE_CONFLICT;
        case couchbase::core::transactions::error_class::FAIL_ATR_FULL:
            return couchbase::transactions::error_class::FAIL_ATR_FULL;
        case couchbase::core::transactions::error_class::FAIL_PATH_ALREADY_EXISTS:
            return couchbase::transactions::error_class::FAIL_PATH_ALREADY_EXISTS;
        case couchbase::core::transactions::error_class::FAIL_EXPIRY:
            return couchbase::transactions::error_class::FAIL_EXPIRY;
    }
}

const couchbase::transactions::transaction_operation_failed&
make_tof(const couchbase::core::transactions::transaction_operation_failed& core_tof)
{

    static couchbase::transactions::transaction_operation_failed tof{ map_error_class(core_tof.ec()),
                                                               core_tof.what(),
                                                               core_tof.should_retry(),
                                                               core_tof.should_rollback(),
                                                               map_final_error(core_tof.to_raise()),
                                                               error(errc::make_error_code(
                                                                 transaction_op_errc_from_external_exception(core_tof.cause()))) };
    return tof;
}

} // namespace core::impl
} // namespace couchbase
