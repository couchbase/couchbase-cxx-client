/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include <core/logger/logger.hxx>

namespace couchbase::core::transactions
{
static const std::string txn_format_string("[transactions] - ");
static const std::string attempt_format_string("[transactions]({}/{}) - ");
static const std::string lost_attempt_format_string("[lost_attempt_cleanup]({}) - ");
static const std::string attempt_cleanup_format_string("[attempt_cleanup] - ");

#define CB_TXN_LOG(level, ...) COUCHBASE_LOG(__FILE__, __LINE__, COUCHBASE_LOGGER_FUNCTION, level, __VA_ARGS__)

#define ADD_CTX(ctx, ...) fmt::format(attempt_format_string, ctx->transaction_id(), ctx->id()) + __VA_ARGS__
#define ADD_LOST_ATTEMPT(ctx, ...) fmt::format(couchbase::core::transactions::lost_attempt_format_string, fmt::ptr(ctx)) + __VA_ARGS__
#define ADD_ATTEMPT_CLEANUP(...) couchbase::core::transactions::attempt_cleanup_format_string + __VA_ARGS__
#define ADD_TXN(...) couchbase::core::transactions::txn_format_string + __VA_ARGS__

#define CB_ATTEMPT_CTX_LOG_TRACE(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::trace, ADD_CTX(ctx, __VA_ARGS__))
#define CB_ATTEMPT_CTX_LOG_DEBUG(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::debug, ADD_CTX(ctx, __VA_ARGS__))
#define CB_ATTEMPT_CTX_LOG_INFO(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::info, ADD_CTX(ctx, __VA_ARGS__))
#define CB_ATTEMPT_CTX_LOG_WARNING(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::warn, ADD_CTX(ctx, __VA_ARGS__))
#define CB_ATTEMPT_CTX_LOG_ERROR(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::err, ADD_CTX(ctx, __VA_ARGS__))
#define CB_ATTEMPT_CTX_LOG_CRITICAL(ctx, ...) CB_TXN_LOG(couchbase::core::logger::level::critical, ADD_CTX(ctx, __VA_ARGS__))

#define CB_LOST_ATTEMPT_CLEANUP_LOG_TRACE(...) CB_TXN_LOG(couchbase::core::logger::level::trace, ADD_LOST_ATTEMPT(this, __VA_ARGS__))
#define CB_LOST_ATTEMPT_CLEANUP_LOG_DEBUG(...) CB_TXN_LOG(couchbase::core::logger::level::debug, ADD_LOST_ATTEMPT(this, __VA_ARGS__))
#define CB_LOST_ATTEMPT_CLEANUP_LOG_INFO(...) CB_TXN_LOG(couchbase::core::logger::level::info, ADD_LOST_ATTEMPT(this, __VA_ARGS__))
#define CB_LOST_ATTEMPT_CLEANUP_LOG_WARNING(...) CB_TXN_LOG(couchbase::core::logger::level::warn, ADD_LOST_ATTEMPT(this, __VA_ARGS__))
#define CB_LOST_ATTEMPT_CLEANUP_LOG_ERROR(...) CB_TXN_LOG(couchbase::core::logger::level::err, ADD_LOST_ATTEMPT(this, __VA_ARGS__))
#define CB_LOST_ATTEMPT_CLEANUP_LOG_CRITICAL(...) CB_TXN_LOG(couchbase::core::logger::level::critical, ADD_LOST_ATTEMPT(this, __VA_ARGS__))

#define CB_ATTEMPT_CLEANUP_LOG_TRACE(...) CB_TXN_LOG(couchbase::core::logger::level::trace, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))
#define CB_ATTEMPT_CLEANUP_LOG_DEBUG(...) CB_TXN_LOG(couchbase::core::logger::level::debug, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))
#define CB_ATTEMPT_CLEANUP_LOG_INFO(...) CB_TXN_LOG(couchbase::core::logger::level::info, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))
#define CB_ATTEMPT_CLEANUP_LOG_WARNING(...) CB_TXN_LOG(couchbase::core::logger::level::warn, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))
#define CB_ATTEMPT_CLEANUP_LOG_ERROR(...) CB_TXN_LOG(couchbase::core::logger::level::err, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))
#define CB_ATTEMPT_CLEANUP_LOG_CRITICAL(...) CB_TXN_LOG(couchbase::core::logger::level::critical, ADD_ATTEMPT_CLEANUP(__VA_ARGS__))

#define CB_TXN_LOG_TRACE(...) CB_TXN_LOG(couchbase::core::logger::level::trace, ADD_TXN(__VA_ARGS__))
#define CB_TXN_LOG_DEBUG(...) CB_TXN_LOG(couchbase::core::logger::level::debug, ADD_TXN(__VA_ARGS__))
#define CB_TXN_LOG_INFO(...) CB_TXN_LOG(couchbase::core::logger::level::info, ADD_TXN(__VA_ARGS__))
#define CB_TXN_LOG_WARNING(...) CB_TXN_LOG(couchbase::core::logger::level::warn, ADD_TXN(__VA_ARGS__))
#define CB_TXN_LOG_ERROR(...) CB_TXN_LOG(couchbase::core::logger::level::err, ADD_TXN(__VA_ARGS__))
#define CB_TXN_LOG_CRITICAL(...) CB_TXN_LOG(couchbase::core::logger::level::critical, ADD_TXN(__VA_ARGS__))

} // namespace couchbase::core::transactions
