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
static const std::string txn_format_string("[transactions]");
static const std::string attempt_format_string("[transactions]({}/{}):");
static const std::string lost_attempt_format_string("[lost_attempt_cleanup]");
static const std::string attempt_cleanup_format_string("[attempt_cleanup]");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"

#define CB_TXN_LOG(level, ...) COUCHBASE_LOG(__FILE__, __LINE__, COUCHBASE_LOGGER_FUNCTION, level, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG(level, ctx, msg, ...)                                                                                           \
    COUCHBASE_LOG(                                                                                                                         \
      __FILE__, __LINE__, COUCHBASE_LOGGER_FUNCTION, level, attempt_format_string + msg, ctx->transaction_id(), ctx->id(), ##__VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_TRACE(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::trace, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_DEBUG(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::debug, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_INFO(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::info, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_WARNING(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::warn, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_ERROR(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::err, __VA_ARGS__)
#define CB_ATTEMPT_CTX_LOG_CRITICAL(...) CB_ATTEMPT_CTX_LOG(couchbase::core::logger::level::critical, __VA_ARGS__)

#define CB_TXN_LOG_WITH_PREFIX(level, prefix, msg, ...) CB_TXN_LOG(level, prefix + msg, ##__VA_ARGS__)

#define CB_LOST_ATTEMPT_CLEANUP_LOG_TRACE(...)                                                                                             \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::trace, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)
#define CB_LOST_ATTEMPT_CLEANUP_LOG_DEBUG(...)                                                                                             \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::debug, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)
#define CB_LOST_ATTEMPT_CLEANUP_LOG_INFO(...)                                                                                              \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::info, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)
#define CB_LOST_ATTEMPT_CLEANUP_LOG_WARNING(...)                                                                                           \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::warn, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)
#define CB_LOST_ATTEMPT_CLEANUP_LOG_ERROR(...)                                                                                             \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::err, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)
#define CB_LOST_ATTEMPT_CLEANUP_LOG_CRITICAL(...)                                                                                          \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::critical, couchbase::core::transactions::lost_attempt_format_string, __VA_ARGS__)

#define CB_ATTEMPT_CLEANUP_LOG_TRACE(...)                                                                                                  \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::trace, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)
#define CB_ATTEMPT_CLEANUP_LOG_DEBUG(...)                                                                                                  \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::debug, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)
#define CB_ATTEMPT_CLEANUP_LOG_INFO(...)                                                                                                   \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::info, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)
#define CB_ATTEMPT_CLEANUP_LOG_WARNING(...)                                                                                                \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::warn, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)
#define CB_ATTEMPT_CLEANUP_LOG_ERROR(...)                                                                                                  \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::err, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)
#define CB_ATTEMPT_CLEANUP_LOG_CRITICAL(...)                                                                                               \
    CB_TXN_LOG_WITH_PREFIX(                                                                                                                \
      couchbase::core::logger::level::critical, couchbase::core::transactions::attempt_cleanup_format_string, __VA_ARGS__)

#define CB_TXN_LOG_TRACE(...)                                                                                                              \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::trace, couchbase::core::transactions::txn_format_string, __VA_ARGS__)
#define CB_TXN_LOG_DEBUG(...)                                                                                                              \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::debug, couchbase::core::transactions::txn_format_string, __VA_ARGS__)
#define CB_TXN_LOG_INFO(...)                                                                                                               \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::info, couchbase::core::transactions::txn_format_string, __VA_ARGS__)
#define CB_TXN_LOG_WARNING(...)                                                                                                            \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::warn, couchbase::core::transactions::txn_format_string, __VA_ARGS__)
#define CB_TXN_LOG_ERROR(...)                                                                                                              \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::err, couchbase::core::transactions::txn_format_string, __VA_ARGS__)
#define CB_TXN_LOG_CRITICAL(...)                                                                                                           \
    CB_TXN_LOG_WITH_PREFIX(couchbase::core::logger::level::critical, couchbase::core::transactions::txn_format_string, __VA_ARGS__)

#pragma clang diagnostic pop

} // namespace couchbase::core::transactions
