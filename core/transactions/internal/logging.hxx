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

#include <spdlog/fmt/ostr.h>
#include <spdlog/logger.h>

#define TXN_LOG "transactions"
#define ATTEMPT_CLEANUP_LOG "attempt_cleanup"
#define LOST_ATTEMPT_CLEANUP_LOG "lost_attempt_cleanup"

namespace couchbase::core::transactions
{
static const std::string attempt_format_string("[{}/{}]:");

std::shared_ptr<spdlog::logger>
init_txn_log();
std::shared_ptr<spdlog::logger>
init_attempt_cleanup_log();
std::shared_ptr<spdlog::logger>
init_lost_attempts_log();

static std::shared_ptr<spdlog::logger> txn_log = init_txn_log();
static std::shared_ptr<spdlog::logger> attempt_cleanup_log = init_attempt_cleanup_log();
static std::shared_ptr<spdlog::logger> lost_attempts_cleanup_log = init_lost_attempts_log();

} // namespace couchbase::core::transactions
