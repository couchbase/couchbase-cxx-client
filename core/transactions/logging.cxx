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

#include "internal/logging.hxx"
#include "core/transactions.hxx"

#include <spdlog/sinks/stdout_sinks.h>

namespace couchbase::core::transactions
{
// TODO: consider always using async logger?  Makes life easier, I think, in
//       wrappers.  For instance, in python the GIL may, or may not, be held
//       by the thread that is logging.   So there are deadlock possibilities
//       that can only be worked around by making the custom sink asynchronous.
//       A single thread in the thread pool that services each async logger would
//       keep ordering in place, _and_ make a much simpler sink to implement.
//       That would have to also be done in the client, and of course, there is the
//       sizing of the queue to take into account, etc...
std::shared_ptr<spdlog::logger>
init_txn_log()
{
    static auto txnlogger = spdlog::stderr_logger_mt(TXN_LOG);
    return txnlogger;
}

std::shared_ptr<spdlog::logger>
init_attempt_cleanup_log()
{
    static auto txnlogger = spdlog::stderr_logger_mt(ATTEMPT_CLEANUP_LOG);
    return txnlogger;
}
std::shared_ptr<spdlog::logger>
init_lost_attempts_log()
{
    static auto txnlogger = spdlog::stderr_logger_mt(LOST_ATTEMPT_CLEANUP_LOG);
    return txnlogger;
}

// TODO: better integration with client, so we don't need to repeat this private
// method.
spdlog::level::level_enum
translate_level(core::logger::level level)
{
    switch (level) {
        case core::logger::level::trace:
            return spdlog::level::level_enum::trace;
        case core::logger::level::debug:
            return spdlog::level::level_enum::debug;
        case core::logger::level::info:
            return spdlog::level::level_enum::info;
        case core::logger::level::warn:
            return spdlog::level::level_enum::warn;
        case core::logger::level::err:
            return spdlog::level::level_enum::err;
        case core::logger::level::critical:
            return spdlog::level::level_enum::critical;
        case core::logger::level::off:
            return spdlog::level::level_enum::off;
    }
    return spdlog::level::level_enum::trace;
}

void
set_transactions_log_level(core::logger::level level)
{
    spdlog::level::level_enum lvl = translate_level(level);
    txn_log->set_level(lvl);
    attempt_cleanup_log->set_level(lvl);
    lost_attempts_cleanup_log->set_level(lvl);
}

// This cannot be done in multiple threads at the same time.   We could
// consider a mutex, but eventually we will merge with the cxx_client so
// this will be fine for now.   Unsure if this will lead to issues if called
// while logging is happening in other threads.  Do this once, at startup.
void
create_loggers(core::logger::level level, spdlog::sink_ptr sink)
{
    if (nullptr != sink) {
        sink->set_level(translate_level(level));
        txn_log->flush();
        txn_log->sinks().clear();
        txn_log->sinks().push_back(sink);
        attempt_cleanup_log->flush();
        attempt_cleanup_log->sinks().clear();
        attempt_cleanup_log->sinks().push_back(sink);
        lost_attempts_cleanup_log->flush();
        lost_attempts_cleanup_log->sinks().clear();
        lost_attempts_cleanup_log->sinks().push_back(sink);
    }
    set_transactions_log_level(level);
}

} // namespace couchbase::core::transactions
