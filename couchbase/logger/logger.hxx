/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 *   A note on the thread safety of the logger API:
 *
 *   The API is thread safe unless the underlying logger object is changed
 * during runtime. This means some methods can only be safely called if the
 * caller guarantees no other threads exist and/or are calling the logging
 * functions.
 *
 *   The caveat being we should not change the underlying logger object during
 * run-time, the exception to this is during the initial memcached startup,
 * where we are running in a single thread at the point we switch from console
 * logging to file logging.
 */

#pragma once

#include <fmt/core.h>
#include <memory>
#include <optional>
#include <spdlog/fwd.h>
#include <string>

namespace couchbase::logger
{
struct configuration;

/**
 * the various severity levels we can log at
 */
enum class level { trace, debug, info, warn, err, critical, off };

level
level_from_str(const std::string& str);

/**
 * Initialize the logger.
 *
 * The default level for the created logger is set to INFO
 *
 * See note about thread safety at the top of the file
 *
 * @param logger_settings the configuration for the logger
 * @return optional error message if something goes wrong
 */
std::optional<std::string>
create_file_logger(const configuration& logger_settings);

/**
 * Initialize the logger with the blackhole logger object
 *
 * This method is intended to be used by unit tests which don't need any output (but may call methods who tries to fetch the logger)
 *
 * See note about thread safety at the top of the file
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 *                           (if it already exists for instance)
 */
void
create_blackhole_logger();

/**
 * Initialize the logger with the logger which logs to the console
 *
 * See note about thread safety at the top of the file
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 */
void
create_console_logger();

/**
 * Get the underlying logger object
 *
 * See note about thread safety at the top of the file.
 *
 * This will return null if a logger has not been initialized through one of the following:
 *
 * - create_file_logger()
 * - create_blackhole_logger()
 * - create_console_logger()
 */
spdlog::logger*
get();

/**
 * Reset the underlying logger object
 *
 * See note about thread safety at the top of the file
 */
void
reset();

/**
 * Engines that create their own instances of an spdlog::logger should register the logger here to ensure that the verbosity of the logger
 * is updated when memcached receives a request to update verbosity
 *
 * @param l spdlog::logger instance
 */
void
register_spdlog_logger(std::shared_ptr<spdlog::logger> l);

/**
 * Engines that create their own instances of an spdlog::logger should unregister
 * the logger here to ensure that resources can be freed when their loggers
 * go out of scope, or unsubscribe from runtime verbosity changes
 *
 * @param n The name of the spdlog::logger
 */
void
unregister_spdlog_logger(const std::string& n);

/**
 * Check the log level of all spdLoggers is equal to the given level
 * @param log severity level
 * @return true if all registered loggers have the specified severity level
 */
bool
check_log_levels(level level);

/**
 * Set the log level of all registered spdLoggers
 * @param log severity level
 */
void
set_log_levels(level level);

/**
 * Checks whether a specific level should be logged based on the current
 * configuration.
 * @param level severity level to check
 * @return true if we should log at this level
 */
bool
should_log(level lvl);

namespace detail
{
/**
 * Logs a message at a specific severity level.
 * @param lvl severity level to log at
 * @param msg message to log
 */
void
log(level lvl, std::string_view msg);
} // namespace detail

/**
 * Logs a formatted message at a specific severity level.
 * @param lvl severity level to log at
 * @param msg message to log
 * @param args the formatting arguments
 */
template<typename String, typename... Args>
inline void
log(level lvl, const String& msg, Args&&... args)
{
    detail::log(lvl, fmt::format(msg, std::forward<Args>(args)...));
}

/**
 * Tell the logger to flush its buffers
 */
void
flush();

/**
 * Tell the logger to shut down (flush buffers) and release _ALL_
 * loggers (you'd need to create new loggers after this method)
 */
void
shutdown();

/**
 * @return whether or not the logger has been initialized
 */
bool
is_initialized();

} // namespace couchbase::logger

/**
 * We implement this macro to avoid having argument evaluation performed
 * on log messages which likely will not actually be logged due to their
 * severity value not matching the logger.
 */
#define COUCHBASE_LOG(severity, ...)                                                                                                       \
    do {                                                                                                                                   \
        if (couchbase::logger::should_log(severity)) {                                                                                     \
            couchbase::logger::log(severity, __VA_ARGS__);                                                                                 \
        }                                                                                                                                  \
    } while (false)

#define LOG_TRACE(...) COUCHBASE_LOG(couchbase::logger::level::trace, __VA_ARGS__)
#define LOG_DEBUG(...) COUCHBASE_LOG(couchbase::logger::level::debug, __VA_ARGS__)
#define LOG_INFO(...) COUCHBASE_LOG(couchbase::logger::level::info, __VA_ARGS__)
#define LOG_WARNING(...) COUCHBASE_LOG(couchbase::logger::level::warn, __VA_ARGS__)
#define LOG_ERROR(...) COUCHBASE_LOG(couchbase::logger::level::err, __VA_ARGS__)
#define LOG_CRITICAL(...) COUCHBASE_LOG(couchbase::logger::level::critical, __VA_ARGS__)

/**
 * Convenience macros which log with the given level, and message, if the given
 * level is currently enabled.
 * @param msg Fixed string (implicitly convertible to `std::string_view`)
 *
 * For example:
 *
 *   LOG_INFO_RAW("Starting flusher");
 *   LOG_INFO_RAW(std:string{...});
 */
#define COUCHBASE_LOG_RAW(severity, msg)                                                                                                   \
    do {                                                                                                                                   \
        if (couchbase::logger::should_log(severity)) {                                                                                     \
            couchbase::logger::detail::log(severity, msg);                                                                                 \
        }                                                                                                                                  \
    } while (false)

#define LOG_TRACE_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::trace, msg)
#define LOG_DEBUG_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::debug, msg)
#define LOG_INFO_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::info, msg)
#define LOG_WARNING_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::warn, msg)
#define LOG_ERROR_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::err, msg)
#define LOG_CRITICAL_RAW(msg) COUCHBASE_LOG_RAW(couchbase::logger::level::critical, msg)
