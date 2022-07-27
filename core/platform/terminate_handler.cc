/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "terminate_handler.h"

#include "backtrace.h"
#include "core/logger/logger.hxx"
#include "core/meta/version.hxx"

#include <cstdlib>
#include <exception>

namespace couchbase::core::platform
{
static bool should_include_backtrace = true;
static std::terminate_handler default_terminate_handler = nullptr;

// Logs details on the handled exception. Attempts to log to
// `terminate_logger` if non-null; otherwise prints to stderr.
static void
log_handled_exception()
{
#ifdef WIN32
    // Windows doesn't like us re-throwing the exception in the handler (and
    // seems to result in immediate process termination). As such skip logging
    // the exception here.
    return;
#else
    // Attempt to get the exception's what() message.
    try {
        static int tried_throw = 0;
        // try once to re-throw currently active exception (so we can print
        // its what() message).
        if (tried_throw++ == 0) {
            throw;
        }
    } catch (const std::exception& e) {
        LOG_CRITICAL("Caught unhandled std::exception-derived exception. what(): {}", e.what());
    } catch (...) {
        LOG_CRITICAL_RAW("Caught unknown/unhandled exception.");
    }
#endif
}

// Log the symbolified backtrace to this point.
static void
log_backtrace()
{
    static const char format_str[] = "Call stack:\n%s";

    char buffer[8192];
    if (print_backtrace_to_buffer("    ", buffer, sizeof(buffer))) {
        LOG_CRITICAL("Call stack:\n{}", buffer);
    } else {
        // Exceeded buffer space - print directly to stderr FD (requires no
        // buffering, but has the disadvantage that we don't get it in the log).
        fprintf(stderr, format_str, "");
        print_backtrace_to_file(stderr);
        fflush(stderr);
        LOG_CRITICAL_RAW("Call stack exceeds 8k, rendered to STDERR");
    }
}

// Replacement terminate_handler which prints the exception's what() and a
// backtrace of the current stack before chaining to the default handler.
static void
backtrace_terminate_handler()
{
    static bool meta_reported = false;
    if (!meta_reported) {
        LOG_CRITICAL(R"(*** Fatal error encountered during exception handling ({}) ***)", meta::sdk_build_info_short());
        meta_reported = true;
    }
    log_handled_exception();

    if (should_include_backtrace) {
        log_backtrace();
    }

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr && default_terminate_handler != backtrace_terminate_handler) {
        default_terminate_handler();
    }

#if !defined(HAVE_BREAKPAD)
    // Shut down the logger (and flush everything). If breakpad is installed
    // then we'll let it do it.
    logger::shutdown();
#endif

    std::abort();
}

void
install_backtrace_terminate_handler()
{
    if (default_terminate_handler != nullptr) {
        // restore the previously saved one before (re)installing ours.
        std::set_terminate(default_terminate_handler);
    }
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);
}

void
set_terminate_handler_print_backtrace(bool print)
{
    should_include_backtrace = print;
}

} // namespace couchbase::core::platform
