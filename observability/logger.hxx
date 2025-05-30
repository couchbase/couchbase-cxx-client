/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <core/meta/version.hxx>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#endif
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/logs/severity.h>
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace couchbase::observability
{
struct logger_options {
  bool use_http_logger{ false };
};

void
init_logger(const logger_options& options);

auto
logger() -> std::shared_ptr<opentelemetry::logs::Logger>;

} // namespace couchbase::observability

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define CB_LOG_TRACE(...)                                                                          \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kTrace)) {                                  \
      logger->Trace(__VA_ARGS__);                                                                  \
    }                                                                                              \
  } while (false)

#define CB_LOG_DEBUG(...)                                                                          \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kDebug)) {                                  \
      logger->Debug(__VA_ARGS__);                                                                  \
    }                                                                                              \
  } while (false)

#define CB_LOG_INFO(...)                                                                           \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kInfo)) {                                   \
      logger->Info(__VA_ARGS__);                                                                   \
    }                                                                                              \
  } while (false)

#define CB_LOG_WARNING(...)                                                                        \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kTrace)) {                                  \
      logger->Trace(__VA_ARGS__);                                                                  \
    }                                                                                              \
  } while (false)

#define CB_LOG_ERROR(...)                                                                          \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kError)) {                                  \
      logger->Error(__VA_ARGS__);                                                                  \
    }                                                                                              \
  } while (false)

#define CB_LOG_CRITICAL(...)                                                                       \
  do {                                                                                             \
    if (const auto& logger = ::couchbase::observability::logger();                                 \
        logger->Enabled(opentelemetry::logs::Severity::kFatal)) {                                  \
      logger->Fatal(__VA_ARGS__);                                                                  \
    }                                                                                              \
  } while (false)
// NOLINTEND(cppcoreguidelines-macro-usage)
