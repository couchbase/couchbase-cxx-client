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

#include <couchbase/logger.hxx>

#include "core/logger/configuration.hxx"
#include "core/logger/level.hxx"
#include "core/logger/logger.hxx"

namespace couchbase::logger
{
namespace
{
auto
convert_log_level(couchbase::logger::log_level level) -> core::logger::level
{
  switch (level) {
    case log_level::trace:
      return core::logger::level::trace;
    case log_level::debug:
      return core::logger::level::debug;
    case log_level::info:
      return core::logger::level::info;
    case log_level::warn:
      return core::logger::level::warn;
    case log_level::error:
      return core::logger::level::err;
    case log_level::critical:
      return core::logger::level::critical;
    case log_level::off:
    default:
      break;
  }
  return core::logger::level::off;
}
} // namespace

void
set_level(log_level level)
{
  core::logger::set_log_levels(convert_log_level(level));
}

void
initialize_console_logger()
{
  couchbase::core::logger::create_console_logger();
}

void
initialize_file_logger(std::string_view filename)
{
  couchbase::core::logger::configuration configuration{};
  configuration.filename = filename;
  couchbase::core::logger::create_file_logger(configuration);
}

void
initialize_protocol_logger(std::string_view filename)
{
  couchbase::core::logger::configuration configuration{};
  configuration.filename = filename;
  couchbase::core::logger::create_protocol_logger(configuration);
}

void
flush_all_loggers()
{
  core::logger::flush();
}

void
shutdown_all_loggers()
{
  core::logger::shutdown();
}
} // namespace couchbase::logger
