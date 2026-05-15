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

#include "service.hxx"

#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>

#include <grpc++/grpc++.h>
#include <spdlog/common.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <memory>
#include <string>

#define SPDLOG_DISABLE_TID_CACHING

spdlog::level::level_enum
translate_log_level(couchbase::core::logger::level level)
{
  switch (level) {
    case couchbase::core::logger::level::trace:
      return spdlog::level::trace;
    case couchbase::core::logger::level::debug:
      return spdlog::level::debug;
    case couchbase::core::logger::level::info:
      return spdlog::level::info;
    case couchbase::core::logger::level::warn:
      return spdlog::level::warn;
    case couchbase::core::logger::level::err:
      return spdlog::level::err;
    case couchbase::core::logger::level::critical:
      return spdlog::level::critical;
    case couchbase::core::logger::level::off:
      return spdlog::level::off;
  }
  return spdlog::level::off;
}

int
main(int argc, const char* argv[])
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string server_address = "0.0.0.0:8060";
  auto logger_level = couchbase::core::logger::level::info;

  if (argc > 1) {
    server_address = std::string("0.0.0.0:") + std::string(argv[1]);
  }

  if (argc > 2) {
    logger_level = couchbase::core::logger::level_from_str(argv[2]);
  }

  if (const auto* log_level_env = std::getenv("LOG_LEVEL"); log_level_env != nullptr) {
    std::string level_env = log_level_env;
    std::transform(level_env.begin(), level_env.end(), level_env.begin(), [](auto c) {
      return std::tolower(c);
    });
    logger_level = couchbase::core::logger::level_from_str(level_env);
  }

  // Initialize performer logger
  spdlog::set_default_logger(spdlog::stdout_color_mt("fit_cxx"));
  spdlog::set_pattern("[%Y-%m-%d %T.%e] %4oms [%^FIT %4!l%$] [%P,%t] %v");
  spdlog::set_level(translate_log_level(logger_level));

  // Initialize couchbase client logger
  couchbase::core::logger::create_console_logger();
  couchbase::core::logger::set_log_levels(logger_level);

  TxnService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, ::grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  const std::unique_ptr<grpc::Server> server{ builder.BuildAndStart() };
  if (server == nullptr) {
    spdlog::critical("Failed to start gRPC server on '{}'. Exiting.", server_address);
    return 1;
  }

  spdlog::info("C++ SDK: {}, listening on '{}', build_info: {}",
               couchbase::core::meta::sdk_semver(),
               server_address,
               couchbase::core::meta::sdk_build_info_json());
  server->Wait();
  return 0;
}
