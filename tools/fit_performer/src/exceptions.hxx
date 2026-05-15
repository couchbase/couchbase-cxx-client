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

#include <grpcpp/support/status.h>
#include <grpcpp/support/status_code_enum.h>

#include <stdexcept>

class performer_exception : public std::runtime_error
{
public:
  explicit performer_exception(const std::string& what)
    : std::runtime_error(what)
  {
  }

  explicit performer_exception(grpc::StatusCode code, const std::string& what)
    : std::runtime_error(what)
    , code_{ code }
  {
  }

  [[nodiscard]] auto to_grpc_status() const -> grpc::Status
  {
    return grpc::Status{ code_, what() };
  }

  static auto not_found(const std::string& what) -> performer_exception
  {
    return performer_exception{ grpc::StatusCode::NOT_FOUND, what };
  }

  static auto unimplemented(const std::string& what) -> performer_exception
  {
    return performer_exception{ grpc::StatusCode::UNIMPLEMENTED, what };
  }

  static auto invalid_argument(const std::string& what) -> performer_exception
  {
    return performer_exception{ grpc::StatusCode::INVALID_ARGUMENT, what };
  }

  static auto failed_precondition(const std::string& what) -> performer_exception
  {
    return performer_exception{ grpc::StatusCode::FAILED_PRECONDITION, what };
  }

  static auto internal(const std::string& what) -> performer_exception
  {
    return performer_exception{ grpc::StatusCode::INTERNAL, what };
  }

private:
  grpc::StatusCode code_{ grpc::StatusCode::INTERNAL };
};
