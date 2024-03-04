/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include <string>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
class internal_operation_error_context;
#endif

class operation_error_context
{
  public:
    operation_error_context();
    explicit operation_error_context(internal_operation_error_context ctx);
    operation_error_context(operation_error_context&& other);
    operation_error_context& operator=(operation_error_context&& other);
    operation_error_context(const operation_error_context& other) = delete;
    operation_error_context& operator=(const operation_error_context& other) = delete;
    ~operation_error_context();

    [[nodiscard]] auto to_json_pretty() const -> std::string;

    [[nodiscard]] auto to_json() const -> std::string;

  private:
    std::unique_ptr<internal_operation_error_context> internal_;
};
} // namespace couchbase
