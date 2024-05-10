/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include <tao/json/value.hpp>

namespace couchbase
{
using internal_error_context = tao::json::value;

enum class error_context_json_format {
    compact = 0,
    pretty,
};

class error_context
{
  public:
    error_context() = default;
    explicit error_context(internal_error_context internal);
    error_context(internal_error_context internal, internal_error_context internal_metadata);

    [[nodiscard]] auto to_json(error_context_json_format format = error_context_json_format::compact) const -> std::string;

    template<typename T>
    T as() const
    {
        if constexpr (std::is_same_v<T, internal_error_context>) {
            return internal_;
        } else {
            return internal_.as<T>();
        }
    }

    /**
     * @internal
     */
    [[nodiscard]] auto internal_metadata(error_context_json_format format = error_context_json_format::compact) const -> std::string;

  private:
    internal_error_context internal_;
    internal_error_context internal_metadata_;
};
} // namespace couchbase
