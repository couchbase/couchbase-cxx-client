/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

namespace test::utils
{

struct move_only_context {
  public:
    explicit move_only_context(std::string input)
      : payload_(std::move(input))
    {
    }

    move_only_context(move_only_context&& other) = default;

    move_only_context& operator=(move_only_context&& other) = default;

    ~move_only_context() = default;

    move_only_context(const move_only_context& other) = delete;

    move_only_context& operator=(const move_only_context& other) = delete;

    [[nodiscard]] const std::string& payload() const
    {
        return payload_;
    }

  private:
    std::string payload_;
};

} // namespace test::utils
