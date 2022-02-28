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

#include <chrono>
#include <fmt/core.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace test::utils
{
std::string
uniq_id(const std::string& prefix)
{
    return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string
read_test_data(const std::string& file)
{
    auto ss = std::ostringstream{};
    std::ifstream input_file(fmt::format("../../test/data/{}", file));
    ss << input_file.rdbuf();
    return ss.str();
}
} // namespace test::utils
