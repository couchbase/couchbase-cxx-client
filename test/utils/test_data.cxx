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

#include "core/mcbp/big_endian.hxx"

#include <spdlog/fmt/bundled/core.h>
#include <spdlog/fmt/bundled/ranges.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace test::utils
{
auto
uniq_id(const std::string& prefix) -> std::string
{
  return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

namespace
{
auto
read_all(const std::string& path) -> std::string
{
  const auto file_size = std::filesystem::file_size(path);
  std::ifstream input_file(path);
  std::string content;
  content.resize(file_size);
  input_file.read(content.data(), static_cast<std::streamsize>(file_size));
  return content;
}
} // namespace

auto
read_test_data(const std::string& file) -> std::string
{
  std::vector candidates{
    file,
    fmt::format("data/{}", file),
    fmt::format("test/data/{}", file),
    fmt::format("../test/data/{}", file),
    fmt::format("../../test/data/{}", file),
    fmt::format("../../../test/data/{}", file),
  };
  for (const auto& path : candidates) {
    if (std::error_code ec{}; std::filesystem::exists(path, ec) && !ec) {
      return read_all(path);
    }
  }
  throw std::runtime_error(
    fmt::format("unable to load test_data.\nCurrent directory: {}\ncandidates: {}",
                std::filesystem::current_path().string(),
                fmt::join(candidates, ",\n\t")));
}
} // namespace test::utils
