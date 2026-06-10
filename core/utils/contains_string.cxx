/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include "contains_string.hxx"

namespace couchbase::core::utils
{

namespace
{
constexpr auto ascii_lower = [](unsigned char c) -> unsigned char {
  return (c >= 'A' && c <= 'Z') ? static_cast<unsigned char>(c + ('a' - 'A')) : c;
};
} // namespace

auto
contains_string(std::string_view input, std::string_view substr, bool ignore_case) -> bool
{
  if (substr.empty()) {
    return true;
  }

  if (input.empty() || substr.size() > input.size()) {
    return false;
  }

  if (!ignore_case) {
    return input.find(substr) != std::string_view::npos;
  }

  const auto end = input.size() - substr.size() + 1;
  for (std::size_t i = 0; i < end; i++) {
    bool match = true;
    for (std::size_t j = 0; j < substr.size(); j++) {
      if (ascii_lower(static_cast<unsigned char>(input[i + j])) !=
          ascii_lower(static_cast<unsigned char>(substr[j]))) {
        match = false;
        break;
      }
    }
    if (match) {
      return true;
    }
  }
  return false;
}
} // namespace couchbase::core::utils
