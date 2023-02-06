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

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <string_view>
#include <vector>

namespace couchbase::core::utils
{
using binary = std::vector<std::byte>;

template<typename T>
[[nodiscard]] binary
to_binary(const T* data, const std::size_t size) noexcept
{
    static_assert(sizeof(T) == 1);
    binary copy;
    copy.reserve(size);
    copy.insert(copy.end(), reinterpret_cast<const std::byte*>(data), reinterpret_cast<const std::byte*>(data + size));
    return copy;
}

[[nodiscard]] binary
to_binary(std::string_view value) noexcept;

template<typename InputIterator, typename OutputIterator>
OutputIterator
to_binary(InputIterator first, InputIterator last, OutputIterator result) noexcept
{
    return std::transform(first, last, result, [](auto e) { return static_cast<std::byte>(e); });
}

template<typename Container, typename OutputIterator>
OutputIterator
to_binary(Container container, OutputIterator result) noexcept
{
    return to_binary(std::begin(container), std::end(container), result);
}
} // namespace couchbase::core::utils
