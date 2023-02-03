/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "uuid.h"

#include "string_hex.h"

#include <iomanip>
#include <random>

void
couchbase::core::uuid::random(couchbase::core::uuid::uuid_t& uuid)
{
    static thread_local std::mt19937_64 gen{ std::random_device()() };
    std::uniform_int_distribution<std::uint64_t> dis;

    // The uuid is 16 bytes, which is the same as two 64-bit integers
    auto* ptr = reinterpret_cast<std::uint64_t*>(uuid.data());
    ptr[0] = dis(gen);
    ptr[1] = dis(gen);

    // Make sure that it looks like a version 4
    uuid[6] &= 0x0f;
    uuid[6] |= 0x40;
}

couchbase::core::uuid::uuid_t
couchbase::core::uuid::random()
{
    uuid_t ret;
    random(ret);
    return ret;
}

couchbase::core::uuid::uuid_t
couchbase::core::uuid::from_string(std::string_view str)
{
    uuid_t ret;
    if (str.size() != 36) {
        throw std::invalid_argument("couchbase::core::uuid::from_string: string was wrong size got: " + std::to_string(str.size()) +
                                    " (expected: 36)");
    }

    std::size_t jj = 0;
    for (std::size_t ii = 0; ii < 36; ii += 2) {
        switch (ii) {
            case 8:
            case 13:
            case 18:
            case 23:
                if (str[ii] != '-') {
                    throw std::invalid_argument("couchbase::core::uuid::from_string: hyphen not found where "
                                                "expected");
                }
                ++ii; // fall-through
            default:
                ret[jj++] = std::uint8_t(from_hex({ str.data() + ii, 2 }));
        }
    }
    return ret;
}

inline char
to_char(std::uint8_t c)
{
    if (c <= 9) {
        return static_cast<char>('0' + c);
    }
    return static_cast<char>('a' + (c - 10));
}

std::string
couchbase::core::uuid::to_string(const couchbase::core::uuid::uuid_t& uuid)
{
    std::string ret(36, '-');
    std::size_t i = 0;

    for (const auto& byte : uuid) {
        ret[i] = to_char(static_cast<std::uint8_t>(byte >> 4U) & 0x0fU);
        ++i;
        ret[i] = to_char(byte & 0x0fU);
        ++i;
        if (i == 6 || i == 11 || i == 16 || i == 21) {
            ++i;
        }
    }
    return ret;
}
