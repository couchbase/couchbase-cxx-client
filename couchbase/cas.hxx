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

#include <cstdint>

namespace couchbase
{
/**
 * CAS is a special type that represented in protocol using unsigned 64-bit integer, but only equality checks allowed.
 *
 * The user should not interpret the integer value of the CAS.
 */
struct cas {
    std::uint64_t value;

    bool operator==(const cas& other) const
    {
        return this->value == other.value;
    }

    bool operator!=(const cas& other) const
    {
        return !(*this == other);
    }

    [[nodiscard]] bool empty() const
    {
        return value == 0;
    }
};
} // namespace couchbase
