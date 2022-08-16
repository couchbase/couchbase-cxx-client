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

#include <couchbase/subdoc/opcode.hxx>

#include <cinttypes>
#include <string>
#include <vector>

namespace couchbase::subdoc
{
/**
 * Internal structure to represent subdocument operations.
 *
 * @since 1.0.0
 * @internal
 */
struct command {
    opcode opcode_;
    std::string path_;
    std::vector<std::byte> value_;
    bool create_path_{ false };
    bool xattr_{ false };
    bool expand_macro_{ false };
    std::size_t original_index_{};
};
} // namespace couchbase::subdoc
