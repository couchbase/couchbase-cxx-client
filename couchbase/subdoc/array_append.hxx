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

#include <couchbase/subdoc/command.hxx>
#include <couchbase/subdoc/mutate_in_macro.hxx>

#include <string>
#include <vector>

namespace couchbase
{
class mutate_in_specs;
namespace core::impl::subdoc
{
std::vector<std::byte>
join_values(const std::vector<std::vector<std::byte>>& values);
} // namespace core::impl::subdoc

namespace subdoc
{
/**
 * An intention to perform a SubDocument array_append operation.
 *
 * @since 1.0.0
 * @committed
 */
class array_append
{
  public:
    /**
     * Sets that this is an extended attribute (xattr) field.
     *
     * @return this, for chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto xattr() -> array_append&
    {
        xattr_ = true;
        return *this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto create_path() -> array_append&
    {
        create_path_ = true;
        return *this;
    }

  private:
    friend couchbase::mutate_in_specs;

    array_append(std::string path, std::vector<std::vector<std::byte>> values)
      : path_(std::move(path))
      , values_(std::move(values))
    {
    }

    [[nodiscard]] auto encode(std::size_t original_index) const -> command
    {
        return { opcode::array_push_last, path_, core::impl::subdoc::join_values(values_), create_path_, xattr_, false, original_index };
    }

    std::string path_;
    std::vector<std::vector<std::byte>> values_;
    bool xattr_{ false };
    bool create_path_{ false };
};
} // namespace subdoc
} // namespace couchbase
