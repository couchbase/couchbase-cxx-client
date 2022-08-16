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

namespace subdoc
{
/**
 * An intention to perform a SubDocument replace operation.
 *
 * @since 1.0.0
 * @committed
 */
class replace
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
    auto xattr() -> replace&
    {
        xattr_ = true;
        return *this;
    }

  private:
    friend couchbase::mutate_in_specs;

    replace(std::string path, std::vector<std::byte> value)
      : path_(std::move(path))
      , value_(std::move(value))
    {
    }

    replace(std::string path, mutate_in_macro value)
      : path_(std::move(path))
      , value_(to_binary(value))
      , expand_macro_(true)
    {
    }

    [[nodiscard]] auto encode(std::size_t original_index) const -> command
    {
        return { path_.empty() ? opcode::set_doc : opcode::replace, path_, value_, false, xattr_, expand_macro_, original_index };
    }

    std::string path_;
    std::vector<std::byte> value_;
    bool xattr_{ false };
    bool expand_macro_{ false };
};
} // namespace subdoc
} // namespace couchbase