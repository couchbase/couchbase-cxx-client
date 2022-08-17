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

#include <couchbase/codec/json_transcoder.hxx>
#include <couchbase/subdoc/command.hxx>

#include <string>
#include <vector>

namespace couchbase
{
class mutate_in_specs;

namespace subdoc
{
/**
 * An intention to perform a SubDocument counter operation.
 *
 * @since 1.0.0
 * @committed
 */
class counter
{
  public:
    /**
     * Sets that this is an extended attribute (xattr) field.
     *
     * @param value new value for the option
     * @return this, for chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto xattr(bool value = true) -> counter&
    {
        xattr_ = value;
        return *this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     *
     * @param value new value for the option
     * @return this, for chaining
     *
     * @since 1.0.0
     * @committed
     */
    auto create_path(bool value = true) -> counter&
    {
        create_path_ = value;
        return *this;
    }

  private:
    friend couchbase::mutate_in_specs;

    counter(std::string path, std::int64_t value)
      : path_(std::move(path))
      , delta_{ value }
    {
    }

    [[nodiscard]] auto encode(std::size_t original_index) const -> command
    {
        return {
            opcode::counter, path_, std::move(codec::json_transcoder::encode(delta_).data), create_path_, xattr_, false, original_index
        };
    }

    std::string path_;
    std::int64_t delta_;
    bool xattr_{ false };
    bool create_path_{ false };
};
} // namespace subdoc
} // namespace couchbase
