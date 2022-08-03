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

#include <chrono>
#include <optional>
#include <type_traits>

namespace couchbase
{
/**
 * Common options that used by most operations.
 *
 * @since 1.0.0
 * @committed
 */
template<typename derived_class>
class common_options
{
  public:
    /**
     * Specifies a custom per-operation timeout.
     *
     * @note: if a custom timeout is provided through this builder, it will override the default set
     * on the environment.
     *
     * @param timeout the timeout to use for this operation.
     * @return this options builder for chaining purposes.
     *
     * @since 1.0.0
     * @committed
     */
    auto timeout(const std::chrono::milliseconds timeout) -> derived_class&
    {
        timeout_ = timeout;
        return self();
    }

    /**
     * Immutable value object representing consistent options.
     *
     * @since 1.0.0
     * @internal
     */
    struct built {
        const std::optional<std::chrono::milliseconds> timeout;
    };

  protected:
    /**
     * @return immutable representation of the common options
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto build_common_options() const -> built
    {
        return { timeout_ };
    }

    /**
     * Allows to return the right options builder instance for child implementations.
     *
     * @return derived_class
     *
     * @since 1.0.0
     * @internal
     */
    auto self() -> derived_class&
    {
        return *static_cast<derived_class*>(this);
    }

  private:
    std::optional<std::chrono::milliseconds> timeout_{};
};

} // namespace couchbase
