/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <array>
#include <cstddef>
#include <new>
#include <utility>

namespace couchbase::core::utils
{
/**
 * Holds a T that is constructed in place and never destroyed.
 *
 * For a function-local static that code running at exit can still reach: an exit hook registered
 * before the static was constructed runs after its destructor would.
 */
template<typename T>
class no_destructor
{
public:
  template<typename... Args>
  explicit no_destructor(Args&&... args)
    : object_{ new (storage_.data()) T(std::forward<Args>(args)...) }
  {
  }

  no_destructor(const no_destructor&) = delete;
  no_destructor(no_destructor&&) = delete;
  auto operator=(const no_destructor&) -> no_destructor& = delete;
  auto operator=(no_destructor&&) -> no_destructor& = delete;
  ~no_destructor() = default;

  [[nodiscard]] auto get() -> T&
  {
    return *object_;
  }

private:
  alignas(T) std::array<std::byte, sizeof(T)> storage_{};
  T* object_;
};
} // namespace couchbase::core::utils
