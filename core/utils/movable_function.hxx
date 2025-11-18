/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022 Couchbase, Inc.
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

#include <functional>
#include <memory>
#include <mutex>

/* replace with standard version once http://wg21.link/P0288 will be accepted and implemented */
namespace couchbase::core::utils
{
template<typename Signature>
class movable_function : public std::function<Signature>
{
  template<typename Functor, typename En = void>
  struct wrapper;

  template<typename Functor>
  struct wrapper<Functor, std::enable_if_t<std::is_copy_constructible_v<Functor>>> {
    Functor fn;

    template<typename... Args>
    auto operator()(Args&&... args)
    {
      return fn(std::forward<Args>(args)...);
    }
  };

  template<typename Functor>
  struct copy_wrapper {
    Functor fn;
    explicit copy_wrapper(Functor&& f)
      : fn(std::move(f))
    {
    }
  };

  template<typename Functor>
  struct wrapper<Functor,
                 std::enable_if_t<!std::is_copy_constructible_v<Functor> &&
                                  std::is_move_constructible_v<Functor>>> {
    std::shared_ptr<copy_wrapper<Functor>> fn_ptr_;
    mutable std::mutex mtx_;

    explicit wrapper(Functor&& f)
      : fn_ptr_(std::make_shared<copy_wrapper<Functor>>(std::move(f)))
    {
    }

    wrapper(const wrapper& other)
    {
      std::scoped_lock lock(other.mtx_);
      fn_ptr_ = other.fn_ptr_;
    }

    auto operator=(const wrapper& other) -> wrapper&
    {
      if (this != &other) {
        std::scoped_lock lock(mtx_, other.mtx_);
        fn_ptr_ = other.fn_ptr_;
      }
      return *this;
    }

    wrapper(wrapper&& other) noexcept
    {
      std::scoped_lock lock(other.mtx_);
      fn_ptr_ = std::move(other.fn_ptr_);
    }

    auto operator=(wrapper&& other) noexcept -> wrapper&
    {
      if (this != &other) {
        std::scoped_lock lock(mtx_, other.mtx_);
        fn_ptr_ = std::move(other.fn_ptr_);
      }
      return *this;
    }

    template<typename... Args>
    auto operator()(Args&&... args)
    {
      return std::move(fn_ptr_->fn)(std::forward<Args>(args)...);
    }

    ~wrapper() noexcept = default;
  };

  using base = std::function<Signature>;

public:
  movable_function() noexcept = default;
  movable_function(const movable_function&) = delete;
  movable_function(std::nullptr_t) noexcept
    : base(nullptr)
  {
  }

  template<typename Functor>
  movable_function(Functor&& f)
    : base(wrapper<Functor>{ std::forward<Functor>(f) })
  {
  }

  movable_function(movable_function&& other) noexcept
    : base(std::move(static_cast<base&&>(other)))
  {
    other = nullptr;
  }

  auto operator=(const movable_function&) -> movable_function& = delete;

  auto operator=(movable_function&& other) noexcept -> movable_function&
  {
    base::operator=(std::move(static_cast<base&&>(other)));
    other = nullptr;
    return *this;
  }

  auto operator=(std::nullptr_t /* other */) -> movable_function&
  {
    base::operator=(nullptr);
    return *this;
  }

  template<typename Functor>
  auto operator=(Functor&& f) -> movable_function&
  {
    base::operator=(wrapper<Functor>{ std::forward<Functor>(f) });
    return *this;
  }

  using base::operator bool;
  using base::operator();
};

} // namespace couchbase::core::utils
