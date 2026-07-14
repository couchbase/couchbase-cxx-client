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

#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

/* replace with std::move_only_function once http://wg21.link/P0288 is available (C++23) */
namespace couchbase::core::utils
{
#ifdef __clang_analyzer__
// Declared but never defined: an opaque sink (analysis only; analysis does not link, and it is
// absent from every real build). See emplace() below -- handing the owned target pointer to a
// non-const opaque sink marks it "escaped" so MallocChecker stops tracking an allocation it would
// otherwise lose inside asio's deeply templated post/dispatch machinery and report as a spurious
// leak. The parameter is NON-const on purpose: checkPointerEscapeAux only releases a symbol through
// a const pointer when it is new/new[]-family, but unconditionally through a non-const pointer.
void
movable_function_analyzer_escape(void*) noexcept;
#endif

template<typename Signature>
class movable_function;

/**
 * A move-only, type-erased callable, equivalent in spirit to std::move_only_function (C++23).
 *
 * A target that fits the inline buffer and is nothrow-move-constructible is stored in place, so the
 * common asio/KV handlers (a captured shared_ptr `self` plus a few members) are type-erased with no
 * heap allocation; larger targets are held on the heap. The type owns its target and transfers
 * ownership on move (copying is not provided). It offers `operator bool` and comparison against
 * nullptr, and throws std::bad_function_call when invoked while empty.
 */
template<typename R, typename... Args>
class movable_function<R(Args...)>
{
#ifdef __clang_analyzer__
  // Analyzer-only implementation (compiled only under the Clang Static Analyzer, never in real
  // builds). The production small-buffer path below frees its target through an indirect
  // `vtable_->destroy(buffer_)` function pointer the analyzer cannot resolve; blind to the free,
  // MallocChecker reports spurious "Potential memory leak" at every caller that stores a
  // heap-owning handler (libstdc++'s own std::move_only_function hits the identical limitation,
  // llvm-project#69602). Under analysis we instead own the target via
  // std::unique_ptr<callable_base>, whose destruction is an ordinary virtual delete that
  // MallocChecker models natively -- no manual vtable, no unresolved indirect free, no escape
  // heuristics. This models the same own-once / free-once / move-transfers semantics as the real
  // path, so it does not mask genuine leaks.
  struct callable_base {
    callable_base() = default;
    callable_base(const callable_base&) = delete;
    callable_base(callable_base&&) = delete;
    auto operator=(const callable_base&) -> callable_base& = delete;
    auto operator=(callable_base&&) -> callable_base& = delete;
    virtual ~callable_base() = default;
    virtual auto invoke(Args... args) -> R = 0;
  };

  template<typename F>
  struct callable_model : callable_base {
    F fn;
    // Forwarding ctor so the target is copied from an lvalue or moved from an rvalue, matching the
    // production `decayed(std::forward<F>(f))` -- a plain `F&&` parameter would reject lvalue
    // targets.
    template<typename U>
    explicit callable_model(U&& u)
      : fn(std::forward<U>(u))
    {
    }
    auto invoke(Args... args) -> R override
    {
      if constexpr (std::is_void_v<R>) {
        fn(std::forward<Args>(args)...);
      } else {
        return fn(std::forward<Args>(args)...);
      }
    }
  };

  std::unique_ptr<callable_base> holder_{};

  template<typename F>
  void emplace(F&& f)
  {
    holder_ = std::make_unique<callable_model<std::decay_t<F>>>(std::forward<F>(f));
    // Escape the owned block: some callers (e.g. asio::post/dispatch) move *this into templated
    // machinery the analyzer cannot follow, where it would otherwise lose this allocation and
    // report a spurious leak. holder_.get() is a direct object pointer, so the non-const escape
    // releases exactly the callable_model block (see the sink declaration).
    movable_function_analyzer_escape(static_cast<void*>(holder_.get()));
  }

  void move_from(movable_function&& other) noexcept
  {
    holder_ = std::move(other.holder_);
  }

  void reset() noexcept
  {
    holder_.reset();
  }
#else
  // Sized to hold typical handlers (e.g. a shared_ptr self, a time_point, and a shared_ptr span)
  // inline; larger callables fall back to the heap. The 6-pointer estimate is 48 bytes on 64-bit;
  // a 48-byte floor keeps that inline capacity on 32-bit too (where 6 pointers would be only 24
  // bytes), so the small-buffer optimization does not silently regress to more heap allocations.
  static constexpr std::size_t buffer_size = (6 * sizeof(void*) > 48) ? (6 * sizeof(void*)) : 48;

  struct vtable {
    R (*invoke)(void* self, Args&&... args);
    void (*move)(void* dst, void* src) noexcept; // move-construct dst from src, then destroy src
    void (*destroy)(void* self) noexcept;
  };

  template<typename F>
  static constexpr bool fits_inline_v =
    sizeof(F) <= buffer_size && alignof(F) <= alignof(std::max_align_t) &&
    std::is_nothrow_move_constructible_v<F>;

  template<typename F>
  static auto inline_vtable() -> const vtable&
  {
    // The target lives in buffer_ (an unsigned char array) via placement-new, so accesses go
    // through std::launder to obtain a pointer with the target's lifetime rather than the array's.
    static const vtable vt{
      [](void* self, Args&&... args) -> R {
        // For a void signature the target's result is discardable (std::is_invocable_r_v<void, ...>
        // admits a value-returning callable), so a value-returning target must be invoked without a
        // return statement -- `return <non-void>;` in a void thunk is ill-formed.
        if constexpr (std::is_void_v<R>) {
          (*std::launder(static_cast<F*>(self)))(std::forward<Args>(args)...);
        } else {
          return (*std::launder(static_cast<F*>(self)))(std::forward<Args>(args)...);
        }
      },
      [](void* dst, void* src) noexcept {
        ::new (dst) F(std::move(*std::launder(static_cast<F*>(src))));
        std::launder(static_cast<F*>(src))->~F();
      },
      [](void* self) noexcept {
        std::launder(static_cast<F*>(self))->~F();
      },
    };
    return vt;
  }

  template<typename F>
  static auto heap_vtable() -> const vtable&
  {
    // The buffer holds an owned F* (placement-new'd in emplace()), so every access to that pointer
    // goes through std::launder to reflect the pointer object's lifetime, not the array's.
    static const vtable vt{
      [](void* self, Args&&... args) -> R {
        // Same void-result handling as the inline thunk: discard a value-returning target rather
        // than emit `return <non-void>;` in a void-returning thunk.
        if constexpr (std::is_void_v<R>) {
          (**std::launder(static_cast<F**>(self)))(std::forward<Args>(args)...);
        } else {
          return (**std::launder(static_cast<F**>(self)))(std::forward<Args>(args)...);
        }
      },
      [](void* dst, void* src) noexcept {
        // Begin the lifetime of the owned pointer in dst (uninitialized storage) rather than
        // assigning through an F** into it, matching the placement-new in emplace(). The alias
        // keeps this a new-expression rather than something that reads as a C-style cast.
        using owned_ptr = F*;
        ::new (dst) owned_ptr(*std::launder(static_cast<F**>(src)));
        *std::launder(static_cast<F**>(src)) = nullptr;
      },
      [](void* self) noexcept {
        delete *std::launder(static_cast<F**>(self));
      },
    };
    return vt;
  }

  // Zero-initialized so the object has no uninitialized members; the bytes are always overwritten
  // by emplace()/move_from() before vtable_ is set and are only ever read back through vtable_
  // (null while the buffer holds no target), so the initialization is defensive, not relied upon.
  alignas(std::max_align_t) unsigned char buffer_[buffer_size]{};
  const vtable* vtable_{ nullptr };

// MSVC emits C4702 (unreachable code) during a late optimization pass, not during parsing, so a
// push/pop around the individual store does not catch it -- the suppression must wrap the whole
// function. For an F whose selected constructor can only throw (e.g. throwing_copy_functor in the
// unit tests) MSVC proves construction never returns and flags every following statement as
// unreachable; every other F reaches them normally, so the warning is a false positive here.
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4702)
#endif
  template<typename F>
  void emplace(F&& f)
  {
    using decayed = std::decay_t<F>;
    if constexpr (fits_inline_v<decayed>) {
      ::new (static_cast<void*>(buffer_)) decayed(std::forward<F>(f));
      vtable_ = &inline_vtable<decayed>();
    } else {
      // Begin the lifetime of a decayed* in the buffer (symmetric with the inline placement-new
      // above), then read it back through the heap vtable.
      ::new (static_cast<void*>(buffer_)) decayed*(new decayed(std::forward<F>(f)));
      vtable_ = &heap_vtable<decayed>();
    }
  }
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

  void move_from(movable_function&& other) noexcept
  {
    if (other.vtable_ != nullptr) {
      other.vtable_->move(buffer_, other.buffer_);
      vtable_ = other.vtable_;
      other.vtable_ = nullptr;
    }
  }

  void reset() noexcept
  {
    if (vtable_ != nullptr) {
      vtable_->destroy(buffer_);
      vtable_ = nullptr;
    }
  }
#endif

  // Constrain on constructibility as well as invocability: emplace() stores the target by
  // constructing a std::decay_t<F> from the forwarded argument, so a callable that cannot be
  // constructed there (e.g. a non-movable functor passed as an rvalue) is removed from overload
  // resolution rather than producing a hard error inside emplace().
  template<typename F>
  using enable_if_callable = std::enable_if_t<!std::is_same_v<std::decay_t<F>, movable_function> &&
                                              std::is_constructible_v<std::decay_t<F>, F> &&
                                              std::is_invocable_r_v<R, std::decay_t<F>&, Args...>>;

public:
  movable_function() noexcept = default;
  movable_function(std::nullptr_t) noexcept
  {
  }

  template<typename F, typename = enable_if_callable<F>>
  movable_function(F&& f)
  {
    emplace(std::forward<F>(f));
  }

  movable_function(movable_function&& other) noexcept
  {
    move_from(std::move(other));
  }

  auto operator=(movable_function&& other) noexcept -> movable_function&
  {
    if (this != &other) {
      reset();
      move_from(std::move(other));
    }
    return *this;
  }

  movable_function(const movable_function&) = delete;
  auto operator=(const movable_function&) -> movable_function& = delete;

  ~movable_function()
  {
    reset();
  }

  auto operator=(std::nullptr_t) noexcept -> movable_function&
  {
    reset();
    return *this;
  }

// Same MSVC C4702 false positive as emplace(), and the same whole-function suppression: for an F
// whose constructor can only throw, the temporary below can never return normally, so MSVC flags
// the assignment and return as unreachable, even though every other F reaches them normally.
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4702)
#endif
  template<typename F, typename = enable_if_callable<F>>
  auto operator=(F&& f) -> movable_function&
  {
    // Strong exception guarantee, matching std::function / std::move_only_function: build the new
    // target in a temporary first -- the only step that can throw (heap allocation, or a throwing
    // copy when f is an lvalue) -- then adopt it through the noexcept move-assignment. A bare
    // reset()+emplace() would give only the basic guarantee: a throwing emplace() would leave the
    // previous target already destroyed and *this empty.
    *this = movable_function(std::forward<F>(f));
    return *this;
  }
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

  explicit operator bool() const noexcept
  {
#ifdef __clang_analyzer__
    return static_cast<bool>(holder_);
#else
    return vtable_ != nullptr;
#endif
  }

  friend auto operator==(const movable_function& f, std::nullptr_t) noexcept -> bool
  {
    return !static_cast<bool>(f);
  }

  friend auto operator==(std::nullptr_t, const movable_function& f) noexcept -> bool
  {
    return !static_cast<bool>(f);
  }

  friend auto operator!=(const movable_function& f, std::nullptr_t) noexcept -> bool
  {
    return static_cast<bool>(f);
  }

  friend auto operator!=(std::nullptr_t, const movable_function& f) noexcept -> bool
  {
    return static_cast<bool>(f);
  }

  auto operator()(Args... args) const -> R
  {
#ifdef __clang_analyzer__
    if (!holder_) {
      throw std::bad_function_call{};
    }
    return holder_->invoke(std::forward<Args>(args)...);
#else
    if (vtable_ == nullptr) {
      throw std::bad_function_call{};
    }
    return vtable_->invoke(const_cast<void*>(static_cast<const void*>(buffer_)),
                           std::forward<Args>(args)...);
#endif
  }
};

} // namespace couchbase::core::utils
