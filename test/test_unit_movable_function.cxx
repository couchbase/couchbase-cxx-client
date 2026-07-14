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

#include "test_helper.hxx"

#include "core/utils/movable_function.hxx"

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace
{
// Per-type heap-allocation counters. A test functor that inherits heap_counted routes its own heap
// allocations through a class-specific operator new/delete, so a test can assert whether
// movable_function put the target on the heap -- the heap fallback runs `new decayed(...)`, which
// dispatches to this operator new -- or stored it inline (placement-new into the buffer, which does
// not). Only allocations of the functor type are counted, so unrelated ones (e.g. a captured
// shared_ptr's control block) never perturb the result.
//
// This deliberately does NOT replace the GLOBAL operator new. Valgrind's memcheck and the
// sanitizers redirect the global allocation symbols for their own bookkeeping, which either
// preempts a user-defined global replacement -- leaving its counter stuck at zero, the exact
// failure this replaces -- or collides with it at link time. A class-specific operator new is an
// ordinary member the runtime always dispatches to, so these counts are reliable under plain,
// sanitized, and valgrind builds alike. The tests are single-threaded, so plain int counters
// suffice.
struct heap_counted {
  static int allocations;
  static int frees;

  static void reset() noexcept
  {
    allocations = 0;
    frees = 0;
  }

  auto operator new(std::size_t size) -> void*
  {
    ++allocations;
    return ::operator new(size);
  }

  void operator delete(void* ptr) noexcept
  {
    if (ptr != nullptr) {
      ++frees;
    }
    ::operator delete(ptr);
  }

  // Over-aligned targets take the aligned allocation functions, not the plain ones above, so an
  // over-aligned functor's heap block would otherwise go uncounted. Route those through the counter
  // too, so the heap-fallback-on-alignment path is observable.
  auto operator new(std::size_t size, std::align_val_t align) -> void*
  {
    ++allocations;
    return ::operator new(size, align);
  }

  void operator delete(void* ptr, std::align_val_t align) noexcept
  {
    if (ptr != nullptr) {
      ++frees;
    }
    ::operator delete(ptr, align);
  }
};
int heap_counted::allocations = 0;
int heap_counted::frees = 0;
} // namespace

using couchbase::core::utils::movable_function;

namespace
{
// A callable large enough to force the heap fallback (>inline buffer). It counts its live instances
// (to prove the target is constructed and destroyed exactly once across moves — no leak, no
// double-destroy on the hand-written heap vtable) and, via heap_counted, its heap allocations and
// frees (to prove exactly one heap block is taken and released).
struct tracked_callable : heap_counted {
  static int live_instances;
  std::array<char, 256> payload{};
  int value{ 0 };

  explicit tracked_callable(int v)
    : value(v)
  {
    ++live_instances;
  }
  tracked_callable(const tracked_callable& other)
    : value(other.value)
  {
    ++live_instances;
  }
  tracked_callable(tracked_callable&& other) noexcept
    : value(other.value)
  {
    ++live_instances;
  }
  auto operator=(const tracked_callable&) -> tracked_callable& = delete;
  auto operator=(tracked_callable&&) -> tracked_callable& = delete;
  ~tracked_callable()
  {
    --live_instances;
  }
  auto operator()() const -> int
  {
    return value;
  }
};
int tracked_callable::live_instances = 0;

// A callable whose type is neither copyable nor movable. It satisfies the call signature but cannot
// be stored, so it must be rejected by overload resolution (SFINAE) rather than hard-error inside
// emplace().
struct non_movable_callable {
  non_movable_callable() = default;
  non_movable_callable(const non_movable_callable&) = delete;
  non_movable_callable(non_movable_callable&&) = delete;
  auto operator=(const non_movable_callable&) -> non_movable_callable& = delete;
  auto operator=(non_movable_callable&&) -> non_movable_callable& = delete;
  void operator()() const
  {
  }
};

struct movable_callable {
  auto operator()() const -> int
  {
    return 0;
  }
};

// Small functors that fit the inline buffer: movable_function stores them in place, so their
// inherited operator new is never called and heap_counted::allocations stays zero.
struct small_copyable_functor : heap_counted {
  std::shared_ptr<int> a{};
  std::shared_ptr<int> b{};
  auto operator()() const -> int
  {
    return *a + *b;
  }
};

struct small_move_only_functor : heap_counted {
  std::unique_ptr<int> p{};
  auto operator()() const -> int
  {
    return *p;
  }
};

// A functor too large for the inline buffer, forcing the heap fallback: exactly one operator new.
struct large_functor : heap_counted {
  std::array<char, 256> payload{};
  auto operator()() const -> std::size_t
  {
    // Reference payload so the capture is used; it is zero-initialized, so this adds nothing to the
    // returned size.
    return payload.size() + static_cast<std::size_t>(payload[0]);
  }
};

// Small enough to be stored inline, and counts its live instances so a test can prove the inline
// move and destroy thunks construct and destroy the target exactly once -- the SBO path that
// tracked_callable (256 bytes, always heap) never exercises.
struct small_tracked_functor : heap_counted {
  static int live_instances;
  int value{ 0 };

  explicit small_tracked_functor(int v)
    : value(v)
  {
    ++live_instances;
  }
  small_tracked_functor(const small_tracked_functor& other)
    : value(other.value)
  {
    ++live_instances;
  }
  small_tracked_functor(small_tracked_functor&& other) noexcept
    : value(other.value)
  {
    ++live_instances;
  }
  auto operator=(const small_tracked_functor&) -> small_tracked_functor& = delete;
  auto operator=(small_tracked_functor&&) -> small_tracked_functor& = delete;
  ~small_tracked_functor()
  {
    --live_instances;
  }
  auto operator()() const -> int
  {
    return value;
  }
};
int small_tracked_functor::live_instances = 0;

// Small and well-aligned, but its move constructor is not noexcept. The inline buffer is chosen on
// move-safety, not only size, so the nothrow-move gate must route this to the heap regardless.
struct throwing_move_functor : heap_counted {
  int value{ 0 };

  explicit throwing_move_functor(int v)
    : value(v)
  {
  }
  throwing_move_functor(const throwing_move_functor&) = default;
  throwing_move_functor(throwing_move_functor&& other) noexcept(false)
    : value(other.value)
  {
  }
  auto operator=(const throwing_move_functor&) -> throwing_move_functor& = delete;
  auto operator=(throwing_move_functor&&) -> throwing_move_functor& = delete;
  auto operator()() const -> int
  {
    return value;
  }
};

// Over-aligned beyond max_align_t while still small enough (on the usual targets) to fit the inline
// buffer by size, so only the alignment gate -- not the size gate -- can route it to the heap.
#if defined(_MSC_VER)
#pragma warning(push)
// structure was padded due to alignment specifier (intentional here)
#pragma warning(disable : 4324)
#endif
struct alignas(2 * alignof(std::max_align_t)) over_aligned_functor : heap_counted {
  int value{ 0 };

  explicit over_aligned_functor(int v)
    : value(v)
  {
  }
  over_aligned_functor(const over_aligned_functor&) = default;
  over_aligned_functor(over_aligned_functor&&) noexcept = default;
  auto operator=(const over_aligned_functor&) -> over_aligned_functor& = delete;
  auto operator=(over_aligned_functor&&) -> over_aligned_functor& = delete;
  auto operator()() const -> int
  {
    return value;
  }
};
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

// A callable whose copy constructor throws but whose move is noexcept. Assigned as an lvalue it is
// copied into the target, so emplace() throws mid-assignment -- the case that distinguishes the
// basic from the strong exception guarantee.
struct throwing_copy_functor {
  struct bomb {
    bomb() = default;
    bomb(const bomb& /* other */)
    {
      throw std::runtime_error("copy");
    }
    bomb(bomb&& /* other */) noexcept = default;
    auto operator=(const bomb&) -> bomb& = default;
    auto operator=(bomb&&) noexcept -> bomb& = default;
    ~bomb() = default;
  };
  bomb b{};
  auto operator()() const -> int
  {
    return 1;
  }
};

// The wrapper's constructor is constrained on constructibility, not only invocability: an ordinary
// callable is storable, a non-movable one is removed from overload resolution.
static_assert(std::is_constructible_v<movable_function<int()>, movable_callable>,
              "a movable callable must be storable in movable_function");
static_assert(!std::is_constructible_v<movable_function<void()>, non_movable_callable>,
              "a non-movable callable must be rejected by SFINAE, not hard-error in emplace()");
} // namespace

TEST_CASE("unit: movable_function invokes and returns", "[unit]")
{
  movable_function<int(int)> f = [](int x) {
    return x + 1;
  };
  REQUIRE(f(41) == 42);
}

TEST_CASE("unit: movable_function invokes a capturing void functor repeatedly", "[unit]")
{
  int calls = 0;
  movable_function<void()> f = [&calls]() {
    ++calls;
  };
  f();
  f();
  REQUIRE(calls == 2);
}

TEST_CASE("unit: movable_function holds a move-only functor", "[unit]")
{
  auto p = std::make_unique<int>(7);
  movable_function<int()> f = [p = std::move(p)]() {
    return *p;
  };
  REQUIRE(f() == 7);
}

TEST_CASE("unit: movable_function forwards a move-only argument", "[unit]")
{
  movable_function<int(std::unique_ptr<int>&&)> f = [](std::unique_ptr<int>&& p) {
    return *p;
  };
  REQUIRE(f(std::make_unique<int>(9)) == 9);
}

TEST_CASE("unit: movable_function move constructor transfers and empties the source", "[unit]")
{
  movable_function<int()> a = []() {
    return 5;
  };
  movable_function<int()> b = std::move(a);
  REQUIRE(static_cast<bool>(b));
  REQUIRE(b() == 5);
  REQUIRE_FALSE(static_cast<bool>(a));
}

TEST_CASE("unit: movable_function move assignment transfers and empties the source", "[unit]")
{
  movable_function<int()> a = []() {
    return 5;
  };
  movable_function<int()> b;
  b = std::move(a);
  REQUIRE(static_cast<bool>(b));
  REQUIRE(b() == 5);
  REQUIRE_FALSE(static_cast<bool>(a));
}

TEST_CASE("unit: movable_function bool and nullptr semantics", "[unit]")
{
  movable_function<void()> f;
  REQUIRE_FALSE(static_cast<bool>(f));
  f = []() {
  };
  REQUIRE(static_cast<bool>(f));
  f = nullptr;
  REQUIRE_FALSE(static_cast<bool>(f));
}

TEST_CASE("unit: movable_function compares against nullptr", "[unit]")
{
  movable_function<void()> f;
  REQUIRE(f == nullptr);
  REQUIRE(nullptr == f);
  REQUIRE_FALSE(f != nullptr);
  f = []() {
  };
  REQUIRE(f != nullptr);
  REQUIRE(nullptr != f);
  REQUIRE_FALSE(f == nullptr);
}

TEST_CASE("unit: movable_function stores a small copyable functor without heap allocation",
          "[unit]")
{
  heap_counted::reset();
  movable_function<int()> f =
    small_copyable_functor{ {}, std::make_shared<int>(1), std::make_shared<int>(2) };
  REQUIRE(heap_counted::allocations == 0); // stored inline: the functor is not heap-allocated
  REQUIRE(f() == 3);
}

TEST_CASE("unit: movable_function stores a small move-only functor without heap allocation",
          "[unit]")
{
  heap_counted::reset();
  movable_function<int()> f = small_move_only_functor{ {}, std::make_unique<int>(3) };
  REQUIRE(heap_counted::allocations == 0); // stored inline: the functor is not heap-allocated
  REQUIRE(f() == 3);
}

TEST_CASE("unit: movable_function falls back to the heap for a large functor", "[unit]")
{
  heap_counted::reset();
  movable_function<std::size_t()> f = large_functor{};
  REQUIRE(heap_counted::allocations == 1); // too large for the inline buffer: one heap allocation
  REQUIRE(f() == 256);
}

TEST_CASE("unit: invoking an empty movable_function throws", "[unit]")
{
  SECTION("default-constructed")
  {
    movable_function<int()> f;
    REQUIRE_THROWS_AS(f(), std::bad_function_call);
  }
  SECTION("assigned nullptr")
  {
    movable_function<int()> f = []() {
      return 1;
    };
    f = nullptr;
    REQUIRE_THROWS_AS(f(), std::bad_function_call);
  }
  SECTION("moved-from")
  {
    movable_function<int()> f = []() {
      return 1;
    };
    movable_function<int()> g = std::move(f);
    // Invoking the moved-from source is exactly the contract under test: a moved-from
    // movable_function is empty and throws bad_function_call. The static analyzer flags this as a
    // use-after-move regardless, so suppress it there only.
#if defined(__clang__) && defined(__clang_analyzer__)
    [[clang::suppress]]
#endif
    REQUIRE_THROWS_AS(f(), std::bad_function_call);
  }
}

TEST_CASE("unit: movable_function moving a heap-held functor transfers ownership exactly once",
          "[unit]")
{
  REQUIRE(tracked_callable::live_instances == 0);
  heap_counted::reset();
  {
    movable_function<int()> a{ tracked_callable{ 42 } };
    REQUIRE(tracked_callable::live_instances == 1); // one live instance owned on the heap

    // Move-construction steals the heap pointer; it must not construct or destroy a target.
    movable_function<int()> b = std::move(a);
    REQUIRE(tracked_callable::live_instances == 1);
    REQUIRE_FALSE(static_cast<bool>(a));
    REQUIRE(b() == 42);
  }
  // Destroying the sole owner runs the target's destructor and frees the heap block.
  REQUIRE(tracked_callable::live_instances == 0);
  REQUIRE(heap_counted::allocations == 1); // exactly one heap block taken for the target
  REQUIRE(heap_counted::frees ==
          heap_counted::allocations); // released once: not leaked, not double-freed
}

TEST_CASE("unit: move-assigning over a populated movable_function destroys the old target once",
          "[unit]")
{
  REQUIRE(tracked_callable::live_instances == 0);
  {
    movable_function<int()> b{ tracked_callable{ 1 } };
    movable_function<int()> a{ tracked_callable{ 2 } };
    REQUIRE(tracked_callable::live_instances == 2);

    b = std::move(a); // must destroy b's current target before taking a's
    REQUIRE(tracked_callable::live_instances == 1);
    REQUIRE(b() == 2);
    REQUIRE_FALSE(static_cast<bool>(a));
  }
  REQUIRE(tracked_callable::live_instances == 0);
}

TEST_CASE("unit: movable_function self-move-assignment leaves it valid", "[unit]")
{
  movable_function<int()> f = []() {
    return 7;
  };
  auto& ref = f;
  f = std::move(ref); // guarded no-op; must not destroy the held target
  REQUIRE(static_cast<bool>(f));
  REQUIRE(f() == 7);
}

TEST_CASE("unit: movable_function<void()> discards a value-returning target", "[unit]")
{
  // std::is_invocable_r_v<void, F&> admits a callable whose result is discardable, matching
  // std::function / std::move_only_function. Binding one to a void signature must compile -- the
  // invoke thunk drops the result rather than emitting `return <non-void>;` in a void thunk -- and
  // ignore the value. Exercises the common asio tail-return idiom (e.g. `return self->do_next();`
  // in a void handler). Both storage paths route through their own thunk.
  SECTION("inline path")
  {
    int calls = 0;
    movable_function<void()> f = [&calls]() {
      ++calls;
      return 42; // discarded
    };
    f();
    REQUIRE(calls == 1);
  }
  SECTION("heap path")
  {
    std::array<char, 256> payload{};
    movable_function<void()> f = [payload]() {
      return payload.size(); // discarded
    };
    f(); // must compile and run without returning the value through the void thunk
  }
}

TEST_CASE("unit: movable_function moving an inline-held functor transfers ownership exactly once",
          "[unit]")
{
  REQUIRE(small_tracked_functor::live_instances == 0);
  heap_counted::reset();
  {
    movable_function<int()> a{ small_tracked_functor{ 42 } };
    REQUIRE(small_tracked_functor::live_instances == 1); // stored inline...
    REQUIRE(heap_counted::allocations == 0);             // ...so no heap block

    // Inline move-construction: construct into dst, destroy src -> still exactly one live instance.
    movable_function<int()> b = std::move(a);
    REQUIRE(small_tracked_functor::live_instances == 1);
    REQUIRE_FALSE(static_cast<bool>(a));
    REQUIRE(b() == 42);

    // Move-assignment over a populated target destroys the old target before taking the new one.
    movable_function<int()> c{ small_tracked_functor{ 7 } };
    REQUIRE(small_tracked_functor::live_instances == 2);
    b = std::move(c);
    REQUIRE(small_tracked_functor::live_instances == 1);
    REQUIRE(b() == 7);
  }
  // The inline destroy thunk ran for the survivor: no leak, no double-destroy on the SBO path.
  REQUIRE(small_tracked_functor::live_instances == 0);
  REQUIRE(heap_counted::allocations == 0); // never touched the heap
}

TEST_CASE("unit: movable_function heap-routes a small functor whose move can throw", "[unit]")
{
  heap_counted::reset();
  movable_function<int()> f = throwing_move_functor{ 5 };
  // Small and well-aligned, but the inline buffer requires a nothrow move; this one can throw, so
  // the nothrow-move gate -- not size -- forces the heap.
  REQUIRE(heap_counted::allocations == 1);
  REQUIRE(f() == 5);
}

TEST_CASE("unit: movable_function heap-routes an over-aligned functor", "[unit]")
{
  heap_counted::reset();
  movable_function<int()> f = over_aligned_functor{ 9 };
  // Fits by size but over-aligned, so the alignment gate forces the heap; heap_counted's aligned
  // operator new records the block that the plain overload would have missed.
  REQUIRE(heap_counted::allocations == 1);
  REQUIRE(f() == 9);
}

TEST_CASE("unit: assigning a throwing-to-construct callable preserves the current target", "[unit]")
{
  // Strong exception guarantee: if constructing the new target throws (here, a throwing copy of an
  // lvalue functor), assignment leaves the existing target intact rather than empty. A bare
  // reset()+emplace() would destroy the old target first and leave f empty on throw.
  movable_function<int()> f = []() {
    return 7;
  };
  throwing_copy_functor bomb{};
  REQUIRE_THROWS_AS(f = bomb, std::runtime_error); // lvalue: copied into the target, which throws
  REQUIRE(static_cast<bool>(f));
  REQUIRE(f() == 7);
}
