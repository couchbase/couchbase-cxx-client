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

#include "framework/test_registry.hxx"

#include "core/utils/movable_function.hxx"

#include <any>
#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace couchbase::test
{

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

using ::couchbase::core::utils::movable_function;

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

namespace
{
void
a_target_is_invoked_and_its_result_returned([[maybe_unused]] context& ctx)
{
  movable_function<int(int)> f = [](int x) {
    return x + 1;
  };
  assert_eq(f(41), 42, "the argument reaches the target and its result comes back");
}

void
a_capturing_void_target_is_invoked_each_time([[maybe_unused]] context& ctx)
{
  int calls = 0;
  movable_function<void()> f = [&calls]() {
    ++calls;
  };
  f();
  f();
  assert_eq(calls, 2, "every call reaches the target");
}

void
a_move_only_target_is_held([[maybe_unused]] context& ctx)
{
  auto p = std::make_unique<int>(7);
  movable_function<int()> f = [p = std::move(p)]() {
    return *p;
  };
  assert_eq(f(), 7, "the moved-in capture survives into the call");
}

void
a_move_only_argument_is_forwarded([[maybe_unused]] context& ctx)
{
  movable_function<int(std::unique_ptr<int>&&)> f = [](std::unique_ptr<int>&& p) {
    return *p;
  };
  assert_eq(f(std::make_unique<int>(9)), 9, "the argument is forwarded, not copied");
}

void
move_construction_transfers_the_target_and_empties_the_source([[maybe_unused]] context& ctx)
{
  movable_function<int()> a = []() {
    return 5;
  };
  movable_function<int()> b = std::move(a);
  assert_true(static_cast<bool>(b), "the destination holds the target");
  assert_eq(b(), 5, "the target still answers");
  // A moved-from movable_function is required to be empty; reading it here is
  // that specification, not a use-after-move.
  // cppcheck-suppress accessMoved
  assert_false(static_cast<bool>(a), "the source is left empty");
}

void
move_assignment_transfers_the_target_and_empties_the_source([[maybe_unused]] context& ctx)
{
  movable_function<int()> a = []() {
    return 5;
  };
  movable_function<int()> b;
  b = std::move(a);
  assert_true(static_cast<bool>(b), "the destination holds the target");
  assert_eq(b(), 5, "the target still answers");
  // A moved-from movable_function is required to be empty; reading it here is
  // that specification, not a use-after-move.
  // cppcheck-suppress accessMoved
  assert_false(static_cast<bool>(a), "the source is left empty");
}

void
assigning_nullptr_empties_the_function([[maybe_unused]] context& ctx)
{
  movable_function<void()> f;
  assert_false(static_cast<bool>(f), "a default-constructed function is empty");
  f = []() {
  };
  assert_true(static_cast<bool>(f), "an assigned target makes it non-empty");
  f = nullptr;
  assert_false(static_cast<bool>(f), "assigning nullptr releases the target");
}

void
emptiness_is_visible_through_comparison_with_nullptr([[maybe_unused]] context& ctx)
{
  movable_function<void()> f;
  assert_true(f == nullptr, "an empty function equals nullptr");
  assert_true(nullptr == f, "the comparison is symmetric");
  assert_false(f != nullptr, "and its negation agrees");
  f = []() {
  };
  assert_true(f != nullptr, "a populated function differs from nullptr");
  assert_true(nullptr != f, "the comparison is symmetric");
  assert_false(f == nullptr, "and its negation agrees");
}

void
a_small_copyable_functor_is_stored_inline([[maybe_unused]] context& ctx)
{
  heap_counted::reset();
  movable_function<int()> f =
    small_copyable_functor{ {}, std::make_shared<int>(1), std::make_shared<int>(2) };
  assert_eq(heap_counted::allocations, 0, "the functor is not heap-allocated");
  assert_eq(f(), 3, "the inline target answers");
}

void
a_small_move_only_functor_is_stored_inline([[maybe_unused]] context& ctx)
{
  heap_counted::reset();
  movable_function<int()> f = small_move_only_functor{ {}, std::make_unique<int>(3) };
  assert_eq(heap_counted::allocations, 0, "the functor is not heap-allocated");
  assert_eq(f(), 3, "the inline target answers");
}

void
a_large_functor_falls_back_to_the_heap([[maybe_unused]] context& ctx)
{
  heap_counted::reset();
  movable_function<std::size_t()> f = large_functor{};
  assert_eq(heap_counted::allocations, 1, "too large for the inline buffer: one heap allocation");
  assert_eq(f(), std::size_t{ 256 }, "the heap-held target answers");
}

void
invoking_a_default_constructed_function_throws([[maybe_unused]] context& ctx)
{
  movable_function<int()> f;
  assert_throws<std::bad_function_call>(
    [&f]() {
      static_cast<void>(f());
    },
    "an empty function refuses the call rather than dispatching through nothing");
}

void
invoking_a_function_assigned_nullptr_throws([[maybe_unused]] context& ctx)
{
  movable_function<int()> f = []() {
    return 1;
  };
  // The premise of this case, as distinct from the default-constructed one: the function held a
  // target, and assigning nullptr is what took it away.
  assert_true(static_cast<bool>(f), "the function starts out holding a target");
  f = nullptr;
  assert_throws<std::bad_function_call>(
    [&f]() {
      static_cast<void>(f());
    },
    "an emptied function refuses the call rather than dispatching through nothing");
}

void
invoking_a_moved_from_function_throws([[maybe_unused]] context& ctx)
{
  movable_function<int()> f = []() {
    return 1;
  };
  movable_function<int()> g = std::move(f);
  // Invoking the moved-from source is exactly the contract under test: a moved-from
  // movable_function is empty and throws bad_function_call. The static analyzer flags the call as
  // a use-after-move regardless, so it is suppressed on the call itself: scan-build reports the
  // lambda body, which an attribute on the enclosing statement does not always cover.
  assert_throws<std::bad_function_call>(
    [&f]() {
#if defined(__clang__) && defined(__clang_analyzer__)
      [[clang::suppress]]
#endif
      static_cast<void>(f());
    },
    "a moved-from function refuses the call rather than dispatching through nothing");
}

void
moving_a_heap_held_target_transfers_ownership_exactly_once([[maybe_unused]] context& ctx)
{
  assert_eq(tracked_callable::live_instances, 0, "no target is live before the case starts");
  heap_counted::reset();
  {
    movable_function<int()> a{ tracked_callable{ 42 } };
    assert_eq(tracked_callable::live_instances, 1, "one live instance owned on the heap");

    // Move-construction steals the heap pointer; it must not construct or destroy a target.
    movable_function<int()> b = std::move(a);
    assert_eq(tracked_callable::live_instances, 1, "the move neither copies nor destroys");
    // A moved-from movable_function is required to be empty; reading it here is
    // that specification, not a use-after-move.
    // cppcheck-suppress accessMoved
    assert_false(static_cast<bool>(a), "the source is left empty");
    assert_eq(b(), 42, "the destination holds the same target");
  }
  // Destroying the sole owner runs the target's destructor and frees the heap block.
  assert_eq(tracked_callable::live_instances, 0, "the survivor's destructor ran");
  assert_eq(heap_counted::allocations, 1, "exactly one heap block taken for the target");
  assert_eq(
    heap_counted::frees, heap_counted::allocations, "released once: not leaked, not double-freed");
}

void
move_assignment_destroys_the_old_heap_target_once([[maybe_unused]] context& ctx)
{
  assert_eq(tracked_callable::live_instances, 0, "no target is live before the case starts");
  {
    movable_function<int()> b{ tracked_callable{ 1 } };
    movable_function<int()> a{ tracked_callable{ 2 } };
    assert_eq(tracked_callable::live_instances, 2, "both targets are live");

    b = std::move(a); // must destroy b's current target before taking a's
    assert_eq(tracked_callable::live_instances, 1, "the overwritten target is destroyed once");
    assert_eq(b(), 2, "the destination holds the assigned target");
    // A moved-from movable_function is required to be empty; reading it here is
    // that specification, not a use-after-move.
    // cppcheck-suppress accessMoved
    assert_false(static_cast<bool>(a), "the source is left empty");
  }
  assert_eq(tracked_callable::live_instances, 0, "the survivor's destructor ran");
}

void
self_move_assignment_leaves_the_target_intact([[maybe_unused]] context& ctx)
{
  movable_function<int()> f = []() {
    return 7;
  };
  auto& ref = f;
  f = std::move(ref); // guarded no-op; must not destroy the held target
  assert_true(static_cast<bool>(f), "the function still holds a target");
  assert_eq(f(), 7, "and it is the target it held before");
}

// std::is_invocable_r_v<void, F&> admits a callable whose result is discardable, matching
// std::function / std::move_only_function. Binding one to a void signature must compile -- the
// invoke thunk drops the result rather than emitting `return <non-void>;` in a void thunk -- and
// ignore the value. Exercises the common asio tail-return idiom (e.g. `return self->do_next();`
// in a void handler). Both storage paths route through their own thunk.
void
an_inline_value_returning_target_binds_to_a_void_signature([[maybe_unused]] context& ctx)
{
  int calls = 0;
  movable_function<void()> f = [&calls]() {
    ++calls;
    return 42; // discarded
  };
  f();
  assert_eq(calls, 1, "the call reaches the target and its result is dropped");
}

void
a_heap_held_value_returning_target_binds_to_a_void_signature([[maybe_unused]] context& ctx)
{
  std::array<char, 256> payload{};
  movable_function<void()> f = [payload]() {
    return payload.size(); // discarded
  };
  f(); // must compile and run without returning the value through the void thunk
}

void
moving_an_inline_held_target_transfers_ownership_exactly_once([[maybe_unused]] context& ctx)
{
  assert_eq(small_tracked_functor::live_instances, 0, "no target is live before the case starts");
  heap_counted::reset();
  {
    movable_function<int()> a{ small_tracked_functor{ 42 } };
    assert_eq(small_tracked_functor::live_instances, 1, "one live instance stored inline...");
    assert_eq(heap_counted::allocations, 0, "...so no heap block");

    // Inline move-construction: construct into dst, destroy src -> still exactly one live instance.
    movable_function<int()> b = std::move(a);
    assert_eq(small_tracked_functor::live_instances, 1, "the move neither copies nor leaks");
    // A moved-from movable_function is required to be empty; reading it here is
    // that specification, not a use-after-move.
    // cppcheck-suppress accessMoved
    assert_false(static_cast<bool>(a), "the source is left empty");
    assert_eq(b(), 42, "the destination holds the same target");

    // Move-assignment over a populated target destroys the old target before taking the new one.
    movable_function<int()> c{ small_tracked_functor{ 7 } };
    assert_eq(small_tracked_functor::live_instances, 2, "both targets are live");
    b = std::move(c);
    assert_eq(small_tracked_functor::live_instances, 1, "the overwritten target is destroyed once");
    assert_eq(b(), 7, "the destination holds the assigned target");
  }
  // The inline destroy thunk ran for the survivor: no leak, no double-destroy on the SBO path.
  assert_eq(small_tracked_functor::live_instances, 0, "the survivor's destructor ran");
  assert_eq(heap_counted::allocations, 0, "the inline path never touched the heap");
}

void
a_small_functor_whose_move_can_throw_is_routed_to_the_heap([[maybe_unused]] context& ctx)
{
  heap_counted::reset();
  movable_function<int()> f = throwing_move_functor{ 5 };
  // Small and well-aligned, but the inline buffer requires a nothrow move; this one can throw, so
  // the nothrow-move gate -- not size -- forces the heap.
  assert_eq(heap_counted::allocations, 1, "the nothrow-move gate forces the heap");
  assert_eq(f(), 5, "the heap-held target answers");
}

void
an_over_aligned_functor_is_routed_to_the_heap([[maybe_unused]] context& ctx)
{
  heap_counted::reset();
  movable_function<int()> f = over_aligned_functor{ 9 };
  // Fits by size but over-aligned, so the alignment gate forces the heap; heap_counted's aligned
  // operator new records the block that the plain overload would have missed.
  assert_eq(heap_counted::allocations, 1, "the alignment gate forces the heap");
  assert_eq(f(), 9, "the heap-held target answers");
}

void
an_assignment_whose_target_throws_preserves_the_current_one([[maybe_unused]] context& ctx)
{
  // Strong exception guarantee: if constructing the new target throws (here, a throwing copy of an
  // lvalue functor), assignment leaves the existing target intact rather than empty. A bare
  // reset()+emplace() would destroy the old target first and leave f empty on throw.
  movable_function<int()> f = []() {
    return 7;
  };
  throwing_copy_functor bomb{};
  assert_throws<std::runtime_error>(
    [&f, &bomb]() {
      f = bomb; // lvalue: copied into the target, which throws
    },
    "the failed copy propagates");
  assert_true(static_cast<bool>(f), "the function still holds a target");
  assert_eq(f(), 7, "and it is the target it held before");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_target_is_invoked_and_its_result_returned), {}, timeout::instant },
      { CASE(a_capturing_void_target_is_invoked_each_time), {}, timeout::instant },
      { CASE(a_move_only_target_is_held), {}, timeout::instant },
      { CASE(a_move_only_argument_is_forwarded), {}, timeout::instant },
      { CASE(move_construction_transfers_the_target_and_empties_the_source), {}, timeout::instant },
      { CASE(move_assignment_transfers_the_target_and_empties_the_source), {}, timeout::instant },
      { CASE(assigning_nullptr_empties_the_function), {}, timeout::instant },
      { CASE(emptiness_is_visible_through_comparison_with_nullptr), {}, timeout::instant },
      { CASE(a_small_copyable_functor_is_stored_inline), {}, timeout::instant },
      { CASE(a_small_move_only_functor_is_stored_inline), {}, timeout::instant },
      { CASE(a_large_functor_falls_back_to_the_heap), {}, timeout::instant },
      { CASE(invoking_a_default_constructed_function_throws), {}, timeout::instant },
      { CASE(invoking_a_function_assigned_nullptr_throws), {}, timeout::instant },
      { CASE(invoking_a_moved_from_function_throws), {}, timeout::instant },
      { CASE(moving_a_heap_held_target_transfers_ownership_exactly_once), {}, timeout::instant },
      { CASE(move_assignment_destroys_the_old_heap_target_once), {}, timeout::instant },
      { CASE(self_move_assignment_leaves_the_target_intact), {}, timeout::instant },
      { CASE(an_inline_value_returning_target_binds_to_a_void_signature), {}, timeout::instant },
      { CASE(a_heap_held_value_returning_target_binds_to_a_void_signature), {}, timeout::instant },
      { CASE(moving_an_inline_held_target_transfers_ownership_exactly_once), {}, timeout::instant },
      { CASE(a_small_functor_whose_move_can_throw_is_routed_to_the_heap), {}, timeout::instant },
      { CASE(an_over_aligned_functor_is_routed_to_the_heap), {}, timeout::instant },
      { CASE(an_assignment_whose_target_throws_preserves_the_current_one), {}, timeout::instant },
    },
  };
}

namespace
{
using callback = ::couchbase::core::utils::movable_function<void(int)>;

struct matching_callable {
  void operator()(int) const
  {
  }
};

struct wrong_signature {
  void operator()(int, int) const
  {
  }
};

// Asking whether movable_function is copy constructible must answer, not recurse.
//
// The constructor constraint is instantiated with F = const movable_function&, and its
// constructibility term then asks for the very trait being computed. std::conjunction stops at the
// self-exclusion before that term is instantiated; joining the terms with && does not, because &&
// instantiates every operand whatever the first one answers. std::any asks exactly this question
// about anything it is handed, which is how the recursion was reached from
// core/io/mcbp_command.hxx.
//
// These are static assertions because the defect is a compile error, not a wrong answer: where it
// bites, this translation unit does not build. Only a toolchain whose std::is_constructible is a
// class template rather than a compiler builtin reaches it -- gcc 8 does, gcc 9 and later do not --
// so on a modern compiler these hold either way and pin the constraint's meaning instead.
static_assert(!std::is_copy_constructible_v<callback>);
static_assert(!std::is_constructible_v<std::any, callback>);
static_assert(std::is_move_constructible_v<callback>);

// The constraint still admits and rejects what it did before.
static_assert(std::is_constructible_v<callback, matching_callable>);
static_assert(!std::is_constructible_v<callback, wrong_signature>);
static_assert(!std::is_constructible_v<callback, int>);
} // namespace

} // namespace couchbase::test
