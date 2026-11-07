/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// What a benchmark case is written against. A benchmark file provides benchmarks() returning a
// bench_suite, the way a test file provides tests().

#include "framework/requirement.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace couchbase::test::bench
{
struct options {
  std::chrono::milliseconds warmup{ 1000 };
  std::chrono::milliseconds min_time{ 2000 };
  std::chrono::milliseconds max_time{ 10000 };
  // Relative half-width of the median's 95% confidence interval, in percent.
  double ci_target_percent{ 1.0 };
};

namespace detail
{
struct case_run;

// Stores p to a thread_local volatile object, so concurrent callers do not race. The store
// survives link-time optimisation, and the address it publishes keeps the bytes at p from being
// treated as dead.
void
escape(const volatile char* p) noexcept;
} // namespace detail

// The handle a case receives. Each batched() or per_op() call measures one key: the case name, or
// the case name, '/' and the variant. A variant is one or more '/'-separated segments, each
// [A-Za-z_][A-Za-z0-9_]*. A key used twice in one binary, or a variant outside that rule, throws
// and so fails the case. Setup done before the call and teardown after it are not timed.
class state
{
public:
  explicit state(detail::case_run& run);

  auto ctx() -> context&;

  // fn() runs one iteration and is called concurrently from `threads` threads. One sample is the
  // mean nanoseconds per iteration of a batch, and the batch size doubles until a batch takes at
  // least 1 ms. Only batches of at least 1 ms, or at the size cap, are recorded, and a key that
  // records none fails. The loop over fn is instantiated here, in the case's translation unit, so
  // fn is inlined into it. A result that fn does not pass to do_not_optimize() may be removed, and
  // the work with it.
  template<typename F>
  void batched(std::string_view variant, std::size_t threads, F&& fn)
  {
    using fn_type = std::remove_reference_t<F>;
    const void* target = std::addressof(fn);
    run_batched(
      variant,
      threads,
      [](void* f, std::uint64_t iterations) {
        auto& call = *static_cast<fn_type*>(f);
        for (std::uint64_t i = 0; i < iterations; ++i) {
          call();
        }
      },
      const_cast<void*>(target));
  }

  // fn(thread_index) performs one operation and returns false when it failed. The clock is read
  // immediately before and after fn, and the latency of a successful operation goes into a
  // per-thread histogram. A failed operation counts as an error and its latency is discarded.
  void per_op(std::string_view variant,
              std::size_t threads,
              const std::function<bool(std::size_t)>& fn);

private:
  void run_batched(std::string_view variant,
                   std::size_t threads,
                   void (*loop)(void*, std::uint64_t iterations),
                   void* fn);

  detail::case_run* run_;
};

// Keeps `value`, and the computation that produced it, from being optimised away.
template<typename T>
inline void
do_not_optimize(T&& value) noexcept
{
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : : "g"(&value) : "memory");
#else
  detail::escape(&reinterpret_cast<const volatile char&>(value));
#if defined(_MSC_VER)
  _ReadWriteBarrier();
#endif
#endif
}

struct bench_case {
  std::string name;
  void (*func)(state&);
  std::vector<requirement_ptr> requirements{};
};

struct bench_suite {
  std::string name;
  std::vector<bench_case> cases;
};

// Each benchmark file defines this.
auto
benchmarks() -> bench_suite;
} // namespace couchbase::test::bench
