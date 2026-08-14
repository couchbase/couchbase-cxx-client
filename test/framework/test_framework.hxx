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

// A small hand-rolled test framework for this repository's test suites.
//
// This deliberately does NOT use Catch2 (the couchbase-cxx-client default). Its design is
// ported from the author's stand-alone harness, adapted from C++23 to the C++17 that this
// library targets: std::print -> fmt, std::move_only_function -> plain function pointers,
// std::jthread -> std::thread, std::source_location -> the builtin-based shim below.
//
// A test file provides `tests()` returning a `test_suite`; the runner (see test_runner.hxx)
// executes each `test_case` on a worker thread with a per-case timeout and reports the outcome.

#include "requirement.hxx"

#include <chrono>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// The wrapper (not <spdlog/fmt/bundled/format.h>) is required: it selects bundled fmt and derives
// the header-only-vs-compiled mode from SPDLOG_COMPILED_LIB, which test_framework_main picks up by
// linking spdlog::spdlog. Including the bundled header directly would bypass that and reintroduce
// the duplicate fmt definitions that MSVC rejects -- see the note in cmake/TestFramework.cmake.
#include <spdlog/fmt/fmt.h>

namespace couchbase::test
{

// C++17 stand-in for std::source_location (C++20). Where __builtin_FILE/LINE/FUNCTION exist they
// evaluate at the call site when used as default arguments, so an assertion helper can capture its
// caller's location without a macro. On a toolchain that lacks the builtins the shim degrades to an
// unknown location rather than failing to compile.
//
// The probe is deliberately __has_builtin (plus bare __clang__, whose older releases lack the
// probe). MSVC exposes the builtins from 16.6 but only gained __has_builtin in VS 2022 17.1, so
// 16.6-17.0 takes the "unknown" branch: correct, just less informative. Not special-cased because
// no CI leg builds those versions, and an untested #if is worse than a documented limitation.
#if defined(__has_builtin)
#define COUCHBASE_TEST_HAS_LOCATION_BUILTINS                                                       \
  (__has_builtin(__builtin_FILE) && __has_builtin(__builtin_LINE) &&                               \
   __has_builtin(__builtin_FUNCTION))
#elif defined(__clang__)
#define COUCHBASE_TEST_HAS_LOCATION_BUILTINS 1
#else
#define COUCHBASE_TEST_HAS_LOCATION_BUILTINS 0
#endif

class source_location
{
public:
#if COUCHBASE_TEST_HAS_LOCATION_BUILTINS
  static constexpr auto current(const char* file = __builtin_FILE(),
                                int line = __builtin_LINE(),
                                const char* function = __builtin_FUNCTION()) noexcept
    -> source_location
#else
  static constexpr auto current(const char* file = "unknown",
                                int line = 0,
                                const char* function = "unknown") noexcept -> source_location
#endif
  {
    source_location loc;
    loc.file_ = file;
    loc.line_ = static_cast<std::uint_least32_t>(line);
    loc.function_ = function;
    return loc;
  }

  [[nodiscard]] constexpr auto file_name() const noexcept -> const char*
  {
    return file_;
  }
  [[nodiscard]] constexpr auto line() const noexcept -> std::uint_least32_t
  {
    return line_;
  }
  [[nodiscard]] constexpr auto function_name() const noexcept -> const char*
  {
    return function_;
  }

private:
  const char* file_{ "" };
  std::uint_least32_t line_{ 0 };
  const char* function_{ "" };
};

#undef COUCHBASE_TEST_HAS_LOCATION_BUILTINS

// Timeout presets — use these instead of raw millisecond values so timing expectations are
// explicit. Mirrors the presets in the source harness.
namespace timeout
{
using namespace std::chrono_literals;
inline constexpr auto instant = 500ms;   // pure computation, no I/O
inline constexpr auto fast = 2'000ms;    // minimal I/O
inline constexpr auto network = 5'000ms; // network operations
// integration and slow are equal by coincidence, not by definition: they express different
// intents (waiting on an external system vs. a deliberate in-test delay) and either may be
// retuned without regard for the other. Pick by what the case is waiting for, not by the value.
inline constexpr auto integration = 30'000ms; // external processes / real cluster
inline constexpr auto slow = 30'000ms;        // intentional delays
} // namespace timeout

inline constexpr auto default_timeout = timeout::network;

// Thrown by skip() to mark a case skipped at runtime — for the rare precondition no requirement
// can express. Prefer a requirement: a skip from inside the body is invisible until the case runs,
// and cannot be reported by --list-tests. The runner reports it distinctly from a failure.
class test_skip_exception : public std::exception
{
public:
  explicit test_skip_exception(std::string reason)
    : reason_{ std::move(reason) }
  {
  }
  [[nodiscard]] auto what() const noexcept -> const char* override
  {
    return reason_.c_str();
  }
  [[nodiscard]] auto reason() const noexcept -> const std::string&
  {
    return reason_;
  }

private:
  std::string reason_;
};

// Thrown by the assert_* helpers on a failed assertion. The runner catches it and marks the
// case failed (without aborting the process, so a self-test can exercise the failure path).
class test_assertion_failure : public std::exception
{
public:
  explicit test_assertion_failure(std::string message)
    : message_{ std::move(message) }
  {
  }
  [[nodiscard]] auto what() const noexcept -> const char* override
  {
    return message_.c_str();
  }

private:
  std::string message_;
};

struct test_case {
  std::string name;
  // An ordinary function, so it can be navigated to and called like one. What it needs from the
  // environment is declared beside it rather than checked inside it.
  void (*func)(context&);
  std::vector<requirement_ptr> requirements{};
  std::chrono::milliseconds timeout{ default_timeout };
};

struct test_suite {
  std::string name;
  std::vector<test_case> test_cases;
  std::vector<test_case> slow_test_cases{};
  // Run once after the last case, unless one of them timed out. A timed-out case leaves a worker
  // detached and possibly still inside the very library the teardown would unload, so test_main.cxx
  // skips it and leaves through _Exit instead. What test/main.cxx does with OPENSSL_cleanup()
  // today: a process-wide teardown that belongs to the binary rather than to any case in it.
  void (*teardown)() = nullptr;
};

// Each test file defines this.
auto
tests() -> test_suite;

// ── Assertions ────────────────────────────────────────────────────────────────

[[noreturn]] inline void
skip(std::string reason)
{
  throw test_skip_exception(std::move(reason));
}

inline void
assert_true(bool value,
            std::string_view message = "expected true",
            source_location loc = source_location::current())
{
  if (!value) {
    throw test_assertion_failure(fmt::format("{}:{}: {}", loc.file_name(), loc.line(), message));
  }
}

inline void
assert_false(bool value,
             std::string_view message = "expected false",
             source_location loc = source_location::current())
{
  assert_true(!value, message, loc);
}

// Report both operands when they are formattable. Without this the message says only "expected
// equal", so a failure tells you the values differed but not what they were -- the one place where
// not using Catch2 (whose expression decomposition prints operands) costs something concrete. The
// `if constexpr` keeps assert_eq usable with types fmt cannot format.
template<typename A, typename B>
inline void
assert_eq(const A& actual,
          const B& expected,
          std::string_view message = "expected equal",
          source_location loc = source_location::current())
{
  if (!(actual == expected)) {
    if constexpr (fmt::is_formattable<A>::value && fmt::is_formattable<B>::value) {
      throw test_assertion_failure(fmt::format("{}:{}: {} (actual: {}, expected: {})",
                                               loc.file_name(),
                                               loc.line(),
                                               message,
                                               actual,
                                               expected));
    } else {
      throw test_assertion_failure(fmt::format("{}:{}: {}", loc.file_name(), loc.line(), message));
    }
  }
}

template<typename A, typename B>
inline void
assert_ne(const A& actual,
          const B& unexpected,
          std::string_view message = "expected different",
          source_location loc = source_location::current())
{
  if (actual == unexpected) {
    if constexpr (fmt::is_formattable<A>::value) {
      throw test_assertion_failure(
        fmt::format("{}:{}: {} (both are: {})", loc.file_name(), loc.line(), message, actual));
    } else {
      throw test_assertion_failure(fmt::format("{}:{}: {}", loc.file_name(), loc.line(), message));
    }
  }
}

inline void
assert_contains(std::string_view haystack,
                std::string_view needle,
                std::string_view message = "expected to contain",
                source_location loc = source_location::current())
{
  if (haystack.find(needle) == std::string_view::npos) {
    throw test_assertion_failure(fmt::format(R"({}:{}: {} ("{}" is not in "{}"))",
                                             loc.file_name(),
                                             loc.line(),
                                             message,
                                             needle,
                                             haystack));
  }
}

inline void
assert_starts_with(std::string_view value,
                   std::string_view prefix,
                   std::string_view message = "expected prefix",
                   source_location loc = source_location::current())
{
  if (value.size() < prefix.size() || value.compare(0, prefix.size(), prefix) != 0) {
    throw test_assertion_failure(fmt::format(R"({}:{}: {} ("{}" does not start with "{}"))",
                                             loc.file_name(),
                                             loc.line(),
                                             message,
                                             value,
                                             prefix));
  }
}

// An absolute tolerance, not a relative one: the suite compares durations and byte counts, where
// "within 50ms" is the statement being made and a ratio would mean something different at each
// magnitude.
inline void
assert_near(double actual,
            double expected,
            double tolerance,
            std::string_view message = "expected within tolerance",
            source_location loc = source_location::current())
{
  const auto difference = actual > expected ? actual - expected : expected - actual;
  if (!(difference <= tolerance)) {
    throw test_assertion_failure(fmt::format("{}:{}: {} (actual: {}, expected: {} ± {})",
                                             loc.file_name(),
                                             loc.line(),
                                             message,
                                             actual,
                                             expected,
                                             tolerance));
  }
}

// Fail here, unconditionally. For a branch that must not be reached, where an assertion would have
// to invent a condition to state what the control flow already says.
[[noreturn]] inline void
fail(std::string_view message, source_location loc = source_location::current())
{
  throw test_assertion_failure(fmt::format("{}:{}: {}", loc.file_name(), loc.line(), message));
}

// Invoke `fn` and require it not to throw. The exception's own message is reported: a case that
// merely says "threw" leaves the reader to reproduce the failure to find out what it was.
template<typename Fn>
inline void
assert_no_throw(Fn&& fn,
                std::string_view message = "expected no exception",
                source_location loc = source_location::current())
{
  try {
    std::forward<Fn>(fn)();
  } catch (const test_skip_exception&) {
    throw;
  } catch (const test_assertion_failure&) {
    throw;
  } catch (const std::exception& e) {
    throw test_assertion_failure(
      fmt::format("{}:{}: {} ({})", loc.file_name(), loc.line(), message, e.what()));
  } catch (...) {
    throw test_assertion_failure(
      fmt::format("{}:{}: {} (an exception not derived from std::exception)",
                  loc.file_name(),
                  loc.line(),
                  message));
  }
}

// Invoke `fn` and require it to throw an exception of type Exc.
template<typename Exc, typename Fn>
inline void
assert_throws(Fn&& fn,
              std::string_view message = "expected exception",
              source_location loc = source_location::current())
{
  bool threw_expected = false;
  // The two control-flow types are matched ahead of `Exc`, and the order is the whole point:
  // handlers are tried in order and both derive from std::exception, so with `Exc` naming any base
  // of them -- std::exception itself, most obviously -- the expected-exception handler would claim
  // them and the case would report passed. A swallowed skip is the failure this framework exists to
  // make impossible: the case is counted as having verified something it had just declared it could
  // not, and nothing in the report says otherwise.
  //
  // Neither type is assertable here as a consequence: a case that wants to observe a skip or a
  // failure drives run() and asserts on the run_result, the way the self-tests do. Naming one as
  // `Exc` leaves its handler below unreachable, which is a hard error in this project's default
  // build: -Wexceptions under -Werror (cmake/CompilerWarnings.cmake). So the wrong order is caught
  // at compile time here and only misreports where those warnings are not fatal.
  try {
    std::forward<Fn>(fn)();
  } catch (const test_skip_exception&) {
    throw; // skip() states that the case does not apply, whatever type was asked for
  } catch (const test_assertion_failure&) {
    throw; // a nested failure carries its own location and message; neither survives being counted
  } catch (const Exc&) {
    threw_expected = true;
  } catch (...) {
    throw test_assertion_failure(fmt::format(
      "{}:{}: {} (a different exception type was thrown)", loc.file_name(), loc.line(), message));
  }
  if (!threw_expected) {
    throw test_assertion_failure(
      fmt::format("{}:{}: {} (nothing was thrown)", loc.file_name(), loc.line(), message));
  }
}

// Invoke `fn`, require it to throw `Exc`, and require that exception's message to contain
// `substring`. `Exc` defaults to std::exception because the common use is to assert what went wrong
// rather than which type carried it; a case that cares about the type names it.
//
// The substring leads. A callable is often several lines long, and with it first what is being
// asserted is only readable after scrolling past the thing doing the asserting.
//
// The control-flow types are matched ahead of `Exc` for the reason given on assert_throws above,
// and here the default makes it load-bearing rather than defensive: `Exc` is a base of both, so any
// other order would record a skip() from inside the callable as the expected exception.
template<typename Exc = std::exception, typename Fn>
inline void
assert_throws_with(std::string_view substring,
                   Fn&& fn,
                   std::string_view message = "expected exception",
                   source_location loc = source_location::current())
{
  std::string what;
  bool threw_expected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const test_skip_exception&) {
    throw;
  } catch (const test_assertion_failure&) {
    throw;
  } catch (const Exc& e) {
    threw_expected = true;
    what = e.what();
  } catch (...) {
    throw test_assertion_failure(fmt::format(
      "{}:{}: {} (a different exception type was thrown)", loc.file_name(), loc.line(), message));
  }
  if (!threw_expected) {
    throw test_assertion_failure(
      fmt::format("{}:{}: {} (nothing was thrown)", loc.file_name(), loc.line(), message));
  }
  // Both halves: the substring alone does not tell the reader what the exception actually said, and
  // that is the thing they need in order to decide whether the code or the expectation is wrong.
  if (what.find(substring) == std::string_view::npos) {
    throw test_assertion_failure(fmt::format(
      R"({}:{}: {} ("{}" is not in "{}"))", loc.file_name(), loc.line(), message, substring, what));
  }
}

} // namespace couchbase::test
