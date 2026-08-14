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
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

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

// ── Rendering an operand ──────────────────────────────────────────────────────
//
// A failure message wants to show the values that differed, and showing them is the only reason a
// test framework needs formatting at all. Doing it with a formatting library means every test
// translation unit pays for that library's headers: <spdlog/fmt/fmt.h> preprocesses to about 63,800
// lines as this tree compiles it, against 45,900 for catch2/catch_test_macros.hpp -- so roughly the
// cost of the Catch2 header it replaces, spent to render an int or a string.
//
// So the framework renders operands itself, through this customisation point. The printers below
// need nothing beyond <string> and <type_traits>; the header as a whole still includes what its
// declarations need. A type with no specialisation is not an error: its assertion still fires, and
// simply omits the operands rather than dragging in a way to print them.
//
// A test that wants richer text in a message is free to build one -- with fmt, or anything else --
// and pass it; that cost then falls on the file that asked for it.
namespace detail
{
// what() hands back c_str(), so a raw control byte written into a message ends it there: a NUL in
// an operand leaves the reader with "(actual: \"" and no expectation. Every printer that can carry
// one routes through here, so the char and string forms cannot drift apart.
inline void
escape_into(std::string& text, char value)
{
  switch (value) {
    case '\0':
      text += "\\0";
      return;
    case '\n':
      text += "\\n";
      return;
    case '\r':
      text += "\\r";
      return;
    case '\t':
      text += "\\t";
      return;
    default:
      break;
  }
  const auto byte = static_cast<unsigned char>(value);
  if (byte < 0x20U || byte == 0x7fU) {
    constexpr char digits[] = "0123456789abcdef";
    text += "\\x";
    text += digits[byte >> 4U];
    text += digits[byte & 0x0fU];
    return;
  }
  text += value;
}

[[nodiscard]] inline auto
quoted(std::string_view value) -> std::string
{
  std::string text{ '"' };
  for (const auto byte : value) {
    escape_into(text, byte);
  }
  text += '"';
  return text;
}
} // namespace detail

template<typename T, typename Enable = void>
struct operand_printer {
  static constexpr bool available = false;
  [[nodiscard]] static auto to_text(const T& /* value */) -> std::string
  {
    return {};
  }
};

template<typename T>
struct operand_printer<
  T,
  std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool> && !std::is_same_v<T, char>>> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const T& value) -> std::string
  {
    return std::to_string(value);
  }
};

template<>
struct operand_printer<bool> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const bool& value) -> std::string
  {
    return value ? "true" : "false";
  }
};

template<>
struct operand_printer<char> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const char& value) -> std::string
  {
    std::string text{ '\'' };
    detail::escape_into(text, value);
    text += '\'';
    return text;
  }
};

template<typename T>
struct operand_printer<T, std::enable_if_t<std::is_floating_point_v<T>>> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const T& value) -> std::string
  {
    // The shortest form that reads back as the same value. std::to_string pads to six fixed
    // decimals, so everything below 5e-7 trimmed to "0": a tolerance printed as "+/- 0", and two
    // different values printed identically with no reason given -- which is the failure the
    // char-pointer static_assert further down this header exists to prevent. <sstream> would cost
    // more to include than everything else here put together, so this goes through snprintf and
    // widens the precision until the text parses back to the value it came from.
    char buffer[64]{};
    if constexpr (std::is_same_v<T, long double>) {
      for (int precision = 6; precision <= 21; ++precision) {
        static_cast<void>(std::snprintf(buffer, sizeof(buffer), "%.*Lg", precision, value));
        if (std::strtold(buffer, nullptr) == value) {
          break;
        }
      }
    } else {
      const auto widened = static_cast<double>(value);
      for (int precision = 6; precision <= 17; ++precision) {
        static_cast<void>(std::snprintf(buffer, sizeof(buffer), "%.*g", precision, widened));
        if (std::strtod(buffer, nullptr) == widened) {
          break;
        }
      }
    }
    return std::string{ buffer };
  }
};

// Quoted, so a trailing space or an empty string is visible rather than being read as a typo in the
// message.
template<>
struct operand_printer<std::string> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const std::string& value) -> std::string
  {
    return detail::quoted(value);
  }
};

template<>
struct operand_printer<std::string_view> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const std::string_view& value) -> std::string
  {
    return detail::quoted(value);
  }
};

// A string literal has type const char[N], and std::decay_t applies array-to-pointer decay, so the
// operand reaches a printer as const char* and this is the specialisation selected. Taken by value,
// which is what a decayed pointer is.
template<>
struct operand_printer<const char*> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const char* value) -> std::string
  {
    return value == nullptr ? "(null)" : detail::quoted(value);
  }
};

// Written out rather than inheriting from the const overload: clang-format 22 and 24 disagree on
// how to lay out an empty derived-struct body, and CI runs 22.
template<>
struct operand_printer<char*> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const char* value) -> std::string
  {
    return operand_printer<const char*>::to_text(value);
  }
};

// The underlying value, not a name: the framework cannot know the enumerators, and a number the
// reader can look up beats no operands at all.
template<typename T>
struct operand_printer<T, std::enable_if_t<std::is_enum_v<T>>> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const T& value) -> std::string
  {
    return std::to_string(static_cast<std::intmax_t>(value));
  }
};

namespace detail
{
template<typename T>
inline constexpr bool printable = operand_printer<std::decay_t<T>>::available;

template<typename T>
[[nodiscard]] inline auto
render(const T& value) -> std::string
{
  // std::decay_t strips volatile, so printable<volatile bool> is true and this branch is
  // instantiated -- but a const volatile lvalue cannot bind to the printer's const& parameter, and
  // the error would land inside this header rather than on the assertion that caused it. Copying
  // is the only legal way to drop volatile. Gated, because a plain static_cast would break the
  // string-literal path, where decay_t<const char[N]> is const char*.
  if constexpr (std::is_volatile_v<T>) {
    // Scalars only, and the assert says so rather than leaving a page of template errors: dropping
    // volatile needs a copy, and a class type's copy constructor takes const& and cannot bind a
    // const volatile lvalue. Reading such an object through a non-volatile reference is not an
    // option either -- that is undefined for an object that really is volatile.
    static_assert(std::is_scalar_v<std::decay_t<T>>,
                  "a volatile operand must be a scalar: dropping volatile requires a copy, and a "
                  "class type cannot be copied from a const volatile lvalue");
    return operand_printer<std::decay_t<T>>::to_text(std::decay_t<T>{ value });
  } else {
    return operand_printer<std::decay_t<T>>::to_text(value);
  }
}

// "file.cxx:12: what the assertion means". Built by concatenation rather than by a format call, so
// nothing here needs a formatting library.
[[nodiscard]] inline auto
at(source_location loc, std::string_view message) -> std::string
{
  std::string text{ loc.file_name() };
  text += ':';
  text += std::to_string(loc.line());
  text += ": ";
  text.append(message.data(), message.size());
  return text;
}
} // namespace detail

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
    throw test_assertion_failure(detail::at(loc, message));
  }
}

inline void
assert_false(bool value,
             std::string_view message = "expected false",
             source_location loc = source_location::current())
{
  assert_true(!value, message, loc);
}

// Comparing two char pointers compares addresses, not text: two distinct buffers holding "hello"
// are unequal, and both operands then render as "hello" -- a failure message showing two identical
// values and no reason. Only rejected when BOTH sides are char pointers; the mixed form
// assert_eq(e.what(), std::string{...}) compares by value and is what the suite already writes.
namespace detail
{
template<typename T>
inline constexpr bool is_char_pointer =
  std::is_same_v<std::decay_t<T>, char*> || std::is_same_v<std::decay_t<T>, const char*>;

template<typename A, typename B>
inline constexpr bool both_char_pointers = is_char_pointer<A> && is_char_pointer<B>;
} // namespace detail

// Report both operands where the types can be rendered. Without this the message says only
// "expected equal", so a failure tells you the values differed but not what they were -- the one
// place where not using Catch2 (whose expression decomposition prints operands) costs something
// concrete.
template<typename A, typename B>
inline void
assert_eq(const A& actual,
          const B& expected,
          std::string_view message = "expected equal",
          source_location loc = source_location::current())
{
  static_assert(!detail::both_char_pointers<A, B>,
                "comparing two char pointers compares addresses, not text: wrap one side in "
                "std::string_view or std::string");
  if (!(actual == expected)) {
    auto text = detail::at(loc, message);
    if constexpr (detail::printable<A> && detail::printable<B>) {
      text +=
        " (actual: " + detail::render(actual) + ", expected: " + detail::render(expected) + ")";
    }
    throw test_assertion_failure(std::move(text));
  }
}

template<typename A, typename B>
inline void
assert_ne(const A& actual,
          const B& unexpected,
          std::string_view message = "expected different",
          source_location loc = source_location::current())
{
  static_assert(!detail::both_char_pointers<A, B>,
                "comparing two char pointers compares addresses, not text: wrap one side in "
                "std::string_view or std::string");
  if (actual == unexpected) {
    auto text = detail::at(loc, message);
    if constexpr (detail::printable<A>) {
      text += " (both are: " + detail::render(actual) + ")";
    }
    throw test_assertion_failure(std::move(text));
  }
}

inline void
assert_contains(std::string_view haystack,
                std::string_view needle,
                std::string_view message = "expected to contain",
                source_location loc = source_location::current())
{
  if (haystack.find(needle) == std::string_view::npos) {
    throw test_assertion_failure(detail::at(loc, message) + " (" + detail::quoted(needle) +
                                 " is not in " + detail::quoted(haystack) + ")");
  }
}

inline void
assert_starts_with(std::string_view value,
                   std::string_view prefix,
                   std::string_view message = "expected prefix",
                   source_location loc = source_location::current())
{
  if (value.size() < prefix.size() || value.compare(0, prefix.size(), prefix) != 0) {
    throw test_assertion_failure(detail::at(loc, message) + " (" + detail::quoted(value) +
                                 " does not start with " + detail::quoted(prefix) + ")");
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
    throw test_assertion_failure(detail::at(loc, message) + " (actual: " + detail::render(actual) +
                                 ", expected: " + detail::render(expected) + " ± " +
                                 detail::render(tolerance) + ")");
  }
}

// Fail here, unconditionally. For a branch that must not be reached, where an assertion would have
// to invent a condition to state what the control flow already says.
[[noreturn]] inline void
fail(std::string_view message, source_location loc = source_location::current())
{
  throw test_assertion_failure(detail::at(loc, message));
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
    throw test_assertion_failure(detail::at(loc, message) + " (" + e.what() + ")");
  } catch (...) {
    throw test_assertion_failure(detail::at(loc, message) +
                                 " (an exception not derived from std::exception)");
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
    throw test_assertion_failure(detail::at(loc, message) +
                                 " (a different exception type was thrown)");
  }
  if (!threw_expected) {
    throw test_assertion_failure(detail::at(loc, message) + " (nothing was thrown)");
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
    throw test_assertion_failure(detail::at(loc, message) +
                                 " (a different exception type was thrown)");
  }
  if (!threw_expected) {
    throw test_assertion_failure(detail::at(loc, message) + " (nothing was thrown)");
  }
  // Both halves: the substring alone does not tell the reader what the exception actually said, and
  // that is the thing they need in order to decide whether the code or the expectation is wrong.
  if (what.find(substring) == std::string_view::npos) {
    throw test_assertion_failure(detail::at(loc, message) + " (" + detail::quoted(substring) +
                                 " is not in " + detail::quoted(what) + ")");
  }
}

} // namespace couchbase::test
