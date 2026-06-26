/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

/**
 * @file
 *
 * Presentation layer for the external_circuit_breaker demo.  Everything
 * here is purely cosmetic — it turns the breaker's events into something
 * a human (or the human's manager) can read over the shoulder.  The
 * example::cb breaker has no dependency on this namespace; copy the
 * circuit_breaker.hxx / circuit_breaker.cxx pair without any of the ui
 * helpers and it will work just the same.
 */

#pragma once

#include "circuit_breaker.hxx"

#include <cstddef>
#include <string>
#include <string_view>

namespace example::ui
{

namespace style
{
/**
 * Prepare the platform's standard output for the demo UI.  On Windows this
 * sets the console output code page to UTF-8 so the box-drawing characters,
 * arrows, and glyph icons render correctly.  ANSI virtual-terminal
 * processing is enabled lazily on the first call to style::enabled(), where
 * it is gated on isatty() and the NO_COLOR environment variable.  On Unix
 * it is a no-op — terminals handle both natively.
 *
 * Safe to call more than once.  Should be called once, early in main(),
 * before any ui::print_* function.
 */
auto
initialize() -> void;

[[nodiscard]] auto
enabled() -> bool;

constexpr const char* reset = "\033[0m";
constexpr const char* bold = "\033[1m";
constexpr const char* dim = "\033[2m";
constexpr const char* red = "\033[31m";
constexpr const char* green = "\033[32m";
constexpr const char* yellow = "\033[33m";
constexpr const char* blue = "\033[34m";
constexpr const char* magenta = "\033[35m";
constexpr const char* cyan = "\033[36m";
constexpr const char* gray = "\033[90m";
} // namespace style

[[nodiscard]] auto
c(const char* code) -> const char*;
[[nodiscard]] auto
paint(std::string_view text, const char* code) -> std::string;

[[nodiscard]] auto
color_for(example::cb::circuit_state s) -> const char*;
[[nodiscard]] auto
icon_for(example::cb::circuit_state s) -> const char*;

/**
 * Colored, icon-prefixed state label.  The visible width (icon + space +
 * name) is deterministic, so the caller can pad around it.
 */
[[nodiscard]] auto
state_badge(example::cb::circuit_state s) -> std::string;
[[nodiscard]] auto
state_badge_visible_width(example::cb::circuit_state s) -> std::size_t;

[[nodiscard]] auto
short_node(std::string_view id) -> std::string;

auto
print_intro(std::string_view title, std::string_view subtitle) -> void;
auto
print_demo_header(std::string_view tag, std::string_view title, std::string_view sub) -> void;
auto
print_step(std::string_view msg) -> void;
auto
print_note(std::string_view msg) -> void;
auto
print_success(std::string_view msg) -> void;
auto
print_warning(std::string_view msg) -> void;
auto
print_error(std::string_view msg) -> void;

/**
 * Pretty-print a breaker transition.  Strips the "[node XXX] " prefix that
 * example::cb::circuit_breaker embeds in the reason string and rerenders
 * the node id in its own visually distinct slot.
 */
auto
print_transition(example::cb::circuit_state from,
                 example::cb::circuit_state to,
                 std::string_view why) -> void;

auto
print_metrics_table(example::cb::circuit_breaker& breaker) -> void;
auto
print_legend() -> void;

} // namespace example::ui
