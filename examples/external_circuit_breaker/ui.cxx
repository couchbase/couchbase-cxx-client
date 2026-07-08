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

#include "ui.hxx"

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace example::ui
{

auto
c(const char* code) -> const char*
{
  return style::enabled() ? code : "";
}

auto
paint(std::string_view text, const char* code) -> std::string
{
  if (!style::enabled()) {
    return std::string{ text };
  }
  std::string out;
  out.reserve(text.size() + 16);
  out.append(code).append(text).append(style::reset);
  return out;
}

namespace
{

[[nodiscard]] auto
repeat(std::string_view s, std::size_t n) -> std::string
{
  std::string out;
  out.reserve(s.size() * n);
  for (std::size_t i = 0; i < n; ++i) {
    out.append(s);
  }
  return out;
}

[[nodiscard]] auto
pad_right(std::string_view text, std::size_t width) -> std::string
{
  std::string out{ text };
  if (out.size() < width) {
    out.append(width - out.size(), ' ');
  }
  return out;
}

[[nodiscard]] auto
pad_left(std::string_view text, std::size_t width) -> std::string
{
  std::string out;
  if (text.size() < width) {
    out.append(width - text.size(), ' ');
  }
  out.append(text);
  return out;
}

/**
 * Approximate monospace display width of a UTF-8 string.  Counts code
 * points (every byte whose top two bits are not 10) and assumes each code
 * point occupies one cell — good enough for the box-drawing chars,
 * em-dashes and arrows used in this demo.
 */
[[nodiscard]] auto
visible_width(std::string_view s) -> std::size_t
{
  std::size_t w = 0;
  for (char ch : s) {
    if ((static_cast<unsigned char>(ch) & 0xC0U) != 0x80U) {
      ++w;
    }
  }
  return w;
}

[[nodiscard]] auto
pad_right_visible(std::string_view text, std::size_t width) -> std::string
{
  std::string out{ text };
  auto vw = visible_width(text);
  if (vw < width) {
    out.append(width - vw, ' ');
  }
  return out;
}

/**
 * Word-wrap @p text into lines no wider than @p max_width visible cells,
 * breaking on whitespace where possible.  A single token longer than
 * max_width is emitted on its own line and left to overflow rather than
 * truncated — better to mangle alignment than to silently drop content.
 */
[[nodiscard]] auto
wrap_visible(std::string_view text, std::size_t max_width) -> std::vector<std::string>
{
  std::vector<std::string> lines;
  if (max_width == 0) {
    lines.emplace_back(text);
    return lines;
  }
  std::string current;
  std::size_t current_w = 0;
  std::size_t i = 0;
  while (i < text.size()) {
    while (i < text.size() && text[i] == ' ') {
      ++i;
    }
    if (i >= text.size()) {
      break;
    }
    auto word_start = i;
    while (i < text.size() && text[i] != ' ') {
      ++i;
    }
    auto word = text.substr(word_start, i - word_start);
    auto word_w = visible_width(word);
    auto needed = current.empty() ? word_w : current_w + 1 + word_w;
    if (!current.empty() && needed > max_width) {
      lines.push_back(std::move(current));
      current.clear();
      current_w = 0;
    }
    if (current.empty()) {
      current.assign(word);
      current_w = word_w;
    } else {
      current.push_back(' ');
      current.append(word);
      current_w += 1 + word_w;
    }
  }
  if (!current.empty()) {
    lines.push_back(std::move(current));
  }
  if (lines.empty()) {
    lines.emplace_back();
  }
  return lines;
}

} // namespace

auto
color_for(example::cb::circuit_state s) -> const char*
{
  switch (s) {
    case example::cb::circuit_state::closed:
      return style::green;
    case example::cb::circuit_state::open:
      return style::red;
    case example::cb::circuit_state::half_open:
      return style::yellow;
    case example::cb::circuit_state::forced_open:
      return style::magenta;
    case example::cb::circuit_state::disabled:
      return style::gray;
  }
  return style::gray;
}

auto
icon_for(example::cb::circuit_state s) -> const char*
{
  switch (s) {
    case example::cb::circuit_state::closed:
      return "\xE2\x9C\x93"; // ✓
    case example::cb::circuit_state::open:
      return "\xE2\x9C\x97"; // ✗
    case example::cb::circuit_state::half_open:
      return "\xE2\x97\x90"; // ◐
    case example::cb::circuit_state::forced_open:
      return "\xE2\x96\xA0"; // ■
    case example::cb::circuit_state::disabled:
      return "\xE2\x97\x8B"; // ○
  }
  return "?";
}

auto
state_badge(example::cb::circuit_state s) -> std::string
{
  std::string label;
  label.append(icon_for(s)).append(" ").append(example::cb::to_string(s));
  return paint(label, color_for(s));
}

auto
state_badge_visible_width(example::cb::circuit_state s) -> std::size_t
{
  return 2 + std::string_view{ example::cb::to_string(s) }.size();
}

auto
short_node(std::string_view id) -> std::string
{
  if (id.size() <= 12) {
    return std::string{ id };
  }
  std::string out;
  out.reserve(12);
  out.append(id.substr(0, 8)).append("\xE2\x80\xA6"); // …
  out.append(id.substr(id.size() - 4));
  return out;
}

auto
print_intro(std::string_view title, std::string_view subtitle) -> void
{
  constexpr std::size_t inner = 78;
  // Leave one column of padding on each side of the box interior, so the
  // wrappable region is two columns shy of `inner`.
  constexpr std::size_t text_width = inner - 2;
  const auto horiz = repeat("\xE2\x94\x80", inner); // ─
  auto print_line = [&](std::string_view body, const char* style_code) -> void {
    std::cout << c(style::cyan) << "\xE2\x94\x82" << c(style::reset) << c(style_code) << " "
              << pad_right_visible(body, inner - 1) << c(style::reset) << c(style::cyan)
              << "\xE2\x94\x82" << c(style::reset) << "\n";
  };
  std::cout << "\n"
            << c(style::bold) << c(style::cyan) << "\xE2\x95\xAD" << horiz << "\xE2\x95\xAE"
            << c(style::reset) << "\n";
  for (const auto& line : wrap_visible(title, text_width)) {
    print_line(line, style::bold);
  }
  if (!subtitle.empty()) {
    for (const auto& line : wrap_visible(subtitle, text_width)) {
      print_line(line, style::dim);
    }
  }
  std::cout << c(style::bold) << c(style::cyan) << "\xE2\x95\xB0" << horiz << "\xE2\x95\xAF"
            << c(style::reset) << "\n";
}

auto
print_demo_header(std::string_view tag, std::string_view title, std::string_view sub) -> void
{
  std::cout << "\n"
            << c(style::bold) << c(style::blue) << tag << c(style::reset) << "  " << c(style::bold)
            << title << c(style::reset) << "\n";
  if (!sub.empty()) {
    std::cout << "  " << c(style::dim) << sub << c(style::reset) << "\n";
  }
  std::cout << c(style::dim) << repeat("\xE2\x94\x80", 80) // ─
            << c(style::reset) << "\n";
}

auto
print_step(std::string_view msg) -> void
{
  std::cout << "  " << c(style::cyan) << "\xE2\x96\xB8" << c(style::reset) << " " << msg << "\n";
}

auto
print_note(std::string_view msg) -> void
{
  std::cout << "    " << c(style::dim) << msg << c(style::reset) << "\n";
}

auto
print_success(std::string_view msg) -> void
{
  std::cout << "  " << paint("\xE2\x9C\x93", style::green) << " " << msg << "\n";
}

auto
print_warning(std::string_view msg) -> void
{
  std::cout << "  " << paint("\xE2\x9A\xA0", style::yellow) << " " << msg << "\n";
}

auto
print_error(std::string_view msg) -> void
{
  std::cout << "  " << paint("\xE2\x9C\x97", style::red) << " " << msg << "\n";
}

auto
print_transition(example::cb::circuit_state from,
                 example::cb::circuit_state to,
                 std::string_view why) -> void
{
  std::string_view node{};
  std::string_view reason = why;
  if (reason.size() > 6 && reason.substr(0, 6) == "[node ") {
    if (auto end = reason.find("] ", 6); end != std::string_view::npos) {
      node = reason.substr(6, end - 6);
      reason = reason.substr(end + 2);
    }
  }
  std::cout << "    " << paint("\xE2\x9A\xA1", style::yellow) // ⚡
            << "  " << state_badge(from) << " " << c(style::bold) << "\xE2\x86\x92"
            << c(style::reset) // →
            << " " << state_badge(to);
  if (!node.empty()) {
    std::cout << "  " << c(style::dim) << "node " << c(style::reset) << c(style::gray)
              << short_node(node) << c(style::reset);
  }
  std::cout << "  " << c(style::dim) << reason << c(style::reset) << "\n";
}

auto
print_metrics_table(example::cb::circuit_breaker& breaker) -> void
{
  auto rows = breaker.all_metrics();
  std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) -> bool {
    return a.first.id() < b.first.id();
  });
  if (rows.empty()) {
    print_note("no per-node breakers have been exercised yet");
    return;
  }

  // Node IDs are fixed-size for any given cluster but vary across cluster
  // versions: legacy servers synthesize an 8-char CRC32, newer ones return
  // a 36-char UUID.  Size the column to the widest id actually present so
  // the table stays compact on legacy clusters and stays aligned on new
  // ones, with a floor of "Node" header width.
  std::size_t w_node = std::string_view{ "Node" }.size();
  for (const auto& [nid, m] : rows) {
    w_node = std::max(w_node, visible_width(nid.id()));
  }
  constexpr std::size_t w_state = 13;
  constexpr std::size_t w_total = 6;
  constexpr std::size_t w_ok = 4;
  constexpr std::size_t w_fail = 5;
  constexpr std::size_t w_slow = 5;
  constexpr std::size_t w_pct = 6;
  constexpr std::size_t w_rej = 4;

  auto hline = [&](std::string_view left, std::string_view mid, std::string_view right) -> void {
    std::cout << "  " << c(style::dim) << left << repeat("\xE2\x94\x80", w_node + 2) << mid
              << repeat("\xE2\x94\x80", w_state + 2) << mid << repeat("\xE2\x94\x80", w_total + 2)
              << mid << repeat("\xE2\x94\x80", w_ok + 2) << mid
              << repeat("\xE2\x94\x80", w_fail + 2) << mid << repeat("\xE2\x94\x80", w_slow + 2)
              << mid << repeat("\xE2\x94\x80", w_pct + 2) << mid
              << repeat("\xE2\x94\x80", w_rej + 2) << right << c(style::reset) << "\n";
  };
  auto cell_v = [&]() -> std::string {
    return std::string{ c(style::dim) } + "\xE2\x94\x82" + c(style::reset);
  };

  std::cout << "\n";
  hline("\xE2\x94\x8C", "\xE2\x94\xAC", "\xE2\x94\x90"); // ┌ ┬ ┐
  std::cout << "  " << cell_v() << c(style::bold) << " " << pad_right("Node", w_node) << " "
            << c(style::reset) << cell_v() << c(style::bold) << " " << pad_right("State", w_state)
            << " " << c(style::reset) << cell_v() << c(style::bold) << " "
            << pad_left("Total", w_total) << " " << c(style::reset) << cell_v() << c(style::bold)
            << " " << pad_left("OK", w_ok) << " " << c(style::reset) << cell_v() << c(style::bold)
            << " " << pad_left("Fail", w_fail) << " " << c(style::reset) << cell_v()
            << c(style::bold) << " " << pad_left("Slow", w_slow) << " " << c(style::reset)
            << cell_v() << c(style::bold) << " " << pad_left("Fail%", w_pct) << " "
            << c(style::reset) << cell_v() << c(style::bold) << " " << pad_left("Rej", w_rej) << " "
            << c(style::reset) << cell_v() << "\n";
  hline("\xE2\x94\x9C", "\xE2\x94\xBC", "\xE2\x94\xA4"); // ├ ┼ ┤

  for (const auto& [nid, m] : rows) {
    auto state_cell_padding = w_state - state_badge_visible_width(m.state);
    std::cout << "  " << cell_v() << " " << pad_right(nid.id(), w_node) << " " << cell_v() << " "
              << state_badge(m.state) << std::string(state_cell_padding, ' ') << " " << cell_v()
              << " " << pad_left(std::to_string(m.total_calls), w_total) << " " << cell_v() << " "
              << pad_left(std::to_string(m.successful_calls), w_ok) << " " << cell_v() << " "
              << pad_left(std::to_string(m.failed_calls), w_fail) << " " << cell_v() << " "
              << pad_left(std::to_string(m.slow_calls), w_slow) << " " << cell_v() << " "
              << pad_left(std::to_string(m.failure_rate_percent) + "%", w_pct) << " " << cell_v()
              << " " << pad_left(std::to_string(m.rejected_calls), w_rej) << " " << cell_v()
              << "\n";
  }
  hline("\xE2\x94\x94", "\xE2\x94\xB4", "\xE2\x94\x98"); // └ ┴ ┘
}

auto
print_legend() -> void
{
  std::cout << "\n  " << c(style::bold) << "Legend" << c(style::reset) << "   ";
  std::cout << paint("\xE2\x9C\x93 CLOSED", style::green) << "  "
            << paint("\xE2\x9C\x97 OPEN", style::red) << "  "
            << paint("\xE2\x97\x90 HALF_OPEN", style::yellow) << "  "
            << paint("\xE2\x96\xA0 FORCED_OPEN", style::magenta) << "  "
            << paint("\xE2\x97\x8B DISABLED", style::gray) << "\n";
}

} // namespace example::ui
