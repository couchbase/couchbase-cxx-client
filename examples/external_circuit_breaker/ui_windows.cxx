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

#if defined(_WIN32)

#include "ui.hxx"

#include <cstdio>
#include <cstdlib>

#include <io.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace example::ui::style
{

namespace
{

/**
 * Try to put the stdout console into ANSI/virtual-terminal mode.  Returns
 * true on Windows 10 1511+ (which is where VT support landed) and on any
 * modern Windows Terminal host.  Returns false on legacy consoles, when
 * stdout is redirected to a file or pipe, or when another host has captured
 * the handle in a way that does not support VT.
 */
auto
try_enable_virtual_terminal() -> bool
{
  HANDLE h = ::GetStdHandle(STD_OUTPUT_HANDLE);
  if (h == nullptr || h == INVALID_HANDLE_VALUE) {
    return false;
  }
  DWORD mode = 0;
  if (::GetConsoleMode(h, &mode) == 0) {
    return false; // stdout is not a console (redirected to pipe/file).
  }
  const DWORD with_vt = mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING;
  if (::SetConsoleMode(h, with_vt) != 0) {
    return true;
  }
  return false;
}

auto
no_color_env_set() -> bool
{
  char* value = nullptr;
  std::size_t value_len = 0;
  if (_dupenv_s(&value, &value_len, "NO_COLOR") == 0 && value != nullptr) {
    const bool disabled = value[0] != '\0';
    std::free(value);
    return disabled;
  }
  return false;
}

} // namespace

auto
initialize() -> void
{
  // Tell the console to interpret our output as UTF-8 so the box-drawing
  // characters, arrows, and glyph icons render correctly.  This is
  // independent of ANSI color support and is worth doing even when the
  // terminal does not support VT escape sequences.
  ::SetConsoleOutputCP(CP_UTF8);
}

auto
enabled() -> bool
{
  static const bool on = []() -> bool {
    if (no_color_env_set()) {
      return false;
    }
    if (_isatty(_fileno(stdout)) == 0) {
      return false;
    }
    return try_enable_virtual_terminal();
  }();
  return on;
}

} // namespace example::ui::style

#endif // defined(_WIN32)
