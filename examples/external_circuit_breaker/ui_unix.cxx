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

#if !defined(_WIN32)

#include "ui.hxx"

#include <cstdio>
#include <cstdlib>

#include <unistd.h>

namespace example::ui::style
{

auto
initialize() -> void
{
  // POSIX terminals handle ANSI escapes and UTF-8 natively — nothing to do.
}

auto
enabled() -> bool
{
  static const bool on = []() -> bool {
    const char* no_color = std::getenv("NO_COLOR"); // NOLINT(concurrency-mt-unsafe)
    if (no_color != nullptr && no_color[0] != '\0') {
      return false;
    }
    return ::isatty(::fileno(stdout)) != 0;
  }();
  return on;
}

} // namespace example::ui::style

#endif // !defined(_WIN32)
