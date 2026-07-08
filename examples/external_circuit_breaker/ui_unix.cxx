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

#include "demo_common.hxx"
#include "ui.hxx"

#include <cstdio>

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
  // Cache the answer in a static so we only consult the environment / TTY
  // once per process. NO_COLOR is the de-facto opt-out (see no-color.org)
  // — any non-empty value disables ANSI styling regardless of the TTY
  // check. demo::safe_getenv() centralises the std::getenv wrapping; on
  // POSIX it pins the concurrency-mt-unsafe NOLINT to a single file.
  static const bool on = []() -> bool {
    if (example::demo::safe_getenv("NO_COLOR").has_value()) {
      return false;
    }
    return ::isatty(::fileno(stdout)) != 0;
  }();
  return on;
}

} // namespace example::ui::style

#endif // !defined(_WIN32)
