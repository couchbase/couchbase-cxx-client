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

#pragma once

#include <couchbase/error.hxx>

#include <optional>
#include <string>

namespace example::demo
{

/**
 * Classifier shared by every example flow.  The breaker should open for
 * signs of remote-side trouble (timeouts, temp failures, unavailable
 * services), not for user errors the app could legitimately produce.
 */
[[nodiscard]] auto
is_breaker_failure(const couchbase::error& err) -> bool;

/**
 * Portable @c std::getenv wrapper that returns @c std::nullopt for unset or
 * empty values. Lives in @c demo_common so both @c main.cxx and the
 * platform-specific @c ui_unix.cxx / @c ui_windows.cxx can share the
 * single implementation rather than each rolling their own.
 *
 * On Windows uses @c _dupenv_s; on POSIX uses @c std::getenv (with the
 * concurrency-mt-unsafe NOLINT scoped to this file).
 */
[[nodiscard]] auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>;

} // namespace example::demo
