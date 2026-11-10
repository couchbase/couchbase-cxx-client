/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026 Couchbase, Inc.
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

#include <functional>

namespace couchbase::core::logger::unit_test_api
{
// Runs `callback` immediately after scoped_registry_lock unlocks registry_mutex. An empty
// function clears the hook.
void
set_after_registry_unlock(std::function<void()> callback);

// Runs `callback` in get_file_logger_for_log() or get_protocol_logger_for_log() after capturing
// the active logger and pool snapshot. An empty function clears the hook.
void
set_after_log_snapshot(std::function<void()> callback);

// Runs `callback` in read_logger() or read_logger_for_log() between matching cache.version and
// locking cache.logger. An empty function clears the hook.
void
set_after_cached_version_match(std::function<void()> callback);
} // namespace couchbase::core::logger::unit_test_api
