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

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace couchbase::core
{
/**
 * Tuning knobs for the streaming row engine (row_streamer). These are internal to core — there is
 * no public API to set them yet; sensible static defaults apply unless a core caller (e.g. via
 * cluster_options::streaming) overrides them.
 */
struct row_streamer_options {
  // JSONPointer nesting depth the lexer descends before treating a value as a row.
  std::uint32_t lexer_depth{ 4 };
  // Buffered row bytes above which socket reads pause, and below which they resume (hysteresis).
  std::size_t high_water_bytes{ std::size_t{ 2 } * 1024 * 1024 };
  std::size_t low_water_bytes{ std::size_t{ 512 } * 1024 };
  // Hard ceiling on a single row (or the trailing metadata) before a synthetic error is raised.
  std::size_t max_row_bytes{ std::size_t{ 64 } * 1024 * 1024 };
  // Capacity of the row hand-off channel, in rows. A secondary bound; the byte watermarks above
  // are the primary back-pressure limit.
  std::size_t row_buffer_size{ 100 };
  // Maximum time to wait for the server between socket reads while a read is in flight (an idle
  // timer armed only while pulling, so a legitimately slow consumer is never timed out). Zero
  // disables the timer.
  std::chrono::milliseconds idle_timeout{ 0 };
  // Whether the request being streamed is read-only (idempotent). Mirrors the buffered path's
  // http_command branch: an idle-timeout terminal is reported as unambiguous_timeout for a
  // read-only request (definitely not applied, safe to retry) and as ambiguous_timeout otherwise
  // (a mutating query such as UPDATE ... RETURNING may have partially executed, so a retry layer
  // must not treat it as safe to replay).
  bool is_read_only{ false };
};
} // namespace couchbase::core
