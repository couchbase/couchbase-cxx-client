/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <system_error>

namespace couchbase::core::io
{
// Which timeout a whole-stream deadline reports when it fires. The two are not interchangeable: a
// retry layer may replay a request that definitely did not apply, and must not replay one that may
// have partially executed. A deadline has no other terminal, so no other value exists.
enum class deadline_terminal {
  // The request is read-only, so the server cannot have applied it.
  unambiguous,
  // The request may have mutated state before the deadline closed the body.
  ambiguous,
};

// Whether a set_deadline call installed anything.
enum class deadline_state {
  // Installed. The body closes at the deadline unless something ends it first.
  armed,
  // Nothing was installed, and nothing needs to be: the body has already ended, by expiry, by
  // cancellation, or by having been read to completion. A replayed buffered response starts here.
  //
  // This is not end-of-stream. A consumer pulling rows reaches it before its last row, because
  // the body ends when its bytes are read and the rows parsed from them are handed out
  // afterwards. A caller arming per operation sees it for the final rows of every stream.
  body_already_ended,
};

// The error_code `terminal` is reported as.
[[nodiscard]] auto
terminal_error_code(deadline_terminal terminal) -> std::error_code;
} // namespace couchbase::core::io
