/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/platform/uuid.h"
#include "core/protocol/client_opcode.hxx"

#include <spdlog/fmt/bundled/core.h>

#include <cstdint>
#include <string>

namespace couchbase::core::operations
{
/**
 * Build the diagnostic identifier for an mcbp command: the opcode rendered as
 * two lowercase hex digits, a slash, and a random v4 UUID
 * (e.g. "01/3f2a1c9e-...-...", 39 chars).
 *
 * This identifier never travels on the wire; it appears only in log lines,
 * retry traces, and the returned key_value error context. mcbp_command
 * therefore builds it lazily on first use rather than for every operation.
 */
inline auto
make_command_id(protocol::client_opcode opcode) -> std::string
{
  return fmt::format(
    "{:02x}/{}", static_cast<std::uint8_t>(opcode), uuid::to_string(uuid::random()));
}
} // namespace couchbase::core::operations
