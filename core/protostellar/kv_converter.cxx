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

// snappy compression for the couchbase2 KV converter, kept out of kv_converter.hxx so the header
// (included by white-box tests) does not leak a dependency on <snappy.h> / the snappy library.

#include "core/protostellar/kv_converter.hxx"

#include "core/protostellar/dispatcher.hxx"

#include <snappy.h>

#include <cstddef>

namespace couchbase::core::protostellar::kv
{
auto
maybe_compress(const std::vector<std::byte>& value, const compression_settings& compression)
  -> std::optional<std::string>
{
  if (!compression.enabled || value.size() < compression.min_size) {
    return std::nullopt;
  }
  const auto raw = utils::to_string(value);
  std::string compressed;
  snappy::Compress(raw.data(), raw.size(), &compressed);
  // Only worth sending compressed when it shrinks to at most min_ratio of the original; a value
  // landing exactly on the ratio is sent compressed, as it is in the other SDKs.
  if (static_cast<double>(compressed.size()) <=
      static_cast<double>(raw.size()) * compression.min_ratio) {
    return compressed;
  }
  return std::nullopt;
}

auto
snappy_uncompress(const std::string& compressed) -> std::optional<std::string>
{
  // snappy::Uncompress resizes its output to the length declared in the frame header before it
  // validates the body, so a few bytes can ask for a multi-gigabyte allocation. Read the declared
  // length first and refuse anything the transport could not have carried: the resulting
  // std::bad_alloc would be thrown inside a gRPC completion and take the process with it.
  std::size_t length{};
  if (!snappy::GetUncompressedLength(compressed.data(), compressed.size(), &length)) {
    return std::nullopt;
  }
  if (length > max_receive_message_size) {
    return std::nullopt;
  }
  std::string uncompressed;
  if (snappy::Uncompress(compressed.data(), compressed.size(), &uncompressed)) {
    return uncompressed;
  }
  return std::nullopt;
}
} // namespace couchbase::core::protostellar::kv
