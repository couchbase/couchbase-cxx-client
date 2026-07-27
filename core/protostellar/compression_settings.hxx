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

#include <cstddef>

namespace couchbase::core::protostellar::kv
{

// The user's compression knobs. Defaults mirror the classic SDK (enabled; snappy applied only when
// the value is at least min_size and shrinks below min_ratio).
struct compression_settings {
  bool enabled{ true };
  std::size_t min_size{ 32 };
  double min_ratio{ 0.83 };
};

} // namespace couchbase::core::protostellar::kv
