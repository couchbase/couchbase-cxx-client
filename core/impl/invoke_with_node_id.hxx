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

#include <couchbase/error.hxx>

#include <utility>

namespace couchbase::core::impl
{
/**
 * Forwards (err, result) to the caller's handler, propagating the node_id
 * carried by the error (which make_error populates for KV contexts) onto
 * the result. Using a plain function template avoids the extra std::function
 * allocation a type-erasing wrapper would introduce for the sole purpose of
 * setting node_id - that extra allocation was the source of the 18
 * "Potential memory leak" reports clang-static-analyzer flagged against the
 * previous wrap_with_node_id helper. The handler is taken by forwarding
 * reference and perfect-forwarded to the invocation so that callers moving
 * an rvalue handler consume it without an intermediate copy.
 */
template<typename Handler, typename Result>
void
invoke_with_node_id(Handler&& handler, couchbase::error err, Result result)
{
  result.node_id(err.node_id());
  std::forward<Handler>(handler)(std::move(err), std::move(result));
}
} // namespace couchbase::core::impl
