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

#include <optional>
#include <string>

namespace couchbase::core::io
{
/**
 * Decide whether a freshly received cluster map belongs to this session and must
 * therefore be accepted.
 *
 * A session's bucket binding is fixed for its entire lifetime (set once in the
 * constructor, never reassigned), so the rule reduces to a single invariant:
 * the bucket identity carried by the configuration must match the bucket identity
 * of the session.
 *
 *   - a cluster-level (bucket-less) session accepts only cluster-level configs;
 *   - a bucket-level session accepts only configs for *its own* bucket.
 *
 * @param config_bucket   bucket name embedded in the configuration payload
 *                        (std::nullopt  =>  cluster-level / GCCCP config)
 * @param session_bucket  bucket this session is bound to
 *                        (std::nullopt  =>  cluster-level / GCCCP session)
 * @return true if the configuration must be accepted, false if it must be ignored
 */
[[nodiscard]] inline auto
configuration_belongs_to_session(const std::optional<std::string>& config_bucket,
                                 const std::optional<std::string>& session_bucket) -> bool
{
  // Stage 1 — cluster-level config for a cluster-level session.
  // Neither side names a bucket: this is a GCCCP (cluster) map and the session is
  // not bound to any bucket, so the config belongs here. Accept.
  if (!config_bucket.has_value() && !session_bucket.has_value()) {
    return true;
  }

  // Stage 2 — cardinality mismatch: exactly one side names a bucket. Reject.
  // Either a bucket-less config arrived on a bucket-bound session, or a bucket
  // config arrived on a cluster-level session. In both cases the config does not
  // belong to this session.
  if (config_bucket.has_value() != session_bucket.has_value()) {
    return false;
  }

  // Stage 3 — both sides name a bucket: accept only if it is the *same* bucket.
  // Reaching here guarantees both optionals are engaged, so value() is safe.
  // Guards against applying another bucket's map to this session.
  return config_bucket.value() == session_bucket.value();
}
} // namespace couchbase::core::io
