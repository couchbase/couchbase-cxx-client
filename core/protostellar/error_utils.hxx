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

#include <couchbase/retry_reason.hxx>

#include <grpcpp/grpcpp.h>

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <system_error>

#include "core/document_id.hxx"
#include "core/error_context/key_value_error_context.hxx"

namespace couchbase::core::protostellar
{

// Whether the operation whose failure is being mapped can have changed server state. This is the
// bit RFC 77 keys the DEADLINE_EXCEEDED mapping on, in its "Other errors" table:
//
//   "Readonly ops: UnambiguousTimeoutException Otherwise: AmbiguousTimeoutException [...] Remember
//    the 'Ambiguous' refers to whether the operation may have mutated state. This needs to be based
//    on a readonly status, not an idempotent status. An upsert op is idempotent but mutates state."
//
// So it is deliberately not idempotency: the two disagree exactly where it matters, and keying on
// idempotency would report a timed-out upsert as unambiguous -- telling the caller a write
// definitely did not happen when it may well have.
//
// It is also not "returns a value to the user". get_and_lock and get_and_touch read a document but
// take a lock / rewrite the expiry as they do, so they are `mutating` here despite reading.
enum class operation_kind {
  read_only,
  mutating,
};

// Map a bare gRPC status code to a couchbase error_code. OK yields a default-constructed
// (success) error_code. Exposed separately from map_status so it can be unit-tested directly.
[[nodiscard]] auto
map_status_code(grpc::StatusCode code, operation_kind kind) -> std::error_code;

// Map a gRPC status to a couchbase error_code. Keyed on the status code, with the typed
// google.rpc.Status details (see error_message) refining FAILED_PRECONDITION, NOT_FOUND, and
// ALREADY_EXISTS into specific KV/management errors.
[[nodiscard]] auto
map_status(const grpc::Status& status, operation_kind kind) -> std::error_code;

// Human-readable message for a failed status: the message from the attached google.rpc.Status
// rich-error details when present, otherwise the plain gRPC status message.
[[nodiscard]] auto
error_message(const grpc::Status& status) -> std::string;

// Build a KV error context from a gRPC status: maps the status code to an error_code and, on
// failure, attaches the server-provided message (see error_message) as the context's
// human-readable explanation so callers see why the gateway rejected the operation.
//
// The retry counters are parameters rather than fixed at zero because only the caller knows them:
// the retry loop lives in cluster_impl and accumulates them across attempts, while this function
// sees one attempt at a time. RFC 77 makes them part of the error context, so a couchbase2
// operation that was retried has to report it the way the classic transport does
// (core/error_context/key_value.cxx carries the same pair for the same reason).
[[nodiscard]] auto
make_error_context(const grpc::Status& status,
                   const document_id& id,
                   operation_kind kind,
                   std::size_t retry_attempts = 0,
                   std::set<couchbase::retry_reason> retry_reasons = {}) -> key_value_error_context;

// Classify a mapped error_code for the couchbase2 retry loop: returns the retry_reason to feed the
// retry strategy, or nullopt when the error is not retryable at the transport. Transport-level
// unavailability (UNAVAILABLE -> temporary_failure) and a locked document (document_locked ->
// key_value_locked, RFC 77) retry; timeouts, cas mismatches, auth, etc. are terminal. Idempotency
// is enforced downstream by the retry strategy, not here.
[[nodiscard]] auto
retry_reason_for(std::error_code ec) -> std::optional<couchbase::retry_reason>;

} // namespace couchbase::core::protostellar
