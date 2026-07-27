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

// The one place the generated couchbase.query.v1 headers are included. Include this instead of
// <couchbase/query/v1/query.pb.h> or <couchbase/query/v1/query.grpc.pb.h>.
//
// protobuf emits a class-scoped alias for every enum value ("static constexpr Status STATUS_TIMEOUT
// = ..."), and the Windows SDK spells much of that same STATUS_* namespace as object-like macros:
// <winnt.h> defines STATUS_TIMEOUT as ((DWORD)0x00000102L), and asio and gRPC reach <winnt.h>
// transitively. On MSVC the alias then expands into a parenthesised constant and the generated
// header does not parse. No other compiler is affected, so the Linux and macOS legs cannot catch
// it.
//
// The whole QueryResponse.MetaData.Status family is suppressed, not just the one name that collides
// today: <ntstatus.h> covers far more of the STATUS_* namespace than <winnt.h> does (STATUS_SUCCESS
// among it) and any dependency may start pulling it in. push_macro/pop_macro keeps the suppression
// scoped to the includes below, so a translation unit that includes this header does not silently
// lose a macro it uses elsewhere -- which a bare #undef in a header would do. Pushing and popping a
// macro that is not defined is a no-op, so this needs no _MSC_VER guard.
//
// When the schema pin in cmake/Protostellar.cmake moves, bin/check-proto-macro-collisions reports
// generated aliases that collide with a Windows macro, including families other than this one.

#pragma push_macro("STATUS_ABORTED")
#pragma push_macro("STATUS_CLOSED")
#pragma push_macro("STATUS_COMPLETED")
#pragma push_macro("STATUS_ERRORS")
#pragma push_macro("STATUS_FATAL")
#pragma push_macro("STATUS_RUNNING")
#pragma push_macro("STATUS_STOPPED")
#pragma push_macro("STATUS_SUCCESS")
#pragma push_macro("STATUS_TIMEOUT")
#pragma push_macro("STATUS_UNKNOWN")
#undef STATUS_ABORTED
#undef STATUS_CLOSED
#undef STATUS_COMPLETED
#undef STATUS_ERRORS
#undef STATUS_FATAL
#undef STATUS_RUNNING
#undef STATUS_STOPPED
#undef STATUS_SUCCESS
#undef STATUS_TIMEOUT
#undef STATUS_UNKNOWN

#include <couchbase/query/v1/query.grpc.pb.h>
#include <couchbase/query/v1/query.pb.h>

#pragma pop_macro("STATUS_UNKNOWN")
#pragma pop_macro("STATUS_TIMEOUT")
#pragma pop_macro("STATUS_SUCCESS")
#pragma pop_macro("STATUS_STOPPED")
#pragma pop_macro("STATUS_RUNNING")
#pragma pop_macro("STATUS_FATAL")
#pragma pop_macro("STATUS_ERRORS")
#pragma pop_macro("STATUS_COMPLETED")
#pragma pop_macro("STATUS_CLOSED")
#pragma pop_macro("STATUS_ABORTED")
