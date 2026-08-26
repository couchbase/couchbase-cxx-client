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

// What a test file includes to register its cases.

// A stray Catch2 include in a migrated file would otherwise show up as a redefinition warning far
// from the cause, or not at all. Checked before anything else here, so it fires on the include that
// brought Catch2 in rather than on the first macro that collides.
#if defined(TEST_CASE) || defined(CATCH_VERSION_MAJOR)
#error A Catch2 header is included alongside framework/test_registry.hxx. \
  A test file uses one or the other.
#endif

#include "test_runner.hxx"

#include <string>

namespace couchbase::test
{

// The suite name, injected by couchbase_add_test() from the file's path under test/. A
// hand-written string drifts from the file it names the moment the file is renamed, and nothing
// notices.
#if !defined(COUCHBASE_TEST_SUITE_NAME)
#error                                                                                             \
  "COUCHBASE_TEST_SUITE_NAME is missing. This file must be registered with couchbase_add_test()."
#endif
inline const std::string suite_name{ COUCHBASE_TEST_SUITE_NAME };

} // namespace couchbase::test

// The two aggregate fields a test_case begins with: the case name and the function.
//
// Writing the name out separately means a typo compiles, and the string is both the ctest entry and
// the filter key -- so a mistyped one produces a case that cannot be selected by the name anyone
// would guess, and nothing catches it. Here the identifier has to resolve.
//
// Deliberately confined to the registration point. No macro wraps a case body: the cases stay
// ordinary functions that go-to-definition finds and a debugger shows by name.
//
// NAMING RULE. Because the name IS the function's identifier, a case name is always
// [A-Za-z_][A-Za-z0-9_]* -- no spaces, no punctuation, no colons, and lower_snake_case by
// convention rather than by enforcement -- and the
// rest of the toolchain is built on that: cmake/TestFrameworkAddTests.cmake registers the name in a
// CMake list, ctest -R takes it as a pattern, and a developer types it as an argv entry in bash,
// zsh, cmd or PowerShell. None of those needs quoting or escaping while the rule holds.
//
// A Catch2 case being migrated is converted to such an identifier ONCE, at migration, and the
// sentence form is not preserved anywhere: drop the category prefix, since the labels and the
// requirements already say it, and join the remaining words with underscores. So
// "integration: cluster remains usable in a forked child" becomes the function
// cluster_remains_usable_in_a_forked_child. Do not encode the spaces and decode them later, and do
// not add an escaping scheme downstream to carry a name the rule forbids -- rename the case.
//
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CASE(fn) #fn, &fn
