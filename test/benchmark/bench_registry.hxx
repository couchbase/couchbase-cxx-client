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

// What a benchmark file includes to register its cases: bench_framework.hxx, the CASE(fn) macro and
// couchbase::test::suite_name, which couchbase_add_benchmark() sets from the file's path.

#if !defined(COUCHBASE_BENCH_SUITE_NAME)
#error                                                                                             \
  "COUCHBASE_BENCH_SUITE_NAME is missing. This file must be registered with couchbase_add_benchmark()."
#endif
#if defined(COUCHBASE_TEST_SUITE_NAME)
#error A benchmark file is not also a test file: it is registered with couchbase_add_benchmark() only.
#endif

// test_registry.hxx provides CASE(fn) and requires a suite name of its own.
#define COUCHBASE_TEST_SUITE_NAME COUCHBASE_BENCH_SUITE_NAME
#include "framework/test_registry.hxx"

#include "bench_framework.hxx"
