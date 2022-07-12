/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

/**
 * @mainpage
 *
 * @note You may read about related Couchbase software at https://docs.couchbase.com/
 *
 * This is API reference for C++ library for connecting to a Couchbase Server and performing data operations and queries.
 *
 * We use interface stability taxonomy to categorize publicly visible API. See the following pages for more details:
 *
 * * @ref stability_committed "Committed" interfaces considered as generally available (GA) API and should be preferred in production
 * applications.
 * * @ref stability_uncommitted "Uncommitted" interfaces might be changed in the future, and eventually promoted to committed.
 * * @ref stability_volatile "Volatile" interfaces should not be used in production application, and the library does not give any
 * guarantees on them.
 * * @ref stability_internal "Internal" interfaces like volatile, but they might be used by other project maintained by Couchbase.
 */
