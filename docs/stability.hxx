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
 * @page stability_committed Committed Interfaces
 * @brief List of the committed interfaces.
 *
 * A committed interface is the highest grade of stability, and is the preferred attribute level for consumers of the library.
 *
 * Couchbase tries at best effort to preserve committed interfaces between major versions, and changes to committed interfaces within a
 * major version is highly exceptional. Such exceptions may include situations where the interface may lead to data corruption, security
 * holes etc. Explicitly note that backwards-compatible extensions are always allowed since they don't break old code.
 *
 * @note This is the default interface level for an API, unless the API is specifically marked otherwise.
 */

/**
 * @page stability_volatile Volatile Interfaces
 * @brief List of the volatile interfaces.
 *
 * Types/Methods/Interfaces marked as volatile can change any time and for any reason.
 *
 * They may be volatile for reasons including:
 *
 * * Depends on specific implementation detail within the library which may change in the response.
 * * Depends on specific implementation detail within the server which may change in the response.
 * * Has been introduced as part of a trial phase for the specific feature.
 */

/**
 * @page stability_uncommitted Uncommitted Interfaces
 * @brief List of the uncommitted interfaces.
 *
 * No commitment is made about the interface.
 *
 * It may be changed in incompatible ways and dropped from one release to another. The difference between an uncommitted interface and a
 * @ref stability_volatile "volatile" interface is its maturity and likelihood of being changed. Uncommitted interfaces may mature into @ref
 * stability_committed "committed" interfaces.
 */

/**
 * @page stability_internal Internal Interfaces
 * @brief List of the internal interfaces.
 *
 * This is internal API and may not be relied on at all.
 *
 * Similar to @ref stability_volatile "volatile" interfaces, no promises are made about these kinds of interfaces and they change or may
 * vanish at every point in time. But in addition to that, those interfaces are explicitly marked as being designed for internal use and not
 * for external consumption in the first place.
 *
 * @warning Use at your own risk
 */

/**
 * @page api_stability Interfaces by Stability
 * @brief Indexes of the API grouped by stability.
 *
 * We use interface stability taxonomy to categorize publicly visible API. See the following pages for more details:
 *
 * @subpage stability_committed considered as generally available (GA) API and should be preferred in production applications.
 *
 * @subpage stability_volatile might be changed in the future, and eventually promoted to committed.
 *
 * @subpage stability_uncommitted should not be used in production application, and the library does not give any guarantees on them.
 *
 * @subpage stability_internal like volatile, but they might be used by other project maintained by Couchbase.
 */
