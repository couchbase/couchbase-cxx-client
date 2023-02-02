/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-Present Couchbase, Inc.
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

/*
 * This header defines macros for the various features to help
 * users adopt early features or use conditional compilation to avoid
 * unnecessary or untested code.
 *
 * Feel free to update this header with more macros.
 */

/**
 * couchbase::core::meta::sdk_version() function is available
 */
#define COUCHBASE_CXX_CLIENT_HAS_SDK_SEMVER 1
