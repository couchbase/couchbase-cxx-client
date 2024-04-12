/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include <core/error_context/analytics.hxx>
#include <core/error_context/http.hxx>
#include <core/error_context/query.hxx>
#include <core/error_context/search.hxx>

#include <couchbase/error.hxx>

namespace couchbase::core::impl
{
error
make_error(const core::error_context::query& core_ctx);

error
make_error(const core::error_context::search& core_ctx);

error
make_error(const core::error_context::analytics& core_ctx);

error
make_error(const core::error_context::http& core_ctx);
} // namespace couchbase::core::impl
