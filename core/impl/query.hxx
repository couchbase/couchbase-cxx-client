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

#include "core/operations/document_query.hxx"

#include <couchbase/query_options.hxx>

namespace couchbase::core::impl
{
core::operations::query_request
build_query_request(std::string statement, std::optional<std::string> query_context, query_options::built options);

query_result
build_result(operations::query_response& resp);

query_error_context
build_context(operations::query_response& resp);
} // namespace couchbase::core::impl
