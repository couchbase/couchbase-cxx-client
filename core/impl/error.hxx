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

#include <couchbase/error.hxx>
#include <couchbase/key_value_error_context.hxx>
#include <couchbase/query_error_context.hxx>
#include <couchbase/subdocument_error_context.hxx>
#include <couchbase/transaction_error_context.hxx>
#include <couchbase/transaction_op_error_context.hxx>

#include <couchbase/transactions/transaction_operation_failed.hxx>

#include "core/error_context/analytics.hxx"
#include "core/error_context/http.hxx"
#include "core/error_context/query.hxx"
#include "core/error_context/search.hxx"
#include "core/transactions/internal/exceptions_internal.hxx"

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

error
make_error(const couchbase::key_value_error_context& core_ctx);

error
make_error(const couchbase::subdocument_error_context& core_ctx);

error
make_error(const couchbase::query_error_context& core_ctx);

error
make_error(const couchbase::transaction_error_context& core_ctx);

error
make_error(const couchbase::transaction_op_error_context& core_ctx);

const couchbase::transactions::transaction_operation_failed&
make_tof(const couchbase::core::transactions::transaction_operation_failed& core_tof);


} // namespace couchbase::core::impl
