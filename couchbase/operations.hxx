/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/operations/document_append.hxx>
#include <couchbase/operations/document_decrement.hxx>
#include <couchbase/operations/document_exists.hxx>
#include <couchbase/operations/document_get.hxx>
#include <couchbase/operations/document_get_and_lock.hxx>
#include <couchbase/operations/document_get_and_touch.hxx>
#include <couchbase/operations/document_get_projected.hxx>
#include <couchbase/operations/document_increment.hxx>
#include <couchbase/operations/document_insert.hxx>
#include <couchbase/operations/document_lookup_in.hxx>
#include <couchbase/operations/document_mutate_in.hxx>
#include <couchbase/operations/document_prepend.hxx>
#include <couchbase/operations/document_remove.hxx>
#include <couchbase/operations/document_replace.hxx>
#include <couchbase/operations/document_touch.hxx>
#include <couchbase/operations/document_unlock.hxx>
#include <couchbase/operations/document_upsert.hxx>

#include <couchbase/operations/mcbp_noop.hxx>
#include <couchbase/operations/http_noop.hxx>

#include <couchbase/operations/document_analytics.hxx>
#include <couchbase/operations/document_query.hxx>
#include <couchbase/operations/document_search.hxx>
#include <couchbase/operations/document_view.hxx>
