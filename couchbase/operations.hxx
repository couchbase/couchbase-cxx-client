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

#include <couchbase/document_id.hxx>
#include <couchbase/timeout_defaults.hxx>

#include <couchbase/operations/document_get.hxx>
#include <couchbase/operations/document_get_and_lock.hxx>
#include <couchbase/operations/document_get_and_touch.hxx>
#include <couchbase/operations/document_insert.hxx>
#include <couchbase/operations/document_upsert.hxx>
#include <couchbase/operations/document_replace.hxx>
#include <couchbase/operations/document_append.hxx>
#include <couchbase/operations/document_prepend.hxx>
#include <couchbase/operations/document_remove.hxx>
#include <couchbase/operations/document_lookup_in.hxx>
#include <couchbase/operations/document_mutate_in.hxx>
#include <couchbase/operations/document_touch.hxx>
#include <couchbase/operations/document_exists.hxx>
#include <couchbase/operations/document_unlock.hxx>
#include <couchbase/operations/document_increment.hxx>
#include <couchbase/operations/document_decrement.hxx>
#include <couchbase/operations/document_get_projected.hxx>

#include <couchbase/operations/mcbp_noop.hxx>
#include <couchbase/operations/http_noop.hxx>

#include <couchbase/operations/document_query.hxx>
#include <couchbase/operations/document_search.hxx>
#include <couchbase/operations/document_analytics.hxx>
#include <couchbase/operations/document_view.hxx>

#include <couchbase/operations/bucket_get_all.hxx>
#include <couchbase/operations/bucket_get.hxx>
#include <couchbase/operations/bucket_drop.hxx>
#include <couchbase/operations/bucket_flush.hxx>
#include <couchbase/operations/bucket_create.hxx>
#include <couchbase/operations/bucket_update.hxx>

#include <couchbase/operations/role_get_all.hxx>
#include <couchbase/operations/user_get_all.hxx>
#include <couchbase/operations/user_get.hxx>
#include <couchbase/operations/user_drop.hxx>
#include <couchbase/operations/user_upsert.hxx>
#include <couchbase/operations/group_get_all.hxx>
#include <couchbase/operations/group_get.hxx>
#include <couchbase/operations/group_drop.hxx>
#include <couchbase/operations/group_upsert.hxx>

#include <couchbase/operations/scope_get_all.hxx>
#include <couchbase/operations/scope_create.hxx>
#include <couchbase/operations/scope_drop.hxx>
#include <couchbase/operations/collection_create.hxx>
#include <couchbase/operations/collection_drop.hxx>

#include <couchbase/operations/cluster_developer_preview_enable.hxx>
#include <couchbase/operations/collections_manifest_get.hxx>

#include <couchbase/operations/query_index_get_all.hxx>
#include <couchbase/operations/query_index_drop.hxx>
#include <couchbase/operations/query_index_create.hxx>
#include <couchbase/operations/query_index_build_deferred.hxx>

#include <couchbase/operations/search_get_stats.hxx>
#include <couchbase/operations/search_index_get_all.hxx>
#include <couchbase/operations/search_index_get.hxx>
#include <couchbase/operations/search_index_get_stats.hxx>
#include <couchbase/operations/search_index_get_documents_count.hxx>
#include <couchbase/operations/search_index_upsert.hxx>
#include <couchbase/operations/search_index_drop.hxx>
#include <couchbase/operations/search_index_control_ingest.hxx>
#include <couchbase/operations/search_index_control_query.hxx>
#include <couchbase/operations/search_index_control_plan_freeze.hxx>
#include <couchbase/operations/search_index_analyze_document.hxx>

#include <couchbase/operations/analytics_get_pending_mutations.hxx>
#include <couchbase/operations/analytics_dataverse_create.hxx>
#include <couchbase/operations/analytics_dataverse_drop.hxx>
#include <couchbase/operations/analytics_dataset_create.hxx>
#include <couchbase/operations/analytics_dataset_drop.hxx>
#include <couchbase/operations/analytics_dataset_get_all.hxx>
#include <couchbase/operations/analytics_index_get_all.hxx>
#include <couchbase/operations/analytics_index_create.hxx>
#include <couchbase/operations/analytics_index_drop.hxx>
#include <couchbase/operations/analytics_link_connect.hxx>
#include <couchbase/operations/analytics_link_disconnect.hxx>
#include <couchbase/operations/analytics_link_create.hxx>
#include <couchbase/operations/analytics_link_replace.hxx>
#include <couchbase/operations/analytics_link_drop.hxx>
#include <couchbase/operations/analytics_link_get_all.hxx>

#include <couchbase/operations/view_index_get_all.hxx>
#include <couchbase/operations/view_index_get.hxx>
#include <couchbase/operations/view_index_drop.hxx>
#include <couchbase/operations/view_index_upsert.hxx>

#include <couchbase/io/mcbp_command.hxx>
