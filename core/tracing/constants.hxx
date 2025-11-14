/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
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

#include "core/protocol/client_opcode.hxx"
#include "core/service_type.hxx"

#include <gsl/assert>
#include <string_view>

namespace couchbase::core::tracing
{
namespace operation
{
constexpr auto step_dispatch = "dispatch_to_server";
constexpr auto step_request_encoding = "request_encoding";

constexpr auto query = "query";
constexpr auto analytics = "analytics";
constexpr auto search = "search";
constexpr auto views = "views";
constexpr auto manager = "manager";
constexpr auto eventing = "eventing";

// TODO(DC): Remove these
constexpr auto manager_analytics = "manager_analytics";
constexpr auto manager_query = "manager_query";
constexpr auto manager_buckets = "manager_buckets";
constexpr auto manager_collections = "manager_collections";
constexpr auto manager_search = "manager_search";
constexpr auto manager_users = "manager_users";
constexpr auto manager_views = "manager_views";

// KV operations
constexpr auto mcbp_get = "get";
constexpr auto mcbp_get_replica = "get_replica";
constexpr auto mcbp_upsert = "upsert";
constexpr auto mcbp_replace = "replace";
constexpr auto mcbp_insert = "insert";
constexpr auto mcbp_remove = "remove";
constexpr auto mcbp_get_and_lock = "get_and_lock";
constexpr auto mcbp_get_and_touch = "get_and_touch";
constexpr auto mcbp_exists = "exists";
constexpr auto mcbp_touch = "touch";
constexpr auto mcbp_unlock = "unlock";
constexpr auto mcbp_lookup_in = "lookup_in";
constexpr auto mcbp_lookup_in_replica = "lookup_in_replica";
constexpr auto mcbp_mutate_in = "mutate_in";
constexpr auto mcbp_append = "append";
constexpr auto mcbp_prepend = "prepend";
constexpr auto mcbp_increment = "increment";
constexpr auto mcbp_decrement = "decrement";
constexpr auto mcbp_range_scan_create = "range_scan_create";
constexpr auto mcbp_range_scan_continue = "range_scan_continue";
constexpr auto mcbp_range_scan_cancel = "range_scan_cancel";
constexpr auto mcbp_internal = "internal";

// Multi-command operations
constexpr auto mcbp_get_all_replicas = "get_all_replicas";
constexpr auto mcbp_get_any_replica = "get_any_replica";
constexpr auto mcbp_lookup_in_all_replicas = "lookup_in_all_replicas";
constexpr auto mcbp_lookup_in_any_replica = "lookup_in_any_replica";
constexpr auto mcbp_scan = "scan";
constexpr auto ping = "ping";
constexpr auto diagnostics = "diagnostics";

// Collection management operations
constexpr auto mgr_collections_create_collection = "manager_collections_create_collection";
constexpr auto mgr_collections_drop_collection = "manager_collections_drop_collection";
constexpr auto mgr_collections_update_collection = "manager_collections_update_collection";
constexpr auto mgr_collections_create_scope = "manager_collections_create_scope";
constexpr auto mgr_collections_drop_scope = "manager_collections_drop_scope";
constexpr auto mgr_collections_get_all_scopes = "manager_collections_get_all_scopes";

// Bucket management operations
constexpr auto mgr_buckets_get_bucket = "manager_buckets_get_bucket";
constexpr auto mgr_buckets_get_all_buckets = "manager_buckets_get_all_buckets";
constexpr auto mgr_buckets_create_bucket = "manager_buckets_create_bucket";
constexpr auto mgr_buckets_update_bucket = "manager_buckets_update_bucket";
constexpr auto mgr_buckets_drop_bucket = "manager_buckets_drop_bucket";
constexpr auto mgr_buckets_flush_bucket = "manager_buckets_flush_bucket";

// Query index management operations
constexpr auto mgr_query_build_deferred_indexes = "manager_query_build_deferred_indexes";
constexpr auto mgr_query_create_index = "manager_query_create_index";
constexpr auto mgr_query_create_primary_index = "manager_query_create_primary_index";
constexpr auto mgr_query_drop_index = "manager_query_drop_index";
constexpr auto mgr_query_drop_primary_index = "manager_query_drop_primary_index";
constexpr auto mgr_query_get_all_indexes = "manager_query_get_all_indexes";
constexpr auto mgr_query_watch_indexes = "manager_query_watch_indexes";
constexpr auto mgr_query_get_all_deferred_indexes = "manager_query_get_all_deferred_indexes";
constexpr auto mgr_query_build_indexes = "manager_query_build_indexes";

// Search index management operations
constexpr auto mgr_search_get_index = "manager_search_get_index";
constexpr auto mgr_search_get_all_indexes = "manager_search_get_all_indexes";
constexpr auto mgr_search_upsert_index = "manager_search_upsert_index";
constexpr auto mgr_search_drop_index = "manager_search_drop_index";
constexpr auto mgr_search_get_indexed_documents_count =
  "manager_search_get_indexed_documents_count";
constexpr auto mgr_search_pause_ingest = "manager_search_pause_ingest";
constexpr auto mgr_search_resume_ingest = "manager_search_resume_ingest";
constexpr auto mgr_search_allow_querying = "manager_search_allow_querying";
constexpr auto mgr_search_disallow_querying = "manager_search_disallow_querying";
constexpr auto mgr_search_freeze_plan = "manager_search_freeze_plan";
constexpr auto mgr_search_unfreeze_plan = "manager_search_unfreeze_plan";
constexpr auto mgr_search_analyze_document = "manager_search_analyze_document";

// Analytics index management operations
constexpr auto mgr_analytics_create_dataverse = "manager_analytics_create_dataverse";
constexpr auto mgr_analytics_drop_dataverse = "manager_analytics_drop_dataverse";
constexpr auto mgr_analytics_create_dataset = "manager_analytics_create_dataset";
constexpr auto mgr_analytics_drop_dataset = "manager_analytics_drop_dataset";
constexpr auto mgr_analytics_get_all_datasets = "manager_analytics_get_all_datasets";
constexpr auto mgr_analytics_create_index = "manager_analytics_create_index";
constexpr auto mgr_analytics_drop_index = "manager_analytics_drop_index";
constexpr auto mgr_analytics_get_all_indexes = "manager_analytics_get_all_indexes";
constexpr auto mgr_analytics_connect_link = "manager_analytics_connect_link";
constexpr auto mgr_analytics_disconnect_link = "manager_analytics_disconnect_link";
constexpr auto mgr_analytics_get_pending_mutations = "manager_analytics_get_pending_mutations";
constexpr auto mgr_analytics_create_link = "manager_analytics_create_link";
constexpr auto mgr_analytics_replace_link = "manager_analytics_replace_link";
constexpr auto mgr_analytics_drop_link = "manager_analytics_drop_link";
constexpr auto mgr_analytics_get_links = "manager_analytics_get_links";
} // namespace operation

namespace attributes
{
// Attributes present on all spans
namespace common
{
constexpr auto system = "db.system.name";
constexpr auto cluster_name = "couchbase.cluster.name";
constexpr auto cluster_uuid = "couchbase.cluster.uuid";
} // namespace common

// Operation-level attributes
namespace op
{
constexpr auto service = "couchbase.service";
constexpr auto retry_count = "couchbase.retries";
constexpr auto durability_level = "couchbase.durability";
constexpr auto bucket_name = "db.namespace";
constexpr auto scope_name = "couchbase.scope.name";
constexpr auto collection_name = "couchbase.collection.name";
constexpr auto query_statement = "db.query.text";
constexpr auto operation_name = "db.operation.name";
} // namespace op

// Dispatch-level attributes
namespace dispatch
{
constexpr auto server_duration = "couchbase.server_duration";
constexpr auto local_id = "couchbase.local_id";
constexpr auto server_address = "server.address";
constexpr auto server_port = "server.port";
constexpr auto peer_address = "network.peer.address";
constexpr auto peer_port = "network.peer.port";
constexpr auto network_transport = "network.transport";
constexpr auto operation_id = "couchbase.operation_id";
} // namespace dispatch
} // namespace attributes

namespace service
{
constexpr auto key_value = "kv";
constexpr auto query = "query";
constexpr auto search = "search";
constexpr auto view = "views";
constexpr auto analytics = "analytics";
constexpr auto management = "management";
constexpr auto eventing = "eventing";
constexpr auto transactions = "transactions";
} // namespace service

constexpr auto
span_name_for_http_service(service_type type)
{
  switch (type) {
    case service_type::query:
      return operation::query;

    case service_type::analytics:
      return operation::analytics;

    case service_type::search:
      return operation::search;

    case service_type::view:
      return operation::views;

    case service_type::management:
      return operation::manager;

    case service_type::eventing:
      return operation::eventing;

    case service_type::key_value:
      return "unexpected_http_service";
  }
  return "unknown_http_service";
}

constexpr auto
service_name_for_http_service(service_type type)
{
  switch (type) {
    case service_type::query:
      return service::query;

    case service_type::analytics:
      return service::analytics;

    case service_type::search:
      return service::search;

    case service_type::view:
      return service::view;

    case service_type::management:
      return service::management;

    case service_type::eventing:
      return service::eventing;

    case service_type::key_value:
      return "unexpected_http_service";
  }
  return "unknown_http_service";
}

constexpr auto
span_name_for_mcbp_command(protocol::client_opcode opcode)
{
  switch (opcode) {
    case protocol::client_opcode::get:
      return operation::mcbp_get;

    case protocol::client_opcode::upsert:
      return operation::mcbp_upsert;

    case protocol::client_opcode::insert:
      return operation::mcbp_insert;

    case protocol::client_opcode::replace:
      return operation::mcbp_replace;

    case protocol::client_opcode::remove:
      return operation::mcbp_remove;

    case protocol::client_opcode::increment:
      return operation::mcbp_increment;

    case protocol::client_opcode::decrement:
      return operation::mcbp_decrement;

    case protocol::client_opcode::append:
      return operation::mcbp_append;

    case protocol::client_opcode::prepend:
      return operation::mcbp_prepend;

    case protocol::client_opcode::touch:
      return operation::mcbp_touch;

    case protocol::client_opcode::get_and_touch:
      return operation::mcbp_get_and_touch;

    case protocol::client_opcode::get_replica:
      return operation::mcbp_get_replica;

    case protocol::client_opcode::get_and_lock:
      return operation::mcbp_get_and_lock;

    case protocol::client_opcode::unlock:
      return operation::mcbp_unlock;

    case protocol::client_opcode::subdoc_multi_lookup:
      return operation::mcbp_lookup_in;

    case protocol::client_opcode::subdoc_multi_mutation:
      return operation::mcbp_mutate_in;

    case protocol::client_opcode::observe:
      return operation::mcbp_exists;

    case protocol::client_opcode::range_scan_create:
      return operation::mcbp_range_scan_create;

    case protocol::client_opcode::range_scan_continue:
      return operation::mcbp_range_scan_continue;

    case protocol::client_opcode::range_scan_cancel:
      return operation::mcbp_range_scan_cancel;

    case protocol::client_opcode::noop:
    case protocol::client_opcode::version:
    case protocol::client_opcode::stat:
    case protocol::client_opcode::verbosity:
    case protocol::client_opcode::hello:
    case protocol::client_opcode::sasl_list_mechs:
    case protocol::client_opcode::sasl_auth:
    case protocol::client_opcode::sasl_step:
    case protocol::client_opcode::get_all_vbucket_seqnos:
    case protocol::client_opcode::dcp_open:
    case protocol::client_opcode::dcp_add_stream:
    case protocol::client_opcode::dcp_close_stream:
    case protocol::client_opcode::dcp_stream_request:
    case protocol::client_opcode::dcp_get_failover_log:
    case protocol::client_opcode::dcp_stream_end:
    case protocol::client_opcode::dcp_snapshot_marker:
    case protocol::client_opcode::dcp_mutation:
    case protocol::client_opcode::dcp_deletion:
    case protocol::client_opcode::dcp_expiration:
    case protocol::client_opcode::dcp_set_vbucket_state:
    case protocol::client_opcode::dcp_noop:
    case protocol::client_opcode::dcp_buffer_acknowledgement:
    case protocol::client_opcode::dcp_control:
    case protocol::client_opcode::dcp_system_event:
    case protocol::client_opcode::dcp_prepare:
    case protocol::client_opcode::dcp_seqno_acknowledged:
    case protocol::client_opcode::dcp_commit:
    case protocol::client_opcode::dcp_abort:
    case protocol::client_opcode::dcp_seqno_advanced:
    case protocol::client_opcode::dcp_oso_snapshot:
    case protocol::client_opcode::list_buckets:
    case protocol::client_opcode::select_bucket:
    case protocol::client_opcode::observe_seqno:
    case protocol::client_opcode::evict_key:
    case protocol::client_opcode::get_failover_log:
    case protocol::client_opcode::last_closed_checkpoint:
    case protocol::client_opcode::get_meta:
    case protocol::client_opcode::upsert_with_meta:
    case protocol::client_opcode::insert_with_meta:
    case protocol::client_opcode::remove_with_meta:
    case protocol::client_opcode::create_checkpoint:
    case protocol::client_opcode::checkpoint_persistence:
    case protocol::client_opcode::return_meta:
    case protocol::client_opcode::get_random_key:
    case protocol::client_opcode::seqno_persistence:
    case protocol::client_opcode::get_keys:
    case protocol::client_opcode::set_collections_manifest:
    case protocol::client_opcode::get_collections_manifest:
    case protocol::client_opcode::get_collection_id:
    case protocol::client_opcode::get_scope_id:
    case protocol::client_opcode::get_cluster_config:
    case protocol::client_opcode::get_error_map:
      return operation::mcbp_internal;

    case protocol::client_opcode::invalid:
      return "invalid_command";
  }
  return "unknown_command";
}

} // namespace couchbase::core::tracing
