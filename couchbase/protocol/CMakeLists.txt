add_library(
  couchbase_protocol OBJECT
  client_request.cxx
  client_response.cxx
  cmd_append.cxx
  cmd_cluster_map_change_notification.cxx
  cmd_decrement.cxx
  cmd_exists.cxx
  cmd_get.cxx
  cmd_get_and_lock.cxx
  cmd_get_and_touch.cxx
  cmd_get_cluster_config.cxx
  cmd_get_collection_id.cxx
  cmd_get_collections_manifest.cxx
  cmd_get_error_map.cxx
  cmd_get_meta.cxx
  cmd_hello.cxx
  cmd_increment.cxx
  cmd_insert.cxx
  cmd_lookup_in.cxx
  cmd_mutate_in.cxx
  cmd_noop.cxx
  cmd_prepend.cxx
  cmd_remove.cxx
  cmd_replace.cxx
  cmd_sasl_auth.cxx
  cmd_sasl_list_mechs.cxx
  cmd_sasl_step.cxx
  cmd_select_bucket.cxx
  cmd_touch.cxx
  cmd_unlock.cxx
  cmd_upsert.cxx
  status.cxx)
set_target_properties(couchbase_protocol PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_protocol
  PRIVATE project_options
          project_warnings
          snappy
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_protocol PRIVATE ${PROJECT_SOURCE_DIR})
