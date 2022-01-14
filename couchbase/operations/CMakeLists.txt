add_subdirectory(management)

add_library(
  couchbase_operations OBJECT
  document_analytics.cxx
  document_append.cxx
  document_decrement.cxx
  document_exists.cxx
  document_get.cxx
  document_get_and_lock.cxx
  document_get_and_touch.cxx
  document_get_projected.cxx
  document_increment.cxx
  document_insert.cxx
  document_lookup_in.cxx
  document_mutate_in.cxx
  document_prepend.cxx
  document_query.cxx
  document_remove.cxx
  document_replace.cxx
  document_search.cxx
  document_touch.cxx
  document_unlock.cxx
  document_upsert.cxx
  document_view.cxx
  http_noop.cxx
  mcbp_noop.cxx)
set_target_properties(couchbase_operations PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_operations
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_operations PRIVATE ${PROJECT_SOURCE_DIR})
