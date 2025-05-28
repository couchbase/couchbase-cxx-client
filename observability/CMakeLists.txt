add_library(couchbase_observability OBJECT fmt_log_record_exporter.cxx)
set_target_properties(couchbase_observability PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_observability
  PRIVATE project_options
          project_warnings
          opentelemetry_logs
          spdlog::spdlog)
target_include_directories(couchbase_observability PRIVATE ${PROJECT_BINARY_DIR}/generated ${PROJECT_SOURCE_DIR})
