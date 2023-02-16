add_library(couchbase_tracing OBJECT threshold_logging_tracer.cxx)
set_target_properties(couchbase_tracing PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_tracing
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_tracing PRIVATE
        ${PROJECT_BINARY_DIR}/generated
        ${PROJECT_SOURCE_DIR})
