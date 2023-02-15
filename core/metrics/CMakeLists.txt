add_library(couchbase_metrics OBJECT logging_meter.cxx)
set_target_properties(couchbase_metrics PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_metrics
  PRIVATE project_options
          project_warnings
          hdr_histogram_static
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_metrics PRIVATE
        ${PROJECT_BINARY_DIR}/generated
        ${PROJECT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c/src)
