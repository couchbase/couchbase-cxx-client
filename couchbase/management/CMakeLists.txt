add_library(
  couchbase_management OBJECT
  analytics_link_azure_blob_external.cxx
  analytics_link_couchbase_remote.cxx
  analytics_link_s3_external.cxx)
set_target_properties(couchbase_management PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_management
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_management PRIVATE ${PROJECT_SOURCE_DIR})
