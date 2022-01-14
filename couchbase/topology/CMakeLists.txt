add_library(couchbase_topology OBJECT configuration.cxx)
set_target_properties(couchbase_topology PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_topology
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_topology PRIVATE ${PROJECT_SOURCE_DIR})
