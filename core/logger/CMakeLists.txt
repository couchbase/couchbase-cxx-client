add_library(couchbase_logger STATIC logger.cxx custom_rotating_file_sink.cxx)
set_target_properties(couchbase_logger PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_logger
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_logger PRIVATE ../..)
