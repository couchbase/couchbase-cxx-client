add_library(
  couchbase_utils STATIC
  connection_string.cxx
  url_codec.cxx
  json.cxx
  duration_parser.cxx
  json_streaming_lexer.cxx)
set_target_properties(couchbase_utils PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_dependencies(couchbase_utils jsonsl)
target_link_libraries(
  couchbase_utils
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog
          jsonsl)
target_include_directories(couchbase_utils PRIVATE ${PROJECT_SOURCE_DIR})
