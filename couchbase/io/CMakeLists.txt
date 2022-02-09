add_library(couchbase_io STATIC mcbp_message.cxx mcbp_parser.cxx http_parser.cxx)
set_target_properties(couchbase_io PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_dependencies(couchbase_io http_parser)
target_link_libraries(
  couchbase_io
  PRIVATE project_options
          project_warnings
          snappy
          http_parser
          fmt::fmt
          spdlog::spdlog)
target_include_directories(couchbase_io PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/third_party/http_parser)
