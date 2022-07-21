add_library(couchbase_backtrace OBJECT backtrace.c)
target_include_directories(couchbase_backtrace PRIVATE ${PROJECT_BINARY_DIR}/generated ../..)
target_link_libraries(couchbase_backtrace PRIVATE ${PLATFORM_LIBRARIES})
set_target_properties(couchbase_backtrace PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(
  couchbase_platform STATIC
        dirutils.cc
        random.cc
        string_hex.cc
        uuid.cc
        base64.cc
        terminate_handler.cc)
set_target_properties(couchbase_platform PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(couchbase_platform PRIVATE ${PROJECT_BINARY_DIR}/generated ../..)
target_link_libraries(couchbase_platform PRIVATE project_options project_warnings couchbase_backtrace)
