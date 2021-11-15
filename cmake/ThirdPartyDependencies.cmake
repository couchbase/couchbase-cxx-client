add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/gsl)

set(TAOCPP_JSON_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TAOCPP_JSON_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/json)

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/spdlog)
set_target_properties(spdlog PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)
set(PEGTL_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(PEGTL_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/snappy)
set_target_properties(snappy PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(HDR_LOG_REQUIRED OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c)
target_compile_options(hdr_histogram_static PRIVATE -Wno-unused-parameter)
set_target_properties(hdr_histogram_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/gsl/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/cxx_function)

add_library(http_parser OBJECT ${PROJECT_SOURCE_DIR}/third_party/http_parser/http_parser.c)
set_target_properties(http_parser PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)
