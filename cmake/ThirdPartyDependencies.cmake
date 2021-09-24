add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/gsl)

set(TAOCPP_JSON_BUILD_TESTS OFF)
set(TAOCPP_JSON_BUILD_EXAMPLES OFF)
add_subdirectory(third_party/json)

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/spdlog)

set(SNAPPY_BUILD_TESTS OFF)
set(SNAPPY_INSTALL OFF)
set(PEGTL_BUILD_TESTS OFF)
set(PEGTL_BUILD_EXAMPLES OFF)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/snappy)

set(HDR_LOG_REQUIRED OFF)
set(HDR_HISTOGRAM_BUILD_SHARED OFF)
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c/src)

include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/gsl/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/http_parser)

add_library(http_parser OBJECT ${PROJECT_SOURCE_DIR}/third_party/http_parser/http_parser.c)
set_target_properties(http_parser PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)
