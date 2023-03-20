add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/gsl)

set(TAOCPP_JSON_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TAOCPP_JSON_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/json)

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/fmt)
set_target_properties(fmt PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/spdlog)
set_target_properties(spdlog PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)
set(PEGTL_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(PEGTL_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/snappy)
set_target_properties(snappy PROPERTIES POSITION_INDEPENDENT_CODE ON)
if(NOT MSVC)
  # https://github.com/google/snappy/pull/156
  target_compile_options(snappy PRIVATE -Wno-sign-compare)
endif()

set(HDR_LOG_REQUIRED OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c)
if(NOT MSVC)
    target_compile_options(hdr_histogram_static PRIVATE -Wno-unused-parameter)
endif()
set_target_properties(hdr_histogram_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/gsl/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/fmt/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/cxx_function)
include_directories(BEFORE SYSTEM ${PROJECT_SOURCE_DIR}/third_party/expected/include)

add_library(http_parser OBJECT ${PROJECT_SOURCE_DIR}/third_party/http_parser/http_parser.c)
set_target_properties(http_parser PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)

add_library(jsonsl OBJECT ${PROJECT_SOURCE_DIR}/third_party/jsonsl/jsonsl.c)
set_target_properties(jsonsl PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)
target_include_directories(jsonsl PUBLIC SYSTEM ${PROJECT_SOURCE_DIR}/third_party/jsonsl)
