cmake_minimum_required(VERSION 3.17)
# node.js uses cmake 3.17
message(STATUS "CMake version ${CMAKE_VERSION}")

set(couchbase_cxx_client_BUILD_NUMBER 0)
if(DEFINED ENV{BUILD_NUMBER})
  set(couchbase_cxx_client_BUILD_NUMBER $ENV{BUILD_NUMBER})
endif()

if(NOT DEFINED COUCHBASE_CXX_CLIENT_MASTER_PROJECT)
  if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(COUCHBASE_CXX_CLIENT_MASTER_PROJECT ON)
  else()
    set(COUCHBASE_CXX_CLIENT_MASTER_PROJECT OFF)
  endif()
endif()

project(
  couchbase_cxx_client
  VERSION "1.0.0.${couchbase_cxx_client_BUILD_NUMBER}"
  LANGUAGES CXX C)
message(STATUS "Couchbase C++ client ${couchbase_cxx_client_VERSION}")

include(cmake/PreventInSourceBuilds.cmake)
include(cmake/StandardProjectSettings.cmake)

# 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)

# 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

include(cmake/BuildTracing.cmake)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

include(cmake/Backtrace.cmake)

if(COUCHBASE_CXX_CLIENT_MASTER_PROJECT)
  set_project_warnings(project_warnings)
  enable_sanitizers(project_options)
endif()

find_package(Threads REQUIRED)

include(cmake/ThirdPartyDependencies.cmake)

option(COUCHBASE_CXX_CLIENT_STATIC_STDLIB "Statically link C++ standard library" FALSE)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if(COUCHBASE_CXX_CLIENT_STATIC_STDLIB)
    target_compile_options(project_options INTERFACE -static-libgcc -static-libstdc++)
    target_link_libraries(project_options INTERFACE -static-libgcc -static-libstdc++)
  endif()
endif()

include(cmake/OpenSSL.cmake)

include(cmake/VersionInfo.cmake)

add_subdirectory(couchbase/meta)
add_subdirectory(couchbase/platform)
add_subdirectory(couchbase/logger)
add_subdirectory(couchbase/crypto)
add_subdirectory(couchbase/sasl)
add_subdirectory(couchbase/topology)
add_subdirectory(couchbase/utils)
add_subdirectory(couchbase/protocol)
add_subdirectory(couchbase/io)
add_subdirectory(couchbase/management)
add_subdirectory(couchbase/operations)
add_subdirectory(couchbase/tracing)
add_subdirectory(couchbase/metrics)

if(COUCHBASE_CXX_CLIENT_BUILD_SHARED)
  add_library(couchbase_cxx_client SHARED couchbase/cluster.cxx couchbase/document_id.cxx couchbase/cluster_options.cxx)
else()
  add_library(couchbase_cxx_client STATIC couchbase/cluster.cxx couchbase/document_id.cxx couchbase/cluster_options.cxx)
endif()
set_target_properties(couchbase_cxx_client PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(couchbase_cxx_client PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(
  couchbase_cxx_client SYSTEM
  PUBLIC ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include
         ${PROJECT_SOURCE_DIR}/third_party/gsl/include
         ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include
         ${PROJECT_SOURCE_DIR}/third_party/json/include
         ${PROJECT_SOURCE_DIR}/third_party/cxx_function
         ${PROJECT_SOURCE_DIR}/third_party/fmt/include
         ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
target_link_libraries(couchbase_cxx_client PRIVATE project_options project_warnings)

include(cmake/DetectStandardFilesystem.cmake)
couchbase_cxx_check_filesystem(couchbase_cxx_client "filesystem" "std::filesystem" "stdc++fs;c++fs" STD_FILESYSTEM)
if(STD_FILESYSTEM)
  message(STATUS "Using std::filesystem")
else()
  message(FATAL_ERROR "Couchbase C++ Client requires C++17, including an implementation of std::filesystem.")
endif()

target_link_libraries(
  couchbase_cxx_client
  PUBLIC fmt::fmt
         spdlog::spdlog
         couchbase_backtrace
         couchbase_logger
         couchbase_platform
         couchbase_io
         couchbase_meta
         couchbase_crypto
         couchbase_sasl
         couchbase_topology
         couchbase_utils
         couchbase_protocol
         couchbase_management
         couchbase_operations
         couchbase_operations_management
         couchbase_tracing
         couchbase_metrics)
if(NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  target_link_libraries(couchbase_cxx_client PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

option(COUCHBASE_CXX_CLIENT_BUILD_TESTS "Build test programs" TRUE)
if(COUCHBASE_CXX_CLIENT_BUILD_TESTS)
  include(cmake/Testing.cmake)
endif()
