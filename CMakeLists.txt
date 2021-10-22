cmake_minimum_required(VERSION 3.17)
# node.js uses cmake 3.17

set(couchbase_cxx_client_BUILD_NUMBER 0)
if(DEFINED ENV{BUILD_NUMBER})
  set(couchbase_cxx_client_BUILD_NUMBER $ENV{BUILD_NUMBER})
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
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

include(cmake/Backtrace.cmake)

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

add_library(
  platform OBJECT
  couchbase/platform/string_hex.cc
  couchbase/platform/uuid.cc
  couchbase/platform/random.cc
  couchbase/platform/base64.cc
  couchbase/platform/backtrace.c
  couchbase/platform/terminate_handler.cc)
target_include_directories(platform PRIVATE ${PROJECT_BINARY_DIR}/generated ${PROJECT_SOURCE_DIR})
target_link_libraries(platform PRIVATE ${PLATFORM_LIBRARIES})

add_library(cbcrypto OBJECT couchbase/cbcrypto/cbcrypto.cc)
target_link_libraries(cbcrypto PRIVATE project_options project_warnings)
target_include_directories(cbcrypto PRIVATE ${PROJECT_SOURCE_DIR})
if(NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  target_link_libraries(cbcrypto PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

add_library(
  cbsasl OBJECT
  couchbase/cbsasl/client.cc
  couchbase/cbsasl/context.cc
  couchbase/cbsasl/mechanism.cc
  couchbase/cbsasl/plain/plain.cc
  couchbase/cbsasl/scram-sha/scram-sha.cc
  couchbase/cbsasl/scram-sha/stringutils.cc)
target_include_directories(cbsasl PRIVATE ${PROJECT_SOURCE_DIR})

if(NOT APPLE)
  set_target_properties(cbcrypto cbsasl platform PROPERTIES CXX_VISIBILITY_PRESET hidden)
endif()

add_library(couchbase_cxx_client STATIC couchbase/version.cxx)
target_include_directories(couchbase_cxx_client PUBLIC ${PROJECT_BINARY_DIR}/generated ${PROJECT_SOURCE_DIR})
target_include_directories(
  couchbase_cxx_client SYSTEM
  PUBLIC ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include
         ${PROJECT_SOURCE_DIR}/third_party/gsl/include
         ${PROJECT_SOURCE_DIR}/third_party/hdr_histogram_c/src
         ${PROJECT_SOURCE_DIR}/third_party/http_parser
         ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include
         ${PROJECT_SOURCE_DIR}/third_party/json/include
         ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
target_link_libraries(
  couchbase_cxx_client
  PRIVATE project_options
          project_warnings
          platform
          cbcrypto
          cbsasl
          hdr_histogram_static
          http_parser
          snappy
          spdlog::spdlog_header_only)
if(NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  target_link_libraries(couchbase_cxx_client PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

set_target_properties(
  cbcrypto
  cbsasl
  platform
  snappy
  hdr_histogram_static
  couchbase_cxx_client
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

option(COUCHBASE_CXX_CLIENT_BUILD_TESTS "Build test programs" TRUE)
if(COUCHBASE_CXX_CLIENT_BUILD_TESTS)
  include(cmake/Testing.cmake)
endif()
