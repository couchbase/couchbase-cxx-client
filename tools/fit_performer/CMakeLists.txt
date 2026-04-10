# gRPC and Protobuf targets are provided by cmake/GrpcProtobuf.cmake which is included from ThirdPartyDependencies.cmake
# before this point.
if(NOT TARGET gRPC::grpc++)
  message(FATAL_ERROR "gRPC::grpc++ target not found. Ensure cmake/GrpcProtobuf.cmake was included.")
endif()
if(NOT TARGET protobuf::libprotobuf)
  message(FATAL_ERROR "protobuf::libprotobuf target not found. Ensure cmake/GrpcProtobuf.cmake was included.")
endif()

# Fetch the FIT protocol files from the official repository
include(FetchContent)
FetchContent_Declare(
  fit_protocol
  GIT_REPOSITORY https://github.com/couchbaselabs/fit-protocol
  GIT_TAG 1e05d887dbed4e993f942e20424bb923185c72ab
  GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(fit_protocol)

# generate the protobuf/grpc stuff
set(PROTO_IN "${fit_protocol_SOURCE_DIR}/operational")
set(PROTO_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto_generated")
file(MAKE_DIRECTORY "${PROTO_SRC_DIR}")
file(GLOB PROTO_FILES "${PROTO_IN}/*.proto")
message("PROTO_FILES: ${PROTO_FILES}, PROTO_SRC_DIR: ${PROTO_SRC_DIR}")

include("${CMAKE_SOURCE_DIR}/cmake/protobuf_Generate.cmake")

protobuf_generate_cpp_optional(
  PROTO_SOURCES
  PROTO_HEADERS
  ${PROTO_SRC_DIR}
  ${PROTO_FILES})
message("PROTO_SOURCES: ${PROTO_SOURCES}")
set(GRPC_GENERATE_CPP_APPEND_PATH ON)
include("${CMAKE_SOURCE_DIR}/cmake/gRPC_Generate.cmake")
grpc_generate_cpp(
  GRPC_SOURCES
  GRPC_HEADERS
  ${PROTO_SRC_DIR}
  ${PROTO_FILES})

if(MSVC)
  set_source_files_properties(${PROTO_SOURCES} ${GRPC_SOURCES} PROPERTIES COMPILE_OPTIONS "/W0;/FS")
else()
  set_source_files_properties(${PROTO_SOURCES} ${GRPC_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")
endif()

set(FIT_PERFORMER_SOURCES
    src/batcher.hxx
    src/bounds.hxx
    src/commands/bucket_management.cxx
    src/commands/bucket_management.hxx
    src/commands/collection_management.cxx
    src/commands/collection_management.hxx
    src/commands/common.cxx
    src/commands/common.hxx
    src/commands/key_value.cxx
    src/commands/key_value.hxx
    src/commands/query.cxx
    src/commands/query.hxx
    src/commands/query_index_management.cxx
    src/commands/query_index_management.hxx
    src/commands/search.cxx
    src/commands/search.hxx
    src/commands/search_index_management.cxx
    src/commands/search_index_management.hxx
    src/connection.cxx
    src/connection.hxx
    src/counters.hxx
    src/exceptions.hxx
    src/fit.cxx
    src/hooks.cxx
    src/hooks.hxx
    src/metrics/metrics_reporter.cxx
    src/metrics/metrics_reporter.hxx
    src/observability/otel.cxx
    src/observability/otel.hxx
    src/observability/span_owner.cxx
    src/observability/span_owner.hxx
    src/service.cxx
    src/service.hxx
    src/stream.hxx
    src/utils.hxx)
add_executable(fit_performer ${FIT_PERFORMER_SOURCES} ${PROTO_SOURCES} ${GRPC_SOURCES})

set_target_properties(fit_performer PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_project_options(fit_performer)

# fit_performer interoperates heavily with generated protobuf/gRPC code and third-party headers that trigger many
# warnings. Suppress all warnings for this target rather than fighting with set_project_warnings overrides.
set_target_properties(fit_performer PROPERTIES COMPILE_WARNING_AS_ERROR OFF)
if(MSVC)
  # /W0 suppresses all warnings. COMPILE_WARNING_AS_ERROR OFF handles TreatWarningAsError. On MSVC, proto_utils.h must
  # be force-included before gRPC streaming headers to work around non-conformant two-phase lookup that causes
  # SerializationTraits to be undefined when call_op_set.h is processed before proto_utils.h in generated gRPC source
  # files.
  set(_msvc_grpc_fix_header "${CMAKE_CURRENT_BINARY_DIR}/msvc_grpc_proto_utils_fix.h")
  file(WRITE "${_msvc_grpc_fix_header}" "#pragma once\n#include <grpcpp/impl/proto_utils.h>\n")
  target_compile_options(fit_performer PRIVATE /W0 /FS "/FI${_msvc_grpc_fix_header}")
else()
  # -w suppresses all warnings on GCC and Clang. -Wno-c++20-extensions is Clang-specific (GCC/other
  # front-ends may not recognise it), so gate it by compiler id to keep the target cross-platform.
  target_compile_options(
    fit_performer PRIVATE -w $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-c++20-extensions>)
endif()

target_include_directories(
  fit_performer
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_SOURCE_DIR}
          ${CMAKE_SOURCE_DIR}/couchbase
          ${CMAKE_SOURCE_DIR}/core)
target_include_directories(fit_performer SYSTEM PRIVATE ${PROTO_SRC_DIR})

target_link_libraries(
  fit_performer
  PRIVATE ${couchbase_cxx_client_DEFAULT_LIBRARY}
          Microsoft.GSL::GSL
          system_metrics_cxx
          spdlog::spdlog
          gRPC::grpc++
          taocpp::json
          protobuf::libprotobuf
          asio::asio)
if(COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY)
  target_compile_definitions(fit_performer PRIVATE COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY)
  target_link_libraries(fit_performer PRIVATE opentelemetry_exporter_otlp_http opentelemetry_exporter_otlp_http_metric)
endif()
