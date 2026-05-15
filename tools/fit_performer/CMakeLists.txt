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

include_directories(SYSTEM BEFORE ${PROTO_SRC_DIR})
protobuf_generate_cpp_optional(PROTO_SOURCES PROTO_HEADERS ${PROTO_SRC_DIR} ${PROTO_FILES})
message("PROTO_SOURCES: ${PROTO_SOURCES}")
set(GRPC_GENERATE_CPP_APPEND_PATH ON)
include("${CMAKE_SOURCE_DIR}/cmake/gRPC_Generate.cmake")
grpc_generate_cpp(GRPC_SOURCES GRPC_HEADERS ${PROTO_SRC_DIR} ${PROTO_FILES})

set_source_files_properties(${PROTO_SOURCES} ${GRPC_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")
