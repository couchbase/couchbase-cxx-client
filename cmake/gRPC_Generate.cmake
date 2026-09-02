function(GRPC_GENERATE_CPP SRCS HDRS DEST)
  if(TARGET gRPC::grpc_cpp_plugin)
    set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
  else()
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
  endif()
  if(NOT ARGN)
    message(SEND_ERROR "Error: GRPC_GENERATE_CPP() called without any proto files")
    return()
  endif()

  if(GRPC_GENERATE_CPP_APPEND_PATH)
    # Create an include path for each file specified
    foreach(FIL ${ARGN})
      get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
      get_filename_component(ABS_PATH ${ABS_FIL} PATH)
      list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
      if(${_contains_already} EQUAL -1)
          list(APPEND _protobuf_include_path -I ${ABS_PATH})
      endif()
    endforeach()
  else()
    set(_protobuf_include_path -I ${CMAKE_CURRENT_SOURCE_DIR})
  endif()

  if(DEFINED PROTOBUF_IMPORT_DIRS)
    foreach(DIR ${PROTOBUF_IMPORT_DIRS})
      get_filename_component(ABS_PATH ${DIR} ABSOLUTE)
      list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
      if(${_contains_already} EQUAL -1)
          list(APPEND _protobuf_include_path -I ${ABS_PATH})
      endif()
    endforeach()
  endif()

  set(${SRCS})
  set(${HDRS})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WLE ${FIL} NAME_WLE)

    list(APPEND ${SRCS} "${DEST}/${FIL_WLE}.grpc.pb.cc")
    list(APPEND ${HDRS} "${DEST}/${FIL_WLE}.grpc.pb.h")

    add_custom_command(
      OUTPUT "${DEST}/${FIL_WLE}.grpc.pb.cc"
             "${DEST}/${FIL_WLE}.grpc.pb.h"
      COMMAND protobuf::protoc
      ARGS --grpc_out ${DEST} ${_protobuf_include_path} --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN} ${ABS_FIL} --experimental_allow_proto3_optional
      DEPENDS ${ABS_FIL} protobuf::protoc gRPC::grpc_cpp_plugin
      COMMENT "Running C++ gRPC compiler (with --experimental_allow_proto3_optional) on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()
