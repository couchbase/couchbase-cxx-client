# Fetch or find gRPC and Protobuf for the FIT performer.
#
# When system packages are available (e.g. on Linux with libgrpc-dev), find_package
# will succeed and no sources are fetched.  On CI runners (macOS, Windows) where
# these packages are not installed, gRPC is fetched from source with its protobuf
# submodule so both sets of targets are built in-tree.
#
# This module MUST be included before OpenTelemetry so that OTel's protobuf.cmake
# detects the already-available protobuf targets and reuses them instead of fetching
# its own copy.

include(FetchContent)

find_package(gRPC CONFIG QUIET)

if(gRPC_FOUND)
  message(STATUS "Found system gRPC: ${gRPC_VERSION}")
  set(protobuf_MODULE_COMPATIBLE ON CACHE BOOL "")
  find_package(Protobuf CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
  endif()
else()
  message(STATUS "System gRPC not found, fetching from source")

  # CMake 4.x removed compatibility with cmake_minimum_required < 3.5.
  # Several gRPC submodules (c-ares, protobuf/utf8_range) have old minimum
  # versions that would cause hard errors without this policy escape hatch.
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.27")
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
  endif()

  # Determine which submodules gRPC needs
  set(_GRPC_SUBMODULES
    "third_party/re2"
    "third_party/abseil-cpp"
    "third_party/protobuf"
    "third_party/cares/cares"
  )

  # gRPC's bundled zlib (1.2.x) has an fdopen macro in zutil.h that conflicts
  # with newer macOS SDK headers. Use system zlib where available (macOS/Linux)
  # and only fall back to the bundled submodule on Windows.
  if(WIN32)
    list(APPEND _GRPC_SUBMODULES "third_party/zlib")
  endif()

  # If the project already has BoringSSL targets (from cmake/OpenSSL.cmake),
  # reuse them instead of letting gRPC build a second copy.
  if(TARGET ssl AND TARGET crypto)
    # Pre-set gRPC's internal SSL variables so ssl.cmake can be skipped
    set(_gRPC_SSL_LIBRARIES ssl crypto CACHE INTERNAL "")
    get_target_property(_boring_include ssl INTERFACE_INCLUDE_DIRECTORIES)
    if(_boring_include)
      set(_gRPC_SSL_INCLUDE_DIR "${_boring_include}" CACHE INTERNAL "")
    else()
      set(_gRPC_SSL_INCLUDE_DIR "" CACHE INTERNAL "")
    endif()
    set(gRPC_SSL_PROVIDER "" CACHE STRING "" FORCE)
  else()
    # No existing BoringSSL; let gRPC build its own from submodule
    list(APPEND _GRPC_SUBMODULES "third_party/boringssl-with-bazel")
    set(gRPC_SSL_PROVIDER "module" CACHE STRING "" FORCE)
  endif()

  # Fetch gRPC from source with selected submodules.
  # gRPC v1.65.x is the last series that ships protobuf 3.x which is compatible
  # with the protobuf version opentelemetry-cpp 1.23 expects.
  # https://github.com/grpc/grpc/releases
  FetchContent_Declare(
    grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG v1.65.5
    GIT_SHALLOW TRUE
    GIT_SUBMODULES ${_GRPC_SUBMODULES}
  )

  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
  if(WIN32)
    set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "" FORCE)
  else()
    set(gRPC_ZLIB_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  set(gRPC_RE2_PROVIDER "module" CACHE STRING "" FORCE)
  set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
  set(gRPC_PROTOBUF_PACKAGE_TYPE "CONFIG" CACHE STRING "" FORCE)
  set(gRPC_ABSL_PROVIDER "module" CACHE STRING "" FORCE)
  set(gRPC_CARES_PROVIDER "module" CACHE STRING "" FORCE)
  set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(grpc)

  # Abseil-cpp bundled with gRPC v1.65.5 has a bug in its randen HWAES compile
  # options on Apple ARM64: CMake's option deduplication strips the second
  # -Xarch_x86_64 prefix, leaving -msse4.1 unscoped, which causes a hard error
  # on newer AppleClang (16+). Fix by replacing the incorrectly deduped flags
  # with properly SHELL:-prefixed versions on the affected targets.
  # See https://github.com/abseil/abseil-cpp/pull/1710
  #
  # After CMake deduplication the compile options are stored as individual list
  # elements (e.g. "-Xarch_x86_64;-maes;-msse4.1;...") rather than as
  # space-joined pairs, so we must remove them one by one.
  # Both absl_random_internal_randen_hwaes and absl_random_internal_randen_hwaes_impl
  # use ABSL_RANDOM_RANDEN_COPTS and need this fix.
  if(APPLE)
    foreach(_randen_target absl_random_internal_randen_hwaes absl_random_internal_randen_hwaes_impl)
      if(TARGET ${_randen_target})
        get_target_property(_randen_opts ${_randen_target} COMPILE_OPTIONS)
        if(_randen_opts)
          list(REMOVE_ITEM _randen_opts
            "-Xarch_x86_64"
            "-maes"
            "-msse4.1"
            "-Xarch_arm64"
            "-march=armv8-a+crypto"
            "-Wno-unused-command-line-argument"
          )
          list(APPEND _randen_opts
            "SHELL:-Xarch_x86_64 -maes"
            "SHELL:-Xarch_x86_64 -msse4.1"
            "SHELL:-Xarch_arm64 -march=armv8-a+crypto"
            "-Wno-unused-command-line-argument"
          )
          set_target_properties(${_randen_target} PROPERTIES COMPILE_OPTIONS "${_randen_opts}")
        endif()
      endif()
    endforeach()
  endif()

  # OpenTelemetry's cmake/protobuf.cmake checks gRPC_PROVIDER to detect
  # whether gRPC fetched protobuf as a submodule. When that variable is set
  # (and != "find_package") and TARGET libprotobuf exists, OTel reuses the
  # existing protobuf instead of fetching its own copy via FetchContent.
  # Also ensure grpc_SOURCE_DIR is visible since OTel reads version.json from it.
  if(TARGET protobuf::libprotobuf)
    set(Protobuf_FOUND TRUE CACHE BOOL "" FORCE)
    set(gRPC_PROVIDER "fetch_content" CACHE STRING "" FORCE)
    FetchContent_GetProperties(grpc)

    # When protobuf is built from source, protoc has no built-in path to
    # well-known .proto files (google/protobuf/timestamp.proto etc.).
    # Set PROTOBUF_IMPORT_DIRS so protobuf_Generate.cmake and gRPC_Generate.cmake
    # add the necessary -I flag when invoking protoc.
    set(PROTOBUF_IMPORT_DIRS "${grpc_SOURCE_DIR}/third_party/protobuf/src" CACHE STRING "" FORCE)
  endif()

  if(TARGET grpc++ AND NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ ALIAS grpc++)
  endif()
  if(TARGET grpc_cpp_plugin AND NOT TARGET gRPC::grpc_cpp_plugin)
    add_executable(gRPC::grpc_cpp_plugin ALIAS grpc_cpp_plugin)
  endif()

  foreach(_target grpc++ grpc_cpp_plugin)
    if(TARGET ${_target})
      set_target_properties(${_target} PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_CLANG_TIDY ""
        CXX_INCLUDE_WHAT_YOU_USE "")
    endif()
  endforeach()

  if(TARGET libprotobuf)
    set_target_properties(libprotobuf PROPERTIES
      POSITION_INDEPENDENT_CODE ON
      CXX_CLANG_TIDY ""
      CXX_INCLUDE_WHAT_YOU_USE "")
  endif()
endif()
