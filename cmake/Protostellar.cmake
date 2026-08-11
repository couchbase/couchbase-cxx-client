# Vendors the Couchbase Protostellar (couchbase2://) protobuf/gRPC schema and builds the generated
# C++ stubs into the couchbase_cxx_protostellar static library.
#
# For VCS builds the schema is fetched via CPM (DOWNLOAD_ONLY, no CMakeLists), mirroring how
# tools/fit_performer vendors couchbaselabs/fit-protocol.
#
# Offline/tarball builds are wired: cmake/Packaging.cmake enables COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2
# in the inner packaging configure so these CPM packages land in the cache, and cmake/tarball_glob.txt
# carries protostellar/api_common_protos entries so they are embedded in the tarball. Both are load
# bearing for RPM/DEB builds, which run without network access.
#
# gRPC::grpc++, gRPC::grpc_cpp_plugin, protobuf::libprotobuf and protobuf::protoc are provided by
# cmake/GrpcProtobuf.cmake, which CMakeLists.txt includes before this module.

if(NOT TARGET gRPC::grpc++)
  message(FATAL_ERROR "gRPC::grpc++ not found; cmake/GrpcProtobuf.cmake must be included first")
endif()
if(NOT TARGET protobuf::libprotobuf)
  message(FATAL_ERROR "protobuf::libprotobuf not found; cmake/GrpcProtobuf.cmake must be included first")
endif()

# The Protostellar schema. The .proto files live at the repository root under couchbase/**.
# WARNING: do NOT add GIT_SHALLOW — a shallow clone cannot reach an arbitrary pinned commit.
#
# CAVEAT when bumping GIT_TAG: protobuf emits a class-scoped alias per enum value ("static
# constexpr Status STATUS_TIMEOUT = ..."), so an enum value whose name is also an object-like macro
# in the Windows SDK mangles the generated header. This breaks MSVC only — <winnt.h> is reached
# transitively through asio and gRPC, so the Linux and macOS legs stay green and the failure shows
# up an hour later in CI. One name collides today: QueryResponse.MetaData.Status.STATUS_TIMEOUT vs
# winnt.h's ((DWORD)0x00000102L). couchbase/query/v1/query{,.grpc}.pb.h is therefore included only
# through core/protostellar/query_proto.hxx, which suppresses the whole STATUS_* family around the
# include with push_macro/pop_macro. A renamed or added enum value, or the same pattern in another
# proto, introduces a new collision — after bumping, run
#
#   ./bin/check-proto-macro-collisions <build-dir>
#
# which lists the hazardous aliases and any source that includes a hazardous header directly
# instead of through its wrapper.
cpmaddpackage(
  NAME
  protostellar
  GITHUB_REPOSITORY
  "couchbase/protostellar"
  GIT_TAG
  36d4d9aba03a388f7edb05290f1093d9497b1005
  DOWNLOAD_ONLY
  YES)

# The schema imports google/rpc/status.proto (a googleapis common proto that protobuf itself does
# not ship). api-common-protos is the small canonical source for it. The google/protobuf/* imports
# (any, duration, timestamp) are well-known types resolved by protoc from the protobuf install.
cpmaddpackage(
  NAME
  api_common_protos
  GITHUB_REPOSITORY
  "googleapis/api-common-protos"
  GIT_TAG
  3332dec527759859840a3a2ff108c67a54708130
  DOWNLOAD_ONLY
  YES)

set(_ps_out "${CMAKE_CURRENT_BINARY_DIR}/protostellar_generated")

if(TARGET gRPC::grpc_cpp_plugin)
  set(_ps_grpc_plugin "$<TARGET_FILE:gRPC::grpc_cpp_plugin>")
  # Depend on the target so it is built before codegen runs.
  set(_ps_grpc_plugin_dep gRPC::grpc_cpp_plugin)
else()
  find_program(_ps_grpc_plugin grpc_cpp_plugin REQUIRED)
  # No target to depend on; the discovered executable path is the dependency.
  set(_ps_grpc_plugin_dep "${_ps_grpc_plugin}")
endif()

set(_ps_import_flags -I "${protostellar_SOURCE_DIR}" -I "${api_common_protos_SOURCE_DIR}")
# When protobuf is built from source (not a system package) protoc has no built-in path to the
# well-known .proto files; GrpcProtobuf.cmake sets PROTOBUF_IMPORT_DIRS in that case.
if(DEFINED PROTOBUF_IMPORT_DIRS)
  foreach(_dir ${PROTOBUF_IMPORT_DIRS})
    list(APPEND _ps_import_flags -I "${_dir}")
  endforeach()
endif()

# Service protos (generate both message and gRPC stubs), relative to the Protostellar root.
set(_ps_service_protos
    couchbase/admin/analytics/v1/analytics.proto
    couchbase/admin/bucket/v1/bucket.proto
    couchbase/admin/collection/v1/collection.proto
    couchbase/admin/query/v1/query.proto
    couchbase/admin/search/v1/search.proto
    couchbase/analytics/v1/analytics.proto
    couchbase/internal/hooks/v1/hooks.proto
    couchbase/internal/xdcr/v1/xdcr.proto
    couchbase/kv/v1/kv.proto
    couchbase/query/v1/query.proto
    couchbase/routing/v2/routing.proto
    couchbase/search/v1/search.proto
    couchbase/transactions/v1/transactions.proto
    couchbase/view/v1/view.proto)

set(_ps_srcs "")
set(_ps_hdrs "")

# Emit the protobuf (message) sources for one proto given its import-root base and relative path.
# Appends the outputs to _ps_srcs / _ps_hdrs in the caller scope.
function(_ps_generate_cpp base rel out_srcs out_hdrs)
  get_filename_component(_rel_dir "${rel}" DIRECTORY)
  get_filename_component(_rel_we "${rel}" NAME_WE)
  file(MAKE_DIRECTORY "${_ps_out}/${_rel_dir}")
  set(_cc "${_ps_out}/${_rel_dir}/${_rel_we}.pb.cc")
  set(_h "${_ps_out}/${_rel_dir}/${_rel_we}.pb.h")
  add_custom_command(
    OUTPUT "${_cc}" "${_h}"
    COMMAND protobuf::protoc
            --cpp_out "${_ps_out}" ${_ps_import_flags} "${base}/${rel}"
            --experimental_allow_proto3_optional
    DEPENDS "${base}/${rel}" protobuf::protoc
    COMMENT "protoc cpp: ${rel}"
    VERBATIM)
  set(${out_srcs} ${${out_srcs}} "${_cc}" PARENT_SCOPE)
  set(${out_hdrs} ${${out_hdrs}} "${_h}" PARENT_SCOPE)
endfunction()

# Emit the gRPC service sources for one proto.
function(_ps_generate_grpc base rel out_srcs out_hdrs)
  get_filename_component(_rel_dir "${rel}" DIRECTORY)
  get_filename_component(_rel_we "${rel}" NAME_WE)
  file(MAKE_DIRECTORY "${_ps_out}/${_rel_dir}")
  set(_cc "${_ps_out}/${_rel_dir}/${_rel_we}.grpc.pb.cc")
  set(_h "${_ps_out}/${_rel_dir}/${_rel_we}.grpc.pb.h")
  add_custom_command(
    OUTPUT "${_cc}" "${_h}"
    COMMAND protobuf::protoc
            --grpc_out "${_ps_out}" ${_ps_import_flags}
            "--plugin=protoc-gen-grpc=${_ps_grpc_plugin}" "${base}/${rel}"
            --experimental_allow_proto3_optional
    DEPENDS "${base}/${rel}" protobuf::protoc ${_ps_grpc_plugin_dep}
    COMMENT "protoc grpc: ${rel}"
    VERBATIM)
  set(${out_srcs} ${${out_srcs}} "${_cc}" PARENT_SCOPE)
  set(${out_hdrs} ${${out_hdrs}} "${_h}" PARENT_SCOPE)
endfunction()

# google/rpc/status.proto — message stubs only (no service).
_ps_generate_cpp("${api_common_protos_SOURCE_DIR}" "google/rpc/status.proto" _ps_srcs _ps_hdrs)

# google/rpc/error_details.proto — the typed detail blocks (ErrorInfo, PreconditionFailure,
# ResourceInfo, …) that CNG packs into Status.details; RFC 77 error handling needs them decoded.
_ps_generate_cpp(
  "${api_common_protos_SOURCE_DIR}" "google/rpc/error_details.proto" _ps_srcs _ps_hdrs)

foreach(_proto ${_ps_service_protos})
  _ps_generate_cpp("${protostellar_SOURCE_DIR}" "${_proto}" _ps_srcs _ps_hdrs)
  _ps_generate_grpc("${protostellar_SOURCE_DIR}" "${_proto}" _ps_srcs _ps_hdrs)
endforeach()

set_source_files_properties(${_ps_srcs} ${_ps_hdrs} PROPERTIES GENERATED TRUE)

add_library(couchbase_cxx_protostellar STATIC ${_ps_srcs} ${_ps_hdrs})
add_library(couchbase::cxx_protostellar ALIAS couchbase_cxx_protostellar)
# Consumers include the generated headers by their proto path, e.g.
# <couchbase/kv/v1/kv.pb.h>. Exposed SYSTEM so the (warning-heavy) generated code does not trip
# the project's warnings-as-errors in downstream targets.
target_include_directories(couchbase_cxx_protostellar SYSTEM PUBLIC "${_ps_out}")
target_link_libraries(couchbase_cxx_protostellar PUBLIC gRPC::grpc++ protobuf::libprotobuf)
set_target_properties(
  couchbase_cxx_protostellar
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             COMPILE_WARNING_AS_ERROR OFF
             CXX_CLANG_TIDY ""
             CXX_INCLUDE_WHAT_YOU_USE "")
if(COUCHBASE_CXX_CLIENT_PACKAGE_BUILD)
  set_target_properties(
    couchbase_cxx_protostellar PROPERTIES C_VISIBILITY_PRESET hidden CXX_VISIBILITY_PRESET hidden
                                          VISIBILITY_INLINES_HIDDEN ON)
endif()
# The generated stubs MUST be compiled with the same sanitizer flags as everything that includes the
# generated headers, because protobuf changes the LAYOUT of every message under ThreadSanitizer:
# port_def.inc defines PROTOBUF_TSAN from the compiler's thread_sanitizer feature test, and
# PROTOBUF_TSAN_DECLARE_MEMBER -- which appears in every generated message's Impl_ -- then adds a
# `::uint32_t _tsan_detect_race` member. So sizeof(UpsertResponse) is 40 without -fsanitize=thread
# and 48 with it, and every setter additionally stores a race-detection byte past the smaller size.
#
# enable_sanitizers() attaches the flags PUBLIC to the client target, which propagates UP to the
# tests but never DOWN into this library (the client links it PRIVATE). That left RpcMethodHandler
# -- instantiated in the uninstrumented kv.grpc.pb.cc -- reserving a 40-byte `ResponseType rsp` on
# its stack directly below the saved `param` reference, while the instrumented handler's set_cas()
# wrote the race byte at offset 40, zeroing that pointer's low byte. RunHandler then read
# param.request from an address 32 bytes off, got a null, and destroyed a null request: the
# `unit (tsan)` SEGV in cng_component_test at method_handler.h:119 (CXXCBC-917).
#
# PROTOBUF_TSAN cannot be forced to a fixed value instead -- port_def.inc #errors if it is already
# defined -- so matching the flags is the only supported remedy.
enable_sanitizers(couchbase_cxx_protostellar)
# ABI visibility, above: couchbase_cxx_client links this library whenever
# COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 is on, so without hidden visibility the generated
# protobuf/gRPC symbols land in the exported ABI of libcouchbase_cxx_client.so, where an application
# linking its own protobuf/gRPC interposes on them. That is what the shipped library must not do, so
# COUCHBASE_CXX_CLIENT_PACKAGE_BUILD hides them; gRPC and protobuf themselves are covered in
# cmake/GrpcProtobuf.cmake.
#
# Hiding them splits the process into two protobuf worlds: symbols in this archive resolve at static
# link time regardless of visibility, so an executable that links BOTH this library and the shared
# client gets its own copy of every generated message alongside the one inside the .so, with two
# descriptor pools and two type identities for the same message. Nothing in a package links that
# combination, but the CNG tests do, which is why they follow the same switch and link the static
# client only when it is on (test/cng/CMakeLists.txt).
if(MSVC)
  # /bigobj: the generated translation units are large -- search.pb.cc is 18k lines and yields a
  # 7.4 MB object -- and protobuf-generated code this size can exceed the COFF section limit
  # (fatal error C1128). CI builds RelWithDebInfo, but a Debug MSVC build (the default for
  # bin/build-tests) produces markedly larger objects. tools/fit_performer gets this flag from
  # set_project_options() for the same class of file; this target does not call that function.
  #
  # Do NOT add the /FI<proto_utils.h> force-include that tools/fit_performer uses. It was tried
  # here and breaks the build: proto_utils.h reaches grpc/support/port_platform.h, which includes
  # <windows.h> unconditionally ("Get windows.h included everywhere (we need it)"), so forcing it
  # ahead of the translation unit's own includes puts winnt.h's STATUS_TIMEOUT macro in scope before
  # couchbase/query/v1/query.pb.h is parsed. That header declares an enum constant of the same name,
  # so
  #   static constexpr Status STATUS_TIMEOUT = ...
  # expands to ((DWORD)0x00000102L): C2143, plus a member named DWORD (C2789). query.pb.h is the
  # only generated header carrying such a collision. In normal include order it is parsed before
  # anything drags in windows.h, so there is no clash.
  #
  # That force-include exists to work around MSVC's non-conformant two-phase lookup leaving
  # grpc::SerializationTraits undefined when call_op_set.h is processed before proto_utils.h. The
  # ordering does occur here: grpc_cpp_plugin emits the generated .grpc.pb.h includes
  # alphabetically, so async_stream.h (which pulls call_op_set.h) precedes proto_utils.h. It does
  # not, however, produce a diagnostic: the windows-2022 and windows-11-arm legs each compile all 30
  # of this library's translation units and link it without one, so the workaround is not needed
  # here. Should SerializationTraits ever turn up undefined in a *.grpc.pb.cc, the remedy is
  # /permissive-, which makes the lookup conformant without inverting include order -- not the
  # force-include.
  target_compile_options(couchbase_cxx_protostellar PRIVATE /W0 /FS /bigobj)
else()
  target_compile_options(couchbase_cxx_protostellar PRIVATE -w)
endif()

# The hand-written transport (core/protostellar/*.cxx: the gRPC<->asio bridge, credentials, and
# later the cluster component) is compiled into couchbase_cxx_client itself, guarded by
# COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 -- the client library links this stubs library. Keeping
# the transport in the client library (rather than a standalone lib) avoids a dependency cycle
# once cluster_impl calls into it, and lets it reuse core utilities (base64, the Mozilla CA
# bundle). The source list is exported for CMakeLists.txt to append to couchbase_cxx_client_FILES.
set(couchbase_cxx_protostellar_TRANSPORT_FILES
    ${PROJECT_SOURCE_DIR}/core/protostellar/dispatcher.cxx
    ${PROJECT_SOURCE_DIR}/core/protostellar/credentials.cxx
    ${PROJECT_SOURCE_DIR}/core/protostellar/error_utils.cxx
    ${PROJECT_SOURCE_DIR}/core/protostellar/kv_converter.cxx
    ${PROJECT_SOURCE_DIR}/core/protostellar/component.cxx)
