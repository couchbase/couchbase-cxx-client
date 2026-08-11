# Find or build gRPC and Protobuf for the gRPC-based components (the couchbase2:// transport and the
# FIT performer).
#
# COUCHBASE_CXX_CLIENT_SYSTEM_GRPC selects where they come from:
#
#   AUTO  link the platform's packages where they exist, build from source where they do not
#   ON    require the platform's packages; a build that cannot use them is an error
#   OFF   always build from source
#
# AUTO chooses on presence, not on suitability: a platform gRPC that is present but unusable is an
# error under AUTO too, for the reason given at the protobuf floor below.
#
# The packages need OFF, which COUCHBASE_CXX_CLIENT_PACKAGE_BUILD selects: the platform's gRPC is
# linked against the platform's OpenSSL, while this project links BoringSSL statically
# (COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL), so shipping it would put two TLS stacks in one address
# space. A source build also decouples the artefact from the distribution's protobuf version: the
# .proto schemas use proto3 "optional", whose has_*() accessors protobuf only emits from 3.15.
#
# Everything else defaults to AUTO, because a source build of gRPC and its transitive dependencies
# costs far more than the rest of this project put together, and neither reason above applies to a
# build that is not going to be redistributed.
#
# This module MUST be included before OpenTelemetry so OTel's protobuf.cmake detects the
# already-available protobuf targets and reuses them rather than resolving a second copy.

include(FetchContent)

if(COUCHBASE_CXX_CLIENT_PACKAGE_BUILD)
  set(_couchbase_cxx_client_system_grpc_default "OFF")
else()
  set(_couchbase_cxx_client_system_grpc_default "AUTO")
endif()
set(COUCHBASE_CXX_CLIENT_SYSTEM_GRPC
    "${_couchbase_cxx_client_system_grpc_default}"
    CACHE STRING "Where gRPC/Protobuf come from: AUTO (platform packages where they exist), ON
     (require them), OFF (always build from source)")
set_property(CACHE COUCHBASE_CXX_CLIENT_SYSTEM_GRPC PROPERTY STRINGS AUTO ON OFF)

# Normalise the value, and reject anything that is not one of the three.
#
# The STRINGS property above only fills the GUI drop-down; it rejects nothing. Every string CMake
# reads as true takes the ON branch below, so an unrecognised value -- a misspelt AUTO among them --
# would silently select the platform's TLS stack for a binary meant to ship its own. Boolean
# spellings are folded onto ON and OFF rather than refused, because two of the three values are
# already booleans and a caller who writes TRUE means ON.
string(TOUPPER "${COUCHBASE_CXX_CLIENT_SYSTEM_GRPC}" _couchbase_cxx_client_system_grpc)
if(_couchbase_cxx_client_system_grpc MATCHES "^(ON|TRUE|YES|Y|1)$")
  set(_couchbase_cxx_client_system_grpc "ON")
elseif(_couchbase_cxx_client_system_grpc MATCHES "^(OFF|FALSE|NO|N|0)$")
  set(_couchbase_cxx_client_system_grpc "OFF")
elseif(NOT _couchbase_cxx_client_system_grpc STREQUAL "AUTO")
  message(FATAL_ERROR "COUCHBASE_CXX_CLIENT_SYSTEM_GRPC must be AUTO, ON or OFF, not "
                      "'${COUCHBASE_CXX_CLIENT_SYSTEM_GRPC}'.")
endif()
set(COUCHBASE_CXX_CLIENT_SYSTEM_GRPC
    "${_couchbase_cxx_client_system_grpc}"
    CACHE STRING "Where gRPC/Protobuf come from: AUTO (platform packages where they exist), ON
     (require them), OFF (always build from source)" FORCE)

# The platform's packages are built against libstdc++, so a libc++ build cannot link them: the ABI
# mismatch surfaces as unresolved std:: symbols at link time.
set(_couchbase_cxx_client_libcxx FALSE)
if(CMAKE_CXX_FLAGS MATCHES "-stdlib=libc\\+\\+")
  set(_couchbase_cxx_client_libcxx TRUE)
endif()

# AUTO is a string, and if() treats any string that is not a false constant as true, so it must be
# matched before the boolean branch rather than after it.
if(COUCHBASE_CXX_CLIENT_SYSTEM_GRPC STREQUAL "AUTO")
  if(_couchbase_cxx_client_libcxx)
    message(STATUS "Using libc++; skipping system gRPC to avoid an ABI mismatch")
  else()
    find_package(gRPC CONFIG QUIET)
  endif()
elseif(COUCHBASE_CXX_CLIENT_SYSTEM_GRPC)
  # Asked for explicitly: report what stops it rather than quietly building from source, which would
  # ignore what the caller asked for.
  if(_couchbase_cxx_client_libcxx)
    message(
      FATAL_ERROR
        "COUCHBASE_CXX_CLIENT_SYSTEM_GRPC is ON, but this build uses -stdlib=libc++ and the platform's "
        "gRPC is built against libstdc++. Set it to OFF to build gRPC from source.")
  endif()
  find_package(gRPC CONFIG QUIET)
  if(NOT gRPC_FOUND)
    message(
      FATAL_ERROR
        "COUCHBASE_CXX_CLIENT_SYSTEM_GRPC is ON but no gRPC package was found. Install grpc-devel and "
        "grpc-plugins (protobuf >= 3.15), or set it to OFF to build gRPC from source.")
  endif()
endif()

# The schemas use proto3 "optional", whose has_*() accessors protobuf emits only from 3.15. Applied
# as a floor here, an older platform package is named at configure time; accepted, it fails much
# later inside the generated code with nothing pointing at the version.
#
# Too old is an error under AUTO as well, rather than a fall back to the source build. gRPCConfig
# resolves Protobuf as a dependency, so the platform's protobuf::libprotobuf and protobuf::libprotoc
# exist from the moment gRPC is found, and an imported target cannot be withdrawn. A source build
# after that point dies in protobuf's own CMakeLists with "add_library cannot create ALIAS target
# protobuf::libprotobuf because another target with the same name already exists". Probing the
# version before looking for gRPC does not avoid it either: find_package(Protobuf) imports the same
# targets, which moves the collision onto every host carrying protobuf without gRPC.
if(gRPC_FOUND)
  message(STATUS "Found system gRPC: ${gRPC_VERSION}")
  set(protobuf_MODULE_COMPATIBLE ON CACHE BOOL "")
  find_package(Protobuf 3.15 CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf 3.15 MODULE QUIET)
  endif()
  if(NOT Protobuf_FOUND)
    message(
      FATAL_ERROR
        "The platform's gRPC ${gRPC_VERSION} was found, but its protobuf is older than 3.15 and does "
        "not emit the has_*() accessors the schemas need. Set COUCHBASE_CXX_CLIENT_SYSTEM_GRPC=OFF "
        "to build gRPC and protobuf from source.")
  endif()

  # Libraries are not enough: cmake/gRPC_Generate.cmake runs protoc with the gRPC C++ plugin, and
  # distributions ship those compilers in their own packages (protobuf-compiler,
  # protobuf-compiler-grpc / grpc-plugins) separate from the -dev packages that satisfy the
  # find_package calls above. Without this check a build configures cleanly and then fails in code
  # generation, where the missing package is much harder to recognise.
  # Both generators name the TARGETS unconditionally -- cmake/gRPC_Generate.cmake runs
  # "COMMAND protobuf::protoc" and lists both in DEPENDS -- so finding the executables is not enough
  # on its own. Where only a program is found, it is wrapped in the imported target the generators
  # expect; where neither exists, the build is refused here.
  set(_missing_codegen)
  if(NOT TARGET protobuf::protoc)
    find_program(Protobuf_PROTOC_EXECUTABLE protoc)
    if(Protobuf_PROTOC_EXECUTABLE)
      add_executable(protobuf::protoc IMPORTED GLOBAL)
      set_target_properties(protobuf::protoc PROPERTIES IMPORTED_LOCATION "${Protobuf_PROTOC_EXECUTABLE}")
    else()
      list(APPEND _missing_codegen "protoc")
    endif()
  endif()
  if(NOT TARGET gRPC::grpc_cpp_plugin)
    find_program(_grpc_cpp_plugin grpc_cpp_plugin)
    if(_grpc_cpp_plugin)
      add_executable(gRPC::grpc_cpp_plugin IMPORTED GLOBAL)
      set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION "${_grpc_cpp_plugin}")
    else()
      list(APPEND _missing_codegen "grpc_cpp_plugin")
    endif()
  endif()
  if(_missing_codegen)
    message(
      FATAL_ERROR
        "The platform's gRPC ${gRPC_VERSION} was found, but the code generator(s) it needs are not "
        "installed: ${_missing_codegen}. Install the distribution's protobuf-compiler and gRPC "
        "plugin packages, or set COUCHBASE_CXX_CLIENT_SYSTEM_GRPC=OFF to build them from source.")
  endif()
endif()

if(NOT gRPC_FOUND)
  message(STATUS "Building gRPC and protobuf from source")

  # CMake 4.x removed compatibility with cmake_minimum_required < 3.5.
  # Several gRPC submodules (c-ares, protobuf/utf8_range) have old minimum
  # versions that would cause hard errors without this policy escape hatch.
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.27")
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
  endif()

  # Determine which submodules gRPC needs.
  #
  # The list is part of CPM's cache key: a different submodule set produces a different hash and
  # therefore a different third_party_cache directory, so it must be IDENTICAL on every platform
  # that should hit the cache the source tarball ships. That is why third_party/zlib is fetched
  # unconditionally even though only Windows *uses* it (gRPC_ZLIB_PROVIDER below stays "package"
  # elsewhere -- fetching is cheap, the provider switch decides whether it is built). gRPC's bundled
  # zlib has an fdopen macro in zutil.h that conflicts with newer macOS SDK headers, hence system
  # zlib everywhere it exists.
  # Hiding gRPC, protobuf, abseil, re2 and c-ares keeps them out of libcouchbase_cxx_client's
  # dynamic symbol table, where an application linking its own protobuf or gRPC would otherwise
  # interpose on ours at load time. That is what a shipped library needs, so it follows
  # COUCHBASE_CXX_CLIENT_PACKAGE_BUILD.
  #
  # It must NOT be the default, for the same reason the generated stubs are not hidden by default.
  # Hidden symbols cannot be preempted, so an executable linking BOTH the shared client and anything
  # that pulls in gRPC -- every CNG test does, through couchbase_cxx_protostellar -- ends up with a
  # second gRPC beside the one inside the .so. Two ExecCtx thread-locals and two closure lists in one
  # process crash in grpc_closure_list_append, which is what the tsan leg showed across ten tests:
  # it is the only leg that builds gRPC from source, so the only one where the duplicate is real.
  if(COUCHBASE_CXX_CLIENT_PACKAGE_BUILD)
    set(_grpc_visibility_options "CMAKE_C_VISIBILITY_PRESET hidden" "CMAKE_CXX_VISIBILITY_PRESET hidden")
  else()
    set(_grpc_visibility_options)
  endif()

  set(_GRPC_SUBMODULES
    "third_party/re2"
    "third_party/abseil-cpp"
    "third_party/protobuf"
    "third_party/cares/cares"
    "third_party/zlib"
  )

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
  elseif(TARGET PkgConfig::PKG_CONFIG_OPENSSL)
    # OpenSSL was found via pkg-config (when CMake's find_package(OpenSSL) returned an unusable setup).
    # Reuse PkgConfig::PKG_CONFIG_OPENSSL directly so gRPC links the exact same OpenSSL library.
    set(_gRPC_SSL_LIBRARIES PkgConfig::PKG_CONFIG_OPENSSL CACHE INTERNAL "")
    set(gRPC_SSL_PROVIDER "" CACHE STRING "" FORCE)
  else()
    # No project-provided BoringSSL or pkg-config OpenSSL; point gRPC at the OpenSSL package.
    find_package(OpenSSL REQUIRED)
    set(gRPC_SSL_PROVIDER "package" CACHE STRING "" FORCE)
  endif()

  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  # gRPC emits install() rules that gRPC_INSTALL does not gate -- etc/roots.pem, its headers, its
  # pkg-config files. Vendored, those stage into our install tree, where they are files no package
  # owns: rpmbuild fails the build over them, and a DEB would ship gRPC's headers next to ours.
  # Point every gRPC install destination at one throwaway prefix instead, which the RPM and DEB
  # install steps delete in a single line. The alternative -- deleting each path -- silently grows a
  # new leak every time gRPC adds a rule.
  foreach(_grpc_install_dir BINDIR LIBDIR INCLUDEDIR CMAKEDIR SHAREDIR)
    set(gRPC_INSTALL_${_grpc_install_dir}
        "${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/${_grpc_install_dir}"
        CACHE STRING "" FORCE)
  endforeach()
  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  # gRPC's CMakeLists downloads xds/envoy-api/googleapis/opencensus-proto archives from the
  # grpc-bazel-mirror at configure time when their third_party/ directories do not exist. None of
  # the gRPC targets built here need them: a fresh git clone leaves empty submodule-placeholder
  # directories that suppress the download, but a source tarball carries files only, so the
  # placeholders vanish and the download fires. Disabling it keeps from-tarball builds offline.
  set(gRPC_DOWNLOAD_ARCHIVES OFF CACHE BOOL "" FORCE)
  # gRPC's systemd support defaults to AUTO, which links libsystemd whenever the build host happens
  # to have systemd-devel installed. That makes the artefact's dependencies a property of the build
  # machine rather than of the source, and it breaks executables linking the static client, which do
  # not inherit the -lsystemd that the shared library records. It provides only socket activation
  # (sd_listen_fds) for gRPC servers, which nothing here uses.
  set(gRPC_USE_SYSTEMD "OFF" CACHE STRING "" FORCE)
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

  # Fetched via CPM, NOT raw FetchContent, so the source-tarball machinery captures it: packaging
  # configures with CPM_DOWNLOAD_ALL + CPM_USE_NAMED_CACHE_DIRECTORIES and harvests
  # CPM_SOURCE_CACHE into the tarball's third_party_cache/, and from-tarball builds
  # (cmake/TarballRelease.cmake) point CPM back at it. FetchContent populates into the build tree
  # instead, which packaging deletes -- so gRPC would be re-cloned from github.com on every
  # from-tarball build, and an offline build would simply fail. CPM forwards the GIT_* arguments to
  # FetchContent underneath, so fetch semantics are unchanged; only the source location differs.
  #
  # gRPC v1.65.x is the last series that ships protobuf 3.x which is compatible with the protobuf
  # version opentelemetry-cpp 1.23 expects.
  # https://github.com/grpc/grpc/releases
  cpmaddpackage(
    NAME
    grpc
    VERSION
    1.65.5
    GIT_REPOSITORY
    "https://github.com/grpc/grpc.git"
    GIT_TAG
    v1.65.5
    GIT_SHALLOW
    TRUE
    GIT_SUBMODULES
    ${_GRPC_SUBMODULES}
    OPTIONS
    # Send every install rule these projects declare into a throwaway prefix. gRPC's own rules are
    # not gated by gRPC_INSTALL, and re2's are not gated at all -- it installs its headers, its
    # archive and its CMake package unconditionally -- while c-ares additionally installs adig,
    # ahost and acountry into bindir. Left alone they land in our install tree as files no package
    # owns, which fails rpmbuild outright and would put gRPC's headers in the DEB beside ours.
    # Redirecting the GNUInstallDirs variables catches all of them at once, including submodules
    # added later; CPM applies OPTIONS as ordinary variables scoped to this package, so the
    # project's own install destinations are unaffected.
    "CMAKE_INSTALL_BINDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/bin"
    "CMAKE_INSTALL_LIBDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/lib"
    "CMAKE_INSTALL_INCLUDEDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/include"
    "CMAKE_INSTALL_DATADIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/share"
    "CMAKE_INSTALL_DATAROOTDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/share"
    # c-ares builds three command line tools that nothing here uses and that only exist to be
    # installed into bindir.
    "CARES_BUILD_TOOLS OFF"
    ${_grpc_visibility_options}
    "CMAKE_POSITION_INDEPENDENT_CODE ON")

  # Compile all fetched gRPC, Protobuf, Abseil, upb, re2 targets with the project's sanitizer
  # flags so that all translation units agree on struct layouts, member definitions, and TSan
  # instrumentation (mirroring commit be6fb44f9 / CXXCBC-917 for couchbase_cxx_protostellar).
  if(LIST_OF_SANITIZERS AND NOT "${LIST_OF_SANITIZERS}" STREQUAL "")
    function(couchbase_get_targets_recursively out_var dir)
      get_property(_targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
      get_property(_subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
      foreach(_subdir IN LISTS _subdirs)
        couchbase_get_targets_recursively(_sub_targets "${_subdir}")
        list(APPEND _targets ${_sub_targets})
      endforeach()
      set(${out_var} "${_targets}" PARENT_SCOPE)
    endfunction()

    couchbase_get_targets_recursively(_fetched_targets "${grpc_SOURCE_DIR}")
    foreach(_fetched_target IN LISTS _fetched_targets)
      get_target_property(_target_type ${_fetched_target} TYPE)
      if(_target_type STREQUAL "STATIC_LIBRARY"
         OR _target_type STREQUAL "SHARED_LIBRARY"
         OR _target_type STREQUAL "OBJECT_LIBRARY"
         OR _target_type STREQUAL "EXECUTABLE")
        target_compile_options(${_fetched_target} PRIVATE -fsanitize=${LIST_OF_SANITIZERS})
        target_link_options(${_fetched_target} PRIVATE -fsanitize=${LIST_OF_SANITIZERS})
        if("thread" IN_LIST SANITIZERS
           AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
           AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0.0")
          target_compile_options(${_fetched_target} PRIVATE -Wno-error=tsan)
        endif()
      endif()
    endforeach()
  endif()

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
  # Also ensure grpc_SOURCE_DIR is visible: OTel's cmake/protobuf.cmake reads the protobuf version
  # from ${grpc_SOURCE_DIR}/third_party/protobuf/version.json. gRPC has no version.json of its own.
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

  # gRPC unconditionally defines several library targets we do not link
  # against (the couchbase2 transport and the fit_performer tool consume only
  # gRPC::grpc++ and gRPC::grpc_cpp_plugin). Because gRPC's CMakeLists has no
  # per-library opt-out and the subdirectory is added without
  # EXCLUDE_FROM_ALL, every target ends up in the Visual Studio ALL_BUILD
  # and gets compiled on Windows. grpc_unsecure alone is a ~300-source
  # near-duplicate of gRPC without TLS; combined with the admin/auth
  # extras below this is ~15 minutes of pointless compile per Windows
  # build. Marking each EXCLUDE_FROM_ALL removes them from ALL_BUILD
  # while leaving the targets defined, so anything that does
  # transitively need one is still pulled in by CMake on demand.
  foreach(_unused_grpc_target
      grpc_unsecure
      grpc++_unsecure
      grpc++_alts
      grpc++_reflection
      grpcpp_channelz
      grpc_authorization_provider)
    if(TARGET ${_unused_grpc_target})
      set_target_properties(${_unused_grpc_target} PROPERTIES EXCLUDE_FROM_ALL TRUE)
    endif()
  endforeach()
endif()

# gRPC's headers are not clean under this project's warning set: <grpcpp/...> trips
# -Wold-style-cast, -Wsign-conversion and -Wgcc-compat, and with -Werror that is a hard failure in
# any translation unit including them.
#
# It bites on the built-from-source path: the include directories are ordinary -I entries, so every
# warning applies. Platform packages instead put the headers under a system prefix, where the
# compiler treats them as system headers for free.
#
# fit_performer sidesteps this by turning its own warnings off wholesale (/W0 or -w plus
# COMPILE_WARNING_AS_ERROR OFF). That is not an option for couchbase_cxx_client, which compiles
# core/protostellar/*.cxx and must keep full warnings on its own code, so the fix belongs on the
# dependency: mark the interface include directories SYSTEM, exactly as is done for spdlog.
#
# Marking gRPC::grpc++ is sufficient and abseil/protobuf/boringssl need no separate handling --
# both clang and gcc propagate system-ness down the include stack, so a header reached from a system
# header is itself treated as one. Verified: with grpc-src/include as -I, <grpcpp/grpcpp.h> emits 26
# errors across grpcpp and abseil-cpp; with it as -isystem and every other third-party directory
# left as -I, zero.
foreach(_grpc_system_target gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
  if(TARGET ${_grpc_system_target})
    declare_system_library(${_grpc_system_target})
  endif()
endforeach()
