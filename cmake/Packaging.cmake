include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/couchbase DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES LICENSE.txt DESTINATION ${CMAKE_INSTALL_DOCDIR})


# Notices for everything compiled into the binaries.
#
# Vendoring moved these dependencies from distribution packages -- each of which carried its own
# licence -- into our own artefacts, so the obligation to ship their terms moved with them. Apache
# 2.0 requires the NOTICE file to travel with any distribution of the work (gRPC, Protobuf, Abseil),
# and the BSD/MIT projects require their copyright notice to be reproduced (BoringSSL, re2, c-ares,
# zlib, curl, utf8_range).
#
# Installed only when the dependency was actually built from source: a build that links the
# platform's gRPC ships none of its code and owes nothing.
function(couchbase_install_third_party_licence name)
  set(_found FALSE)
  foreach(candidate IN LISTS ARGN)
    if(candidate AND EXISTS "${candidate}")
      install(FILES "${candidate}" DESTINATION "${CMAKE_INSTALL_DOCDIR}/third_party/${name}")
      set(_found TRUE)
    endif()
  endforeach()
  # Said out loud, because a licence that quietly fails to install looks exactly like one that was
  # never required. cli11 went missing this way: it is added from tools/CMakeLists.txt, so its
  # source directory is a child-scope variable and is simply empty here.
  if(NOT _found)
    message(WARNING "No licence file found for vendored component '${name}'; it will not ship with "
                    "the packages even though its code may be compiled in. Checked: ${ARGN}")
  endif()
endfunction()

if(boringssl_SOURCE_DIR)
  couchbase_install_third_party_licence(boringssl "${boringssl_SOURCE_DIR}/LICENSE")
endif()
if(curl_SOURCE_DIR)
  couchbase_install_third_party_licence(curl "${curl_SOURCE_DIR}/COPYING")
endif()
if(opentelemetry_SOURCE_DIR)
  # Statically linked into cbc and fit_performer through the OTLP exporters; the library itself uses
  # only the header-only API, which is equally contained.
  couchbase_install_third_party_licence(opentelemetry-cpp "${opentelemetry_SOURCE_DIR}/LICENSE")
endif()
if(grpc_SOURCE_DIR)
  couchbase_install_third_party_licence(grpc "${grpc_SOURCE_DIR}/LICENSE" "${grpc_SOURCE_DIR}/NOTICE.txt")
  # Fetched as submodules of gRPC and compiled into the same binaries, so their notices ship too.
  couchbase_install_third_party_licence(abseil-cpp "${grpc_SOURCE_DIR}/third_party/abseil-cpp/LICENSE")
  couchbase_install_third_party_licence(protobuf "${grpc_SOURCE_DIR}/third_party/protobuf/LICENSE")
  couchbase_install_third_party_licence(re2 "${grpc_SOURCE_DIR}/third_party/re2/LICENSE")
  couchbase_install_third_party_licence(c-ares "${grpc_SOURCE_DIR}/third_party/cares/cares/LICENSE.md")
  couchbase_install_third_party_licence(zlib "${grpc_SOURCE_DIR}/third_party/zlib/LICENSE")
  couchbase_install_third_party_licence(utf8_range
                                        "${grpc_SOURCE_DIR}/third_party/protobuf/third_party/utf8_range/LICENSE")
endif()
if(api_common_protos_SOURCE_DIR)
  couchbase_install_third_party_licence(api-common-protos "${api_common_protos_SOURCE_DIR}/LICENSE")
endif()

# The SDK's own bundled dependencies. These are compiled into the client library in every
# configuration, and since the tools packages stopped depending on the library package they are
# carried inside cbc and fit_performer as well, so the same obligation applies to those packages.
couchbase_install_third_party_licence(spdlog "${spdlog_SOURCE_DIR}/LICENSE")
couchbase_install_third_party_licence(fmt "${fmt_SOURCE_DIR}/LICENSE.txt"
                                      "${spdlog_SOURCE_DIR}/include/spdlog/fmt/bundled/fmt.license.rst")
couchbase_install_third_party_licence(hdr_histogram "${hdr_histogram_SOURCE_DIR}/COPYING.txt")
couchbase_install_third_party_licence(snappy "${snappy_SOURCE_DIR}/COPYING")
couchbase_install_third_party_licence(llhttp "${llhttp_SOURCE_DIR}/LICENSE-MIT")
couchbase_install_third_party_licence(json "${json_SOURCE_DIR}/LICENSE")
couchbase_install_third_party_licence(gsl "${gsl_SOURCE_DIR}/LICENSE")
# Only fetched when the tools are built, and only cbc contains it.
if(COUCHBASE_CXX_CLIENT_BUILD_TOOLS)
  couchbase_install_third_party_licence(cli11 "${cli11_SOURCE_DIR}/LICENSE")
endif()
couchbase_install_third_party_licence(asio "${asio_SOURCE_DIR}/asio/LICENSE_1_0.txt")
# Checked into this repository rather than fetched, so their paths are fixed.
couchbase_install_third_party_licence(expected "${PROJECT_SOURCE_DIR}/third_party/expected/COPYING")
couchbase_install_third_party_licence(jsonsl "${PROJECT_SOURCE_DIR}/third_party/jsonsl/LICENSE")

set(COUCHBASE_CXX_CLIENT_PKGCONFIG_VERSION
    "${COUCHBASE_CXX_CLIENT_SEMVER}"
    CACHE STRING "The version to use in couchbase_cxx_client.pc")

write_basic_package_version_file(
  couchbase_cxx_client-version.cmake
  VERSION ${couchbase_cxx_client_VERSION}
  COMPATIBILITY SameMinorVersion)
install(FILES ${PROJECT_BINARY_DIR}/couchbase_cxx_client-version.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)

if(COUCHBASE_CXX_CLIENT_BUILD_TOOLS)
  install(TARGETS cbc RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  # fit_performer is packaged separately (couchbase-cxx-client-fit-performer) so nothing
  # depends on it automatically; it is built against the shared SDK for integration testing.
  if(TARGET fit_performer)
    install(TARGETS fit_performer RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()
endif()

if(COUCHBASE_CXX_CLIENT_BUILD_STATIC)
  get_target_property(couchbase_cxx_client_static_IMPORTED_LOCATION couchbase_cxx_client_static IMPORTED_LOCATION)

  # What the installed archive still needs from outside itself.
  #
  # cmake/Bundler.cmake archives STATIC_LIBRARY dependencies and nothing else, and an IMPORTED target
  # carries no link interface of its own, so every dependency resolved as a shared library stays
  # outside the archive. Publishing them here is what stops a consumer linking it from hitting
  # unresolved symbols.
  #
  # Which ones those are is a property of the configuration, not a fixed list. The packaging
  # configuration bundles everything and needs only the platform libraries -- which is also what
  # cbc's own link line carries -- while an install built against the platform's gRPC or OpenSSL
  # needs those named too. The package targets are therefore taken from the graph below; only the
  # platform libraries, which are not targets at all, stay conditional on the platform.
  set(_static_deps "find_dependency(Threads)")
  set(_static_libs "Threads::Threads")
  # Threads::Threads carries the platform's thread flag; pkg-config has no equivalent, so the flag
  # itself goes into Libs.private. It is empty where pthread lives in libc, which is every platform
  # this currently builds on except EL8, where it is -lpthread.
  set(_static_pc "${CMAKE_THREAD_LIBS_INIT}")

  # Which package targets to name is decided by the link graph, not by re-deriving the option
  # combinations that produced it.
  #
  # Five review rounds were spent adding one more entry to a hand-written list, and a sixth found
  # that zlib reaches the archive through OpenSSL's interface as well as through gRPC -- so a
  # condition of "couchbase2 is on" was wrong in a configuration nobody had considered.
  # cmake/Bundler.cmake already walks the graph to decide what to archive; what it declined to
  # archive is exactly what has to be named here, so that list is the input.
  #
  # The mapping from a target to the find_dependency that recreates it stays explicit, because it is
  # not mechanical: the package providing protobuf::libprotobuf is called Protobuf, the version
  # floor is the one cmake/GrpcProtobuf.cmake enforces, and the pkg-config fallback in
  # cmake/OpenSSL.cmake has no find_dependency form at all.
  get_property(_bundle_external GLOBAL PROPERTY couchbase_cxx_client_static_EXTERNAL_LIBS)
  get_property(_bundle_opts GLOBAL PROPERTY couchbase_cxx_client_static_EXTERNAL_OPTS)

  if("ZLIB::ZLIB" IN_LIST _bundle_external)
    string(APPEND _static_deps "\nfind_dependency(ZLIB)")
    list(APPEND _static_libs "ZLIB::ZLIB")
    string(APPEND _static_pc " -lz")
  endif()

  # Without COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL the client links the platform's OpenSSL, PUBLIC and
  # shared. BoringSSL itself is a pair of STATIC_LIBRARY targets and is archived, so it needs nothing
  # here, and COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL links no TLS at all -- both of which the graph
  # states directly rather than by inference.
  #
  # cmake/OpenSSL.cmake falls back to pkg-config when CMake's own OpenSSL targets are unusable, and
  # the client target in CMakeLists.txt then links PkgConfig::PKG_CONFIG_OPENSSL. Exporting
  # find_dependency(OpenSSL) in that case would hand the consumer back exactly the targets the build
  # rejected, so the fallback is exported as the fallback.
  set(_static_pc_requires)
  if("PkgConfig::PKG_CONFIG_OPENSSL" IN_LIST _bundle_external)
    # The same module the build resolved, not "openssl": on a host where only openssl11 exists --
    # which is the reason this fallback is taken at all -- asking for the wrong one either fails or
    # silently selects a different TLS installation than the archive was compiled against.
    string(
      APPEND
      _static_deps
      "\nfind_dependency(PkgConfig)\npkg_check_modules(PKG_CONFIG_OPENSSL REQUIRED IMPORTED_TARGET ${COUCHBASE_CXX_CLIENT_OPENSSL_PKGCONFIG_MODULE})"
    )
    list(APPEND _static_libs "PkgConfig::PKG_CONFIG_OPENSSL")
    # Requires.private rather than -lssl -lcrypto, so pkg-config resolves the module and carries its
    # library directories and flags across; flattening it to two names drops both.
    list(APPEND _static_pc_requires "${COUCHBASE_CXX_CLIENT_OPENSSL_PKGCONFIG_MODULE}")
  elseif("OpenSSL::SSL" IN_LIST _bundle_external OR "OpenSSL::Crypto" IN_LIST _bundle_external)
    string(APPEND _static_deps "\nfind_dependency(OpenSSL)")
    list(APPEND _static_libs "OpenSSL::SSL" "OpenSSL::Crypto")
    string(APPEND _static_pc " -lssl -lcrypto")
  endif()

  # The generated stubs are archived; the gRPC and Protobuf they call are not, whenever
  # cmake/GrpcProtobuf.cmake resolved them from the platform. Keyed on the graph, this is
  # automatically silent when gRPC was resolved only for the FIT performer, which is a separate
  # executable and puts nothing in this archive.
  if("gRPC::grpc++" IN_LIST _bundle_external OR "grpc++" IN_LIST _bundle_external)
    string(APPEND _static_deps "\nfind_dependency(gRPC)\nfind_dependency(Protobuf 3.15)")
    list(APPEND _static_libs "gRPC::grpc++" "protobuf::libprotobuf")
    string(APPEND _static_pc " -lgrpc++ -lgrpc -lprotobuf")
  endif()

  # Every plain link item the graph carries: Windows import libraries (iphlpapi for GetNetworkParams,
  # bcrypt for the BCrypt* calls in core/crypto, ws2_32 and crypt32 from a source-built gRPC),
  # libexecinfo where backtrace() is not in libc, and whatever else a subdirectory contributed.
  #
  # Taken from the graph rather than written out per platform, because the hand-written version was
  # wrong in a new way every round -- keyed on MINGW when MSVC needed it too, or simply missing an
  # entry contributed somewhere nobody thought to look.
  #
  # Published by NAME: cmake/CompilerOptions.cmake and gRPC both contribute find_library results, and
  # an absolute path from the build host means nothing on a consumer's machine.
  foreach(_ext IN LISTS _bundle_external)
    if(TARGET ${_ext})
      continue()
    endif()
    get_filename_component(_ext_name "${_ext}" NAME_WE)
    string(REGEX REPLACE "^lib" "" _ext_name "${_ext_name}")
    string(REGEX REPLACE "^-l" "" _ext_name "${_ext_name}")
    if(_ext_name AND NOT _ext_name IN_LIST _static_libs)
      list(APPEND _static_libs "${_ext_name}")
      string(APPEND _static_pc " -l${_ext_name}")
    endif()
  endforeach()

  # bcrypt cannot be taken from the graph. core/crypto/cbcrypto.cc calls BCryptOpenAlgorithmProvider
  # under MSVC, but it is compiled straight into the client from the source list -- the
  # core/crypto/CMakeLists.txt that links bcrypt.lib is never added by any add_subdirectory, so that
  # link never happens. Whether bcrypt reaches the graph at all therefore depends on some other
  # dependency happening to pull it in, which is not something to leave a shipped package resting on.
  if(WIN32 AND NOT bcrypt IN_LIST _static_libs)
    list(APPEND _static_libs bcrypt)
    string(APPEND _static_pc " -lbcrypt")
  endif()

  # The three below are not in the graph as link items on every platform -- libc supplies them and
  # the toolchain adds them implicitly -- but a consumer linking a bare archive gets no such help.
  # Deduplicated against the graph above, so naming one twice costs nothing while missing one breaks
  # the package.
  if(CMAKE_DL_LIBS AND NOT CMAKE_DL_LIBS IN_LIST _static_libs)
    list(APPEND _static_libs "${CMAKE_DL_LIBS}")
    string(APPEND _static_pc " -l${CMAKE_DL_LIBS}")
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    foreach(_libc_extra m rt)
      if(NOT _libc_extra IN_LIST _static_libs)
        list(APPEND _static_libs ${_libc_extra})
        string(APPEND _static_pc " -l${_libc_extra}")
      endif()
    endforeach()
  endif()

  # Audit everything published above against the whole graph the bundler walked.
  #
  # The package targets come from that graph, but the platform libraries above are still written by
  # hand, and that half is where the misses were: bcrypt from core/crypto, iphlpapi from the client
  # target, libexecinfo from CompilerOptions -- each contributed by a subdirectory nobody thought to
  # enumerate, and each found by a reviewer rather than by the build. This closes that loop:
  # cmake/Bundler.cmake records every link item it did NOT archive, and anything unrepresented is
  # reported at configure time.
  #
  # A warning, not an error: the mapping from a link item to the name a consumer needs is not always
  # one to one -- a namespaced target and its find_dependency package differ (protobuf::libprotobuf
  # comes from Protobuf), and header-only interface targets contribute nothing to link. Judgement
  # belongs to whoever reads it, so this reports and does not guess.
  # Reduce both sides to bare lowercase names, so a path, an -lfoo, a foo.lib and a namespaced
  # target all compare alike. Compared as list members rather than by regex: a target name is not a
  # pattern, and gRPC::grpc++ is not even a valid one.
  set(_published_names)
  foreach(_pub IN LISTS _static_libs)
    string(REGEX REPLACE "^.*::" "" _pub "${_pub}")
    string(TOLOWER "${_pub}" _pub)
    list(APPEND _published_names "${_pub}")
  endforeach()
  string(REPLACE " " ";" _pc_items "${_static_pc}")
  foreach(_pub IN LISTS _pc_items)
    string(REGEX REPLACE "^-l" "" _pub "${_pub}")
    string(TOLOWER "${_pub}" _pub)
    list(APPEND _published_names "${_pub}")
  endforeach()

  set(_unrepresented)
  foreach(_ext IN LISTS _bundle_external)
    if(_ext MATCHES "::")
      string(REGEX REPLACE "^.*::" "" _ext_name "${_ext}")
    else()
      get_filename_component(_ext_name "${_ext}" NAME_WE)
      string(REGEX REPLACE "^lib" "" _ext_name "${_ext_name}")
      string(REGEX REPLACE "^-l" "" _ext_name "${_ext_name}")
    endif()
    string(TOLOWER "${_ext_name}" _ext_name)
    if(_ext_name AND NOT _ext_name IN_LIST _published_names)
      list(APPEND _unrepresented "${_ext}")
    endif()
  endforeach()
  if(_unrepresented)
    message(
      WARNING
        "The installed static package does not name these dependencies, which cmake/Bundler.cmake "
        "left outside the archive: ${_unrepresented}. A consumer linking "
        "libcouchbase_cxx_client_static.a may get unresolved symbols from them. Add them in "
        "cmake/Packaging.cmake, or record why they need no entry.")
  endif()

  # Link options the archive was built with, kept apart from the libraries because pkg-config wants
  # them verbatim in Libs.private and CMake wants them as INTERFACE_LINK_OPTIONS. A sanitizer build
  # is the case that matters: the objects carry references into the sanitizer runtime, and a
  # consumer linking without the flag gets undefined symbols with no indication why.
  list(JOIN _bundle_opts ";" couchbase_cxx_client_static_LINK_OPTIONS)
  foreach(_opt IN LISTS _bundle_opts)
    string(APPEND _static_pc " ${_opt}")
  endforeach()

  list(JOIN _static_pc_requires ", " couchbase_cxx_client_static_PC_REQUIRES_PRIVATE)

  set(couchbase_cxx_client_static_FIND_DEPENDENCIES "${_static_deps}")
  list(JOIN _static_libs ";" couchbase_cxx_client_static_LINK_LIBRARIES)
  string(STRIP "${_static_pc}" couchbase_cxx_client_static_PC_LIBS_PRIVATE)

  configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client_static-config.cmake.in
    ${PROJECT_BINARY_DIR}/couchbase_cxx_client_static-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client_static)
  install(FILES ${PROJECT_BINARY_DIR}/couchbase_cxx_client_static-config.cmake
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client_static)

  configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client_static.pc.in
                 ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client_static.pc @ONLY)
  install(FILES ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client_static.pc
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

  install(FILES ${couchbase_cxx_client_static_IMPORTED_LOCATION} DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

if(COUCHBASE_CXX_CLIENT_BUILD_SHARED)
  configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client.pc.in
                 ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client.pc @ONLY)
  install(FILES ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
  configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client-config.cmake.in
    ${PROJECT_BINARY_DIR}/couchbase_cxx_client-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)
  install(FILES ${PROJECT_BINARY_DIR}/couchbase_cxx_client-config.cmake
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)

  install(
    TARGETS couchbase_cxx_client
    EXPORT couchbase_cxx_client-targets
    DESTINATION ${CMAKE_INSTALL_LIBDIR})

  install(
    EXPORT couchbase_cxx_client-targets
    NAMESPACE couchbase_cxx_client::
    FILE couchbase_cxx_client-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)
endif()

set(COUCHBASE_CXX_CLIENT_TARBALL_NAME "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_SEMVER}")
set(COUCHBASE_CXX_CLIENT_TARBALL "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz")
set(COUCHBASE_CXX_CLIENT_MANIFEST "${PROJECT_BINARY_DIR}/packaging/MANIFEST")

if(APPLE)
  find_program(TAR gtar)
  find_program(SED gsed)
  find_program(XARGS gxargs)
  find_program(CP gcp)
else()
  find_program(TAR tar)
  find_program(SED sed)
  find_program(XARGS xargs)
  find_program(CP cp)
endif()

add_custom_command(
  OUTPUT ${COUCHBASE_CXX_CLIENT_MANIFEST}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  # Fail loudly if this is not a git checkout (e.g. a bare jj workspace with no .git),
  # instead of silently emitting an empty manifest and a broken empty tarball.
  COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/require-git-checkout.cmake
  COMMAND git ls-files --recurse-submodules | LC_ALL=C sort > ${COUCHBASE_CXX_CLIENT_MANIFEST})

set(COUCHBASE_CXX_TARBALL_THIRD_PARTY_GLOB_FILE ${PROJECT_SOURCE_DIR}/cmake/tarball_glob.txt)

if(COUCHBASE_CXX_RECORD_BUILD_INFO_FOR_TARBALL)
  if(CPM_USE_NAMED_CACHE_DIRECTORIES)
    get_filename_component(opentelemetry_SOURCE_DIR_PARENT "${opentelemetry_SOURCE_DIR}" DIRECTORY)
    get_filename_component(opentelemetry_CPM_HASH "${opentelemetry_SOURCE_DIR_PARENT}" NAME)
  else()
    get_filename_component(opentelemetry_CPM_HASH "${opentelemetry_SOURCE_DIR}" NAME)
  endif()
  file(
    WRITE "${CMAKE_SOURCE_DIR}/cmake/TarballRelease.cmake"
    "
set(CPM_DOWNLOAD_ALL OFF CACHE BOOL \"\" FORCE)
set(CPM_USE_NAMED_CACHE_DIRECTORIES ON CACHE BOOL \"\" FORCE)
set(CPM_USE_LOCAL_PACKAGES OFF CACHE BOOL \"\" FORCE)
set(CPM_SOURCE_CACHE \"\${PROJECT_SOURCE_DIR}/third_party_cache\" CACHE STRING \"\" FORCE)
set(OTELCPP_PROTO_PATH \"\${PROJECT_SOURCE_DIR}/third_party_cache/opentelemetry/${opentelemetry_CPM_HASH}/opentelemetry/third_party/opentelemetry-proto\" CACHE STRING \"\" FORCE)
set(COUCHBASE_CXX_CLIENT_GIT_REVISION \"${COUCHBASE_CXX_CLIENT_GIT_REVISION}\")
set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE \"${COUCHBASE_CXX_CLIENT_GIT_DESCRIBE}\")
set(COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP \"${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}\")
set(COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH \"${COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH}\")
set(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT \"\${PROJECT_SOURCE_DIR}/third_party_cache\" CACHE STRING \"\" FORCE)
message(STATUS \"Building from Tarball: ${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz\")
")
endif()

add_custom_command(
  OUTPUT ${COUCHBASE_CXX_CLIENT_TARBALL}
  WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${TAR} -cf - -C ${PROJECT_SOURCE_DIR} -T ${COUCHBASE_CXX_CLIENT_MANIFEST} | ${TAR} xf - -C
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND
    ${CMAKE_COMMAND} -S "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}" -B
    "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/build"
    -DCPM_SOURCE_CACHE="${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    -DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT="${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF -DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=ON -DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF
    -DCOUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY=ON
    # couchbase2:// pulls gRPC, protobuf, abseil, re2, c-ares and the Protostellar schema. This
    # configure exists to populate third_party_cache, so it must enable every option any consumer of
    # the tarball may build with -- an option left OFF here is a dependency missing from the cache,
    # and a from-tarball build with it ON then reaches the network (or fails, when offline).
    # PACKAGE_BUILD for the same reason: without it gRPC resolves from the platform's packages on a
    # host that has them, and its sources never enter the cache the packages build from.
    -DCOUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2=ON -DCOUCHBASE_CXX_CLIENT_PACKAGE_BUILD=ON
    -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON -DCPM_DOWNLOAD_ALL=ON -DCPM_USE_NAMED_CACHE_DIRECTORIES=ON
    -DCPM_USE_LOCAL_PACKAGES=OFF -DCOUCHBASE_CXX_CLIENT_BUILD_STATIC=ON -DCOUCHBASE_CXX_CLIENT_BUILD_SHARED=ON
    -DCOUCHBASE_CXX_CLIENT_INSTALL=ON -DCOUCHBASE_CXX_RECORD_BUILD_INFO_FOR_TARBALL=ON
    # Pass the frozen instant explicitly: this inner configure runs at build time against a
    # .git-less extracted copy, so without these it would fall back to wall-clock for an
    # out-of-source build (build dir outside the worktree, where git cannot walk up to .git).
    "-DCOUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH=${COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH}"
    "-DCOUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP=${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}"
  COMMAND
    # -d: one pattern per line, with quote processing off. Without it an apostrophe anywhere in the
    # glob file -- a comment is enough -- aborts xargs, and since its status is hidden by the pipe
    # below, every pattern after that line is dropped from a tarball that still reports success.
    # The argument has to reach xargs as backslash-n, which takes four backslashes here: CMake
    # collapses them to two, and the shell that ninja runs the command in collapses those to one.
    ${XARGS} -d "\\\\n" -a ${COUCHBASE_CXX_TARBALL_THIRD_PARTY_GLOB_FILE} -I {} find
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache" -wholename "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache/{}"
    -type f
    | grep -v
        -e "/third_party/benchmark/"
        -e "/grpc/doc/"
        -e "/grpc/examples/"
        -e "/grpc/src/android"
        -e "/grpc/src/csharp"
        -e "/grpc/src/objective-c"
        -e "/grpc/src/php"
        -e "/grpc/src/python"
        -e "/grpc/src/ruby"
        -e "/grpc/test/.*\\.cc"
        -e "/grpc/tools/"
        -e "/protobuf/conformance"
        -e "/protobuf/csharp"
        -e "/protobuf/examples"
        -e "/protobuf/java"
        -e "/protobuf/objectivec"
        -e "/protobuf/php"
        -e "/protobuf/python"
        -e "/protobuf/ruby"
        -e "/protobuf/rust"
        -e "/opentelemetry.*/functional"
        -e "/opentelemetry.*/install"
        -e "/opentelemetry.*/test"
        -e "/opentelemetry/examples"
        -e "/opentelemetry/docker"
        -e "/opentelemetry/exporters/elasticsearch"
        -e "/opentelemetry/exporters/etw"
        -e "/opentelemetry/exporters/prometheus"
        -e "/opentelemetry/exporters/zipkin"
        -e "/opentelemetry/opentracing-.*"
        -e "/opentelemetry/third_party/ms-gsl"
        -e "/opentelemetry/third_party/nlohmann-json"
        -e "/opentelemetry/third_party/prometheus-cpp"
        -e "/opentelemetry/tools"
        -e "crypto_test_data"
        -e "googletest"
    | LC_ALL=C sort -u >
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/third_party_manifest.txt"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache"
  COMMAND ${XARGS} -d "\\\\n" -a "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/third_party_manifest.txt" -I {} ${CP}
          --parents
          {} "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache"
  COMMAND
    ${CMAKE_COMMAND} -E rename
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/third_party_cache"
  COMMAND ${SED} -i "s/VERSION 3.25.0/VERSION 3.22.0/g"
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/third_party_cache/llhttp/*/llhttp/CMakeLists.txt"
  COMMAND ${SED} -i "s/Git REQUIRED/Git/g\;s/NOT GIT/NOT CHECK_DIRTY OR NOT GIT/g"
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/third_party_cache/cpm/CPM_*.cmake"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp"
  # https://reproducible-builds.org/docs/archives/
  # --mtime=@<epoch>: timezone-free fixed mtime from the frozen SOURCE_DATE_EPOCH (not a tz-local
  #   string mislabelled with "Z"). --mode: normalize permission bits so the builder's umask does
  #   not leak into directory/file modes. gzip -n: keep the mtime/filename out of the gzip header.
  COMMAND ${TAR} --sort=name "--mtime=@${COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH}" --owner=0 --group=0
          --numeric-owner "--mode=go+u,go-w" "--use-compress-program=gzip -n" -cf
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz" "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  DEPENDS ${COUCHBASE_CXX_CLIENT_MANIFEST})

add_custom_target(packaging_tarball DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL})

option(COUCHBASE_CXX_CLIENT_DEB_TARGETS "Enable targets for building DEBs" FALSE)
if(COUCHBASE_CXX_CLIENT_DEB_TARGETS)
  find_program(DPKG_BUILDPACKAGE dpkg-buildpackage REQUIRED) # apt install -y dpkg-dev
  find_program(SBUILD sbuild REQUIRED) # apt install -y sbuild
  find_program(MMDEBSTRAP mmdebstrap REQUIRED) # apt install -y mmdebstrap

  # sbuild builds each distro inside a throwaway chroot that mmdebstrap creates rootlessly
  # (unshare mode, no sudo required). mmdebstrap and sbuild expect the Debian host
  # architecture (e.g. amd64, arm64), which differs from CMAKE_SYSTEM_PROCESSOR (x86_64,
  # aarch64) that is used elsewhere for the human-readable result directory names.
  execute_process(
    COMMAND dpkg --print-architecture
    RESULT_VARIABLE _dpkg_arch_result
    OUTPUT_VARIABLE COUCHBASE_CXX_CLIENT_DEB_HOST_ARCH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT _dpkg_arch_result EQUAL 0 OR NOT COUCHBASE_CXX_CLIENT_DEB_HOST_ARCH)
    message(FATAL_ERROR "Failed to determine the Debian host architecture via 'dpkg --print-architecture'")
  endif()

  string(TIMESTAMP COUCHBASE_CXX_CLIENT_DEB_DATE "%a, %d %b %Y %H:%M:%S %z" UTC)

  file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/packaging/workspace/debian/source/")

  set(COUCHBASE_CXX_CLIENT_DEBIAN_CHANGELOG "${PROJECT_BINARY_DIR}/packaging/workspace/debian/changelog")
  configure_file(${PROJECT_SOURCE_DIR}/cmake/debian/changelog.in "${COUCHBASE_CXX_CLIENT_DEBIAN_CHANGELOG}" @ONLY)

  file(COPY ${PROJECT_SOURCE_DIR}/cmake/debian/compat ${PROJECT_SOURCE_DIR}/cmake/debian/control
            ${PROJECT_SOURCE_DIR}/cmake/debian/rules DESTINATION "${PROJECT_BINARY_DIR}/packaging/workspace/debian/")
  file(COPY ${PROJECT_SOURCE_DIR}/cmake/debian/source/format ${PROJECT_SOURCE_DIR}/cmake/debian/source/options
       DESTINATION "${PROJECT_BINARY_DIR}/packaging/workspace/debian/source/")

  set(COUCHBASE_CXX_CLIENT_DEBIAN_ORIG_TARBALL
      "${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client_${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}.orig.tar.gz")
  add_custom_command(
    OUTPUT ${COUCHBASE_CXX_CLIENT_DEBIAN_ORIG_TARBALL}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
    COMMAND ${CMAKE_COMMAND} -E copy "${COUCHBASE_CXX_CLIENT_TARBALL}" "${COUCHBASE_CXX_CLIENT_DEBIAN_ORIG_TARBALL}"
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL})

  set(COUCHBASE_CXX_CLIENT_DEBIAN_TARBALL_EXTRACTED "${PROJECT_BINARY_DIR}/packaging/tarball_extracted.txt")
  add_custom_command(
    OUTPUT ${COUCHBASE_CXX_CLIENT_DEBIAN_TARBALL_EXTRACTED}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging/workspace"
    COMMAND ${TAR} --strip-components=1 -xf "${COUCHBASE_CXX_CLIENT_TARBALL}"
    COMMAND touch ${COUCHBASE_CXX_CLIENT_DEBIAN_TARBALL_EXTRACTED}
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL})

  set(COUCHBASE_CXX_CLIENT_DEBIAN_DSC
      "${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client_${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}-${COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE}.dsc"
  )
  add_custom_command(
    OUTPUT ${COUCHBASE_CXX_CLIENT_DEBIAN_DSC}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging/workspace"
    # Build the source package only (-S) and skip the host build-dependency check (-d): the
    # actual compilation happens inside the sbuild chroot, so the host needs only dpkg-dev.
    COMMAND ${DPKG_BUILDPACKAGE} -S -us -uc -d
    DEPENDS ${COUCHBASE_CXX_CLIENT_DEBIAN_ORIG_TARBALL} ${COUCHBASE_CXX_CLIENT_DEBIAN_TARBALL_EXTRACTED})

  # Per-distribution apt source used to bootstrap the build chroot: the mirror, the keyring
  # that verifies it, the package that installs that keyring inside the chroot (so the
  # sources.list signed-by reference resolves), and the components to enable.
  function(select_apt_source distro out_mirror out_keyring out_keyring_pkg out_components)
    if("${distro}" STREQUAL "kali-rolling")
      set(${out_mirror} "http://http.kali.org/kali" PARENT_SCOPE)
      set(${out_keyring} "/usr/share/keyrings/kali-archive-keyring.gpg" PARENT_SCOPE)
      set(${out_keyring_pkg} "kali-archive-keyring" PARENT_SCOPE)
      set(${out_components} "main" PARENT_SCOPE)
    elseif("${distro}" STREQUAL "bookworm" OR "${distro}" STREQUAL "trixie")
      # http (not https): the --variant=buildd chroot has no ca-certificates, so in-chroot apt over
      # TLS would fail; apt authenticates packages by GPG signature regardless of transport.
      set(${out_mirror} "http://deb.debian.org/debian" PARENT_SCOPE)
      set(${out_keyring} "/usr/share/keyrings/debian-archive-keyring.gpg" PARENT_SCOPE)
      set(${out_keyring_pkg} "debian-archive-keyring" PARENT_SCOPE)
      set(${out_components} "main" PARENT_SCOPE)
    elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
      set(${out_mirror} "http://ports.ubuntu.com/ubuntu-ports" PARENT_SCOPE)
      set(${out_keyring} "/usr/share/keyrings/ubuntu-archive-keyring.gpg" PARENT_SCOPE)
      set(${out_keyring_pkg} "ubuntu-keyring" PARENT_SCOPE)
      set(${out_components} "main universe" PARENT_SCOPE)
    else()
      set(${out_mirror} "http://archive.ubuntu.com/ubuntu" PARENT_SCOPE)
      set(${out_keyring} "/usr/share/keyrings/ubuntu-archive-keyring.gpg" PARENT_SCOPE)
      set(${out_keyring_pkg} "ubuntu-keyring" PARENT_SCOPE)
      set(${out_components} "main universe" PARENT_SCOPE)
    endif()
  endfunction()

  set(COUCHBASE_CXX_CLIENT_SUPPORTED_DISTROS
      "jammy"
      "noble"
      "resolute"
      "bookworm"
      "trixie"
      "kali-rolling"
      CACHE STRING "Semicolon-separated list of distributions to build DEB packages for")

  message(STATUS "Supported distributions for DEB packages: ${COUCHBASE_CXX_CLIENT_SUPPORTED_DISTROS}")

  set(sbuild_results "${PROJECT_BINARY_DIR}/packaging/results")
  set(sbuild_chroots "${PROJECT_BINARY_DIR}/packaging/chroots")
  file(MAKE_DIRECTORY "${sbuild_results}")
  file(MAKE_DIRECTORY "${sbuild_chroots}")

  # Build the distros one at a time (chained through last_output) so several full SDK
  # compilations do not hammer a single machine simultaneously.
  set(last_output "")
  foreach(distro ${COUCHBASE_CXX_CLIENT_SUPPORTED_DISTROS})
    if(distro STREQUAL "")
      continue() # tolerate a trailing ';' or empty entry in the overridable distro list
    endif()
    select_apt_source("${distro}" mirror keyring keyring_pkg components)
    # mmdebstrap verifies the bootstrap against this keyring, which must exist on the build host.
    # Fail early with an actionable message instead of a cryptic mmdebstrap error mid-build.
    if(NOT EXISTS "${keyring}")
      message(FATAL_ERROR "Keyring ${keyring} needed to build '${distro}' DEBs is missing; "
                          "install the '${keyring_pkg}' package on the build host.")
    endif()
    set(timestamp "${PROJECT_BINARY_DIR}/packaging/${distro}_done.txt")
    set(chroot_tarball "${sbuild_chroots}/${distro}-${COUCHBASE_CXX_CLIENT_DEB_HOST_ARCH}.tar.zst")
    set(distro_results
        "${sbuild_results}/couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}-${COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE}.${distro}.${CMAKE_SYSTEM_PROCESSOR}"
    )
    set(dependencies ${COUCHBASE_CXX_CLIENT_DEBIAN_DSC})

    if(last_output)
      list(APPEND dependencies ${last_output})
    endif()

    add_custom_command(
      COMMENT "Building DEB for ${distro}"
      OUTPUT ${timestamp}
      WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${distro_results}" "${sbuild_chroots}"
      COMMAND ${CMAKE_COMMAND} -E rm -f "${chroot_tarball}"
      COMMAND ${MMDEBSTRAP} --variant=buildd --arch=${COUCHBASE_CXX_CLIENT_DEB_HOST_ARCH}
              "--include=${keyring_pkg}" "${distro}" "${chroot_tarball}"
              "deb [signed-by=${keyring}] ${mirror} ${distro} ${components}"
      COMMAND
        ${SBUILD} --chroot-mode=unshare --dist=${distro} --arch=${COUCHBASE_CXX_CLIENT_DEB_HOST_ARCH}
        "--chroot=${chroot_tarball}" "--build-dir=${distro_results}" --no-run-lintian --no-run-piuparts
        --no-run-autopkgtest -j8 "${COUCHBASE_CXX_CLIENT_DEBIAN_DSC}"
      COMMAND touch ${timestamp}
      DEPENDS ${dependencies})

    set(last_output ${timestamp})
  endforeach()

  add_custom_target(packaging_deb DEPENDS ${last_output})
endif()

option(COUCHBASE_CXX_CLIENT_RPM_TARGETS "Enable targets for building RPMs" FALSE)
if(COUCHBASE_CXX_CLIENT_RPM_TARGETS)
  find_program(SPECTOOL spectool REQUIRED) # dnf install -y rpmdevtools

  option(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS
         "Build RPMs by running mock inside a rootless podman container (no mock group/sudo on the host)"
         OFF)
  option(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS_BOOTSTRAP_IMAGE
         "Use mock's podman bootstrap image (needed for cross-distro roots; requires nested podman)"
         OFF)
  if(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS)
    find_program(PODMAN podman REQUIRED) # dnf install -y podman
    # Fail early (like the DEB keyring check) if podman is not usable rootless.
    execute_process(
      COMMAND ${PODMAN} info --format "{{.Host.Security.Rootless}}"
      RESULT_VARIABLE _podman_info_result
      OUTPUT_VARIABLE _podman_rootless
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_VARIABLE _podman_info_error)
    if(NOT _podman_info_result EQUAL 0 OR NOT "${_podman_rootless}" STREQUAL "true")
      message(FATAL_ERROR
        "COUCHBASE_CXX_CLIENT_RPM_ROOTLESS=ON requires a working rootless podman "
        "('podman info' must report Host.Security.Rootless=true). "
        "Install podman and configure /etc/subuid and /etc/subgid for your user, or set the toggle OFF.\n"
        "podman reported: ${_podman_info_error}")
    endif()
    set(COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE "registry.fedoraproject.org/fedora:44"
        CACHE STRING "Base image for the rootless RPM builder (pin by digest for reproducibility)")
    set(COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE "localhost/cxxcbc-rpm-builder:fedora-44"
        CACHE STRING "Tag of the locally built rootless RPM builder image")
    set(COUCHBASE_CXX_CLIENT_RPM_BUILDER_STAMP "${PROJECT_BINARY_DIR}/packaging/rpm-builder-image.stamp")
    add_custom_command(
      OUTPUT ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_STAMP}
      COMMAND ${PODMAN} build --build-arg "BASE=${COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE}"
              -t ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE}
              -f ${PROJECT_SOURCE_DIR}/cmake/rpm/Containerfile ${PROJECT_SOURCE_DIR}/cmake/rpm
      COMMAND ${CMAKE_COMMAND} -E touch ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_STAMP}
      DEPENDS ${PROJECT_SOURCE_DIR}/cmake/rpm/Containerfile
      COMMENT "Building rootless RPM builder image ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE}")
  else()
    # Only the non-rootless path runs mock on the host; the rootless path runs it inside the
    # builder container, so a host mock is not required there.
    find_program(MOCK mock REQUIRED) # dnf install -y mock
  endif()

  string(TIMESTAMP COUCHBASE_CXX_CLIENT_RPM_DATE "%a %b %d %Y" UTC)

  set(COUCHBASE_CXX_CLIENT_SPEC "${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client.spec")
  configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase-cxx-client.spec.in "${COUCHBASE_CXX_CLIENT_SPEC}" @ONLY)

  set(COUCHBASE_CXX_CLIENT_DEFAULT_ROOT "rocky+epel-9-${CMAKE_SYSTEM_PROCESSOR}")
  set(COUCHBASE_CXX_CLIENT_RPM_NAME
      "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}-${COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE}")
  set(COUCHBASE_CXX_CLIENT_SRPM "${PROJECT_BINARY_DIR}/packaging/srpm/${COUCHBASE_CXX_CLIENT_RPM_NAME}.el9.src.rpm")

  # mock_launcher(<out_var> <resultdir>): expands to the command prefix used to run mock,
  # either directly on the host (default) or inside a rootless podman container. The wrapper
  # forwards all mock arguments unchanged, so the two paths are argument-compatible.
  if(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS AND COUCHBASE_CXX_CLIENT_RPM_ROOTLESS_BOOTSTRAP_IMAGE)
    set(_rpm_use_boot 1)
  else()
    set(_rpm_use_boot 0)
  endif()
  macro(mock_launcher out_var resultdir)
    if(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS)
      set(${out_var}
          ${CMAKE_COMMAND} -E env
          "RPM_PODMAN=${PODMAN}"
          "RPM_BUILDER_IMAGE=${COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE}"
          "RPM_PACKAGING_DIR=${PROJECT_BINARY_DIR}/packaging"
          "MOCK_RESULTDIR=${resultdir}"
          "SOURCE_DATE_EPOCH=${COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH}"
          "MOCK_USE_BOOTSTRAP_IMAGE=${_rpm_use_boot}"
          bash "${PROJECT_SOURCE_DIR}/cmake/rpm/mock-in-podman.sh")
    else()
      set(${out_var} ${MOCK})
    endif()
  endmacro()

  mock_launcher(_srpm_mock "${PROJECT_BINARY_DIR}/packaging/srpm")
  set(_srpm_extra_deps "")
  if(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS)
    set(_srpm_extra_deps ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_STAMP})
  endif()
  add_custom_command(
    OUTPUT ${COUCHBASE_CXX_CLIENT_SRPM}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
    COMMAND ${SPECTOOL} --get-files couchbase-cxx-client.spec
    COMMAND ${_srpm_mock} --buildsrpm --root=${COUCHBASE_CXX_CLIENT_DEFAULT_ROOT}
            --resultdir=${PROJECT_BINARY_DIR}/packaging/srpm --spec couchbase-cxx-client.spec --sources .
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL} ${COUCHBASE_CXX_CLIENT_SPEC} ${_srpm_extra_deps})

  add_custom_target(packaging_srpm DEPENDS ${COUCHBASE_CXX_CLIENT_SRPM})

  set(COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS
      "rocky+epel-10-${CMAKE_SYSTEM_PROCESSOR}"
      "rocky+epel-9-${CMAKE_SYSTEM_PROCESSOR}"
      "rocky+epel-8-${CMAKE_SYSTEM_PROCESSOR}"
      "amazonlinux-2023-${CMAKE_SYSTEM_PROCESSOR}"
      "fedora-44-${CMAKE_SYSTEM_PROCESSOR}"
      "fedora-43-${CMAKE_SYSTEM_PROCESSOR}"
      CACHE STRING "Semicolon-separated list of mock roots to build RPM packages for")

  message(STATUS "Supported build roots for RPM packages: ${COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS}")

  # Build the chain of the dependencies from the timestamps, so that everything will be executed one-by-one in order,
  # because the mock cannot run multiple roots simultaneously
  set(last_output "")
  foreach(root ${COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS})
    set(timestamp "${PROJECT_BINARY_DIR}/packaging/rpm/${root}/done.txt")
    set(dependencies ${COUCHBASE_CXX_CLIENT_SRPM})

    if(last_output)
      list(APPEND dependencies ${last_output})
    endif()
    if(COUCHBASE_CXX_CLIENT_RPM_ROOTLESS)
      list(APPEND dependencies ${COUCHBASE_CXX_CLIENT_RPM_BUILDER_STAMP})
    endif()

    mock_launcher(_root_mock "${PROJECT_BINARY_DIR}/packaging/rpm/${root}")
    add_custom_command(
      COMMENT "Building RPM for ${root}"
      OUTPUT ${timestamp}
      WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
      COMMAND ${_root_mock} --rebuild --root=${root} --resultdir=${PROJECT_BINARY_DIR}/packaging/rpm/${root}
              "${COUCHBASE_CXX_CLIENT_SRPM}"
      COMMAND touch ${timestamp}
      DEPENDS ${dependencies})

    set(last_output ${timestamp})
  endforeach()

  # add target that depends on the last root
  add_custom_target(packaging_rpm DEPENDS ${last_output})
endif()

option(COUCHBASE_CXX_CLIENT_APK_TARGETS "Enable targets for building APKs (for Alpine Linux)" FALSE)
if(COUCHBASE_CXX_CLIENT_APK_TARGETS)
  find_program(ABUILD abuild REQUIRED) # apk add alpine-sdk

  set(COUCHBASE_CXX_CLIENT_TARBALL_NAME_ALPINE "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}")
  set(COUCHBASE_CXX_CLIENT_TARBALL_ALPINE
      "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME_ALPINE}.tar.gz")
  if(${COUCHBASE_CXX_CLIENT_NUMBER_OF_COMMITS} GREATER 0)
    # Encode commit count and git hash into _p version for uniqueness
    # Extract first 3 bytes from git hash (7-char short hash), convert to decimal
    # Map each byte mod 100 to create 6-digit suffix
    # Formula: p = commits × 10_000_000 + (byte1%100)*10000 + (byte2%100)*100 + (byte3%100)
    # Example: 1.2.0-75-gfeb729b2 → 1.2.0_p750548341
    string(SUBSTRING "${COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT}" 0 2 _b1_hex)
    string(SUBSTRING "${COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT}" 2 2 _b2_hex)
    string(SUBSTRING "${COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT}" 4 2 _b3_hex)
    math(EXPR _b1 "0x${_b1_hex}")
    math(EXPR _b2 "0x${_b2_hex}")
    math(EXPR _b3 "0x${_b3_hex}")
    math(EXPR _sha_pack "(${_b1} % 100) * 10000 + (${_b2} % 100) * 100 + (${_b3} % 100)")
    math(EXPR _p_version "${COUCHBASE_CXX_CLIENT_NUMBER_OF_COMMITS} * 10000000 + ${_sha_pack}")
    set(COUCHBASE_CXX_CLIENT_TARBALL_NAME_ALPINE
        "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}_p${_p_version}")
    set(COUCHBASE_CXX_CLIENT_TARBALL_ALPINE
        "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME_ALPINE}.tar.gz")
  endif()

  set(cxxcbc_apkbuild_file "${PROJECT_BINARY_DIR}/packaging/APKBUILD")
  configure_file(${PROJECT_SOURCE_DIR}/cmake/APKBUILD.in "${cxxcbc_apkbuild_file}" @ONLY)

  if(NOT
     "${COUCHBASE_CXX_CLIENT_TARBALL}"
     STREQUAL
     "${COUCHBASE_CXX_CLIENT_TARBALL_ALPINE}")
    add_custom_command(
      OUTPUT ${COUCHBASE_CXX_CLIENT_TARBALL_ALPINE}
      WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
      COMMAND ${CMAKE_COMMAND} -E copy "${COUCHBASE_CXX_CLIENT_TARBALL}" "${COUCHBASE_CXX_CLIENT_TARBALL_ALPINE}"
      DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL})
  endif()

  set(cxxcbc_apkbuild_checksum "${PROJECT_BINARY_DIR}/packaging/apk_checksum_updated.txt")
  add_custom_command(
    OUTPUT ${cxxcbc_apkbuild_checksum}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
    COMMAND ${ABUILD} checksum
    COMMAND touch ${cxxcbc_apkbuild_checksum}
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL_ALPINE} ${cxxcbc_apkbuild_file})

  set(cxxcbc_apkbuild_timestamp "${PROJECT_BINARY_DIR}/packaging/apk_timestamp.txt")
  add_custom_command(
    OUTPUT ${cxxcbc_apkbuild_timestamp}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
    # abuild (unlike dpkg/rpm) does not derive SOURCE_DATE_EPOCH from any metadata, so pass the
    # frozen epoch explicitly to clamp .apk mtimes for reproducibility.
    COMMAND ${CMAKE_COMMAND} -E env SOURCE_DATE_EPOCH=${COUCHBASE_CXX_CLIENT_SOURCE_DATE_EPOCH} ${ABUILD} -r
    COMMAND touch ${cxxcbc_apkbuild_timestamp}
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL_ALPINE} ${cxxcbc_apkbuild_checksum})

  add_custom_target(packaging_apk DEPENDS ${cxxcbc_apkbuild_timestamp})
endif()
