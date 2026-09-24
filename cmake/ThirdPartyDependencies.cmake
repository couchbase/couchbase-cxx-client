# NOTE: This file MUST be in sync with couchbase-sdk-cxx-black-duck-manifest.yaml

include(cmake/CPM.cmake)

set(CPM_USE_LOCAL_PACKAGES OFF)

# https://cmake.org/cmake/help/v3.28/policy/CMP0063.html
set(CMAKE_POLICY_DEFAULT_CMP0063 NEW)

function(declare_system_library target)
  get_target_property(target_aliased_name ${target} ALIASED_TARGET)
  if(target_aliased_name)
    set(target ${target_aliased_name})
  endif()
  set_target_properties(${target} PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                                             $<TARGET_PROPERTY:${target},INTERFACE_INCLUDE_DIRECTORIES>)
endfunction()

# Vendored dependencies that declare install rules no option can switch off send them here instead,
# and the packaging install step deletes this one directory. Defined before any of them is added so
# gRPC and curl share it.
set(COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR "vendored-not-installed")

include(cmake/OpenSSL.cmake)

if(NOT TARGET spdlog::spdlog)
  # https://github.com/gabime/spdlog/releases
  cpmaddpackage(
    NAME
    spdlog
    VERSION
    1.15.0
    GITHUB_REPOSITORY
    "gabime/spdlog"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO
    OPTIONS
    "SPDLOG_INSTALL OFF"
    "BUILD_SHARED_LIBS OFF"
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON"
    "SPDLOG_BUILD_SHARED OFF"
    "SPDLOG_FMT_EXTERNAL OFF")
endif()

if((COUCHBASE_CXX_CLIENT_BUILD_TOOLS AND COUCHBASE_CXX_CLIENT_BUILD_FIT_PERFORMER)
   OR COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2)
  # gRPC/Protobuf are pulled in only for gRPC-based components (the FIT performer and the
  # couchbase2:// transport). This MUST come before OpenTelemetry so OTel reuses these targets.
  include(cmake/GrpcProtobuf.cmake)
endif()

if(COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY)
  # OpenTelemetry's OTLP/HTTP exporter needs libcurl, and its cmake/curl.cmake takes the platform's
  # copy when find_package(CURL) succeeds. That copy is linked against the platform's OpenSSL, so it
  # puts a second TLS implementation into every process loading the SDK next to the BoringSSL linked
  # here -- visible as libssl/libcrypto among cbc's dependencies.
  #
  # Declaring curl first is what avoids it. OpenTelemetry declares curl under the name "curl" and
  # then calls FetchContent_MakeAvailable(curl), which does nothing when the name is already
  # populated, and its own alias step is skipped because CURL::libcurl already exists. Its fetch
  # branch is also the wrong shape for packaging: it forces BUILD_SHARED_LIBS ON, producing a
  # libcurl.so this project would have to ship.
  #
  # CMAKE_DISABLE_FIND_PACKAGE_CURL stops it preferring a system libcurl over this one; both of its
  # call sites are QUIET and not REQUIRED, so disabling the search is safe.
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND NOT TARGET CURL::libcurl)
    set(CMAKE_DISABLE_FIND_PACKAGE_CURL ON)

    # Keep the tag in step with the one OpenTelemetry pins in its third_party_release, so the
    # version it expects is the version it gets.
    cpmaddpackage(
      NAME
      curl
      GITHUB_REPOSITORY
      "curl/curl"
      GIT_TAG
      curl-8_12_0
      OPTIONS
      # curl asks find_package(OpenSSL REQUIRED) and then links OpenSSL::SSL and OpenSSL::Crypto by
      # target. Those targets are already BoringSSL aliases, so the only thing missing is a
      # find_package that succeeds without an OpenSSL installation: see
      # cmake/vendored_openssl/FindOpenSSL.cmake. curl appends its own CMake directory to
      # CMAKE_MODULE_PATH, so replacing it here costs nothing.
      "CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/vendored_openssl"
      "COUCHBASE_CXX_CLIENT_BORINGSSL_INCLUDE_DIR ${boringssl_SOURCE_DIR}/src/include"
      "CURL_USE_OPENSSL ON"
      # curl identifies the TLS library, and probes for a few functions, with check_symbol_exists.
      # Those compile a TryCompile project which does NOT inherit ALIAS targets, so the probes
      # cannot resolve OpenSSL::SSL -> BoringSSL and fail outright ("Target links to OpenSSL::SSL
      # but the target was not found"). Every one of them is guarded by if(NOT DEFINED ...), so
      # answering in advance skips them. The answers describe BoringSSL: it is not AWS-LC or
      # LibreSSL, and it does provide SSL_set0_wbio. SRP is not probed at all because
      # CURL_DISABLE_SRP is on below.
      "HAVE_BORINGSSL 1"
      "HAVE_AWSLC 0"
      "HAVE_LIBRESSL 0"
      "HAVE_SSL_SET0_WBIO 1"
      # TLS-SRP probes against <openssl/ssl.h> and would find the platform OpenSSL's declarations
      # while compiling against BoringSSL headers, which omit SRP: curl then fails on implicit
      # declarations of SSL_CTX_set_srp_username. Nothing here uses TLS-SRP.
      "CURL_DISABLE_SRP ON"
      # curl's install(TARGETS libcurl_static EXPORT ...) fails because the BoringSSL targets it
      # links belong to no export set, and nothing here consumes curl's export files.
      "CURL_ENABLE_EXPORT_TARGET OFF"
      # CURL_ENABLE_EXPORT_TARGET only suppresses the CMake export file: curl still installs its
      # archive, its headers, curl-config and libcurl.pc. Send them where gRPC's go, so the single
      # cleanup in the packaging install step covers both.
      "CMAKE_INSTALL_BINDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/bin"
      "CMAKE_INSTALL_LIBDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/lib"
      "CMAKE_INSTALL_INCLUDEDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/include"
      "CMAKE_INSTALL_DATADIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/share"
      "CMAKE_INSTALL_DATAROOTDIR ${COUCHBASE_CXX_CLIENT_VENDORED_INSTALL_JUNK_DIR}/share"
      "BUILD_SHARED_LIBS OFF"
      "BUILD_STATIC_LIBS ON"
      "BUILD_CURL_EXE OFF"
      "BUILD_LIBCURL_DOCS OFF"
      "BUILD_MISC_DOCS OFF"
      "ENABLE_CURL_MANUAL OFF"
      "BUILD_TESTING OFF"
      # Everything below is a dependency the exporter does not need and that would otherwise become
      # a build requirement or another shared library in the package.
      "CURL_USE_LIBPSL OFF"
      "CURL_USE_LIBSSH2 OFF"
      "CURL_USE_LIBSSH OFF"
      "CURL_USE_GSSAPI OFF"
      "USE_LIBIDN2 OFF"
      "CURL_BROTLI OFF"
      "CURL_ZSTD OFF"
      "USE_NGHTTP2 OFF"
      "CURL_DISABLE_LDAP ON"
      "CURL_DISABLE_LDAPS ON"
      "CMAKE_C_VISIBILITY_PRESET hidden"
      "CMAKE_POSITION_INDEPENDENT_CODE ON")

    if(TARGET libcurl_static AND NOT TARGET CURL::libcurl)
      add_library(CURL::libcurl ALIAS libcurl_static)
    endif()
    if(NOT TARGET CURL::libcurl)
      message(FATAL_ERROR "Vendored curl did not provide CURL::libcurl")
    endif()
    declare_system_library(CURL::libcurl)
  endif()

  if(NOT TARGET opentelemetry)
    # These curl settings apply to the configuration that does NOT vendor curl above, where
    # OpenTelemetry resolves the platform's libcurl: they are no-ops against a system package, and
    # they keep the from-source path working for a build that does not use BoringSSL.
    set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
    set(CURL_DISABLE_SRP ON CACHE BOOL "" FORCE)

    # https://github.com/open-telemetry/opentelemetry-cpp/releases
    cpmaddpackage(
      NAME
      opentelemetry
      VERSION
      1.23.0
      GITHUB_REPOSITORY
      "open-telemetry/opentelemetry-cpp"
      EXCLUDE_FROM_ALL ON
      OPTIONS
      "protobuf_MSVC_STATIC_RUNTIME OFF"
      "OPENTELEMETRY_INSTALL OFF"
      "WITH_ABI_VERSION_1 OFF"
      "WITH_ABI_VERSION_2 ON"
      "WITH_BENCHMARK OFF"
      "WITH_EXAMPLES OFF"
      "WITH_FUNC_TESTS OFF"
      "WITH_OTLP_GRPC OFF"
      "WITH_OTLP_HTTP ON"
      "WITH_PROMETHEUS OFF"
      "WITH_OPENTRACING OFF"
      "WITH_STL CXX17"
      "BUILD_TESTING OFF"
      "BUILD_SHARED_LIBS OFF"
      "CMAKE_C_VISIBILITY_PRESET hidden"
      "CMAKE_CXX_VISIBILITY_PRESET hidden"
      "CMAKE_POSITION_INDEPENDENT_CODE ON")
  endif()

  declare_system_library(opentelemetry_exporter_otlp_http)
  declare_system_library(opentelemetry_exporter_otlp_http_metric)
endif()

if(NOT TARGET Microsoft.GSL::GSL)
  # https://github.com/microsoft/GSL/releases
  cpmaddpackage(
    NAME
    gsl
    VERSION
    4.0.0
    GITHUB_REPOSITORY
    "microsoft/gsl"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO
    OPTIONS
    "GSL_INSTALL OFF"
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON")
endif()

if(NOT TARGET hdr_histogram_static)
  # https://github.com/HdrHistogram/HdrHistogram_c/releases
  cpmaddpackage(
    NAME
    hdr_histogram
    GIT_TAG
    0.11.8
    VERSION
    0.11.8
    GITHUB_REPOSITORY
    "HdrHistogram/HdrHistogram_c"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO
    OPTIONS
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON"
    "HDR_LOG_REQUIRED OFF"
    "HDR_HISTOGRAM_BUILD_SHARED OFF"
    "HDR_HISTOGRAM_BUILD_PROGRAMS OFF")
endif()

if(NOT TARGET llhttp::llhttp)
  # https://github.com/nodejs/llhttp/releases
  cpmaddpackage(
    NAME
    llhttp
    GIT_TAG
    release/v9.3.0
    VERSION
    9.3.0
    GITHUB_REPOSITORY
    "nodejs/llhttp"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO
    OPTIONS
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON"
    "BUILD_SHARED_LIBS OFF"
    "BUILD_STATIC_LIBS ON")
endif()


if(NOT TARGET snappy AND DEFINED Snappy_DIR)
  find_package(Snappy QUIET)
  if(TARGET Snappy::snappy)
    add_library(snappy ALIAS Snappy::snappy)
  endif()
endif()

if(NOT TARGET snappy)
  # https://github.com/google/snappy/releases
  cpmaddpackage(
    NAME
    snappy
    GIT_TAG
    1.2.2
    VERSION
    1.2.2
    GITHUB_REPOSITORY
    "google/snappy"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO
    OPTIONS
    "SNAPPY_INSTALL OFF"
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON"
    "BUILD_SHARED_LIBS OFF"
    "SNAPPY_BUILD_TESTS OFF"
    "SNAPPY_BUILD_BENCHMARKS OFF")
endif()
if(NOT TARGET Snappy::snappy)
  if(NOT MSVC)
    # https://github.com/google/snappy/pull/156
    target_compile_options(snappy PRIVATE -Wno-sign-compare)
  endif()
endif()

if(NOT TARGET taocpp::json)
  # https://github.com/taocpp/json/releases
  cpmaddpackage(
    NAME
    json
    GIT_TAG
    1.0.0-beta.14
    VERSION
    1.0.0-beta.14
    GITHUB_REPOSITORY
    "taocpp/json"
    OPTIONS
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON"
    "BUILD_SHARED_LIBS OFF"
    "PEGTL_INSTALL ${COUCHBASE_CXX_CLIENT_INSTALL}"
    "PEGTL_INSTALL_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/pegtl"
    "PEGTL_INSTALL_DOC_DIR ${CMAKE_INSTALL_DATAROOTDIR}/doc/tao/pegtl"
    "PEGTL_BUILD_TESTS OFF"
    "PEGTL_BUILD_EXAMPLES OFF"
    "PEGTL_USE_BOOST_FILESYSTEM OFF"
    "TAOCPP_JSON_INSTALL ${COUCHBASE_CXX_CLIENT_INSTALL}"
    "TAOCPP_JSON_INSTALL_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/taocpp-json"
    "TAOCPP_JSON_INSTALL_DOC_DIR ${CMAKE_INSTALL_DATAROOTDIR}/doc/tao/json"
    "TAOCPP_JSON_BUILD_TESTS OFF"
    "TAOCPP_JSON_BUILD_EXAMPLES OFF")
endif()

if(NOT TARGET asio::asio)
  # https://github.com/chriskohlhoff/asio/tags
  cpmaddpackage(
    NAME
    asio
    GIT_TAG
    asio-1-34-2
    VERSION
    1.34.2
    GITHUB_REPOSITORY
    "chriskohlhoff/asio"
    EXCLUDE_FROM_ALL ON
    SYSTEM NO)
endif()

# ASIO doesn't use CMake, we have to configure it manually. Extra notes for using on Windows:
#
# 1) If _WIN32_WINNT is not set, ASIO assumes _WIN32_WINNT=0x0501, i.e. Windows XP target, which is definitely not the
# platform which most users target.
#
# 2) WIN32_LEAN_AND_MEAN is defined to make Winsock2 work.
if(asio_ADDED)
  add_library(asio STATIC ${asio_SOURCE_DIR}/asio/src/asio.cpp ${asio_SOURCE_DIR}/asio/src/asio_ssl.cpp)

  target_include_directories(asio SYSTEM PUBLIC ${asio_SOURCE_DIR}/asio/include)
  target_compile_definitions(asio PUBLIC ASIO_STANDALONE=1 ASIO_NO_DEPRECATED=1 ASIO_SEPARATE_COMPILATION=1)
  target_link_libraries(asio PRIVATE Threads::Threads)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
    target_link_libraries(asio PUBLIC $<TARGET_OBJECTS:ssl> $<TARGET_OBJECTS:crypto>)
    # Add BoringSSL include directories before asio's own include directories.
    target_include_directories(
      asio BEFORE PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:ssl,INTERFACE_INCLUDE_DIRECTORIES>>
                          $<BUILD_INTERFACE:$<TARGET_PROPERTY:crypto,INTERFACE_INCLUDE_DIRECTORIES>>)
  elseif(NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
    target_link_libraries(asio PRIVATE OpenSSL::SSL OpenSSL::Crypto)
  endif()
  set_target_properties(
    asio
    PROPERTIES C_VISIBILITY_PRESET hidden
               CXX_VISIBILITY_PRESET hidden
               POSITION_INDEPENDENT_CODE TRUE)

  if(WIN32)
    # macro see @ https://stackoverflow.com/a/40217291/1746503
    macro(get_win32_winnt version)
      if(CMAKE_SYSTEM_VERSION)
        set(ver ${CMAKE_SYSTEM_VERSION})
        string(
          REGEX MATCH
                "^([0-9]+).([0-9])"
                ver
                ${ver})
        string(
          REGEX MATCH
                "^([0-9]+)"
                verMajor
                ${ver})
        # Check for Windows 10, b/c we'll need to convert to hex 'A'.
        if("${verMajor}" MATCHES "10")
          set(verMajor "A")
          string(
            REGEX
            REPLACE "^([0-9]+)"
                    ${verMajor}
                    ver
                    ${ver})
        endif("${verMajor}" MATCHES "10")
        # Remove all remaining '.' characters.
        string(
          REPLACE "."
                  ""
                  ver
                  ${ver})
        # Prepend each digit with a zero.
        string(
          REGEX
          REPLACE "([0-9A-Z])"
                  "0\\1"
                  ver
                  ${ver})
        set(${version} "0x${ver}")
      endif()
    endmacro()

    if(NOT DEFINED _WIN32_WINNT)
      get_win32_winnt(ver)
      set(_WIN32_WINNT ${ver})
    endif()

    message(STATUS "Set _WIN32_WINNT=${_WIN32_WINNT}")

    target_compile_definitions(asio INTERFACE _WIN32_WINNT=${_WIN32_WINNT} WIN32_LEAN_AND_MEAN)
    target_compile_options(asio INTERFACE /bigobj)
  endif()

  add_library(asio::asio ALIAS asio)
endif()

add_library(jsonsl OBJECT ${PROJECT_SOURCE_DIR}/third_party/jsonsl/jsonsl.c)
set_target_properties(jsonsl PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)
target_include_directories(jsonsl SYSTEM PUBLIC ${PROJECT_SOURCE_DIR}/third_party/jsonsl)

declare_system_library(snappy)
declare_system_library(llhttp::llhttp)
declare_system_library(hdr_histogram_static)
declare_system_library(Microsoft.GSL::GSL)
declare_system_library(spdlog::spdlog)
declare_system_library(asio)
declare_system_library(taocpp::json)
declare_system_library(snappy)
