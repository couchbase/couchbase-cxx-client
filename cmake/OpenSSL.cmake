option(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL "Rely on application to link OpenSSL library" FALSE)

# The pkg-config module both fallbacks below ask for, named once because cmake/Packaging.cmake has to
# request the same one in the installed package config: a consumer that resolves a different module
# than the archive was built against gets a different TLS installation, quietly.
set(COUCHBASE_CXX_CLIENT_OPENSSL_PKGCONFIG_MODULE
    "openssl11"
    CACHE STRING "pkg-config module used when find_package(OpenSSL) is unusable")
option(COUCHBASE_CXX_CLIENT_USE_HOMEBREW_TO_DETECT_OPENSSL "Use homebrew to determine OpenSSL root directory" TRUE)
option(COUCHBASE_CXX_CLIENT_USE_SCOOP_TO_DETECT_OPENSSL "Use scoop to determine OpenSSL root directory" TRUE)
# COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL is what this option was called before 1.4.0. Forward it
# rather than ignore it: the new option defaults FALSE, so a configuration still passing the old
# name would fall through to the platform OpenSSL and link a TLS stack it did not ask for, with no
# diagnostic. An explicit COUCHBASE_CXX_CLIENT_STATIC_AWSLC wins, because set(... CACHE) does not
# overwrite a value already in the cache.
if(DEFINED COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
  # WARNING, not DEPRECATION: a deprecation warning is silenced by -Wno-deprecated and turned
  # fatal by CMAKE_ERROR_DEPRECATED. A forwarded option must be neither.
  message(
    WARNING
      "DEPRECATED: COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=${COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL}. "
      "The statically linked TLS library is AWS-LC, and the option is renamed to "
      "COUCHBASE_CXX_CLIENT_STATIC_AWSLC. The value is forwarded to the new name for the whole "
      "1.4.x series, so this is a notice, not an error. Builds that set the old name must move off "
      "it before upgrading to 1.5.0.")
  set(COUCHBASE_CXX_CLIENT_STATIC_AWSLC
      ${COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL}
      CACHE BOOL "Build and statically link AWS-LC library")
endif()
option(COUCHBASE_CXX_CLIENT_STATIC_AWSLC "Build and statically link AWS-LC library" FALSE)

if(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  message(
    STATUS "COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL is set, assuming OpenSSL headers and symbols are available already"
  )
elseif(COUCHBASE_CXX_CLIENT_STATIC_AWSLC)
  # AWS-LC declares cmake_minimum_required(VERSION 3.5..3.31), which CMake 4 refuses outright.
  # CMAKE_POLICY_VERSION_MINIMUM arrived in 4.0 and is the documented way to lower the floor for
  # a dependency; it applies to every cmake_minimum_required that follows, not only AWS-LC's.
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "4.0")
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
  endif()

  set(COUCHBASE_CXX_CLIENT_AWSLC_VERSION "5.8.0")
  cpmaddpackage(
    NAME
    awslc
    VERSION
    ${COUCHBASE_CXX_CLIENT_AWSLC_VERSION}
    GITHUB_REPOSITORY
    "aws/aws-lc"
    GIT_TAG
    "v${COUCHBASE_CXX_CLIENT_AWSLC_VERSION}"
    OPTIONS
    "BUILD_TESTING OFF"
    # Drops AWS-LC's bssl and openssl command-line tools, which this project does not link or
    # ship, and leaves ssl as its only C++ target, so the C4577 suppression covers all of its
    # C++.
    "BUILD_TOOL OFF"
    # AWS-LC treats a missing Go toolchain as a hard error, and the macOS runners carry none.
    # With tests off and FIPS unset, Go only regenerates sources it also ships under
    # generated-src.
    "DISABLE_GO ON"
    "BUILD_SHARED_LIBS OFF"
    "CMAKE_C_VISIBILITY_PRESET hidden"
    "CMAKE_CXX_VISIBILITY_PRESET hidden"
    "CMAKE_POSITION_INDEPENDENT_CODE ON")
  if(APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles(
      "int main() {
       #if defined(__ARM_FEATURE_SHA2)
       return 0;
       #else
       #error __ARM_FEATURE_SHA2 not defined
       #endif
     }"
      HAVE_ARM_FEATURE_SHA2)
    if(NOT HAVE_ARM_FEATURE_SHA2)
      message(
        WARNING
          "The compiler ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}) does not support AWS-LC, use OpenSSL or different compiler"
      )
    endif()
  endif()
  # AWS-LC puts a symbol_prefix_include directory on ssl's and crypto's include paths, but only a
  # BORINGSSL_PREFIX build creates it. CMake rejects a non-existent include directory on an
  # imported target, so a couchbase2 build that reaches these through the imported gRPC targets
  # fails to generate. Nothing reads prefixed headers, so an empty directory is enough.
  file(MAKE_DIRECTORY "${awslc_BINARY_DIR}/symbol_prefix_include")

  if(APPLE)
    # The Apple linker has no --exclude-libs, and -load_hidden would have to precede each archive
    # on the link line and would link it a second time. An unexported_symbols_list is
    # order-independent. Mach-O prefixes every C symbol with an underscore.
    file(STRINGS "${PROJECT_SOURCE_DIR}/cmake/tls_symbol_prefixes.txt" _tls_prefixes REGEX "^[^#]")
    list(TRANSFORM _tls_prefixes REPLACE "^(.+)$" "_\\1*")
    string(JOIN "\n" _tls_patterns ${_tls_prefixes})
    set(COUCHBASE_CXX_CLIENT_UNEXPORTED_TLS_SYMBOLS "${PROJECT_BINARY_DIR}/unexported_tls_symbols.txt")
    file(WRITE "${COUCHBASE_CXX_CLIENT_UNEXPORTED_TLS_SYMBOLS}" "${_tls_patterns}\n")
  endif()

  # AWS-LC's install rules put its own openssl/*.h and pkg-config files into this project's
  # prefix, where they would collide with the platform OpenSSL's. Excluding the directory drops
  # them; its targets are still built, because this project links them.
  set_property(DIRECTORY "${awslc_SOURCE_DIR}" PROPERTY EXCLUDE_FROM_ALL YES)

  if(MSVC)
    # AWS-LC compiles ssl with -WX under _HAS_EXCEPTIONS=0 and no /EHsc. MSVC then reports every
    # noexcept in the standard headers as C4577, which -WX makes fatal. The warning describes
    # AWS-LC's own flags, not this project's code.
    target_compile_options(ssl PRIVATE /wd4577)
  endif()
  add_library(OpenSSL::SSL ALIAS ssl)
  add_library(OpenSSL::Crypto ALIAS crypto)
else()
  option(COUCHBASE_CXX_CLIENT_STATIC_OPENSSL "Statically link OpenSSL library" FALSE)
  if(COUCHBASE_CXX_CLIENT_STATIC_OPENSSL)
    set(OPENSSL_USE_STATIC_LIBS ON)
  endif()
  if(NOT OPENSSL_ROOT_DIR)
    if(APPLE AND COUCHBASE_CXX_CLIENT_USE_HOMEBREW_TO_DETECT_OPENSSL)
      execute_process(
        COMMAND brew --prefix openssl
        OUTPUT_VARIABLE OPENSSL_ROOT_DIR
        ERROR_VARIABLE HOMEBREW_STDERR
        ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(OPENSSL_ROOT_DIR)
        message(STATUS "Found OpenSSL prefix using Homebrew: ${OPENSSL_ROOT_DIR}")
      else()
        execute_process(
          COMMAND brew --prefix openssl@1.1
          OUTPUT_VARIABLE OPENSSL_ROOT_DIR
          ERROR_VARIABLE HOMEBREW_STDERR
          ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(OPENSSL_ROOT_DIR)
          message(STATUS "Found OpenSSL 1.1 prefix using Homebrew: ${OPENSSL_ROOT_DIR}")
        endif()
      endif()
    endif()
    if(MSVC AND COUCHBASE_CXX_CLIENT_USE_SCOOP_TO_DETECT_OPENSSL)
      find_program(POWERSHELL powershell)
      execute_process(
        COMMAND ${POWERSHELL} scoop prefix openssl1
        OUTPUT_VARIABLE OPENSSL_ROOT_DIR
        ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(OPENSSL_ROOT_DIR)
        message(STATUS "Found OpenSSL prefix using scoop: ${OPENSSL_ROOT_DIR}")
      endif()
    endif()
  endif()

  find_package(OpenSSL 1.1)
  if(OpenSSL_FOUND)
    try_compile(
      OPENSSL_USABLE ${CMAKE_CURRENT_BINARY_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/test_openssl.cxx
      LINK_LIBRARIES OpenSSL::SSL CXX_STANDARD 17)
    if(OPENSSL_USABLE)
      message(STATUS "OPENSSL_VERSION: ${OPENSSL_VERSION}")
      message(STATUS "OPENSSL_INCLUDE_DIR: ${OPENSSL_INCLUDE_DIR}")
      message(STATUS "OPENSSL_LIBRARIES: ${OPENSSL_LIBRARIES}")
    else()
      if(UNIX)
        message(
          STATUS
            "Cannot use OpenSSL ${OPENSSL_VERSION} at \"${OPENSSL_INCLUDE_DIR}\" and \"${OPENSSL_LIBRARIES}\". Will try to use from pkg-config."
        )
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(
          PKG_CONFIG_OPENSSL
          REQUIRED
          IMPORTED_TARGET
          GLOBAL
          ${COUCHBASE_CXX_CLIENT_OPENSSL_PKGCONFIG_MODULE})
        if(PKG_CONFIG_OPENSSL_FOUND)
          message(STATUS "PKG_CONFIG_OPENSSL_VERSION: ${PKG_CONFIG_OPENSSL_VERSION}")
          message(STATUS "PKG_CONFIG_OPENSSL_INCLUDE_DIRS: ${PKG_CONFIG_OPENSSL_INCLUDE_DIRS}")
          message(STATUS "PKG_CONFIG_OPENSSL_LIBRARIES: ${PKG_CONFIG_OPENSSL_LIBRARIES}")
        else()
          message(FATAL_ERROR "Cannot Find OpenSSL using pkg-config, find_package() returns unusable setup")
        endif()
      else()
        message(FATAL_ERROR "OpenSSL discovered by find_package() returns unusable setup")
      endif()
    endif()
  elseif(UNIX)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(
      PKG_CONFIG_OPENSSL
      REQUIRED
      IMPORTED_TARGET
      GLOBAL
      ${COUCHBASE_CXX_CLIENT_OPENSSL_PKGCONFIG_MODULE})
    if(PKG_CONFIG_OPENSSL_FOUND)
      message(STATUS "PKG_CONFIG_OPENSSL_VERSION: ${PKG_CONFIG_OPENSSL_VERSION}")
      message(STATUS "PKG_CONFIG_OPENSSL_INCLUDE_DIRS: ${PKG_CONFIG_OPENSSL_INCLUDE_DIRS}")
      message(STATUS "PKG_CONFIG_OPENSSL_LIBRARIES: ${PKG_CONFIG_OPENSSL_LIBRARIES}")
    else()
      message(FATAL_ERROR "Cannot Find OpenSSL using pkg-config")
    endif()
  else()
    message(FATAL_ERROR "Cannot build Couchbase C++ SDK without OpenSSL")
  endif()
endif()

# Read more at https://wiki.wireshark.org/TLS
option(COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE
       "Path to file to write per-session secrets (Useful for Wireshark SSL/TLS dissection)")

option(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE
       "Download and embed Mozilla certificates from https://curl.se/ca/cacert.pem" TRUE)

option(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT "Path to downloaded certificate bundle and its checksum")

if(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE)
  if(NOT EXISTS "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}")
    set(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT "${CMAKE_CURRENT_BINARY_DIR}")
  endif()
  message(STATUS "Using ${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT} to keep Mozilla certificates")
  if(NOT EXISTS "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.sha256")
    message(STATUS "Download CA bundle checksum: https://curl.se/ca/cacert.pem.sha256")
    file(
      DOWNLOAD "https://curl.se/ca/cacert.pem.sha256"
      "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.sha256"
      TLS_VERIFY ON
      STATUS DOWNLOAD_STATUS)
    list( GET DOWNLOAD_STATUS 0 STATUS_CODE)
    if(NOT ${STATUS_CODE} EQUAL 0)
      list( GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)
      message(FATAL_ERROR "Unable to download CA bundle checksum file, status=${STATUS_CODE}: ${ERROR_MESSAGE}")
    endif()
  endif()
  file(READ "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.sha256" HASH_FILE_CONTENT)
  string(
    REGEX MATCH
          "^([0-9a-f]+)"
          COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256
          ${HASH_FILE_CONTENT})
  if(NOT COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256)
    message(FATAL_ERROR "Failed to extract expected hash from file")
  endif()
  if(EXISTS "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.crt")
    file(SHA256 "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.crt" SHA256_OF_CRT_FILE)
    if(NOT
       COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256
       STREQUAL
       SHA256_OF_CRT_FILE)
      message(FATAL_ERROR "SHA256 of mozilla-ca-bundle.crt does not match")
    endif()
  else()
    message(STATUS "Download CA bundle: https://curl.se/ca/cacert.pem")
    file(
      DOWNLOAD "https://curl.se/ca/cacert.pem"
      "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.crt"
      TLS_VERIFY ON
      EXPECTED_HASH SHA256=${COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256}
      STATUS DOWNLOAD_STATUS)
    list( GET DOWNLOAD_STATUS 0 STATUS_CODE)
    if(NOT ${STATUS_CODE} EQUAL 0)
      list( GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)
      message(FATAL_ERROR "Unable to download CA bundle, status=${STATUS_CODE}: ${ERROR_MESSAGE}")
    endif()
  endif()

  file(READ "${COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT}/mozilla-ca-bundle.crt" CA_BUNDLE_CONTENT)
  string(
    REGEX MATCH
          "Certificate data from Mozilla (as of|last updated on): ([^\n]*)"
          CA_BUNDLE_DATE_LINE
          ${CA_BUNDLE_CONTENT})
  if(CA_BUNDLE_DATE_LINE)
    set(COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE "${CMAKE_MATCH_2}")
  else()
    message(
      WARNING
        "Failed to extract Mozilla CA bundle date from certificate file; "
        "COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE will be set to 'unknown'.")
    set(COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE "unknown")
  endif()
else()
  set(CA_BUNDLE_CONTENT "")
endif()

set(CA_BUNDLE_CPP_FILE "${CMAKE_CURRENT_BINARY_DIR}/generated/mozilla_ca_bundle.cxx")

# We parse CRT file because C++ does not allow string literals over 64k, and turning everything into bytes does not
# seems to be cross-platform and fast.
string(
  REGEX MATCHALL
        "[^=#]+=+\n-----BEGIN CERTIFICATE-----[^-]+-----END CERTIFICATE-----"
        CERTIFICATES
        "${CA_BUNDLE_CONTENT}")
list(LENGTH CERTIFICATES NUMBER_OF_CERTIFICATES)
file(
  WRITE ${CA_BUNDLE_CPP_FILE}
  "
#include \"core/mozilla_ca_bundle.hxx\"

#include <gsl/span>

#include <array>
#include <string_view>

namespace couchbase::core::default_ca
{
constexpr inline std::array<certificate, ${NUMBER_OF_CERTIFICATES}> certificates{
")
foreach(CERTIFICATE ${CERTIFICATES})
  string(
    REGEX MATCH
          "[ \t\r\n]*([^=\n]+)[ \t\r\n]*=+\n(-----BEGIN CERTIFICATE-----[^-]+-----END CERTIFICATE-----)"
          PARTS
          ${CERTIFICATE})
  file(APPEND ${CA_BUNDLE_CPP_FILE}
"  certificate{ R\"(${CMAKE_MATCH_1})\",
               R\"(${CMAKE_MATCH_2})\", },

")
endforeach()

file(
  APPEND ${CA_BUNDLE_CPP_FILE}
"};

auto
mozilla_ca_certs() -> gsl::span<const certificate>
{
  return certificates;
}

auto
mozilla_ca_certs_date() -> std::string_view
{
  return \"${COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE}\";
}

auto
mozilla_ca_certs_sha256() -> std::string_view
{
  return \"${COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256}\";
}
} // namespace couchbase::core::default_ca
")

set(OPENSSL_HEADERS_TO_PROXY
    crypto.h
    evp.h
    hmac.h
    md5.h
    rand.h
    sha.h
    ssl.h
    x509.h)
foreach(HEADER_NAME IN LISTS OPENSSL_HEADERS_TO_PROXY)
  configure_file("${PROJECT_SOURCE_DIR}/cmake/include_ssl.hxx.in"
                 "${CMAKE_CURRENT_BINARY_DIR}/generated/include_ssl/${HEADER_NAME}" @ONLY)
endforeach()
