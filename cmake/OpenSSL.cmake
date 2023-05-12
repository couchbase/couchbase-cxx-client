option(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL "Rely on application to link OpenSSL library" FALSE)
option(COUCHBASE_CXX_CLIENT_USE_HOMEBREW_TO_DETECT_OPENSSL "Use homebrew to determine OpenSSL root directory" TRUE)
option(COUCHBASE_CXX_CLIENT_USE_SCOOP_TO_DETECT_OPENSSL "Use scoop to determine OpenSSL root directory" TRUE)

if(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  message(
    STATUS "COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL is set, assuming OpenSSL headers and symbols are available already"
  )
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
          openssl11)
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
      openssl11)
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

if(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE)
  file(DOWNLOAD "https://curl.se/ca/cacert.pem.sha256" "${CMAKE_CURRENT_BINARY_DIR}/mozilla-ca-bundle.sha256"
       TLS_VERIFY ON)
  file(READ "${CMAKE_CURRENT_BINARY_DIR}/mozilla-ca-bundle.sha256" HASH_FILE_CONTENT)
  string(
    REGEX MATCH
          "^([0-9a-f]+)"
          COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256
          ${HASH_FILE_CONTENT})
  if(NOT COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256)
    message(FATAL_ERROR "Failed to extract expected hash from file")
  endif()
  file(
    DOWNLOAD "https://curl.se/ca/cacert.pem" "${CMAKE_CURRENT_BINARY_DIR}/mozilla-ca-bundle.crt"
    TLS_VERIFY ON
    EXPECTED_HASH SHA256=${COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256})

  file(READ "${CMAKE_CURRENT_BINARY_DIR}/mozilla-ca-bundle.crt" CA_BUNDLE_CONTENT)
  string(
    REGEX MATCH
          "Certificate data from Mozilla as of: ([^\n]*)"
          CA_BUNDLE_DATE_LINE
          ${CA_BUNDLE_CONTENT})
  set(COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE "${CMAKE_MATCH_1}")
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

namespace couchbase::core::default_ca
{
constexpr inline std::size_t number_of_certificates{ ${NUMBER_OF_CERTIFICATES} };
constexpr inline certificate certificates[]{
")
foreach(CERTIFICATE ${CERTIFICATES})
  string(
    REGEX MATCH
          "[ \t\r\n]*([^=\n]+)[ \t\r\n]*=+\n(-----BEGIN CERTIFICATE-----[^-]+-----END CERTIFICATE-----)"
          PARTS
          ${CERTIFICATE})
  file(APPEND ${CA_BUNDLE_CPP_FILE} "    { R\"(${CMAKE_MATCH_1})\",\n      R\"(${CMAKE_MATCH_2})\" },\n\n")
endforeach()

file(
  APPEND ${CA_BUNDLE_CPP_FILE}
  "    { \"\", \"\" },
};

auto
mozilla_ca_certs() -> gsl::span<const certificate>
{
    return { certificates, number_of_certificates };
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
