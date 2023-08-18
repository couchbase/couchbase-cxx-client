option(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL "Rely on application to link OpenSSL library" FALSE)
option(COUCHBASE_CXX_CLIENT_USE_HOMEBREW_TO_DETECT_OPENSSL "Use homebrew to determine OpenSSL root directory" TRUE)
option(COUCHBASE_CXX_CLIENT_USE_SCOOP_TO_DETECT_OPENSSL "Use scoop to determine OpenSSL root directory" TRUE)
option(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL "Statically link BoringSSL library" FALSE)
option(COUCHBASE_CXX_CLIENT_BORINGSSL_PIC "Position Independent Code when building BoringSSL library" TRUE)
option(COUCHBASE_CXX_CLIENT_BORINGSSL_VERBOSE_MAKEFILE "Enable verbose output when building BoringSSL library" FALSE)

if(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  message(
    STATUS "COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL is set, assuming OpenSSL headers and symbols are available already"
  )
elseif(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
  if(NOT DEFINED COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH)
    message(STATUS "COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH is not set, using origin/master.")
    set(COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH "origin/master")
  endif()
  message(STATUS "COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH=${COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH}")

  if(NOT DEFINED COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX)
    find_program(GIT git)
    if(GIT)
      execute_process(
        COMMAND git describe --always --long HEAD
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE COUCHBASE_CXX_CLIENT_GIT_DESCRIBE)
      if(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE MATCHES
         "^([0-9]+\\.[0-9]+\\.[0-9]+)(-([a-zA-Z0-9\\.]+))?(-([0-9]+)-g([a-zA-Z0-9]+))?$")
        string(
          REPLACE "."
                  "_"
                  CXX_CLIENT_VERSION
                  ${CMAKE_MATCH_1})
        set(COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX "COUCHBASE_${CXX_CLIENT_VERSION}")
        if(CMAKE_MATCH_3) # pre-release
          string(
            REPLACE "."
                    "_"
                    CXX_CLIENT_PRE_RELEASE
                    ${CMAKE_MATCH_3})
          set(COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX
              "${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}_${CXX_CLIENT_PRE_RELEASE}")
        endif()
      endif()
    endif()
  else()
    message(STATUS "Cannot find git setting COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH to unknown.")
    set(COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX "COUCHBASE_UNKNOWN")
  endif()
  message(STATUS "COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}")
  set(BORINGSSL_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/boringssl)
  set(BORINGSSL_INCLUDE_DIR ${BORINGSSL_OUTPUT_DIR}/include)
  set(BORINGSSL_LIB_DIR ${BORINGSSL_OUTPUT_DIR}/lib)
  set(BORINGSSL_SETUP FALSE)

  # If these exists, no need to fetch the content and rebuild BoringSSL, we have already completed that process
  if(WIN32)
    if(EXISTS "${BORINGSSL_LIB_DIR}/crypto${CMAKE_STATIC_LIBRARY_SUFFIX}"
       AND EXISTS "${BORINGSSL_LIB_DIR}/ssl${CMAKE_STATIC_LIBRARY_SUFFIX}"
       AND EXISTS "${BORINGSSL_INCLUDE_DIR}/boringssl_prefix_symbols.h")
      set(BORINGSSL_SETUP TRUE)
    endif()
  else()
    if(EXISTS "${BORINGSSL_LIB_DIR}/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}"
       AND EXISTS "${BORINGSSL_LIB_DIR}/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}"
       AND EXISTS "${BORINGSSL_INCLUDE_DIR}/boringssl_prefix_symbols.h")
      set(BORINGSSL_SETUP TRUE)
    endif()
  endif()

  if(NOT BORINGSSL_SETUP)
    include(FetchContent)
    FetchContent_Declare(
      boringssl
      GIT_REPOSITORY https://github.com/google/boringssl.git
      GIT_TAG ${COUCHBASE_CXX_CLIENT_BORINGSSL_BRANCH})
    # we do not want this to be a part of the build since we _need_ to build it ourselves twice:  build -> get symbols
    # -> build again w/ symbols prefixed once the build is complete we only care about the headers and libs
    message("Cloning borringssl...")
    FetchContent_Populate(boringssl)
    message("Cloned borringssl to ${boringssl_SOURCE_DIR}")
    set(BORINGSSL_SRC_DIR "${boringssl_SOURCE_DIR}")
    set(BORINGSSL_BIN_DIR "${boringssl_BINARY_DIR}")

    # we need Go in order to read BoringSSL's symbols via the utils they provide...thanks Google!
    find_program(GO_EXECUTABLE go)
    if(NOT GO_EXECUTABLE)
      message(FATAL_ERROR "Could not find Go")
    else()
      message("Found Go: ${GO_EXECUTABLE}")
    endif()

    file(MAKE_DIRECTORY ${BORINGSSL_OUTPUT_DIR})
    file(MAKE_DIRECTORY ${BORINGSSL_INCLUDE_DIR})
    file(MAKE_DIRECTORY ${BORINGSSL_LIB_DIR})

    if(WIN32)
      execute_process(
        COMMAND
          cmd /C build_boringssl_win.bat ${BORINGSSL_SRC_DIR} ${BORINGSSL_BIN_DIR} ${BORINGSSL_OUTPUT_DIR}
          ${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX} ${CMAKE_BUILD_TYPE} ${COUCHBASE_CXX_CLIENT_BORINGSSL_PIC}
          ${COUCHBASE_CXX_CLIENT_BORINGSSL_VERBOSE_MAKEFILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin
        RESULT_VARIABLE BUILD_RESULT)
    else()
      execute_process(
        COMMAND
          bash build_boringssl ${BORINGSSL_SRC_DIR} ${BORINGSSL_BIN_DIR} ${BORINGSSL_OUTPUT_DIR}
          ${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX} ${CMAKE_BUILD_TYPE} ${COUCHBASE_CXX_CLIENT_BORINGSSL_PIC}
          ${COUCHBASE_CXX_CLIENT_BORINGSSL_VERBOSE_MAKEFILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin
        RESULT_VARIABLE BUILD_RESULT)
    endif()
    if(NOT
       BUILD_RESULT
       EQUAL
       "0")
      message(FATAL_ERROR "Failed to build BoringSSL.  Failed with ${BUILD_RESULT}.")
    endif()
  endif()

  # create the OpenSSL targets the CXX client relies on
  add_library(
    OpenSSL::SSL
    STATIC
    IMPORTED
    GLOBAL)
  add_library(
    OpenSSL::Crypto
    STATIC
    IMPORTED
    GLOBAL)
  if(WIN32)
    set_property(TARGET OpenSSL::SSL PROPERTY IMPORTED_LOCATION ${BORINGSSL_LIB_DIR}/ssl${CMAKE_STATIC_LIBRARY_SUFFIX})
    set_property(TARGET OpenSSL::Crypto PROPERTY IMPORTED_LOCATION
                                                 ${BORINGSSL_LIB_DIR}/crypto${CMAKE_STATIC_LIBRARY_SUFFIX})
  else()
    set_property(TARGET OpenSSL::SSL PROPERTY IMPORTED_LOCATION
                                              ${BORINGSSL_LIB_DIR}/libssl${CMAKE_STATIC_LIBRARY_SUFFIX})
    set_property(TARGET OpenSSL::Crypto PROPERTY IMPORTED_LOCATION
                                                 ${BORINGSSL_LIB_DIR}/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX})
  endif()
  set_property(TARGET OpenSSL::SSL PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BORINGSSL_INCLUDE_DIR})
  set_property(TARGET OpenSSL::Crypto PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BORINGSSL_INCLUDE_DIR})
  file(READ "${BORINGSSL_OUTPUT_DIR}/boringssl_sha.txt" COUCHBASE_CXX_CLIENT_BORINGSSL_SHA)
  string(STRIP "${COUCHBASE_CXX_CLIENT_BORINGSSL_SHA}" COUCHBASE_CXX_CLIENT_BORINGSSL_SHA)
  message("COUCHBASE_CXX_CLIENT_BORINGSSL_SHA=${COUCHBASE_CXX_CLIENT_BORINGSSL_SHA}")

  # NOTEs: linux seems to need to link `Threads::Threads`, does not appear to be required for macOS use `OUTPUT_VARIABLE
  # TRY_COMPILE_OUTPUT` for debugging
  string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER)
  if(CMAKE_BUILD_TYPE_LOWER MATCHES "deb")
    try_compile(
      BORINGSSL_USABLE ${CMAKE_CURRENT_BINARY_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/test_openssl.cxx
      CMAKE_FLAGS "-DINCLUDE_DIRECTORIES:STRING=${BORINGSSL_INCLUDE_DIR}"
      COMPILE_DEFINITIONS "-DBORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}"
      OUTPUT_VARIABLE TRY_COMPILE_OUTPUT
      LINK_LIBRARIES
        OpenSSL::Crypto
        OpenSSL::SSL
        Threads::Threads
        CXX_STANDARD
        17)
    message(STATUS ${TRY_COMPILE_OUTPUT})
  else()
    try_compile(
      BORINGSSL_USABLE ${CMAKE_CURRENT_BINARY_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/test_openssl.cxx
      CMAKE_FLAGS "-DINCLUDE_DIRECTORIES:STRING=${BORINGSSL_INCLUDE_DIR}"
      COMPILE_DEFINITIONS "-DBORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}"
      LINK_LIBRARIES
        OpenSSL::Crypto
        OpenSSL::SSL
        Threads::Threads
        CXX_STANDARD
        17)
  endif()
  if(BORINGSSL_USABLE)
    message(STATUS "BORING_SSL success.")
  else()
    message(STATUS "BORING_SSL fail.")
  endif()

  # lets remove all the things b/c we _should_ have all that we need
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_BINARY_DIR}/_deps/boringssl-build"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_BINARY_DIR}/_deps/boringssl-src"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_BINARY_DIR}/_deps/boringssl-subbuild")
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
