#[=======================================================================[
FindBoringSSL
-----------

Find the BoringSSL encryption library.

This module finds an installed BoringSSL library.

This module defines the following :prop_tgt:`IMPORTED` targets:

``OpenSSL::SSL``
  The BoringSSL ``ssl`` library, if found.
``OpenSSL::Crypto``
  The BoringSSL ``crypto`` library, if found.

Result Variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``BORINGSSL_FOUND``
  System has the BoringSSL library. If no components are requested it only
  requires the crypto library.
``BORINGSSL_INCLUDE_DIR``
  The BoringSSL include directory.
``BORINGSSLL_CRYPTO_LIBRARY``
  The BoringSSL crypto library.
``BORINGSSL_SSL_LIBRARY``
  The BoringSSL SSL library.
``BORINGSSL_PREFIX_SYMBOLS_FOUND``
  TRUE if BoringSSL prefixed symbols headers are found.

Hints
^^^^^

The following variables may be set to control search behavior:

``BORINGSSL_ROOT_DIR``
  Set to the root directory of an BoringSSL installation.

#]=======================================================================]

# We want to _only_ use boringssl specific include and libs
function(valid_boringssl_path var var_name)
  message("var_name=${var_name}")
  message("var=${var}")
  if(NOT
     var
     MATCHES
     "${BORINGSSL_ROOT_DIR}")
    message("no match!")
    if(var_name MATCHES "CRYPTO")
      set(BORINGSSL_CRYPTO_LIBRARY
          "${var_name}-NOTFOUND"
          PARENT_SCOPE)
    elseif(var_name MATCHES "SSL")
      set(BORINGSSL_SSL_LIBRARY
          "${var_name}-NOTFOUND"
          PARENT_SCOPE)
    elseif(var_name MATCHES "INCLUDE")
      set(BORINGSSL_INCLUDE_DIR
          "${var_name}-NOTFOUND"
          PARENT_SCOPE)
    endif()

  else()
    message("matched!")
  endif()
endfunction()

if(BORINGSSL_ROOT_DIR
   OR NOT
      "$ENV{BORINGSSL_ROOT_DIR}"
      STREQUAL
      "")
  set(_BORINGSSL_ROOT_HINTS ${BORINGSSL_ROOT_DIR} ENV BORINGSSL_ROOT_DIR)
endif()

find_path(
  BORINGSSL_ROOT_DIR
  NAMES include/openssl/ssl.h include/openssl/base.h
  HINTS ${_BORINGSSL_ROOT_HINTS}
  PATHS NO_DEFAULT_PATH)

message(STATUS "BORINGSSL_ROOT_DIR=${BORINGSSL_ROOT_DIR}")
if(BORINGSSL_ROOT_DIR AND EXISTS ${BORINGSSL_ROOT_DIR})
  find_path(
    BORINGSSL_INCLUDE_DIR
    NAMES openssl/ssl.h openssl/base.h
    HINTS ${_BORINGSSL_ROOT_HINTS}/include)
  valid_boringssl_path(${BORINGSSL_INCLUDE_DIR} "BORINGSSL_INCLUDE_DIR")

  if(WIN32)
    set(LIB_CRYPTO "crypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIB_SSL "ssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
  else()
    set(LIB_CRYPTO "libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIB_SSL "libssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()

  find_library(
    BORINGSSL_SSL_LIBRARY
    NAMES ${LIB_SSL}
    HINTS ${BORINGSSL_ROOT_DIR}/lib ${BORINGSSL_ROOT_DIR}/lib/ssl ${BORINGSSL_ROOT_DIR}/ssl
    PATHS
    NO_DEFAULT_PATH)

  find_library(
    BORINGSSL_CRYPTO_LIBRARY
    NAMES ${LIB_CRYPTO}
    HINTS ${BORINGSSL_ROOT_DIR}/lib ${BORINGSSL_ROOT_DIR}/lib/crypto ${BORINGSSL_ROOT_DIR}/crypto
    PATHS
    NO_DEFAULT_PATH)

  valid_boringssl_path(${BORINGSSL_CRYPTO_LIBRARY} "BORINGSSL_CRYPTO_LIBRARY")
  valid_boringssl_path(${BORINGSSL_SSL_LIBRARY} "BORINGSSL_SSL_LIBRARY")
  if(COUCHBASE_CXX_CLIENT_USE_BORINGSSL_PREFIX)
    if(EXISTS "${BORINGSSL_INCLUDE_DIR}/boringssl_prefix_symbols.h")
      set(BORINGSSL_PREFIX_SYMBOLS_FOUND TRUE)
    else()
      set(BORINGSSL_PREFIX_SYMBOLS_FOUND TRUE)
    endif()
  endif()
else()
  # set everything to not-found
  set(BORINGSSL_INCLUDE_DIR "BORINGSSL_INCLUDE_DIR-NOTFOUND")
  set(BORINGSSL_CRYPTO_LIBRARY "BORINGSSL_CRYPTO_LIBRARY-NOTFOUND")
  set(BORINGSSL_SSL_LIBRARY "BORINGSSL_SSL_LIBRARY-NOTFOUND")
  set(BORINGSSL_PREFIX_SYMBOLS_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
# BORINGSSL_FOUND will be set to TRUE if all the required VARS do not end in -NOTFOUND
find_package_handle_standard_args(
  BoringSSL
  REQUIRED_VARS BORINGSSL_CRYPTO_LIBRARY BORINGSSL_SSL_LIBRARY BORINGSSL_INCLUDE_DIR
  FAIL_MESSAGE "Could NOT Find BoringSSL, try to set the BoringSSL root folder in the variable BORINGSSL_ROOT_DIR")

mark_as_advanced(
  BORINGSSL_ROOT_DIR
  BORINGSSL_INCLUDE_DIR
  BORINGSSL_CRYPTO_LIBRARY
  BORINGSSL_SSL_LIBRARY
  BORINGSSL_PREFIX_SYMBOLS_FOUND)

if(BORINGSSL_FOUND)
  if(NOT TARGET OpenSSL::Crypto AND EXISTS "${BORINGSSL_CRYPTO_LIBRARY}")
    add_library(
      OpenSSL::Crypto
      STATIC
      IMPORTED
      GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${BORINGSSL_INCLUDE_DIR}"
                                                     IMPORTED_LOCATION "${BORINGSSL_CRYPTO_LIBRARY}")
  endif()

  if(NOT TARGET OpenSSL::SSL AND EXISTS "${BORINGSSL_SSL_LIBRARY}")
    add_library(
      OpenSSL::SSL
      STATIC
      IMPORTED
      GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${BORINGSSL_INCLUDE_DIR}"
                                                  IMPORTED_LOCATION "${BORINGSSL_SSL_LIBRARY}")
  endif()
endif()
