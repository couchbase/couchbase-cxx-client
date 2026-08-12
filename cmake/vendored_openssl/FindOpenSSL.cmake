# Satisfies find_package(OpenSSL) for a vendored dependency that must link the BoringSSL this
# project already builds, rather than the platform's OpenSSL.
#
# curl asks for OpenSSL unconditionally when CURL_USE_OPENSSL is on, and asks REQUIRED, so on a
# build root that ships no openssl-devel the configure would fail outright -- and where it does
# ship one, curl would link it and pull a second TLS implementation into every process that loads
# the SDK. Neither is wanted, and CMake's own FindOpenSSL cannot describe BoringSSL: it looks for an
# install prefix with a matching version header, while BoringSSL here is a pair of targets built in
# this same tree.
#
# This module is reached only by dependencies that cmake/ThirdPartyDependencies.cmake points at it
# through CMAKE_MODULE_PATH, and only when COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL is on. The
# OpenSSL::SSL and OpenSSL::Crypto targets it reports are the aliases cmake/OpenSSL.cmake already
# defined, so consumers linking them get BoringSSL.

if(NOT TARGET OpenSSL::SSL OR NOT TARGET OpenSSL::Crypto)
  message(FATAL_ERROR "cmake/vendored_openssl/FindOpenSSL.cmake was reached without the project's "
                      "BoringSSL targets. It is only valid on CMAKE_MODULE_PATH for dependencies "
                      "that must link BoringSSL.")
endif()

set(OPENSSL_FOUND TRUE)
set(OpenSSL_FOUND TRUE)

# BoringSSL answers OPENSSL_VERSION_NUMBER as 1.1.1 for source compatibility, and reports itself
# through OPENSSL_IS_BORINGSSL. Anything version-gating above this will not work against it.
set(OPENSSL_VERSION "1.1.1")
set(OPENSSL_INCLUDE_DIR "${COUCHBASE_CXX_CLIENT_BORINGSSL_INCLUDE_DIR}")
set(OPENSSL_INCLUDE_DIRS "${OPENSSL_INCLUDE_DIR}")
set(OPENSSL_SSL_LIBRARY OpenSSL::SSL)
set(OPENSSL_CRYPTO_LIBRARY OpenSSL::Crypto)
set(OPENSSL_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)
set(OPENSSL_USE_STATIC_LIBS TRUE)
