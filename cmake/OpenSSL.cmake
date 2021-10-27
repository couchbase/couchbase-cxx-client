option(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL "Rely on application to link OpenSSL library" FALSE)

if(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  message(
    STATUS "COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL is set, assuming OpenSSL headers and symbols are available already"
  )
else()
  option(COUCHBASE_CXX_CLIENT_STATIC_OPENSSL "Statically link OpenSSL library" FALSE)
  if(COUCHBASE_CXX_CLIENT_STATIC_OPENSSL)
    set(OPENSSL_USE_STATIC_LIBS ON)
  endif()
  include(FindOpenSSL)
  if(OPENSSL_LIBRARIES AND OPENSSL_INCLUDE_DIR)
    message(STATUS "OPENSSL_VERSION: ${OPENSSL_VERSION}")
    message(STATUS "OPENSSL_INCLUDE_DIR: ${OPENSSL_INCLUDE_DIR}")
    message(STATUS "OPENSSL_LIBRARIES: ${OPENSSL_LIBRARIES}")
  else()
    message(FATAL_ERROR "Cannot build Couchbase C++ SDK without OpenSSL")
  endif()
endif()

# Read more at https://wiki.wireshark.org/TLS
option(COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE
       "Path to file to write per-session secrets (Useful for Wireshark SSL/TLS dissection)")
