if(NOT COUCHBASE_CXX_CLIENT_GIT_REVISION)
  find_program(GIT git)
  if(GIT)
    execute_process(
      COMMAND git rev-parse HEAD
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE COUCHBASE_CXX_CLIENT_GIT_REVISION)
  else()
    set(COUCHBASE_CXX_CLIENT_GIT_REVISION "unknown")
  endif()
endif()
string(SUBSTRING "${COUCHBASE_CXX_CLIENT_GIT_REVISION}" 0 7 COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT)

if(NOT COUCHBASE_CXX_CLIENT_GIT_DESCRIBE)
  if(GIT)
    execute_process(
      COMMAND git describe --always --long HEAD
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE COUCHBASE_CXX_CLIENT_GIT_DESCRIBE)
  else()
    set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "unknown")
  endif()
endif()

if(NOT COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP)
  if(GIT)
    execute_process(
      COMMAND git describe --tags --abbrev=0 HEAD
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      RESULT_VARIABLE tag_result
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE last_tag)

    if (tag_result EQUAL 0)
      execute_process(
        COMMAND git log --max-count=1 --no-patch --format=%cd --date=format:%Y-%m-%dT%H:%M:%S ${last_tag}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP)
    endif()
  endif()

  if (NOT COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP)
    string(TIMESTAMP COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP "%Y-%m-%dT%H:%M:%S" UTC)
  endif()
endif()

string(REGEX REPLACE "T.*" "" COUCHBASE_CXX_CLIENT_BUILD_DATE "${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}")

# set(couchbase_cxx_client_BUILD_NUMBER 142)
# set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "1.0.0-beta.4-27-g6807da0") #-> "couchbase_cxx_client-1.0.0-beta.4+142.27.6807da0"
# set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "1.0.0-beta.4-0-g6807da0")  #-> "couchbase_cxx_client-1.0.0-beta.4"
# set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "1.0.0-27-g6807da0")        #-> "couchbase_cxx_client-1.0.0+142.27.6807da0"
# set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "1.0.0-0-g6807da0")         #-> "couchbase_cxx_client-1.0.0"
# set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE "1.0.0")                    #-> "couchbase_cxx_client-1.0.0"
set(COUCHBASE_CXX_CLIENT_SEMVER "${couchbase_cxx_client_VERSION}")
set(COUCHBASE_CXX_CLIENT_PACKAGE_VERSION "${couchbase_cxx_client_VERSION}")
set(COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE "${couchbase_cxx_client_BUILD_NUMBER}")
if(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE MATCHES
   "^([0-9]+\\.[0-9]+\\.[0-9]+)(-([a-zA-Z0-9\\.]+))?(-([0-9]+)-g([a-zA-Z0-9]+))?$")
  set(COUCHBASE_CXX_CLIENT_SEMVER "${CMAKE_MATCH_1}")
  set(COUCHBASE_CXX_CLIENT_PACKAGE_VERSION "${CMAKE_MATCH_1}")
  if(CMAKE_MATCH_3) # pre-release
    set(COUCHBASE_CXX_CLIENT_SEMVER "${COUCHBASE_CXX_CLIENT_SEMVER}-${CMAKE_MATCH_3}")
    set(COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE "${CMAKE_MATCH_3}.${couchbase_cxx_client_BUILD_NUMBER}")
  endif()
  if(CMAKE_MATCH_5 AND CMAKE_MATCH_5 GREATER 0) # number_of_commits.build_number.sha1
    set(COUCHBASE_CXX_CLIENT_SEMVER
        "${COUCHBASE_CXX_CLIENT_SEMVER}+${CMAKE_MATCH_5}.${couchbase_cxx_client_BUILD_NUMBER}.${CMAKE_MATCH_6}")
    if(CMAKE_MATCH_3) # pre-release
      set(COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE
          "${CMAKE_MATCH_3}.${CMAKE_MATCH_5}.${couchbase_cxx_client_BUILD_NUMBER}.${CMAKE_MATCH_6}")
    else()
      set(COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE "${CMAKE_MATCH_5}.${couchbase_cxx_client_BUILD_NUMBER}.${CMAKE_MATCH_6}")
    endif()
  endif()
endif()

configure_file(${PROJECT_SOURCE_DIR}/cmake/build_version.hxx.in
               ${PROJECT_BINARY_DIR}/generated/couchbase/build_version.hxx @ONLY)
configure_file(${PROJECT_SOURCE_DIR}/cmake/build_config.hxx.in
               ${PROJECT_BINARY_DIR}/generated/couchbase/build_config.hxx @ONLY)

file(
  GENERATE
  OUTPUT ${PROJECT_BINARY_DIR}/generated_$<CONFIG>/couchbase/build_info.hxx
  CONTENT
    "
#pragma once

#define COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL \"${COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL}\"

#define OPENSSL_SSL_IMPORTED_LOCATION \"$<$<TARGET_EXISTS:OpenSSL::SSL>:$<TARGET_PROPERTY:OpenSSL::SSL,IMPORTED_LOCATION>>\"
#define OPENSSL_SSL_INTERFACE_INCLUDE_DIRECTORIES \"$<$<TARGET_EXISTS:OpenSSL::SSL>:$<TARGET_PROPERTY:OpenSSL::SSL,INTERFACE_INCLUDE_DIRECTORIES>>\"
#define OPENSSL_SSL_INTERFACE_LINK_LIBRARIES \"$<$<TARGET_EXISTS:OpenSSL::SSL>:$<TARGET_PROPERTY:OpenSSL::SSL,INTERFACE_LINK_LIBRARIES>>\"
#define OPENSSL_CRYPTO_IMPORTED_LOCATION \"$<$<TARGET_EXISTS:OpenSSL::Crypto>:$<TARGET_PROPERTY:OpenSSL::Crypto,IMPORTED_LOCATION>>\"
#define OPENSSL_CRYPTO_INTERFACE_INCLUDE_DIRECTORIES \"$<$<TARGET_EXISTS:OpenSSL::Crypto>:$<TARGET_PROPERTY:OpenSSL::Crypto,INTERFACE_INCLUDE_DIRECTORIES>>\"
#define OPENSSL_CRYPTO_INTERFACE_LINK_LIBRARIES \"$<$<TARGET_EXISTS:OpenSSL::Crypto>:$<TARGET_PROPERTY:OpenSSL::Crypto,INTERFACE_LINK_LIBRARIES>>\"
#define OPENSSL_PKG_CONFIG_INTERFACE_INCLUDE_DIRECTORIES \"$<$<TARGET_EXISTS:PkgConfig::PKG_CONFIG_OPENSSL>:$<TARGET_PROPERTY:PkgConfig::PKG_CONFIG_OPENSSL,INTERFACE_INCLUDE_DIRECTORIES>>\"
#define OPENSSL_PKG_CONFIG_INTERFACE_LINK_LIBRARIES \"$<$<TARGET_EXISTS:PkgConfig::PKG_CONFIG_OPENSSL>:$<TARGET_PROPERTY:PkgConfig::PKG_CONFIG_OPENSSL,INTERFACE_LINK_LIBRARIES>>\"

#define CMAKE_BUILD_TYPE \"$<CONFIG>\"
#define CMAKE_VERSION \"${CMAKE_VERSION}\"

#define COUCHBASE_CXX_CLIENT_DEBUG_BUILD $<STREQUAL:$<UPPER_CASE:$<CONFIG>>,DEBUG>
")
