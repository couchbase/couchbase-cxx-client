option(COUCHBASE_CXX_CLIENT_USE_BORINGSSL_PREFIX "Use BoringSSL option to prefix symbols" FALSE)

# 2023-09-29
set(COUCHBASE_CXX_CLIENT_BORINGSSL_SHA "bd20800c22fc8402611b537287bd6948c3f2a5a8")
include(FetchContent)
FetchContent_Declare(
  boringssl
  GIT_REPOSITORY https://github.com/google/boringssl.git
  GIT_TAG ${COUCHBASE_CXX_CLIENT_BORINGSSL_SHA})
# we need to do a two-phase build: 1. build 2. get the symbols 3. build again w/ symbol prefixing
message(STATUS "Cloning BoringSSL...")
FetchContent_Populate(boringssl)
message(STATUS "Cloned BoringSSL to ${boringssl_SOURCE_DIR}")
set(BORINGSSL_SRC_DIR "${boringssl_SOURCE_DIR}")
set(BORINGSSL_BIN_DIR "${boringssl_BINARY_DIR}")
set(BORINGSSL_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/boringssl)
set(BORINGSSL_INCLUDE_DIR ${BORINGSSL_OUTPUT_DIR}/include)
set(BORINGSSL_LIB_DIR ${BORINGSSL_OUTPUT_DIR}/lib)

if(MINGW)
   set(boringssl_PATCH "${PROJECT_SOURCE_DIR}/cmake/BoringSSL-third_party-fiat-curve25519_64_adx-h.patch")
   message("Applying ${boringssl_PATCH} in ${boringssl_SOURCE_DIR} for MinGW gcc")
   execute_process(
            COMMAND patch --input ${boringssl_PATCH} --ignore-whitespace --strip=0
            WORKING_DIRECTORY ${boringssl_SOURCE_DIR}
            RESULT_VARIABLE PATCH_RESULT)
   if(NOT PATCH_RESULT EQUAL "0")
       message(FATAL_ERROR "Failed to apply patch to BoringSSL. Failed with: ${PATCH_RESULT}.")
   endif()
endif()

# we need Go in order to read BoringSSL's symbols via the utils they provide...thanks Google!
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
  message(FATAL_ERROR "Could not find Go")
else()
  message(STATUS "Found Go: ${GO_EXECUTABLE}")
endif()
file(MAKE_DIRECTORY ${BORINGSSL_OUTPUT_DIR})
file(MAKE_DIRECTORY ${BORINGSSL_INCLUDE_DIR})
file(MAKE_DIRECTORY ${BORINGSSL_LIB_DIR})

# if we are building BoringSSL, we set the prefix
set(COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX "COUCHBASE_CXX")
set(COUCHBASE_CXX_CLIENT_USE_BORINGSSL_PREFIX TRUE)
message(STATUS "COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}")
string(CONCAT BORINGSSL_CMAKE_OPTIONS "-DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON "
              "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
if(WIN32)
  if(MINGW)
    set(LIB_CRYPTO "libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIB_SSL "libssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
  else()
    set(LIB_CRYPTO "crypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(LIB_SSL "ssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()

  execute_process(
    COMMAND cmd /C build_boringssl_win.bat ${BORINGSSL_SRC_DIR} ${BORINGSSL_BIN_DIR} ${BORINGSSL_OUTPUT_DIR}
            ${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX} ${LIB_CRYPTO} ${LIB_SSL}
	    ${BORINGSSL_CMAKE_OPTIONS}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/bin
    RESULT_VARIABLE BUILD_RESULT)
else()
  string(
    CONCAT BORINGSSL_CMAKE_OPTIONS
           "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} "
           "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} "
           "-DCMAKE_AR=${CMAKE_AR} "
           ${BORINGSSL_CMAKE_OPTIONS})
  execute_process(
    COMMAND bash build_boringssl ${BORINGSSL_SRC_DIR} ${BORINGSSL_BIN_DIR} ${BORINGSSL_OUTPUT_DIR}
            ${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX} ${BORINGSSL_CMAKE_OPTIONS}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/bin
    RESULT_VARIABLE BUILD_RESULT)
endif()
if(NOT
   BUILD_RESULT
   EQUAL
   "0")
  message(FATAL_ERROR "Failed to build BoringSSL.  Failed with: ${BUILD_RESULT}.")
endif()

# make sure we can find BoringSSL
set(BORINGSSL_ROOT_DIR "${BORINGSSL_OUTPUT_DIR}")
find_package(BoringSSL)
if(NOT BoringSSL_FOUND)
  message(FATAL_ERROR "Cannot build Couchbase C++ SDK without BoringSSL")
else()
  # NOTEs: linux seems to need to link `Threads::Threads`, does not appear to be required for macOS use `OUTPUT_VARIABLE
  # TRY_COMPILE_OUTPUT` for debugging
  string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER)
  set(BORINGSSL_TEST_COMPILE_DEFINITIONS "")
  if(COUCHBASE_CXX_CLIENT_USE_BORINGSSL_PREFIX)
    set(BORINGSSL_TEST_COMPILE_DEFINITIONS "-DBORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX}")
  endif()

  if(CMAKE_BUILD_TYPE_LOWER MATCHES "deb")
    try_compile(
      BORINGSSL_USABLE ${CMAKE_CURRENT_BINARY_DIR}
      ${PROJECT_SOURCE_DIR}/cmake/test_openssl.cxx
      CMAKE_FLAGS "-DINCLUDE_DIRECTORIES:STRING=${BORINGSSL_INCLUDE_DIR}"
      COMPILE_DEFINITIONS "${BORINGSSL_TEST_COMPILE_DEFINITIONS}"
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
      ${PROJECT_SOURCE_DIR}/cmake/test_openssl.cxx
      CMAKE_FLAGS "-DINCLUDE_DIRECTORIES:STRING=${BORINGSSL_INCLUDE_DIR}"
      COMPILE_DEFINITIONS "${BORINGSSL_TEST_COMPILE_DEFINITIONS}"
      LINK_LIBRARIES
        OpenSSL::Crypto
        OpenSSL::SSL
        Threads::Threads
        CXX_STANDARD
        17)
  endif()
  if(BORINGSSL_USABLE)
    message(STATUS "BORING_SSL test success.")
  else()
    message(STATUS "BORING_SSL test fail.")
  endif()
endif()
