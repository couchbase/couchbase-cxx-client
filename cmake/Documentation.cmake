find_package(Doxygen)
if(DOXYGEN_FOUND)
  message(STATUS "Using doxygen: ${DOXYGEN_VERSION}")
  file(
    GLOB_RECURSE
    COUCHBASE_CXX_CLIENT_PUBLIC_HEADERS
    ${PROJECT_SOURCE_DIR}/couchbase/api/*.hxx
    ${PROJECT_SOURCE_DIR}/docs/*.hxx)

  set(DOXYGEN_INPUT_DIR ${PROJECT_SOURCE_DIR}/couchbase/api)
  set(DOXYGEN_OUTPUT_DIR ${PROJECT_BINARY_DIR}/couchbase-cxx-client-${couchbase_cxx_client_VERSION})
  set(DOXYGEN_INDEX_FILE ${DOXYGEN_OUTPUT_DIR}/html/index.html)
  set(DOXYGEN_CONFIG_TEMPLATE ${PROJECT_SOURCE_DIR}/docs/Doxyfile.in)
  set(DOXYGEN_CONFIG ${PROJECT_BINARY_DIR}/Doxyfile)
  configure_file(${DOXYGEN_CONFIG_TEMPLATE} ${DOXYGEN_CONFIG})
  add_custom_command(
    OUTPUT ${DOXYGEN_INDEX_FILE}
    DEPENDS ${COUCHBASE_CXX_CLIENT_PUBLIC_HEADERS}
    COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_CONFIG}
    WORKING_DIRECTORY ${DOXYGEN_OUTPUT_DIR}
    MAIN_DEPENDENCY ${DOXYGEN_CONFIG}
    ${DOXYGEN_CONFIG_TEMPLATE}
    COMMENT "Generating documentation with Doxygen")
  add_custom_target(doxygen ALL DEPENDS ${DOXYGEN_INDEX_FILE})
else()
  message(STATUS "Could not find doxygen executable. Documentation generation will be disabled.")
endif()
