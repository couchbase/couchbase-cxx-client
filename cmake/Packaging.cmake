include(GNUInstallDirs)

set_target_properties(couchbase_cxx_client PROPERTIES VERSION ${couchbase_cxx_client_VERSION_MAJOR}
                                                      SOVERSION ${couchbase_cxx_client_VERSION})
install(TARGETS couchbase_cxx_client RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(COUCHBASE_CXX_CLIENT_BUILD_TOOLS)
  install(TARGETS cbc RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase-cxx-client.pc.in
               ${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client.pc @ONLY)
install(FILES ${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

set(COUCHBASE_CXX_CLIENT_TARBALL_NAME "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_SEMVER}")
set(COUCHBASE_CXX_CLIENT_MANIFEST "${PROJECT_BINARY_DIR}/packaging/MANIFEST")

if(APPLE)
  find_program(TAR gtar)
else()
  find_program(TAR tar)
endif()

add_custom_target(
  packaging_file_manifest
  COMMAND git ls-files --recurse-submodules | LC_ALL=C sort > ${COUCHBASE_CXX_CLIENT_MANIFEST}
  BYPRODUCTS ${COUCHBASE_CXX_CLIENT_MANIFEST}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_target(
  packaging_tarball
  WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
  COMMAND rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND mkdir "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${TAR} -cf - -C ${PROJECT_SOURCE_DIR} -T ${COUCHBASE_CXX_CLIENT_MANIFEST} | ${TAR} xf - -C
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  # https://reproducible-builds.org/docs/archives/
  COMMAND ${TAR} --sort=name --mtime="${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}Z" --owner=0 --group=0 --numeric-owner -czf
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz" "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  BYPRODUCTS "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz"
  DEPENDS ${COUCHBASE_CXX_CLIENT_MANIFEST})
