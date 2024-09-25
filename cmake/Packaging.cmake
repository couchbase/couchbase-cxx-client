include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/couchbase DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES LICENSE.txt DESTINATION ${CMAKE_INSTALL_DOCDIR})

configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client.pc.in
               ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client.pc @ONLY)
install(FILES ${PROJECT_BINARY_DIR}/packaging/couchbase_cxx_client.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

configure_package_config_file(
  ${PROJECT_SOURCE_DIR}/cmake/couchbase_cxx_client-config.cmake.in couchbase_cxx_client-config.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)
write_basic_package_version_file(
  couchbase_cxx_client-version.cmake
  VERSION ${couchbase_cxx_client_VERSION}
  COMPATIBILITY SameMinorVersion)
install(FILES ${PROJECT_BINARY_DIR}/couchbase_cxx_client-version.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)

if(COUCHBASE_CXX_CLIENT_BUILD_TOOLS)
  install(TARGETS cbc RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

install(
  TARGETS ${couchbase_cxx_client_LIBRARIES}
  EXPORT couchbase_cxx_client-targets
  DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(
  EXPORT couchbase_cxx_client-targets
  NAMESPACE couchbase_cxx_client::
  FILE couchbase_cxx_client-config.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/couchbase_cxx_client)

set(COUCHBASE_CXX_CLIENT_TARBALL_NAME "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_SEMVER}")
set(COUCHBASE_CXX_CLIENT_TARBALL "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz")
set(COUCHBASE_CXX_CLIENT_MANIFEST "${PROJECT_BINARY_DIR}/packaging/MANIFEST")

if(APPLE)
  find_program(TAR gtar)
  find_program(SED gsed)
else()
  find_program(TAR tar)
  find_program(SED sed)
endif()

add_custom_command(
  OUTPUT ${COUCHBASE_CXX_CLIENT_MANIFEST}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMAND git ls-files --recurse-submodules | LC_ALL=C sort > ${COUCHBASE_CXX_CLIENT_MANIFEST})

set(COUCHBASE_CXX_TARBALL_THIRD_PARTY_GLOB_FILE ${PROJECT_SOURCE_DIR}/cmake/tarball_glob.txt)

if(COUCHBASE_CXX_RECORD_BUILD_INFO_FOR_TARBALL)
  file(
    WRITE "${CMAKE_SOURCE_DIR}/cmake/TarballRelease.cmake"
    "
set(CPM_DOWNLOAD_ALL OFF CACHE BOOL \"\" FORCE)
set(CPM_USE_NAMED_CACHE_DIRECTORIES ON CACHE BOOL \"\" FORCE)
set(CPM_USE_LOCAL_PACKAGES OFF CACHE BOOL \"\" FORCE)
set(CPM_SOURCE_CACHE \"\${PROJECT_SOURCE_DIR}/third_party_cache\" CACHE STRING \"\" FORCE)
set(COUCHBASE_CXX_CLIENT_GIT_REVISION \"${COUCHBASE_CXX_CLIENT_GIT_REVISION}\")
set(COUCHBASE_CXX_CLIENT_GIT_DESCRIBE \"${COUCHBASE_CXX_CLIENT_GIT_DESCRIBE}\")
set(COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP \"${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}\")
set(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT \"\${PROJECT_SOURCE_DIR}/third_party_cache\" CACHE STRING \"\" FORCE)
message(STATUS \"Building from Tarball: ${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz\")
")
endif()

add_custom_command(
  OUTPUT ${COUCHBASE_CXX_CLIENT_TARBALL}
  WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${TAR} -cf - -C ${PROJECT_SOURCE_DIR} -T ${COUCHBASE_CXX_CLIENT_MANIFEST} | ${TAR} xf - -C
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND
    ${CMAKE_COMMAND} -S "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}" -B
    "${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/build"
    -DCPM_SOURCE_CACHE="${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    -DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT="${PROJECT_BINARY_DIR}/packaging/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF -DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=ON -DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF
    -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON -DCPM_DOWNLOAD_ALL=ON -DCPM_USE_NAMED_CACHE_DIRECTORIES=ON
    -DCPM_USE_LOCAL_PACKAGES=OFF -DCOUCHBASE_CXX_CLIENT_BUILD_STATIC=OFF -DCOUCHBASE_CXX_CLIENT_INSTALL=ON
    -DCOUCHBASE_CXX_RECORD_BUILD_INFO_FOR_TARBALL=ON
  COMMAND
    xargs --arg-file=${COUCHBASE_CXX_TARBALL_THIRD_PARTY_GLOB_FILE} -I {} find
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache" -wholename "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache/{}"
    -type f | grep -v "crypto_test_data\\|googletest" | uniq >
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/third_party_manifest.txt"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache"
  COMMAND xargs --arg-file="${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/third_party_manifest.txt" -I {} cp --parents {}
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache"
  COMMAND
    ${CMAKE_COMMAND} -E rename
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/filtered_cache/${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp/cache"
    "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/third_party_cache"
  COMMAND ${SED} -i "s/Git REQUIRED/Git/g\;s/NOT GIT/NOT CHECK_DIRTY OR NOT GIT/g"
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/third_party_cache/cpm/CPM_*.cmake"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}/tmp"
  # https://reproducible-builds.org/docs/archives/
  COMMAND ${TAR} --sort=name --mtime="${COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP}Z" --owner=0 --group=0 --numeric-owner -czf
          "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}.tar.gz" "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  COMMAND ${CMAKE_COMMAND} -E rm -rf "${COUCHBASE_CXX_CLIENT_TARBALL_NAME}"
  DEPENDS ${COUCHBASE_CXX_CLIENT_MANIFEST})

add_custom_target(packaging_tarball DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL})

option(COUCHBASE_CXX_CLIENT_RPM_TARGETS "Enable targets for building RPMs" FALSE)
if(COUCHBASE_CXX_CLIENT_RPM_TARGETS)
  find_program(MOCK mock REQUIRED) # dnf install -y mock
  find_program(SPECTOOL spectool REQUIRED) # dnf install -y rpmdevtools

  string(TIMESTAMP COUCHBASE_CXX_CLIENT_RPM_DATE "%a %b %d %Y" UTC)

  set(COUCHBASE_CXX_CLIENT_SPEC "${PROJECT_BINARY_DIR}/packaging/couchbase-cxx-client.spec")
  configure_file(${PROJECT_SOURCE_DIR}/cmake/couchbase-cxx-client.spec.in "${COUCHBASE_CXX_CLIENT_SPEC}" @ONLY)

  set(COUCHBASE_CXX_CLIENT_DEFAULT_ROOT "rocky-9-${CMAKE_SYSTEM_PROCESSOR}")
  set(COUCHBASE_CXX_CLIENT_RPM_NAME
      "couchbase-cxx-client-${COUCHBASE_CXX_CLIENT_PACKAGE_VERSION}-${COUCHBASE_CXX_CLIENT_PACKAGE_RELEASE}")
  set(COUCHBASE_CXX_CLIENT_SRPM "${PROJECT_BINARY_DIR}/packaging/srpm/${COUCHBASE_CXX_CLIENT_RPM_NAME}.el9.src.rpm")

  add_custom_command(
    OUTPUT ${COUCHBASE_CXX_CLIENT_SRPM}
    WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
    COMMAND ${SPECTOOL} --get-files couchbase-cxx-client.spec
    COMMAND ${MOCK} --buildsrpm --root=${COUCHBASE_CXX_CLIENT_DEFAULT_ROOT}
            --resultdir=${PROJECT_BINARY_DIR}/packaging/srpm --spec couchbase-cxx-client.spec --sources .
    DEPENDS ${COUCHBASE_CXX_CLIENT_TARBALL} ${COUCHBASE_CXX_CLIENT_SPEC})

  add_custom_target(packaging_srpm DEPENDS ${COUCHBASE_CXX_CLIENT_SRPM})

  list(APPEND COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS
    "rocky-9-${CMAKE_SYSTEM_PROCESSOR}"
    "rocky-8-${CMAKE_SYSTEM_PROCESSOR}"
    "amazonlinux-2023-${CMAKE_SYSTEM_PROCESSOR}"
  )

  message(STATUS "Supported build roots for RPM packages: ${COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS}")

  # Build the chain of the dependencies from the timestamps, so that everything
  # will be executed one-by-one in order, because the mock cannot run multiple
  # roots simultaneously
  set(last_output "")
  foreach(root ${COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS})
    set(timestamp "${PROJECT_BINARY_DIR}/packaging/rpm/${root}/done.txt")
    set(dependencies ${COUCHBASE_CXX_CLIENT_SRPM})

    if (last_output)
      list(APPEND dependencies ${last_output})
    endif()

    add_custom_command(
      COMMENT "Building RPM for ${root}"
      OUTPUT ${timestamp}
      WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/packaging"
      COMMAND ${MOCK} --rebuild --root=${root}
        --resultdir=${PROJECT_BINARY_DIR}/packaging/rpm/${root} "${COUCHBASE_CXX_CLIENT_SRPM}"
      COMMAND touch ${timestamp}
      DEPENDS ${dependencies})

    set(last_output ${timestamp})
  endforeach()

  # add target that depends on the last root
  add_custom_target(packaging_rpm DEPENDS ${last_output})
endif()
