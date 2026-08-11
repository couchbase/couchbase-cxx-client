# Which client library the tools link.
#
# The tools do not merely consume the SDK's public API: cbc links OpenTelemetry's OTLP exporter and
# fit_performer links gRPC and protobuf for its own service, so each needs protobuf (and, for
# fit_performer, gRPC) in its own right. In a package the shared client embeds those statically and
# hides them, so it exports none of them -- and a tool linking the shared client ends up running a
# second protobuf, a second gRPC and a second BoringSSL alongside the ones inside the library. Two
# gRPC event engines and two BoringSSL instances in one process is the blob duplication this
# packaging exists to avoid, and it is not merely wasted space: each copy carries its own descriptor
# pool, RNG and error queue.
#
# Linking the static library instead puts exactly one copy of every vendored dependency in each
# executable. The tools then have no runtime dependency on libcouchbase_cxx_client.so, which is why
# the RPM, DEB and Alpine tool packages no longer declare one. That is what the packages need, so
# this follows COUCHBASE_CXX_CLIENT_PACKAGE_BUILD.
#
# Off it the tools link the shared library, which is far cheaper to link and is the right trade for
# a build that only needs them to compile and run. It is NOT the shipped configuration: the
# duplication described above comes back, so a tool built that way exercises a different set of
# libraries than the packages contain.
option(COUCHBASE_CXX_CLIENT_STATIC_TOOLS "Link the command line tools against the static client library"
       ${COUCHBASE_CXX_CLIENT_PACKAGE_BUILD})

# The intermediate target rather than the bundled archive: cmake/Bundler.cmake gives the bundle only
# a location and its include directories, so linking it drops every dependency the archive cannot
# contain -- system zlib among them. The intermediate carries the full link interface and the same
# objects, and the archives it pulls in are the same ones the bundle would have contained, so there
# is still exactly one copy of each. The bundle exists to be installed for external consumers.
# Asked for and unavailable is an error, not a downgrade. Falling back to the shared library would
# give back exactly what the option exists to prevent -- a second protobuf, a runtime dependency on
# libcouchbase_cxx_client.so, and tool packages whose declared dependencies no longer describe them
# -- and it would do it silently, in the packaging configuration where it matters most.
if(COUCHBASE_CXX_CLIENT_STATIC_TOOLS)
  if(NOT COUCHBASE_CXX_CLIENT_BUILD_STATIC
     OR NOT TARGET couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
    message(
      FATAL_ERROR
        "COUCHBASE_CXX_CLIENT_STATIC_TOOLS is ON, but the static client library is not being built. "
        "Set COUCHBASE_CXX_CLIENT_BUILD_STATIC=ON, or turn STATIC_TOOLS off to link the tools "
        "against the shared library.")
  endif()
  set(couchbase_cxx_tools_client_library couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
else()
  set(couchbase_cxx_tools_client_library ${couchbase_cxx_client_DEFAULT_LIBRARY})
endif()

if(NOT TARGET CLI11::CLI11)
  # https://github.com/CLIUtils/CLI11/releases
  cpmaddpackage(
    NAME
    cli11
    VERSION
    2.5.0
    GITHUB_REPOSITORY
    "cliutils/cli11"
    OPTIONS
    "EXCLUDE_FROM_ALL ON"
    "BUILD_TESTING OFF"
    "CLI11_BUILD_TESTS OFF"
    "CLI11_BUILD_DOCS OFF"
    "CLI11_BUILD_EXAMPLES OFF"
    "CLI11_SINGLE_FILE_TESTS OFF"
    "CLI11_INSTALL OFF"
    "CLI11_PRECOMPILED ON")
  # cpmaddpackage sets cli11_SOURCE_DIR in this directory's scope only, and cmake/Packaging.cmake
  # needs it to install CLI11's licence alongside cbc. Promoted so it survives the scope boundary.
  set(cli11_SOURCE_DIR
      "${cli11_SOURCE_DIR}"
      CACHE INTERNAL "")
endif()

add_executable(
  cbc
  cbc.cxx
  utils.cxx
  key_generator.cxx
  analytics.cxx
  beam.cxx
  get.cxx
  keygen.cxx
  pillowfight.cxx
  query.cxx
  remove.cxx
  upsert.cxx
  version.cxx
  sysinfo.cxx
  config.cxx)
target_include_directories(cbc PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private
                                       ${PROJECT_BINARY_DIR}/generated)

set_project_warnings(cbc)
set_project_options(cbc)

target_link_libraries(
  cbc
  PRIVATE ${CMAKE_THREAD_LIBS_INIT}
          ${couchbase_cxx_tools_client_library}
          $<BUILD_INTERFACE:CLI11>
          $<BUILD_INTERFACE:Microsoft.GSL::GSL>
          $<BUILD_INTERFACE:taocpp::json>
          $<BUILD_INTERFACE:spdlog::spdlog>
          $<BUILD_INTERFACE:hdr_histogram_static>
          $<BUILD_INTERFACE:asio>)
if(COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY)
  target_link_libraries(cbc PRIVATE $<BUILD_INTERFACE:opentelemetry_exporter_otlp_http>
                                    $<BUILD_INTERFACE:opentelemetry_exporter_otlp_http_metric>)
endif()

propagate_public_compile_definitions(cbc spdlog::spdlog asio)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
  # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the *.PDB
  # from the BoringSSL build
  set_target_properties(cbc PROPERTIES LINK_FLAGS "/ignore:4099")
endif()

add_subdirectory(system_metrics)

target_link_libraries(
  cbc
  PRIVATE system_metrics_cxx)

if(COUCHBASE_CXX_CLIENT_BUILD_FIT_PERFORMER)
  add_subdirectory(fit_performer)
endif()
