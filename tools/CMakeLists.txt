if(NOT TARGET CLI11::CLI11)
  # https://github.com/CLIUtils/CLI11/releases
  cpmaddpackage(
    NAME
    cli11
    VERSION
    2.4.1
    GITHUB_REPOSITORY
    "cliutils/cli11"
    EXCLUDE_FROM_ALL ON
    OPTIONS
    "BUILD_TESTING OFF"
    "CLI11_BUILD_TESTS OFF"
    "CLI11_BUILD_DOCS OFF"
    "CLI11_BUILD_EXAMPLES OFF"
    "CLI11_SINGLE_FILE_TESTS OFF"
    "CLI11_INSTALL OFF"
    "CLI11_PRECOMPILED ON")
endif()

add_executable(
  cbc
  cbc.cxx
  utils.cxx
  analytics.cxx
  beam.cxx
  get.cxx
  pillowfight.cxx
  query.cxx
  version.cxx)
target_include_directories(cbc PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)

target_link_libraries(
  cbc
  PRIVATE
  ${CMAKE_THREAD_LIBS_INIT}
  ${couchbase_cxx_client_DEFAULT_LIBRARY}
  CLI11
  Microsoft.GSL::GSL
  taocpp::json
  spdlog::spdlog
  hdr_histogram_static
  asio)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
  # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the *.PDB
  # from the BoringSSL build
  set_target_properties(cbc PROPERTIES LINK_FLAGS "/ignore:4099")
endif()
