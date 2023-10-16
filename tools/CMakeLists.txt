if(NOT TARGET CLI11::CLI11)
  cpmaddpackage(
    NAME
    cli11
    VERSION
    2.3.2
    GITHUB_REPOSITORY
    "cliutils/cli11"
    OPTIONS
    "CLI11_PRECOMPILED ON")
endif()

add_executable(
  cbc
  cbc.cxx
  utils.cxx
  analytics.cxx
  get.cxx
  pillowfight.cxx
  query.cxx
  version.cxx)
target_include_directories(cbc PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
target_link_libraries(
  cbc
  ${CMAKE_THREAD_LIBS_INIT}
  couchbase_cxx_client
  CLI11
  Microsoft.GSL::GSL
  taocpp::json
  asio)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
  # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the *.PDB
  # from the BoringSSL build
  set_target_properties(cbc PROPERTIES LINK_FLAGS "/ignore:4099")
endif()
