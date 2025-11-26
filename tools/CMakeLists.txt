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
  config.cxx)
target_include_directories(cbc PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)

set_project_warnings(cbc)
set_project_options(cbc)

target_link_libraries(
  cbc
  PRIVATE ${CMAKE_THREAD_LIBS_INIT}
          ${couchbase_cxx_client_DEFAULT_LIBRARY}
          $<BUILD_INTERFACE:CLI11>
          $<BUILD_INTERFACE:Microsoft.GSL::GSL>
          $<BUILD_INTERFACE:taocpp::json>
          $<BUILD_INTERFACE:spdlog::spdlog>
          $<BUILD_INTERFACE:hdr_histogram_static>
          $<BUILD_INTERFACE:asio>
          $<BUILD_INTERFACE:opentelemetry_exporter_otlp_http>
          $<BUILD_INTERFACE:opentelemetry_exporter_otlp_http_metric>
      )
propagate_public_compile_definitions(cbc spdlog::spdlog asio)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
  # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the *.PDB
  # from the BoringSSL build
  set_target_properties(cbc PROPERTIES LINK_FLAGS "/ignore:4099")
endif()
