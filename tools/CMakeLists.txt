add_executable(
  cbc
  cbc.cxx
  utils.cxx
  command_registry.cxx
  analytics.cxx
  get.cxx
  pillowfight.cxx
  query.cxx
  version.cxx)
target_include_directories(cbc PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
target_link_libraries(
  cbc
  ${CMAKE_THREAD_LIBS_INIT}
  couchbase_cxx_client
  docopt)
