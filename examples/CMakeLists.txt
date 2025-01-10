macro(define_example name)
  add_executable(${name} ${name}.cxx)
  target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
  target_link_libraries(
    ${name}
    ${CMAKE_THREAD_LIBS_INIT}
    taocpp::json
    spdlog::spdlog
    asio
    ${couchbase_cxx_client_DEFAULT_LIBRARY})
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
    # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
    # *.PDB from the BoringSSL build
    set_target_properties(${name} PROPERTIES LINK_FLAGS "/ignore:4099")
  endif()
endmacro()

define_example(minimal)
define_example(bulk_base_api)
define_example(bulk_transactional_api)
define_example(bulk_transactional_get_replace)
define_example(game_server)
define_example(async_game_server)
define_example(distributed_mutex)

macro(define_asio_example name)
  add_executable(${name} ${name}.cxx)
  target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
  target_link_libraries(${name} PRIVATE ${CMAKE_THREAD_LIBS_INIT} asio taocpp::json)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
    # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
    # *.PDB from the BoringSSL build
    set_target_properties(${name} PROPERTIES LINK_FLAGS "/ignore:4099")
  endif()
endmacro()

define_asio_example(channels)
