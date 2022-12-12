macro(define_example name)
    add_executable(${name} ${name}.cxx)
    target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
    target_link_libraries(${name} ${CMAKE_THREAD_LIBS_INIT} couchbase_cxx_client)
endmacro()

define_example(game_server)
define_example(async_game_server)
define_example(minimal)
define_example(distributed_mutex)

macro(define_asio_example name)
    add_executable(${name} ${name}.cxx)
    target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
    target_link_libraries(${name} ${CMAKE_THREAD_LIBS_INIT})
endmacro()

define_asio_example(channels)
