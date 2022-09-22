macro(define_example name)
    add_executable(${name} ${name}.cxx)
    target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
    target_link_libraries(${name} ${CMAKE_THREAD_LIBS_INIT} couchbase_cxx_client)
endmacro()

define_example(game_server)