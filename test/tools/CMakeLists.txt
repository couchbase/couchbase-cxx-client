add_executable(tool_kv_loader tool_kv_loader.cxx)
target_include_directories(tool_kv_loader PRIVATE ${PROJECT_BINARY_DIR}/generated)
target_link_libraries(
        tool_kv_loader
        project_options
        project_warnings
        Threads::Threads
        snappy
        couchbase_cxx_client
        test_utils)
