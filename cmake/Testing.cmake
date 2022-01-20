add_subdirectory(third_party/catch2)
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/third_party/catch2/contrib")
enable_testing()
include(Catch)

macro(integration_test name)
  add_executable(test_integration_${name} "${PROJECT_SOURCE_DIR}/test/test_integration_${name}.cxx")
  target_include_directories(test_integration_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    test_integration_${name}
    project_options
    project_warnings
    Catch2::Catch2
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    test_integration_${name}
    PROPERTIES
    LABELS
    "integration")
endmacro()

macro(unit_test name)
  add_executable(test_unit_${name} "${PROJECT_SOURCE_DIR}/test/test_unit_${name}.cxx")
  target_include_directories(test_unit_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    test_unit_${name}
    project_options
    project_warnings
    Catch2::Catch2
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    test_unit_${name}
    PROPERTIES
    LABELS
    "unit")
endmacro()

macro(integration_benchmark name)
  add_executable(benchmark_integration_${name} "${PROJECT_SOURCE_DIR}/test/benchmark_integration_${name}.cxx")
  target_include_directories(benchmark_integration_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    benchmark_integration_${name}
    project_options
    project_warnings
    Catch2::Catch2
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    benchmark_integration_${name}
    PROPERTIES
    LABELS
    "benchmark")
endmacro()

add_subdirectory(${PROJECT_SOURCE_DIR}/test)
