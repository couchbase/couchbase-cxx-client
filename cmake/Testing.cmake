add_subdirectory(third_party/catch2)
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/third_party/catch2/contrib")
enable_testing()
include(Catch)

define_property(
  GLOBAL
  PROPERTY COUCHBASE_INTEGRATION_TESTS
  BRIEF_DOCS "list of integration tests"
  FULL_DOCS "list of integration tests targets")
set_property(GLOBAL PROPERTY COUCHBASE_INTEGRATION_TESTS "")

macro(integration_test name)
  add_executable(test_integration_${name} "${PROJECT_SOURCE_DIR}/test/test_integration_${name}.cxx")
  target_include_directories(test_integration_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    test_integration_${name}
    project_options
    project_warnings
    Catch2::Catch2WithMain
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    test_integration_${name}
    PROPERTIES
    SKIP_REGULAR_EXPRESSION
    "SKIP"
    LABELS
    "integration")
  set_property(GLOBAL APPEND PROPERTY COUCHBASE_INTEGRATION_TESTS "test_integration_${name}")
endmacro()

define_property(
  GLOBAL
  PROPERTY COUCHBASE_TRANSACTION_TESTS
  BRIEF_DOCS "list of transaction tests"
  FULL_DOCS "list of transaction tests targets")
set_property(GLOBAL PROPERTY COUCHBASE_TRANSACTION_TESTS "")

macro(transaction_test name)
  add_executable(test_transaction_${name} "${PROJECT_SOURCE_DIR}/test/test_transaction_${name}.cxx")
  target_include_directories(test_transaction_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    test_transaction_${name}
    project_options
    project_warnings
    Catch2::Catch2WithMain
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    test_transaction_${name}
    PROPERTIES
    SKIP_REGULAR_EXPRESSION
    "SKIP"
    LABELS
    "transaction")
  set_property(GLOBAL APPEND PROPERTY COUCHBASE_TRANSACTION_TESTS "test_transaction_${name}")
endmacro()

define_property(
  GLOBAL
  PROPERTY COUCHBASE_UNIT_TESTS
  BRIEF_DOCS "list of unit tests"
  FULL_DOCS "list of unit tests targets")
set_property(GLOBAL PROPERTY COUCHBASE_UNIT_TESTS "")
macro(unit_test name)
  add_executable(test_unit_${name} "${PROJECT_SOURCE_DIR}/test/test_unit_${name}.cxx")
  target_include_directories(test_unit_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    test_unit_${name}
    project_options
    project_warnings
    Catch2::Catch2WithMain
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    test_unit_${name}
    PROPERTIES
    SKIP_REGULAR_EXPRESSION
    "SKIP"
    LABELS
    "unit")
  set_property(GLOBAL APPEND PROPERTY COUCHBASE_UNIT_TESTS "test_unit_${name}")
endmacro()

define_property(
  GLOBAL
  PROPERTY COUCHBASE_BENCHMARKS
  BRIEF_DOCS "list of benchmarks"
  FULL_DOCS "list of benchmark targets")
set_property(GLOBAL PROPERTY COUCHBASE_BENCHMARKS "")
macro(integration_benchmark name)
  add_executable(benchmark_integration_${name} "${PROJECT_SOURCE_DIR}/test/benchmark_integration_${name}.cxx")
  target_include_directories(benchmark_integration_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
  target_link_libraries(
    benchmark_integration_${name}
    project_options
    project_warnings
    Catch2::Catch2WithMain
    Threads::Threads
    snappy
    couchbase_cxx_client
    test_utils)
  catch_discover_tests(
    benchmark_integration_${name}
    PROPERTIES
    SKIP_REGULAR_EXPRESSION
    "SKIP"
    LABELS
    "benchmark")
  set_property(GLOBAL APPEND PROPERTY COUCHBASE_BENCHMARKS "benchmark_integration_${name}")
endmacro()

add_subdirectory(${PROJECT_SOURCE_DIR}/test)

get_property(integration_targets GLOBAL PROPERTY COUCHBASE_INTEGRATION_TESTS)
add_custom_target(build_integration_tests DEPENDS ${integration_targets})

get_property(unit_targets GLOBAL PROPERTY COUCHBASE_UNIT_TESTS)
add_custom_target(build_unit_tests DEPENDS ${unit_targets})

get_property(transaction_targets GLOBAL PROPERTY COUCHBASE_TRANSACTION_TESTS)
add_custom_target(build_transaction_tests DEPENDS ${transaction_targets})

get_property(benchmark_targets GLOBAL PROPERTY COUCHBASE_BENCHMARKS)
add_custom_target(build_benchmarks DEPENDS ${benchmark_targets})
