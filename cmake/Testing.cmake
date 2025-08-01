if(NOT TARGET Catch2::Catch2)
  cpmaddpackage(
    NAME
    catch2
    VERSION
    3.4.0
    GITHUB_REPOSITORY
    "catchorg/catch2")
endif()

list(APPEND CMAKE_MODULE_PATH "${catch2_SOURCE_DIR}/extras")

if(COUCHBASE_CXX_CLIENT_ENABLE_VALGRIND)
  find_program(VALGRIND valgrind)
  if(VALGRIND)
    set(VALGRIND_COMMAND "${VALGRIND}")
    set(VALGRIND_COMMAND_OPTIONS
        "-v --tool=memcheck --leak-check=full --track-fds=yes --num-callers=50 --show-reachable=yes --track-origins=yes --malloc-fill=0xff --free-fill=0xfe --suppressions=${PROJECT_SOURCE_DIR}/.valgrind_suppressions.txt"
    )
    set(MEMORYCHECK_COMMAND "${VALGRIND}")
    set(MEMORYCHECK_COMMAND_OPTIONS "${VALGRIND_COMMAND_OPTIONS}")
    set(CTEST_MEMORYCHECK_TYPE Valgrind)
    set(CTEST_MEMORYCHECK_COMMAND "${VALGRIND}")
    set(CTEST_MEMORYCHECK_COMMAND_OPTIONS "${VALGRIND_COMMAND_OPTIONS}")
  else()
    message(SEND_ERROR "valgrind requested but executable not found")
  endif()
endif()

include(CTest)
include(Catch)

define_property(
  GLOBAL
  PROPERTY COUCHBASE_INTEGRATION_TESTS
  BRIEF_DOCS "list of integration tests"
  FULL_DOCS "list of integration tests targets")
set_property(GLOBAL PROPERTY COUCHBASE_INTEGRATION_TESTS "")

macro(integration_test name)
  add_executable(test_integration_${name} "${PROJECT_SOURCE_DIR}/test/test_integration_${name}.cxx")
  target_include_directories(test_integration_${name} PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/generated
                                                              ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
  target_include_directories(
    test_integration_${name} SYSTEM PRIVATE
                            ${PROJECT_SOURCE_DIR}/third_party/cxx_function
                            ${PROJECT_SOURCE_DIR}/third_party/expected/include)
  target_link_libraries(
    test_integration_${name}
    project_options
    project_warnings
    ${couchbase_cxx_client_DEFAULT_LIBRARY}
    test_main
    Threads::Threads
    Microsoft.GSL::GSL
    asio
    taocpp::json
    spdlog::spdlog
    test_utils)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
    target_link_libraries(test_integration_${name} OpenSSL::SSL)
    if(WIN32)
      # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
      # *.PDB from the BoringSSL build
      set_target_properties(test_integration_${name} PROPERTIES LINK_FLAGS "/ignore:4099")
    endif()
  endif()
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
  target_include_directories(test_transaction_${name} PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/generated
                                                              ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
  target_include_directories(
    test_transaction_${name} SYSTEM PRIVATE
                            ${PROJECT_SOURCE_DIR}/third_party/cxx_function
                            ${PROJECT_SOURCE_DIR}/third_party/expected/include)
  target_link_libraries(
    test_transaction_${name}
    project_options
    project_warnings
    test_main
    Threads::Threads
    Microsoft.GSL::GSL
    asio
    taocpp::json
    spdlog::spdlog
    ${couchbase_cxx_client_DEFAULT_LIBRARY}
    test_utils)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
    target_link_libraries(test_transaction_${name} OpenSSL::SSL)
    if(WIN32)
      # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
      # *.PDB from the BoringSSL build
      set_target_properties(test_transaction_${name} PROPERTIES LINK_FLAGS "/ignore:4099")
    endif()
  endif()
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
  target_include_directories(test_unit_${name} PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/generated
                                                       ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
  target_include_directories(
    test_unit_${name} SYSTEM PRIVATE
                            ${PROJECT_SOURCE_DIR}/third_party/cxx_function
                            ${PROJECT_SOURCE_DIR}/third_party/expected/include)
  target_link_libraries(
    test_unit_${name}
    project_options
    project_warnings
    test_main
    Threads::Threads
    Microsoft.GSL::GSL
    asio
    taocpp::json
    spdlog::spdlog
    ${couchbase_cxx_client_DEFAULT_LIBRARY}
    test_utils)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
    target_link_libraries(test_unit_${name} OpenSSL::SSL)
    if(WIN32)
      # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
      # *.PDB from the BoringSSL build
      set_target_properties(test_unit_${name} PROPERTIES LINK_FLAGS "/ignore:4099")
    endif()
  endif()
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
  target_include_directories(
    benchmark_integration_${name} PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/generated
                                          ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
  target_include_directories(
    benchmark_integration_${name} SYSTEM PRIVATE
                            ${PROJECT_SOURCE_DIR}/third_party/cxx_function
                            ${PROJECT_SOURCE_DIR}/third_party/expected/include)

  target_link_libraries(
    benchmark_integration_${name}
    project_options
    project_warnings
    test_main
    Threads::Threads
    Microsoft.GSL::GSL
    asio
    taocpp::json
    spdlog::spdlog
    ${couchbase_cxx_client_DEFAULT_LIBRARY}
    test_utils)
  if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
    target_link_libraries(benchmark_integration_${name} OpenSSL::SSL)
    if(WIN32)
      # Ignore the `LNK4099: PDB ['crypto.pdb'|'ssl.pdb'] was not found` warnings, as we don't (atm) keep track fo the
      # *.PDB from the BoringSSL build
      set_target_properties(benchmark_integration_${name} PROPERTIES LINK_FLAGS "/ignore:4099")
    endif()
  endif()
  catch_discover_tests(
    benchmark_integration_${name}
    PROPERTIES
    SKIP_REGULAR_EXPRESSION
    "SKIP"
    LABELS
    "benchmark")
  set_property(GLOBAL APPEND PROPERTY COUCHBASE_BENCHMARKS "benchmark_integration_${name}")
endmacro()

add_library(test_main OBJECT ${PROJECT_SOURCE_DIR}/test/main.cxx)
target_link_libraries(test_main PUBLIC Catch2::Catch2 OpenSSL::SSL)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND WIN32)
  set_target_properties(test_main PROPERTIES LINK_FLAGS "/ignore:4099")
endif()

add_subdirectory(${PROJECT_SOURCE_DIR}/test)

get_property(integration_targets GLOBAL PROPERTY COUCHBASE_INTEGRATION_TESTS)
add_custom_target(build_integration_tests DEPENDS ${integration_targets})

get_property(unit_targets GLOBAL PROPERTY COUCHBASE_UNIT_TESTS)
add_custom_target(build_unit_tests DEPENDS ${unit_targets})

get_property(transaction_targets GLOBAL PROPERTY COUCHBASE_TRANSACTION_TESTS)
add_custom_target(build_transaction_tests DEPENDS ${transaction_targets})

get_property(benchmark_targets GLOBAL PROPERTY COUCHBASE_BENCHMARKS)
add_custom_target(build_benchmarks DEPENDS ${benchmark_targets})
