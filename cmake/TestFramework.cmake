# The shared test framework: the runner, the common main(), and the framework's own self-test.
#
# Hand-rolled (CXXCBC-885), deliberately NOT Catch2. It lives outside either test subdirectory
# because both test trees link it. See test/cng/README.md to build and run the CNG tests.

find_package(Threads REQUIRED)

# Each test executable links this and provides its own tests(). fmt comes from spdlog's bundled
# copy (header include dirs only). ${PROJECT_SOURCE_DIR}/test is PUBLIC so a test includes the
# framework as "framework/test_runner.hxx" wherever in the test tree it sits.
add_library(
  test_framework_main STATIC
  ${PROJECT_SOURCE_DIR}/test/framework/test_runner.cxx
  ${PROJECT_SOURCE_DIR}/test/framework/test_main.cxx)
target_include_directories(test_framework_main PUBLIC ${PROJECT_SOURCE_DIR}/test
                                                      ${PROJECT_SOURCE_DIR})
# spdlog is linked, not merely included, so the framework compiles bundled fmt in the same mode as
# the client library. Borrowing only spdlog's include directories left SPDLOG_COMPILED_LIB undefined
# here, and spdlog/fmt/fmt.h responds by defining FMT_HEADER_ONLY, which emits definitions of
# fmt::v11::vformat, report_error, is_printable, write_loc and friends into test_runner.obj and
# test_main.obj. A test that also links the client then puts those next to the compiled spdlog's
# copies of the same symbols. ld folds that silently; link.exe does not, and fails with LNK2005.
# declare_system_library() already marks spdlog's includes SYSTEM, so linking it keeps its headers
# out of /W4 /WX without the manual BEFORE override.
target_link_libraries(test_framework_main PUBLIC Threads::Threads spdlog::spdlog)
set_project_options(test_framework_main)
set_project_warnings(test_framework_main)

# The framework's own self-test: it drives run() with in-memory suites and asserts on the returned
# run_result, so it links the framework and nothing else.
add_executable(test_framework_selftest
               ${PROJECT_SOURCE_DIR}/test/framework/framework_selftest.cxx)
target_include_directories(
  test_framework_selftest SYSTEM BEFORE
  PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>)
target_link_libraries(test_framework_selftest PRIVATE test_framework_main Threads::Threads)
set_project_options(test_framework_selftest)
set_project_warnings(test_framework_selftest)
add_test(NAME test_framework_selftest COMMAND $<TARGET_FILE:test_framework_selftest>)
# The harness returns 77 when every case was skipped; map that to ctest's "Skipped".
#
# The label must be one a CI leg actually selects, or these cases are compiled, linked and never
# run. Only unit, integration, transaction, benchmark and cng are selected anywhere in bin/ or
# .github/workflows/. cng is the one that fits: its steps deliberately do not apply --test-action
# memcheck, and the inner suites here assert on absolute millisecond budgets that valgrind would
# blow with no multiplier able to reach them.
set_tests_properties(test_framework_selftest PROPERTIES SKIP_RETURN_CODE 77 LABELS "cng")
if(DEFINED COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS AND EXISTS "${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}")
  set_tests_properties(
    test_framework_selftest
    PROPERTIES
      ENVIRONMENT
      "TSAN_OPTIONS=halt_on_error=0,second_deadlock_stack=1,ignore_noninstrumented_modules=1,suppressions=${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}"
  )
endif()
