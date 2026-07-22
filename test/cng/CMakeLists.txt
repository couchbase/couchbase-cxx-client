# CNG (Protostellar) test tree.
#
# Uses a small hand-rolled framework (see framework/, CXXCBC-885), deliberately NOT Catch2.
# See test/cng/README.md to build and run these tests. Enabled via COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS.

find_package(Threads REQUIRED)

# Shared harness: the runner + the shared main(). Each test executable links this and provides
# its own tests(). fmt comes from spdlog's bundled copy (header include dirs only).
add_library(
  cng_test_main STATIC
  ${PROJECT_SOURCE_DIR}/test/cng/framework/test_runner.cxx
  ${PROJECT_SOURCE_DIR}/test/cng/framework/test_main.cxx)
target_include_directories(cng_test_main PUBLIC ${PROJECT_SOURCE_DIR}/test/cng
                                                ${PROJECT_SOURCE_DIR})
target_include_directories(
  cng_test_main SYSTEM BEFORE
  PUBLIC $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>)
target_link_libraries(cng_test_main PUBLIC Threads::Threads)
set_project_options(cng_test_main)
set_project_warnings(cng_test_main)

# add_cng_test(<relpath>) — relpath is under test/cng/ without extension (e.g.
# framework/framework_selftest). The executable/ctest name is the file stem prefixed with cng_.
function(add_cng_test relpath)
  get_filename_component(stem ${relpath} NAME_WE)
  set(target cng_${stem}_test)
  # The name comes from the file stem, not the full relpath, so kv/options.cxx and
  # query/options.cxx would both want cng_options_test. Fail loudly at configure time instead of
  # letting the second add_executable() error out cryptically (or worse, silently win).
  if(TARGET ${target})
    message(FATAL_ERROR "add_cng_test(${relpath}): target ${target} already exists. Two test files "
                        "share a stem -- rename one of them.")
  endif()
  add_executable(${target} ${PROJECT_SOURCE_DIR}/test/cng/${relpath}.cxx)
  target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/test/cng ${PROJECT_SOURCE_DIR})
  target_include_directories(
    ${target} SYSTEM BEFORE
    PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>)
  target_link_libraries(${target} PRIVATE cng_test_main Threads::Threads)
  set_project_options(${target})
  set_project_warnings(${target})
  add_test(NAME ${target} COMMAND $<TARGET_FILE:${target}>)
  # The harness returns 77 when every case was skipped; map that to ctest's "Skipped".
  set_tests_properties(${target} PROPERTIES SKIP_RETURN_CODE 77 LABELS "cng")
endfunction()

add_cng_test(framework/framework_selftest)

# Protostellar codegen smoke test — only when couchbase2 support (and thus the generated stubs
# library) is built. See CXXCBC-886.
if(COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 AND TARGET couchbase_cxx_protostellar)
  add_cng_test(protostellar/codegen_smoke)
  target_link_libraries(cng_codegen_smoke_test PRIVATE couchbase_cxx_protostellar)
endif()
