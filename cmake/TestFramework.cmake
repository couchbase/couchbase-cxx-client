# The shared test framework: the runner, the common main(), the registration helpers, and the
# framework's own self-test.
#
# Hand-rolled (CXXCBC-885), deliberately NOT Catch2. It lives outside either test subdirectory
# because both test trees link it. See test/cng/README.md to build and run the CNG tests.

find_package(Threads REQUIRED)

# Each test executable links this and provides its own tests(). fmt comes from spdlog's bundled
# copy (header include dirs only). ${PROJECT_SOURCE_DIR}/test is PUBLIC so every test spells its
# includes from one root: "framework/test_runner.hxx", "cng/fixtures/live_fixture.hxx".
add_library(
  test_framework_main STATIC
  ${PROJECT_SOURCE_DIR}/test/framework/context.cxx
  ${PROJECT_SOURCE_DIR}/test/framework/requirement.cxx
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

# The shared cluster helpers. Promoted out of test/CMakeLists.txt because the framework's cluster
# probes are built on them, so they have to exist whenever either suite is built rather than only
# when the Catch2 suite is.
if(NOT TARGET test_utils)
  add_subdirectory(test/utils)
endif()

# Exactly one of these provides make_probe_backend(), and couchbase_add_test() picks which by
# whether the test links the client library. OBJECT rather than STATIC so the definition is always
# contributed: an archive is only searched when something already references what it holds, and the
# reference here comes from the framework's own main().
#
# The null one answers every probe with "I cannot ask", which the runner reports as undetermined and
# therefore as a failure. A test that asks about the server from a binary with no way to reach one
# is a registration mistake, and skipping it would hide the mistake for as long as it existed.
add_library(test_framework_null_probes OBJECT ${PROJECT_SOURCE_DIR}/test/framework/null_probes.cxx)
target_include_directories(test_framework_null_probes PRIVATE ${PROJECT_SOURCE_DIR}/test)
target_include_directories(
  test_framework_null_probes SYSTEM BEFORE
  PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>)
set_target_properties(test_framework_null_probes PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_project_options(test_framework_null_probes)
set_project_warnings(test_framework_null_probes)

# The real one drives a connection through test::utils::integration_test_guard, so it is the only
# part of the framework that includes core headers -- and it is a .cxx, which is what keeps
# context.hxx lean.
add_library(test_framework_cluster_probes OBJECT
            ${PROJECT_SOURCE_DIR}/test/framework/cluster_probes.cxx)
target_include_directories(
  test_framework_cluster_probes PRIVATE ${PROJECT_SOURCE_DIR}/test ${PROJECT_SOURCE_DIR}
                                       ${PROJECT_BINARY_DIR}/generated
                                       ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
target_include_directories(
  test_framework_cluster_probes SYSTEM BEFORE
  PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party/expected/include>
          $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>
          $<BUILD_INTERFACE:$<TARGET_PROPERTY:asio,INTERFACE_INCLUDE_DIRECTORIES>>)
propagate_public_compile_definitions(test_framework_cluster_probes spdlog::spdlog asio)
target_link_libraries(test_framework_cluster_probes PRIVATE $<BUILD_INTERFACE:Microsoft.GSL::GSL>
                                                            $<BUILD_INTERFACE:taocpp::json>)
set_target_properties(test_framework_cluster_probes PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_project_options(test_framework_cluster_probes)
set_project_warnings(test_framework_cluster_probes)

# couchbase_discover_tests(<target> PROPERTIES <prop> <value>...)
#
# Registers one ctest entry per case, named <target>.<case>, the way catch_discover_tests does for
# the Catch2 suites. Per-executable registration would be coarser than what the suite already
# depends on: bin/run-integration-tests shards the valgrind leg with `ctest -I i,,n`, whose stride
# runs over ctest entries, and --output-junit builds the CI report from one row per entry.
#
# The list is produced at post-build by running the executable with --list-tests, which reports
# every case regardless of what the environment satisfies -- so the registered set does not depend
# on the machine that built it.
function(couchbase_discover_tests target)
  cmake_parse_arguments(ARG "" "" "PROPERTIES" ${ARGN})

  get_property(is_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
  if(is_multi_config)
    set(ctest_file "${CMAKE_CURRENT_BINARY_DIR}/${target}_tests_$<CONFIG>.cmake")
    set(ctest_file_at_test_time "${CMAKE_CURRENT_BINARY_DIR}/${target}_tests_\${CTEST_CONFIGURATION_TYPE}.cmake")
  else()
    set(ctest_file "${CMAKE_CURRENT_BINARY_DIR}/${target}_tests.cmake")
    set(ctest_file_at_test_time "${ctest_file}")
  endif()

  add_custom_command(
    TARGET ${target}
    POST_BUILD
    BYPRODUCTS "${ctest_file}"
    COMMAND
      "${CMAKE_COMMAND}" -D "TEST_TARGET=${target}" -D "TEST_EXECUTABLE=$<TARGET_FILE:${target}>" -D
      "TEST_PROPERTIES=${ARG_PROPERTIES}" -D "CTEST_FILE=${ctest_file}" -P
      "${PROJECT_SOURCE_DIR}/cmake/TestFrameworkAddTests.cmake"
    COMMENT "Enumerating test cases in ${target}"
    VERBATIM)

  # ctest reads TEST_INCLUDE_FILES at the start of a run, when the generated file may not exist --
  # a fresh configure, or a build that never got as far as linking this target. Registering a test
  # that cannot pass is the point: an unbuilt binary must not read as a binary with no failures.
  #
  # The placeholder runs cmake -E false rather than a command named after the target: a missing
  # command fails with a different message on every platform ("command not found", "file not
  # found", a Windows error box), and one of them has to be recognisable as the intended failure.
  set(include_file "${CMAKE_CURRENT_BINARY_DIR}/${target}_include.cmake")
  file(
    WRITE "${include_file}"
    "if(EXISTS \"${ctest_file_at_test_time}\")\n"
    "  include(\"${ctest_file_at_test_time}\")\n"
    "else()\n"
    # Positional, not add_test(NAME ... COMMAND ...): the keyword signature is a CMakeLists
    # feature. Meeting one in an include file, ctest takes the first argument as the test name and
    # the rest as the command, so it registers an entry literally called "NAME" that reports Not
    # Run. The positional form is what makes the entry carry the target's name and fail for the
    # reason it was registered.
    "  add_test([==[${target}_NOT_BUILT]==] [==[${CMAKE_COMMAND}]==] -E false)\n"
    # The placeholder carries the same labels as the real cases would. Without them a labelled run
    # -- which is how CI runs everything -- would filter the placeholder out, and an unbuilt binary
    # would once again read as a binary with no failures.
    "  set(properties [==[${ARG_PROPERTIES}]==])\n"
    "  set_tests_properties(${target}_NOT_BUILT PROPERTIES \${properties})\n"
    "endif()\n")
  set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${include_file}")
endfunction()

# Which client library a LINK_CLIENT test links. test/cng/CMakeLists.txt narrows this for the
# couchbase2 tests, which also link the generated protobuf stubs directly.
set(couchbase_cxx_client_test_library ${couchbase_cxx_client_DEFAULT_LIBRARY})

# couchbase_add_test(<relpath> LABEL <label> [LINK_CLIENT] [LIBS <lib>...])
#
# relpath is under test/ without the extension (e.g. cng/protostellar/dispatcher). The target name
# is that path with the separators flattened, which makes it unique by construction: keying on the
# file stem instead would collide the moment two subsystems each have a parser.cxx. LINK_CLIENT
# links the client library and its core header include set, for tests that exercise core code.
function(couchbase_add_test relpath)
  cmake_parse_arguments(ARG "LINK_CLIENT" "LABEL" "LIBS" ${ARGN})

  # Every entry is a label some leg in bin/ or .github/workflows/ selects with --label-regex. A
  # label no runner selects is worse than a typo: it configures, builds and links, and the cases
  # are then never executed by anything. Do not add one here without the leg that runs it.
  set(known_labels unit integration transaction benchmark cng)
  if(NOT ARG_LABEL IN_LIST known_labels)
    # A typo here is invisible at build time and then quietly drops the binary out of the CI leg
    # that was supposed to run it.
    message(FATAL_ERROR "couchbase_add_test(${relpath}): LABEL must be one of ${known_labels}, "
                        "got \"${ARG_LABEL}\"")
  endif()

  string(REPLACE "/" "_" target "${relpath}")
  add_executable(${target} ${PROJECT_SOURCE_DIR}/test/${relpath}.cxx)
  target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR})
  target_include_directories(
    ${target} SYSTEM BEFORE
    PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>)
  target_link_libraries(${target} PRIVATE test_framework_main Threads::Threads)
  if(ARG_LINK_CLIENT)
    target_include_directories(${target} PRIVATE ${PROJECT_BINARY_DIR}/generated
                                                 ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
    target_include_directories(
      ${target} SYSTEM BEFORE
      PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party/expected/include>
              $<BUILD_INTERFACE:$<TARGET_PROPERTY:asio,INTERFACE_INCLUDE_DIRECTORIES>>)
    propagate_public_compile_definitions(${target} spdlog::spdlog asio)
    target_link_libraries(
      ${target}
      PRIVATE ${couchbase_cxx_client_test_library} test_framework_cluster_probes test_utils
              $<BUILD_INTERFACE:Microsoft.GSL::GSL> $<BUILD_INTERFACE:taocpp::json>)
  else()
    target_link_libraries(${target} PRIVATE test_framework_null_probes)
  endif()
  if(ARG_LIBS)
    target_link_libraries(${target} PRIVATE ${ARG_LIBS})
  endif()
  set_project_options(${target})
  set_project_warnings(${target})

  # Join the aggregate that build_<suite>_tests depends on. bin/build-tests builds that aggregate
  # rather than `all` whenever CB_TEST_SUITE is set, and a target missing from it is not built --
  # which leaves the discovery file absent and the case registered as the _NOT_BUILT placeholder.
  #
  # cmake/Testing.cmake blanks these properties, and it is included after this file, so an append
  # made while this file is being read is discarded. Only calls from test/CMakeLists.txt, which
  # Testing.cmake reads later, survive -- which is why a framework test that needs to be in an
  # aggregate is registered there and not here. cng has no aggregate; it is built only by `all`.
  if(ARG_LABEL STREQUAL "unit")
    set_property(GLOBAL APPEND PROPERTY COUCHBASE_UNIT_TESTS ${target})
  elseif(ARG_LABEL STREQUAL "integration")
    set_property(GLOBAL APPEND PROPERTY COUCHBASE_INTEGRATION_TESTS ${target})
  elseif(ARG_LABEL STREQUAL "transaction")
    set_property(GLOBAL APPEND PROPERTY COUCHBASE_TRANSACTION_TESTS ${target})
  elseif(ARG_LABEL STREQUAL "benchmark")
    set_property(GLOBAL APPEND PROPERTY COUCHBASE_BENCHMARKS ${target})
  endif()

  # The harness returns 77 when every case was skipped; map that to ctest's "Skipped".
  set(properties SKIP_RETURN_CODE 77 LABELS "${ARG_LABEL}")
  # Sanitizers are applied per target (cmake/Sanitizers.cmake), so dependencies fetched and built
  # as ordinary packages -- gRPC and abseil among them -- carry no instrumentation, and TSan cannot
  # see the happens-before edges inside their event-engine and futex code. Without
  # ignore_noninstrumented_modules it reports those as races: the dispatcher tests drive real gRPC
  # threads and produced 54 such reports. Set on the test rather than in CI so a local `ctest` under
  # TSan behaves the same way.
  if(DEFINED COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS AND EXISTS "${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}")
    list(
      APPEND
      properties
      ENVIRONMENT
      "TSAN_OPTIONS=halt_on_error=0,second_deadlock_stack=1,ignore_noninstrumented_modules=1,suppressions=${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}"
    )
  endif()
  couchbase_discover_tests(${target} PROPERTIES ${properties})
endfunction()

# cng, because that is the leg whose steps deliberately do not apply --test-action memcheck: the
# inner suites here assert on absolute millisecond budgets, which valgrind would blow with no
# multiplier able to reach them.
couchbase_add_test(framework/selftest LABEL cng)
