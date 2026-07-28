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
# spdlog is linked, not merely included, so the framework compiles bundled fmt in the same mode as
# the client library. Borrowing only spdlog's include directories left SPDLOG_COMPILED_LIB undefined
# here, and spdlog/fmt/fmt.h responds by defining FMT_HEADER_ONLY, which emits definitions of
# fmt::v11::vformat, report_error, is_printable, write_loc and friends into test_runner.obj and
# test_main.obj. A test that also links the client -- any add_cng_test(... LINK_CLIENT) -- then puts
# those next to the compiled spdlog's copies of the same symbols. ld folds that silently; link.exe
# does not, and fails with LNK2005. declare_system_library() already marks spdlog's includes SYSTEM,
# so linking it keeps its headers out of /W4 /WX without the manual BEFORE override.
target_link_libraries(cng_test_main PUBLIC Threads::Threads spdlog::spdlog)
set_project_options(cng_test_main)
set_project_warnings(cng_test_main)

# add_cng_test(<relpath> [LINK_CLIENT] [LIBS <lib>...]) — relpath is under test/cng/ without
# extension (e.g. framework/framework_selftest). The executable/ctest name is the file stem
# prefixed with cng_. LINK_CLIENT links the client library and its core header include set, for
# tests that exercise core code (e.g. the connection-string parser).
function(add_cng_test relpath)
  cmake_parse_arguments(ARG "LINK_CLIENT" "" "LIBS" ${ARGN})
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
  if(ARG_LINK_CLIENT)
    target_include_directories(${target} PRIVATE ${PROJECT_BINARY_DIR}/generated
                                                 ${PROJECT_BINARY_DIR}/generated_$<CONFIG>)
    target_include_directories(
      ${target} SYSTEM BEFORE
      PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party/expected/include>
              $<BUILD_INTERFACE:$<TARGET_PROPERTY:asio,INTERFACE_INCLUDE_DIRECTORIES>>)
    propagate_public_compile_definitions(${target} spdlog::spdlog asio)
    target_link_libraries(
      ${target} PRIVATE ${couchbase_cxx_client_DEFAULT_LIBRARY}
                        $<BUILD_INTERFACE:Microsoft.GSL::GSL> $<BUILD_INTERFACE:taocpp::json>)
  endif()
  if(ARG_LIBS)
    target_link_libraries(${target} PRIVATE ${ARG_LIBS})
  endif()
  set_project_options(${target})
  set_project_warnings(${target})
  add_test(NAME ${target} COMMAND $<TARGET_FILE:${target}>)
  # The harness returns 77 when every case was skipped; map that to ctest's "Skipped".
  set_tests_properties(${target} PROPERTIES SKIP_RETURN_CODE 77 LABELS "cng")
  # Same TSan configuration cmake/Testing.cmake attaches to the Catch2 suites. Sanitizers are applied
  # per target (cmake/Sanitizers.cmake), so gRPC and abseil -- fetched and built as ordinary
  # dependencies -- carry no instrumentation, and TSan cannot see the happens-before edges inside
  # their event-engine and futex code. Without ignore_noninstrumented_modules it reports those as
  # races: dispatcher tests drive real gRPC threads and produced 54 such reports. Set on the test
  # rather than in CI so a local `ctest` under TSan behaves the same way.
  if(DEFINED COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS AND EXISTS "${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}")
    set_tests_properties(
      ${target}
      PROPERTIES
        ENVIRONMENT
        "TSAN_OPTIONS=halt_on_error=0,second_deadlock_stack=1,ignore_noninstrumented_modules=1,suppressions=${COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS}"
    )
  endif()
endfunction()

add_cng_test(framework/framework_selftest)

# couchbase2:// connection-string scheme (CXXCBC-887). Exercises the core parser, so it
# links the client library.
add_cng_test(connection_string/couchbase2_scheme LINK_CLIENT)

# origin protocol marker (CXXCBC-888).
add_cng_test(origin/protocol LINK_CLIENT)

# What couchbase2:// does in a build without couchbase2 support (CXXCBC-891). Deliberately outside
# the guard below: the behaviour it pins only exists in the default configuration.
add_cng_test(cluster/couchbase2_build_support LINK_CLIENT)

# Protostellar codegen smoke test — only when couchbase2 support (and thus the generated stubs
# library) is built. See CXXCBC-886.
if(COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 AND TARGET couchbase_cxx_protostellar)
  add_cng_test(protostellar/codegen_smoke)
  target_link_libraries(cng_codegen_smoke_test PRIVATE couchbase_cxx_protostellar)

  # gRPC<->asio bridge (CXXCBC-889). The transport now lives in the client library, so link
  # that (LINK_CLIENT) plus the stubs library for the generated KvService the in-process test
  # server implements.
  add_cng_test(protostellar/dispatcher LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Regression test for the gRPC callback-queue teardown race (CXXCBC-919).
  add_cng_test(protostellar/callback_queue_churn LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # TLS/auth credentials (CXXCBC-890).
  add_cng_test(protostellar/credentials LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Error mapping (CXXCBC-893).
  add_cng_test(protostellar/error_utils LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # KV converter (CXXCBC-892).
  add_cng_test(protostellar/kv_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Protostellar KV component (CXXCBC-891).
  add_cng_test(protostellar/component LINK_CLIENT LIBS couchbase_cxx_protostellar snappy)
  add_cng_test(protostellar/component_kv_operations LINK_CLIENT LIBS couchbase_cxx_protostellar
               snappy)
  add_cng_test(protostellar/live_kv_operations LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # KV transport-level retry, including the overall time budget (CXXCBC-896).
  add_cng_test(protostellar/kv_retry LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live KV verification vs a real gateway (CXXCBC-891).
  add_cng_test(protostellar/live_kv LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live connect verification (CXXCBC-891).
  add_cng_test(protostellar/live_connect LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Observability transport-agnostic recording (CXXCBC-902).
  add_cng_test(protostellar/live_observability LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Streaming responses primitive (CXXCBC-895).
  add_cng_test(protostellar/streaming LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # N1QL query converter (CXXCBC-897).
  add_cng_test(protostellar/query_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live N1QL query verification (CXXCBC-897).
  add_cng_test(protostellar/live_query LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Analytics query converter (CXXCBC-898).
  add_cng_test(protostellar/analytics_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Analytics query verification (CXXCBC-898).
  add_cng_test(protostellar/live_analytics LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # FTS search converter (CXXCBC-899).
  add_cng_test(protostellar/search_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Views converter (CXXCBC-899).
  add_cng_test(protostellar/view_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live FTS search verification (CXXCBC-899).
  add_cng_test(protostellar/live_search LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Views verification (CXXCBC-899).
  add_cng_test(protostellar/live_view LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Bucket management converter (CXXCBC-900).
  add_cng_test(protostellar/bucket_admin_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Bucket management verification (CXXCBC-900).
  add_cng_test(protostellar/live_bucket_admin LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Scope & Collection management converter (CXXCBC-900).
  add_cng_test(protostellar/collection_admin_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Scope & Collection management verification (CXXCBC-900).
  add_cng_test(protostellar/live_collection_admin LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Query index management converter (CXXCBC-901).
  add_cng_test(protostellar/query_index_admin_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)
  add_cng_test(protostellar/query_index_admin_component LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Query index management verification (CXXCBC-901).
  add_cng_test(protostellar/live_query_index_admin LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Search index management converter (CXXCBC-901).
  add_cng_test(protostellar/search_index_admin_converter LINK_CLIENT LIBS couchbase_cxx_protostellar)
  add_cng_test(protostellar/search_index_admin_component LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live Search index management verification (CXXCBC-901).
  add_cng_test(protostellar/live_search_index_admin LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # KV durable-timeout resolution (CXXCBC-904).
  add_cng_test(protostellar/component_timeouts LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Ping / diagnostics feature_not_available over couchbase2 (CXXCBC-907).
  add_cng_test(protostellar/ping_diagnostics LINK_CLIENT LIBS couchbase_cxx_protostellar)
endif()
