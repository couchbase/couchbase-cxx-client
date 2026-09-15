# CNG (Protostellar) test tree.
#
# Uses the hand-rolled framework in test/framework/ (CXXCBC-885), deliberately NOT Catch2.
# See test/cng/README.md to build and run these tests. Enabled via COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS.

# Which client library the LINK_CLIENT tests link, narrowing couchbase_add_test's default.
#
# The couchbase2 tests also link couchbase_cxx_protostellar directly, to construct the generated
# protobuf messages the converters take. Under COUCHBASE_CXX_CLIENT_PACKAGE_BUILD
# cmake/Protostellar.cmake builds those stubs with hidden visibility, so the shared client does not
# export them: an executable linking the shared client AND the stubs archive would then get a second
# copy of every generated message, with its own descriptor pool and its own type identity for the
# same message, and a message built on one side would not be the same type as the one the other side
# expects. Linking the static client keeps exactly one copy in the executable.
#
# Off that switch the stubs keep default visibility, the copies in the executable resolve to the
# ones inside the .so, and the tests link the shared client -- which is what a development or CI
# build wants, because the static client puts a full copy of the client and everything it vendors in
# each of these executables.
#
# The intermediate target rather than the bundled archive: cmake/Bundler.cmake gives the bundle only
# a location and its include directories, so linking it drops every dependency the archive cannot
# contain -- system zlib among them. The intermediate carries the full link interface and the same
# objects, and the archives it pulls in are the same ones the bundle would have contained, so there
# is still exactly one copy of each. The bundle exists to be installed for external consumers.
#
# Where it is unavailable, fall back to the default and skip the couchbase2 tests below rather than
# build a target that would fail this way.
set(cng_test_couchbase2_supported TRUE)
if(COUCHBASE_CXX_CLIENT_PACKAGE_BUILD)
  if(TARGET couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
    set(couchbase_cxx_client_test_library
        couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
  else()
    set(cng_test_couchbase2_supported FALSE)
    if(COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2)
      message(
        WARNING
          "The couchbase2 CNG tests need COUCHBASE_CXX_CLIENT_BUILD_STATIC=ON under "
          "COUCHBASE_CXX_CLIENT_PACKAGE_BUILD and are skipped. They link the generated protobuf "
          "stubs directly, which only agrees with the client library when both come from the same "
          "static archive.")
    endif()
  endif()
endif()

# add_cng_test(<relpath> [LINK_CLIENT] [LIBS <lib>...]) — relpath is under test/cng/ without the
# extension (e.g. protostellar/dispatcher), which couchbase_add_test (cmake/TestFramework.cmake)
# takes relative to test/ and turns into the target name cng_protostellar_dispatcher.
function(add_cng_test relpath)
  couchbase_add_test(cng/${relpath} LABEL cng ${ARGN})
endfunction()

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
if(COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2
   AND TARGET couchbase_cxx_protostellar
   AND cng_test_couchbase2_supported)
  add_cng_test(protostellar/codegen_smoke)
  target_link_libraries(cng_protostellar_codegen_smoke PRIVATE couchbase_cxx_protostellar)

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

  # Streaming backpressure & cancel (CXXCBC-910).
  add_cng_test(protostellar/query_streaming LINK_CLIENT LIBS couchbase_cxx_protostellar)
  # Transport retry (CXXCBC-896 / CXXCBC-906).
  add_cng_test(protostellar/retry LINK_CLIENT LIBS couchbase_cxx_protostellar)

  # Live wait_until_ready (CXXCBC-908).
  add_cng_test(protostellar/live_wait_until_ready LINK_CLIENT LIBS couchbase_cxx_protostellar)
endif()
