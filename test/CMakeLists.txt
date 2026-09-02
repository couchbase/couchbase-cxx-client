add_subdirectory(unit/streaming)

add_subdirectory(unit/json)

add_subdirectory(unit/observability)

add_subdirectory(unit/http)

add_subdirectory(unit/core)

add_subdirectory(unit/management)

# Registered here rather than in cmake/TestFramework.cmake beside the framework's own self-tests:
# only a call from this file reaches build_unit_tests. It is pure computation over the tools helper
# and needs no server, so unit is the label, and unit is a label with an aggregate.
couchbase_add_test(tools/document_body_generator LABEL unit LINK_CLIENT)

integration_test(connect)
integration_test(crud)
integration_test(query)
integration_test(diagnostics)
integration_test(binary_operations)
integration_test(durability)
integration_test(arithmetic)
integration_test(collections)
integration_test(subdoc)
integration_test(analytics)
integration_test(read_replica)
integration_test(tracer)
integration_test(wrapper_tracer)
integration_test(meter)
integration_test(transcoders)
integration_test(range_scan)
integration_test(search)
integration_test(management)
integration_test(management_eventing)
integration_test(management_search_index)
integration_test(http_session_manager)
integration_test(node_id)
integration_test(bucket_bootstrap)
integration_test(handler_drop_lifetime)
integration_test(wait_until_ready)
integration_test(fork)

unit_test(connection_string)
unit_test(capella)
unit_test(contains_string)
unit_test(protocol_status)
unit_test(configuration_belongs_to_session)
unit_test(transaction_content_as)
unit_test(transaction_get_result)
unit_test(staged_mutation)
unit_test(atr_entry)
unit_test(get_multi_transaction_id)
unit_test(get_multi_fetch)
unit_test(options)
unit_test(opaque_ring_table)
unit_test(mcbp_operation_queue)
unit_test(mcbp_output_queue)
unit_test(mcbp_buffer_pool)
unit_test(mcbp_session)
unit_test(mcbp_codec)
unit_test(mcbp_parser)
unit_test(client_request)
unit_test(client_response)
unit_test(mcbp_queue_request)
unit_test(mcbp_command_id)

integration_benchmark(get)
integration_benchmark(replace)

transaction_test(context)
transaction_test(simple)
transaction_test(simple_async)
transaction_test(public_blocking_api)
transaction_test(public_async_api)

unit_test(transaction_logging)
unit_test(transaction_utils)

integration_test(examples)
integration_test(examples_streaming)
transaction_test(examples)

if(COUCHBASE_CXX_CLIENT_COLUMNAR)
  integration_test(columnar_query)
  integration_test(columnar_management)
endif()

# The framework's cluster-probe selftest. Registered here rather than in cmake/TestFramework.cmake
# because couchbase_add_test() appends the target to COUCHBASE_INTEGRATION_TESTS, and
# cmake/Testing.cmake blanks that property after TestFramework.cmake has been read -- an append made
# from there would be discarded, and build_integration_tests would omit the target. It links the
# client, so it has no place in the CNG-only configuration that reads TestFramework.cmake alone.
couchbase_add_test(framework/cluster_probes_selftest LABEL integration LINK_CLIENT)
