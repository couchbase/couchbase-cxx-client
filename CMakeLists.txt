cmake_minimum_required(VERSION 3.17)
# node.js uses cmake 3.17
message(STATUS "CMake version ${CMAKE_VERSION}")

set(couchbase_cxx_client_BUILD_NUMBER 0)
if (DEFINED ENV{BUILD_NUMBER})
    set(couchbase_cxx_client_BUILD_NUMBER $ENV{BUILD_NUMBER})
endif ()

if (NOT DEFINED COUCHBASE_CXX_CLIENT_MASTER_PROJECT)
    if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
        set(COUCHBASE_CXX_CLIENT_MASTER_PROJECT ON)
    else ()
        set(COUCHBASE_CXX_CLIENT_MASTER_PROJECT OFF)
    endif ()
endif ()

project(
        couchbase_cxx_client
        VERSION "1.0.0.${couchbase_cxx_client_BUILD_NUMBER}"
        LANGUAGES CXX C)
message(STATUS "Couchbase C++ client ${couchbase_cxx_client_VERSION}")

include(cmake/PreventInSourceBuilds.cmake)
include(cmake/StandardProjectSettings.cmake)

# 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)

# 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

include(cmake/BuildTracing.cmake)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

include(cmake/Backtrace.cmake)
enable_sanitizers(project_options)

if (COUCHBASE_CXX_CLIENT_MASTER_PROJECT)
    set_project_warnings(project_warnings)
    if (WIN32)
        add_definitions(-D_WIN32_WINNT=0x0601)
    endif ()
endif ()

find_package(Threads REQUIRED)

include(cmake/ThirdPartyDependencies.cmake)

option(COUCHBASE_CXX_CLIENT_STATIC_STDLIB "Statically link C++ standard library" FALSE)
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (COUCHBASE_CXX_CLIENT_STATIC_STDLIB)
        target_compile_options(project_options INTERFACE -static-libgcc -static-libstdc++)
        target_link_libraries(project_options INTERFACE -static-libgcc -static-libstdc++)
    endif ()
endif ()

include(cmake/OpenSSL.cmake)

include(cmake/VersionInfo.cmake)

add_subdirectory(core/meta)
add_subdirectory(core/platform)
add_subdirectory(core/logger)
add_subdirectory(core/crypto)
add_subdirectory(core/sasl)
add_subdirectory(core/tracing)
add_subdirectory(core/metrics)

set(couchbase_cxx_client_FILES
        ${CMAKE_CURRENT_BINARY_DIR}/generated/mozilla_ca_bundle.cxx
        core/agent.cxx
        core/agent_config.cxx
        core/agent_group.cxx
        core/agent_group_config.cxx
        core/analytics_query_options.cxx
        core/bucket.cxx
        core/cluster.cxx
        core/cluster_agent.cxx
        core/cluster_agent_config.cxx
        core/cluster_options.cxx
        core/collections_component.cxx
        core/config_profiles.cxx
        core/core_sdk_shim.cxx
        core/crud_component.cxx
        core/dispatcher.cxx
        core/document_id.cxx
        core/free_form_http_request.cxx
        core/key_value_config.cxx
        core/n1ql_query_options.cxx
        core/range_scan_options.cxx
        core/range_scan_orchestrator.cxx
        core/retry_orchestrator.cxx
        core/scan_result.cxx
        core/search_query_options.cxx
        core/seed_config.cxx
        core/view_query_options.cxx
        core/error_context/key_value.cxx
        core/management/analytics_link_azure_blob_external.cxx
        core/management/analytics_link_couchbase_remote.cxx
        core/management/analytics_link_s3_external.cxx
        core/mcbp/big_endian.cxx
        core/mcbp/buffer_writer.cxx
        core/mcbp/codec.cxx
        core/mcbp/command_code.cxx
        core/mcbp/operation_consumer.cxx
        core/mcbp/operation_queue.cxx
        core/mcbp/packet.cxx
        core/mcbp/queue_request.cxx
        core/mcbp/server_duration.cxx
        core/operations/document_analytics.cxx
        core/operations/document_append.cxx
        core/operations/document_decrement.cxx
        core/operations/document_exists.cxx
        core/operations/document_get.cxx
        core/operations/document_get_and_lock.cxx
        core/operations/document_get_and_touch.cxx
        core/operations/document_get_projected.cxx
        core/operations/document_increment.cxx
        core/operations/document_insert.cxx
        core/operations/document_lookup_in.cxx
        core/operations/document_mutate_in.cxx
        core/operations/document_prepend.cxx
        core/operations/document_query.cxx
        core/operations/document_remove.cxx
        core/operations/document_replace.cxx
        core/operations/document_search.cxx
        core/operations/document_touch.cxx
        core/operations/document_unlock.cxx
        core/operations/document_upsert.cxx
        core/operations/document_view.cxx
        core/operations/http_noop.cxx
        core/operations/management/analytics_dataset_create.cxx
        core/operations/management/analytics_dataset_drop.cxx
        core/operations/management/analytics_dataset_get_all.cxx
        core/operations/management/analytics_dataverse_create.cxx
        core/operations/management/analytics_dataverse_drop.cxx
        core/operations/management/analytics_get_pending_mutations.cxx
        core/operations/management/analytics_index_create.cxx
        core/operations/management/analytics_index_drop.cxx
        core/operations/management/analytics_index_get_all.cxx
        core/operations/management/analytics_link_connect.cxx
        core/operations/management/analytics_link_create.cxx
        core/operations/management/analytics_link_disconnect.cxx
        core/operations/management/analytics_link_drop.cxx
        core/operations/management/analytics_link_get_all.cxx
        core/operations/management/analytics_link_replace.cxx
        core/operations/management/bucket_create.cxx
        core/operations/management/bucket_describe.cxx
        core/operations/management/bucket_drop.cxx
        core/operations/management/bucket_flush.cxx
        core/operations/management/bucket_get.cxx
        core/operations/management/bucket_get_all.cxx
        core/operations/management/bucket_update.cxx
        core/operations/management/change_password.cxx
        core/operations/management/cluster_describe.cxx
        core/operations/management/cluster_developer_preview_enable.cxx
        core/operations/management/collection_create.cxx
        core/operations/management/collection_drop.cxx
        core/operations/management/collections_manifest_get.cxx
        core/operations/management/error_utils.cxx
        core/operations/management/eventing_deploy_function.cxx
        core/operations/management/eventing_drop_function.cxx
        core/operations/management/eventing_get_all_functions.cxx
        core/operations/management/eventing_get_function.cxx
        core/operations/management/eventing_get_status.cxx
        core/operations/management/eventing_pause_function.cxx
        core/operations/management/eventing_resume_function.cxx
        core/operations/management/eventing_undeploy_function.cxx
        core/operations/management/eventing_upsert_function.cxx
        core/operations/management/freeform.cxx
        core/operations/management/group_drop.cxx
        core/operations/management/group_get.cxx
        core/operations/management/group_get_all.cxx
        core/operations/management/group_upsert.cxx
        core/operations/management/query_index_build.cxx
        core/operations/management/query_index_create.cxx
        core/operations/management/query_index_drop.cxx
        core/operations/management/query_index_get_all.cxx
        core/operations/management/query_index_get_all_deferred.cxx
        core/operations/management/role_get_all.cxx
        core/operations/management/scope_create.cxx
        core/operations/management/scope_drop.cxx
        core/operations/management/scope_get_all.cxx
        core/operations/management/search_get_stats.cxx
        core/operations/management/search_index_analyze_document.cxx
        core/operations/management/search_index_control_ingest.cxx
        core/operations/management/search_index_control_plan_freeze.cxx
        core/operations/management/search_index_control_query.cxx
        core/operations/management/search_index_drop.cxx
        core/operations/management/search_index_get.cxx
        core/operations/management/search_index_get_all.cxx
        core/operations/management/search_index_get_documents_count.cxx
        core/operations/management/search_index_get_stats.cxx
        core/operations/management/search_index_upsert.cxx
        core/operations/management/user_drop.cxx
        core/operations/management/user_get.cxx
        core/operations/management/user_get_all.cxx
        core/operations/management/user_upsert.cxx
        core/operations/management/view_index_drop.cxx
        core/operations/management/view_index_get.cxx
        core/operations/management/view_index_get_all.cxx
        core/operations/management/view_index_upsert.cxx
        core/operations/mcbp_noop.cxx
        core/protocol/client_request.cxx
        core/protocol/client_response.cxx
        core/protocol/cmd_append.cxx
        core/protocol/cmd_cluster_map_change_notification.cxx
        core/protocol/cmd_decrement.cxx
        core/protocol/cmd_get.cxx
        core/protocol/cmd_get_and_lock.cxx
        core/protocol/cmd_get_and_touch.cxx
        core/protocol/cmd_get_cluster_config.cxx
        core/protocol/cmd_get_collection_id.cxx
        core/protocol/cmd_get_collections_manifest.cxx
        core/protocol/cmd_get_error_map.cxx
        core/protocol/cmd_get_meta.cxx
        core/protocol/cmd_get_replica.cxx
        core/protocol/cmd_hello.cxx
        core/protocol/cmd_increment.cxx
        core/protocol/cmd_insert.cxx
        core/protocol/cmd_lookup_in.cxx
        core/protocol/cmd_mutate_in.cxx
        core/protocol/cmd_noop.cxx
        core/protocol/cmd_observe_seqno.cxx
        core/protocol/cmd_prepend.cxx
        core/protocol/cmd_remove.cxx
        core/protocol/cmd_replace.cxx
        core/protocol/cmd_sasl_auth.cxx
        core/protocol/cmd_sasl_list_mechs.cxx
        core/protocol/cmd_sasl_step.cxx
        core/protocol/cmd_select_bucket.cxx
        core/protocol/cmd_touch.cxx
        core/protocol/cmd_unlock.cxx
        core/protocol/cmd_upsert.cxx
        core/protocol/frame_info_utils.cxx
        core/protocol/status.cxx
        core/topology/configuration.cxx
        core/utils/binary.cxx
        core/utils/connection_string.cxx
        core/utils/duration_parser.cxx
        core/utils/json.cxx
        core/utils/json_streaming_lexer.cxx
        core/utils/mutation_token.cxx
        core/utils/split_string.cxx
        core/utils/url_codec.cxx
        core/impl/analytics.cxx
        core/impl/search.cxx
        core/impl/analytics_error_category.cxx
        core/impl/append.cxx
        core/impl/best_effort_retry_strategy.cxx
        core/impl/boolean_field_query.cxx
        core/impl/boolean_query.cxx
        core/impl/build_deferred_query_indexes.cxx
        core/impl/cluster.cxx
        core/impl/collection_query_index_manager.cxx
        core/impl/common_error_category.cxx
        core/impl/configuration_profiles_registry.cxx
        core/impl/conjunction_query.cxx
        core/impl/create_query_index.cxx
        core/impl/date_range.cxx
        core/impl/date_range_facet.cxx
        core/impl/date_range_facet_result.cxx
        core/impl/date_range_query.cxx
        core/impl/decrement.cxx
        core/impl/disjunction_query.cxx
        core/impl/dns_srv_tracker.cxx
        core/impl/drop_query_index.cxx
        core/impl/exists.cxx
        core/impl/expiry.cxx
        core/impl/fail_fast_retry_strategy.cxx
        core/impl/field_level_encryption_error_category.cxx
        core/impl/geo_bounding_box_query.cxx
        core/impl/geo_distance_query.cxx
        core/impl/geo_polygon_query.cxx
        core/impl/get.cxx
        core/impl/get_all_query_indexes.cxx
        core/impl/get_all_replicas.cxx
        core/impl/get_and_lock.cxx
        core/impl/get_and_touch.cxx
        core/impl/get_any_replica.cxx
        core/impl/get_replica.cxx
        core/impl/increment.cxx
        core/impl/insert.cxx
        core/impl/internal_date_range_facet_result.cxx
        core/impl/internal_numeric_range_facet_result.cxx
        core/impl/internal_search_error_context.cxx
        core/impl/internal_search_meta_data.cxx
        core/impl/internal_search_result.cxx
        core/impl/internal_search_row.cxx
        core/impl/internal_search_row_locations.cxx
        core/impl/internal_term_facet_result.cxx
        core/impl/key_value_error_category.cxx
        core/impl/key_value_error_context.cxx
        core/impl/lookup_in.cxx
        core/impl/management_error_category.cxx
        core/impl/match_all_query.cxx
        core/impl/match_none_query.cxx
        core/impl/match_phrase_query.cxx
        core/impl/match_query.cxx
        core/impl/mutate_in.cxx
        core/impl/network_error_category.cxx
        core/impl/numeric_range.cxx
        core/impl/numeric_range_facet.cxx
        core/impl/numeric_range_facet_result.cxx
        core/impl/numeric_range_query.cxx
        core/impl/observe_poll.cxx
        core/impl/observe_seqno.cxx
        core/impl/phrase_query.cxx
        core/impl/prefix_query.cxx
        core/impl/prepend.cxx
        core/impl/query.cxx
        core/impl/query_error_category.cxx
        core/impl/query_error_context.cxx
        core/impl/query_string_query.cxx
        core/impl/regexp_query.cxx
        core/impl/remove.cxx
        core/impl/replace.cxx
        core/impl/retry_action.cxx
        core/impl/retry_reason.cxx
        core/impl/search_error_category.cxx
        core/impl/streaming_json_lexter_error_category.cxx
        core/impl/subdoc/array_add_unique.cxx
        core/impl/subdoc/array_append.cxx
        core/impl/subdoc/array_insert.cxx
        core/impl/subdoc/array_prepend.cxx
        core/impl/subdoc/count.cxx
        core/impl/subdoc/counter.cxx
        core/impl/subdoc/exists.cxx
        core/impl/subdoc/get.cxx
        core/impl/subdoc/insert.cxx
        core/impl/subdoc/join_values.cxx
        core/impl/subdoc/lookup_in_macro.cxx
        core/impl/subdoc/lookup_in_specs.cxx
        core/impl/subdoc/mutate_in_macro.cxx
        core/impl/subdoc/mutate_in_specs.cxx
        core/impl/subdoc/remove.cxx
        core/impl/subdoc/replace.cxx
        core/impl/subdoc/upsert.cxx
        core/impl/search.cxx
        core/impl/search_error_context.cxx
        core/impl/search_meta_data.cxx
        core/impl/search_result.cxx
        core/impl/search_row.cxx
        core/impl/search_row_location.cxx
        core/impl/search_row_locations.cxx
        core/impl/search_sort_field.cxx
        core/impl/search_sort_id.cxx
        core/impl/search_sort_score.cxx
        core/impl/term_facet.cxx
        core/impl/term_facet_result.cxx
        core/impl/term_query.cxx
        core/impl/term_range_query.cxx
        core/impl/touch.cxx
        core/impl/transaction_error_category.cxx
        core/impl/transaction_get_result.cxx
        core/impl/transaction_op_error_category.cxx
        core/impl/unlock.cxx
        core/impl/upsert.cxx
        core/impl/view_error_category.cxx
        core/impl/watch_query_indexes.cxx
        core/impl/wildcard_query.cxx
        core/io/dns_client.cxx
        core/io/dns_config.cxx
        core/io/http_parser.cxx
        core/io/mcbp_message.cxx
        core/io/mcbp_parser.cxx
        core/io/mcbp_session.cxx
        core/transactions/atr_cleanup_entry.cxx
        core/transactions/atr_ids.cxx
        core/transactions/attempt_context_impl.cxx
        core/transactions/attempt_context_testing_hooks.cxx
        core/transactions/binary.cxx
        core/transactions/cleanup_testing_hooks.cxx
        core/transactions/exceptions.cxx
        core/transactions/result.cxx
        core/transactions/staged_mutation.cxx
        core/transactions/transaction_attempt.cxx
        core/transactions/transaction_context.cxx
        core/transactions/transaction_get_result.cxx
        core/transactions/transaction_links.cxx
        core/transactions/transaction_options.cxx
        core/transactions/transactions.cxx
        core/transactions/transactions_cleanup.cxx
        core/transactions/transactions_config.cxx
        core/transactions/uid_generator.cxx
        core/transactions/utils.cxx)

if (COUCHBASE_CXX_CLIENT_BUILD_SHARED)
    add_library(couchbase_cxx_client SHARED ${couchbase_cxx_client_FILES})
else ()
    add_library(couchbase_cxx_client STATIC ${couchbase_cxx_client_FILES})
endif ()
set_target_properties(couchbase_cxx_client PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(couchbase_cxx_client PUBLIC ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/private)
target_include_directories(
        couchbase_cxx_client SYSTEM
        PUBLIC ${PROJECT_SOURCE_DIR}/third_party/asio/asio/include
        ${PROJECT_SOURCE_DIR}/third_party/gsl/include
        ${PROJECT_SOURCE_DIR}/third_party/http_parser/include
        ${PROJECT_SOURCE_DIR}/third_party/json/external/PEGTL/include
        ${PROJECT_SOURCE_DIR}/third_party/json/include
        ${PROJECT_SOURCE_DIR}/third_party/cxx_function
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/third_party/expected/include
        ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
target_link_libraries(couchbase_cxx_client PRIVATE project_options project_warnings)

include(cmake/DetectStandardFilesystem.cmake)
couchbase_cxx_check_filesystem(
        couchbase_cxx_client
        "filesystem"
        "std::filesystem"
        "stdc++fs;c++fs"
        STD_FILESYSTEM)
if (STD_FILESYSTEM)
    message(STATUS "Using std::filesystem")
else ()
    message(FATAL_ERROR "Couchbase C++ Client requires C++17, including an implementation of std::filesystem.")
endif ()

target_link_libraries(
        couchbase_cxx_client
        PUBLIC fmt::fmt
        spdlog::spdlog
        snappy
        jsonsl
        hdr_histogram_static
        couchbase_backtrace
        couchbase_logger
        couchbase_platform
        couchbase_meta
        couchbase_crypto
        couchbase_sasl
        couchbase_tracing
        couchbase_metrics)

target_link_libraries(couchbase_cxx_client PRIVATE http_parser)
if (WIN32)
    target_link_libraries(couchbase_cxx_client PRIVATE iphlpapi)
endif ()

if (NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
    if (TARGET PkgConfig::PKG_CONFIG_OPENSSL)
        target_link_libraries(couchbase_cxx_client PUBLIC PkgConfig::PKG_CONFIG_OPENSSL)
    else ()
        target_link_libraries(couchbase_cxx_client PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    endif ()
endif ()

option(COUCHBASE_CXX_CLIENT_BUILD_TESTS "Build test programs" TRUE)
if (COUCHBASE_CXX_CLIENT_BUILD_TESTS)
    include(cmake/Testing.cmake)
endif ()

option(COUCHBASE_CXX_CLIENT_BUILD_DOCS "Build API documentation" TRUE)
if (COUCHBASE_CXX_CLIENT_BUILD_DOCS)
    include(cmake/Documentation.cmake)
endif ()

option(COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES "Build example programs" TRUE)
if (COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif ()

option(COUCHBASE_CXX_CLIENT_BUILD_TOOLS "Build tools" TRUE)
if (COUCHBASE_CXX_CLIENT_BUILD_TOOLS)
    add_subdirectory(third_party/docopt)
    add_subdirectory(tools)
endif ()
