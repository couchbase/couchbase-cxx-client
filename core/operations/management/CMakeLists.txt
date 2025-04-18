add_library(
  couchbase_operations_management OBJECT
        analytics_dataset_create.cxx
        analytics_dataset_drop.cxx
        analytics_dataset_get_all.cxx
        analytics_dataverse_create.cxx
        analytics_dataverse_drop.cxx
        analytics_get_pending_mutations.cxx
        analytics_index_create.cxx
        analytics_index_drop.cxx
        analytics_index_get_all.cxx
        analytics_link_connect.cxx
        analytics_link_create.cxx
        analytics_link_disconnect.cxx
        analytics_link_drop.cxx
        analytics_link_get_all.cxx
        analytics_link_replace.cxx
        bucket_create.cxx
        bucket_describe.cxx
        bucket_drop.cxx
        bucket_flush.cxx
        bucket_get.cxx
        bucket_get_all.cxx
        bucket_update.cxx
        change_password.cxx
        cluster_describe.cxx
        cluster_developer_preview_enable.cxx
        collection_create.cxx
        collection_drop.cxx
        collections_manifest_get.cxx
        collection_update.cxx
        error_utils.cxx
        eventing_deploy_function.cxx
        eventing_drop_function.cxx
        eventing_get_all_functions.cxx
        eventing_get_function.cxx
        eventing_get_status.cxx
        eventing_pause_function.cxx
        eventing_resume_function.cxx
        eventing_undeploy_function.cxx
        eventing_upsert_function.cxx
        freeform.cxx
        group_drop.cxx
        group_get.cxx
        group_get_all.cxx
        group_upsert.cxx
        query_index_build.cxx
        query_index_create.cxx
        query_index_drop.cxx
        query_index_get_all.cxx
        role_get_all.cxx
        scope_create.cxx
        scope_drop.cxx
        scope_get_all.cxx
        search_get_stats.cxx
        search_index_analyze_document.cxx
        search_index_control_ingest.cxx
        search_index_control_plan_freeze.cxx
        search_index_control_query.cxx
        search_index_drop.cxx
        search_index_get.cxx
        search_index_get_all.cxx
        search_index_get_documents_count.cxx
        search_index_get_stats.cxx
        search_index_upsert.cxx
        user_drop.cxx
        user_get.cxx
        user_get_all.cxx
        user_upsert.cxx
        view_index_drop.cxx
        view_index_get.cxx
        view_index_get_all.cxx
        view_index_upsert.cxx)
set_target_properties(couchbase_operations_management PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_operations_management
  PRIVATE project_options
          project_warnings
          spdlog::spdlog)
target_include_directories(couchbase_operations_management PRIVATE ../../..)
