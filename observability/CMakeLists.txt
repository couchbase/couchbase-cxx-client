add_library(couchbase_observability fmt_log_record_exporter.cxx logger.cxx otel_tracer.cxx)
set_target_properties(couchbase_observability PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  couchbase_observability
  PUBLIC project_options
          project_warnings
          opentelemetry_exporter_ostream_logs
          opentelemetry_exporter_otlp_http
          opentelemetry_exporter_otlp_http_log
          opentelemetry_logs
          opentelemetry_trace
          spdlog::spdlog)
target_include_directories(couchbase_observability PRIVATE ${PROJECT_BINARY_DIR}/generated ${PROJECT_SOURCE_DIR})
