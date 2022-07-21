add_library(
  couchbase_sasl OBJECT
        client.cc
        context.cc
        mechanism.cc
        plain/plain.cc
        scram-sha/scram-sha.cc
        scram-sha/stringutils.cc)
set_target_properties(couchbase_sasl PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(couchbase_sasl PRIVATE ../..)
target_link_libraries(
  couchbase_sasl
  PRIVATE project_options
          project_warnings
          fmt::fmt
          spdlog::spdlog)
