add_library(
  test_utils OBJECT
  integration_shortcuts.cxx
  integration_test_guard.cxx
  logger.cxx
  server_version.cxx
  test_context.cxx
  uniq_id.cxx)
set_target_properties(test_utils PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(test_utils PRIVATE project_options project_warnings)
target_include_directories(test_utils PRIVATE ${PROJECT_SOURCE_DIR})
