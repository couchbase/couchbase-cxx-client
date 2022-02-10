add_library(
  test_utils OBJECT
  integration_shortcuts.cxx
  integration_test_guard.cxx
  logger.cxx
  server_version.cxx
  test_context.cxx
  uniq_id.cxx
  wait_until.cxx)
set_target_properties(test_utils PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(test_utils PRIVATE project_options project_warnings OpenSSL::SSL fmt::fmt spdlog::spdlog)
target_include_directories(test_utils PRIVATE ${PROJECT_SOURCE_DIR})
if (MSVC)
    target_compile_definitions(test_utils PRIVATE _CRT_SECURE_NO_WARNINGS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_options(test_utils PUBLIC -Wno-deprecated-declarations)
    endif ()
endif ()

