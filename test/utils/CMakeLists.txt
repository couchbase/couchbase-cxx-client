add_library(
  test_utils OBJECT
  binary.cxx
  integration_shortcuts.cxx
  integration_test_guard.cxx
  logger.cxx
  server_version.cxx
  test_context.cxx
  test_data.cxx
  wait_until.cxx)
set_target_properties(test_utils PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_project_warnings(test_utils)
set_project_options(test_utils)
target_include_directories(
  test_utils SYSTEM BEFORE PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_INCLUDE_DIRECTORIES>>
                                   $<BUILD_INTERFACE:$<TARGET_PROPERTY:asio,INTERFACE_INCLUDE_DIRECTORIES>>)
propagate_public_compile_definitions(test_utils spdlog::spdlog asio)
target_link_libraries(test_utils PRIVATE Microsoft.GSL::GSL taocpp::json)
target_include_directories(test_utils PRIVATE ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/generated)

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
  target_include_directories(
    test_utils SYSTEM PRIVATE $<BUILD_INTERFACE:$<TARGET_PROPERTY:ssl,INTERFACE_INCLUDE_DIRECTORIES>>
                              $<BUILD_INTERFACE:$<TARGET_PROPERTY:crypto,INTERFACE_INCLUDE_DIRECTORIES>>)
elseif(TARGET PkgConfig::PKG_CONFIG_OPENSSL)
  target_link_libraries(test_utils PUBLIC PkgConfig::PKG_CONFIG_OPENSSL)
else()
  target_link_libraries(test_utils PUBLIC OpenSSL::SSL)
endif()

if(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND COUCHBASE_CXX_CLIENT_USE_BORINGSSL_PREFIX)
  target_compile_definitions(test_utils PUBLIC BORINGSSL_PREFIX=${COUCHBASE_CXX_CLIENT_BORINGSSL_PREFIX})
endif()

if(MSVC)
  target_compile_definitions(test_utils PRIVATE _CRT_SECURE_NO_WARNINGS)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_utils PUBLIC -Wno-deprecated-declarations)
  endif()
endif()
