add_library(couchbase_crypto OBJECT cbcrypto.cc)
set_target_properties(couchbase_crypto PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(couchbase_crypto PRIVATE project_options project_warnings)
target_include_directories(couchbase_crypto PRIVATE ${PROJECT_SOURCE_DIR})

if(NOT COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL)
  target_link_libraries(couchbase_crypto PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

if(WIN32)
  target_link_libraries(couchbase_crypto PUBLIC bcrypt.lib)
endif()
