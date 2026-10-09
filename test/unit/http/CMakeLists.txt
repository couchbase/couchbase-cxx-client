couchbase_add_test(unit/http/session_idle LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/session_manager_close LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/session_manager_check_in LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/session_manager_eviction LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/streaming_response LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/tls_hostname_verification LABEL unit LINK_CLIENT)
couchbase_add_test(unit/http/session_pool_lifetime LABEL unit LINK_CLIENT)
# This test configures TLS contexts through the TLS library directly, so it must use the same copy
# of that library as the client. A shared client with a statically linked TLS library has its own
# copy, and a context passed between two copies is invalid. The test therefore links the static
# client, and without one it is built only when the TLS library is linked dynamically.
if(TARGET couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
  set(couchbase_cxx_client_test_library couchbase_cxx_client::couchbase_cxx_client_static_intermediate)
  couchbase_add_test(unit/http/client_certificate_rotation LABEL unit LINK_CLIENT)
elseif(NOT COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL AND NOT COUCHBASE_CXX_CLIENT_STATIC_OPENSSL)
  couchbase_add_test(unit/http/client_certificate_rotation LABEL unit LINK_CLIENT)
endif()
