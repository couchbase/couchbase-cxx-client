# LIBS OpenSSL::SSL wherever a test reaches asio/ssl.hpp: the openssl/*.h files it includes are
# nowhere the compiler looks on a host that keeps OpenSSL outside the default prefix.
couchbase_add_test(unit/http/session_idle LABEL unit LINK_CLIENT LIBS OpenSSL::SSL)
couchbase_add_test(unit/http/session_manager_eviction LABEL unit LINK_CLIENT LIBS OpenSSL::SSL)
couchbase_add_test(unit/http/streaming_response LABEL unit LINK_CLIENT)
