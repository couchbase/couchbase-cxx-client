#include "test_helper_native.hxx"

IntegrationTest::IntegrationTest()
  : cluster(couchbase::cluster(io))
  , ctx(test_context::load_from_environment())
{
    native_init_logger();
    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;
    io_thread = std::thread([this]() { io.run(); });
    open_cluster(cluster, couchbase::origin(auth, connstr));
}

IntegrationTest::~IntegrationTest()
{
    close_cluster(cluster);
    io_thread.join();
}