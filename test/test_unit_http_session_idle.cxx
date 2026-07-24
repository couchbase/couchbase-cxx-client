/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "test_helper.hxx"

#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_session.hxx"
#include "core/io/query_cache.hxx"
#include "core/origin.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <memory>

// A pooled (idle) HTTP service connection must notice when the peer closes it,
// rather than lingering in the pool until the idle timer fires and then being
// handed to the next request as a dead socket (which stalls that request).
// Reference SDKs get this for free from their HTTP stacks (Netty channelInactive,
// Go net/http / Rust hyper liveness checks); the C++ SDK hand-rolls its pool and
// must arm a read while idle to detect the peer FIN/RST.
TEST_CASE("unit: idle HTTP session detects a peer-initiated close", "[unit]")
{
  asio::io_context io;

  // Minimal loopback "server": accept one connection and hold the socket so we
  // can close it on demand to simulate the peer dropping an idle connection.
  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();
  asio::ip::tcp::socket server_socket{ io };
  asio::steady_timer drop_timer{ io };
  acceptor.async_accept(server_socket, [&](std::error_code accept_ec) {
    REQUIRE_FALSE(accept_ec);
    // The connection is now established on the server side.  Drop it shortly so
    // the client has parked it in the pool (set_idle, below) first; this is the
    // peer-initiated close the session must notice.
    drop_timer.expires_after(std::chrono::milliseconds(200));
    drop_timer.async_wait([&](std::error_code) {
      std::error_code ignored;
      server_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
      server_socket.close(ignored);
    });
  });

  // Build the SDK-side http_session pointing at the loopback server.
  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "pass";
  couchbase::core::cluster_options options{};
  couchbase::core::origin origin{ creds, "127.0.0.1", port, options };
  couchbase::core::topology::configuration config{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{ config, options,     cache, "127.0.0.1",
                                          port,   "127.0.0.1", port };

  auto session =
    std::make_shared<couchbase::core::io::http_session>(couchbase::core::service_type::management,
                                                        "client-id",
                                                        "node-uuid",
                                                        io,
                                                        origin,
                                                        "127.0.0.1",
                                                        std::to_string(port),
                                                        http_ctx);

  std::atomic_bool stopped{ false };
  session->on_stop([&]() {
    stopped = true;
    io.stop();
  });

  session->connect([&]() {
    // Connection established: park it in the pool (idle).  The server will drop
    // it ~200ms later; a healthy SDK observes the close while the connection is
    // idle and tears the session down.
    session->set_idle(std::chrono::seconds(30));
  });

  // Bound the test so a missing-feature run cannot hang.
  asio::steady_timer deadline{ io };
  deadline.expires_after(std::chrono::seconds(3));
  deadline.async_wait([&](std::error_code) {
    io.stop();
  });

  io.run();

  CHECK(stopped.load());
  session->stop();
}
