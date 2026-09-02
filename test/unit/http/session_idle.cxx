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

#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

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
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
// A pooled (idle) HTTP service connection must notice when the peer closes it, rather than
// lingering in the pool until the idle timer fires and then being handed to the next request as a
// dead socket (which stalls that request). Reference SDKs get this for free from their HTTP stacks
// (Netty channelInactive, Go net/http / Rust hyper liveness checks); the C++ SDK hand-rolls its
// pool and must arm a read while idle to detect the peer FIN/RST.
void
an_idle_session_detects_a_peer_initiated_close([[maybe_unused]] context& ctx)
{
  asio::io_context io;

  // Minimal loopback "server": accept one connection and hold the socket so it can be closed on
  // demand to simulate the peer dropping an idle connection.
  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();
  asio::ip::tcp::socket server_socket{ io };

  // The peer-initiated close the session must notice, driven by whichever of the accept and the
  // connect completes second rather than by a delay long enough to cover both. Every handler here
  // runs on the single thread that calls io.run() below, so the two flags need no synchronisation.
  bool accepted{ false };
  bool parked_idle{ false };
  auto drop_the_connection_once_idle = [&]() {
    if (!accepted || !parked_idle) {
      return;
    }
    std::error_code ignored;
    server_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    server_socket.close(ignored);
  };

  acceptor.async_accept(server_socket, [&](std::error_code accept_ec) {
    assert_success(accept_ec, "the loopback server accepts the session's connection");
    accepted = true;
    drop_the_connection_once_idle();
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
    // Connection established: park it in the pool. The idle timeout is set well beyond the case's
    // own deadline so that nothing but the peer's close can tear the session down.
    session->set_idle(std::chrono::seconds(30));
    parked_idle = true;
    drop_the_connection_once_idle();
  });

  // Bound the case so a session that never notices the close fails here rather than running to the
  // harness budget.
  asio::steady_timer deadline{ io };
  deadline.expires_after(std::chrono::seconds(3));
  deadline.async_wait([&](std::error_code) {
    io.stop();
  });

  io.run();

  assert_true(stopped.load(), "the idle session tears itself down when the peer closes it");
  session->stop();
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      // A loopback connect and one close, bounded at three seconds inside the case.
      { CASE(an_idle_session_detects_a_peer_initiated_close), {}, timeout::network },
    },
  };
}

} // namespace couchbase::test
