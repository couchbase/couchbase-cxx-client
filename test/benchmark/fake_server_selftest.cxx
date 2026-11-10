/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// The fake MCBP server against the real SDK, through the public couchbase::cluster API.

#include "benchmark/fake_server.hxx"
#include "benchmark/kv_cases.hxx"

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <asio.hpp>
#include <tao/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <future>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>

namespace couchbase::test
{
namespace
{
auto
connect(const bench::fake_server& server) -> couchbase::cluster
{
  const couchbase::cluster_options options(server.username(), server.password());
  auto [err, cluster] = couchbase::cluster::connect(server.connection_string(), options).get();
  assert_success(err, "connect to the fake server");
  return std::move(cluster);
}

void
sdk_bootstraps_against_fake(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  // The bucket session opens on the first operation, so one is needed to see it bootstrap.
  auto [err, result] = collection.upsert("bootstrap", tao::json::value{ { "a", 1 } }).get();
  assert_success(err, "upsert over the bucket session");
  cluster.close().get();
}

void
upsert_then_get_round_trips(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  const tao::json::value document{ { "a", 1 } };
  auto [upsert_err, upsert_result] = collection.upsert("k", document).get();
  assert_success(upsert_err, "upsert k");
  auto [get_err, get_result] = collection.get("k", {}).get();
  assert_success(get_err, "get k");
  assert_true(get_result.content_as<tao::json::value>() == document, "the content round-trips");
  assert_eq(get_result.cas(), upsert_result.cas(), "get returns the cas of the upsert");
  cluster.close().get();
}

// A 4 KiB repetitive value is compressed by the SDK once HELLO accepts Snappy. The server must
// store it compressed and return it unchanged.
void
compressible_value_round_trips(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  const tao::json::value document{ { "payload", std::string(4096, 'x') } };
  auto [upsert_err, upsert_result] = collection.upsert("big", document).get();
  assert_success(upsert_err, "upsert big");
  auto [get_err, get_result] = collection.get("big", {}).get();
  assert_success(get_err, "get big");
  assert_true(get_result.content_as<tao::json::value>() == document, "the content round-trips");
  assert_eq(server.snappy_documents(), std::size_t{ 1 }, "the value is stored compressed");
  cluster.close().get();
}

void
get_missing_is_not_found(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  auto [err, result] = collection.get("missing", {}).get();
  assert_error(err, errc::key_value::document_not_found, "get of an absent key");
  cluster.close().get();
}

void
remove_deletes_and_reports_missing(context& /* ctx */)
{
  const bench::fake_server server;
  auto cluster = connect(server);
  auto collection = cluster.bucket("default").default_collection();
  auto [upsert_err, upsert_result] = collection.upsert("k", tao::json::value{ { "a", 1 } }).get();
  assert_success(upsert_err, "upsert k");
  auto [remove_err, remove_result] = collection.remove("k").get();
  assert_success(remove_err, "remove k");
  auto [get_err, get_result] = collection.get("k", {}).get();
  assert_error(get_err, errc::key_value::document_not_found, "get of a removed key");
  auto [again_err, again_result] = collection.remove("k").get();
  assert_error(again_err, errc::key_value::document_not_found, "remove of an absent key");
  cluster.close().get();
}

void
pipelined_stops_after_its_failure_budget(context& /* ctx */)
{
  // Operations fail with no supplemental message, as most KV failures do, and the first failure is
  // reported by its error code's message.
  const auto run = [](std::size_t max_failures, auto fails) {
    std::size_t started = 0;
    const auto [failure, all_started] = bench::kv::pipelined(
      300,
      [&started, &fails](std::size_t i) {
        ++started;
        std::promise<std::pair<couchbase::error, int>> p;
        p.set_value(
          { fails(i) ? couchbase::error{ errc::common::temporary_failure } : couchbase::error{},
            0 });
        return p.get_future();
      },
      {},
      max_failures);
    return std::make_tuple(started, failure, all_started);
  };
  const auto expected = std::error_code{ errc::common::temporary_failure }.message();
  const auto one = [](std::size_t i) {
    return i == 3;
  };
  const auto every = [](std::size_t) {
    return true;
  };

  // Seeding: one failure stops it.
  const auto [seeded, seed_failure, seed_all] = run(1, one);
  assert_true(seeded < 300 && !seed_all, "seeding starts nothing once a failure is seen");
  assert_eq(seed_failure, expected, "seeding reports the failure");

  // Removal: a scattered failure leaves every operation attempted.
  const auto [removed, remove_failure, remove_all] = run(64, one);
  assert_true(removed == 300 && remove_all, "removal attempts every operation past one failure");
  assert_eq(remove_failure, expected, "removal reports the failure");

  // Removal against a bucket that fails everything stops after its budget, within two windows.
  const auto [stopped, stop_failure, stop_all] = run(64, every);
  assert_true(stopped <= 128 && !stop_all, "removal stops after 64 failures");
  assert_eq(stop_failure, expected, "a stopped removal reports the first failure");
}

void
server_survives_client_close(context& /* ctx */)
{
  const bench::fake_server server;
  for (int round = 0; round < 2; ++round) {
    auto cluster = connect(server);
    auto [err, result] = cluster.bucket("default")
                           .default_collection()
                           .upsert("k", tao::json::value{ { "round", round } })
                           .get();
    assert_success(err, "upsert after reconnecting");
    cluster.close().get();
  }
}
// Sends one 24-byte header and expects the server to close the connection without waiting for the
// body it declares. A server that buffers the body first never answers, and the case times out.
void
expect_closed_on_header(const bench::fake_server& server,
                        std::uint8_t magic,
                        std::uint32_t body_size,
                        const char* what)
{
  const auto connection_string = server.connection_string();
  const auto port = static_cast<std::uint16_t>(
    std::stoi(connection_string.substr(connection_string.rfind(':') + 1)));
  asio::io_context io;
  asio::ip::tcp::socket socket(io);
  socket.connect({ asio::ip::make_address("127.0.0.1"), port });
  std::array<unsigned char, 24> header{};
  header[0] = magic;
  header[8] = static_cast<unsigned char>(body_size >> 24);
  header[9] = static_cast<unsigned char>(body_size >> 16);
  header[10] = static_cast<unsigned char>(body_size >> 8);
  header[11] = static_cast<unsigned char>(body_size);
  asio::write(socket, asio::buffer(header));
  std::array<char, 1> reply{};
  std::error_code ec;
  socket.read_some(asio::buffer(reply), ec);
  assert_true(static_cast<bool>(ec), what);
}

void
bad_magic_closes_before_body(context& /* ctx */)
{
  const bench::fake_server server;
  expect_closed_on_header(server, 0x81, 1024, "a response magic closes the connection");
}

void
oversized_body_closes_before_body(context& /* ctx */)
{
  const bench::fake_server server;
  expect_closed_on_header(server, 0x80, 0xffffffff, "a body over the limit closes the connection");
  auto cluster = connect(server);
  auto [err, result] = cluster.bucket("default")
                         .default_collection()
                         .upsert("k", tao::json::value{ { "a", 1 } })
                         .get();
  assert_success(err, "the server still serves a new connection");
  cluster.close().get();
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(sdk_bootstraps_against_fake), {}, timeout::fast },
      { CASE(upsert_then_get_round_trips), {}, timeout::fast },
      { CASE(compressible_value_round_trips), {}, timeout::fast },
      { CASE(get_missing_is_not_found), {}, timeout::fast },
      { CASE(remove_deletes_and_reports_missing), {}, timeout::fast },
      { CASE(pipelined_stops_after_its_failure_budget), {}, timeout::fast },
      { CASE(server_survives_client_close), {}, timeout::fast },
      { CASE(bad_magic_closes_before_body), {}, timeout::fast },
      { CASE(oversized_body_closes_before_body), {}, timeout::fast },
    },
  };
}
} // namespace couchbase::test
