/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

// log_prefix() and supported_features() take session_info_mutex_ and return a copy, so no caller is
// handed a reference that outlives the lock. supports_feature() and context() read the negotiated
// features from an atomic mask, without the lock.
//
// bootstrap_hello_updates_supported_features_bitmask bootstraps against a listener on loopback.
// Every other case constructs an mcbp_session and never bootstraps it, so its socket is never
// opened.

#include "framework/test_registry.hxx"

#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_context.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/origin.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

#include <type_traits>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::mcbp_session;
using couchbase::core::protocol::hello_feature;

// No case reads the bootstrap state reports, so these do nothing.
class stub_bootstrap_listener : public couchbase::core::impl::bootstrap_state_listener
{
public:
  void report_bootstrap_error(const std::string& /*endpoint*/, std::error_code /*ec*/) override
  {
  }

  void report_bootstrap_success(const std::vector<std::string>& /*endpoints*/) override
  {
  }

  void register_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }

  void unregister_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }
};

// The io_context is never run until stop_and_drain(), so the session performs no network activity.
auto
make_session(asio::io_context& io, std::vector<hello_feature> known_features = {}) -> mcbp_session
{
  auto origin = couchbase::core::origin(
    couchbase::core::cluster_credentials{ "user", "pass" },
    couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1"));
  return { "test-client-id",
           "test-node-uuid",
           io,
           std::move(origin),
           std::make_shared<stub_bootstrap_listener>(),
           std::nullopt,
           std::move(known_features) };
}

// mcbp_session_impl::stop() dispatches cleanup to the io_context strand. Call this before letting
// the session go out of scope, then run the context to drain those tasks so the shared_from_this()
// references they hold are released.
void
stop_and_drain(mcbp_session& session, asio::io_context& io)
{
  session.stop(couchbase::retry_reason::do_not_retry);
  io.run();
}

// Calls stop_and_drain() on every exit from the case, a failing assertion included. The destructor
// of a session that was never stopped calls shared_from_this() and throws std::bad_weak_ptr, which
// terminates the process instead of reporting the failure.
class drain_on_exit
{
public:
  drain_on_exit(mcbp_session& session, asio::io_context& io)
    : session_{ session }
    , io_{ io }
  {
  }
  ~drain_on_exit()
  {
    stop_and_drain(session_, io_);
  }

private:
  mcbp_session& session_;
  asio::io_context& io_;
};

void
log_prefix_is_returned_by_value([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io);
  const drain_on_exit drain{ session, io };

  // The signature is the subject here, not the value: the accessor reads log_prefix_ under
  // session_info_mutex_, so returning a reference would hand the caller state the lock no longer
  // protects. Initialising a std::string does not catch that -- it compiles and copies from a
  // const& return just as well -- so the type is asserted directly.
  static_assert(std::is_same_v<decltype(session.log_prefix()), std::string>,
                "log_prefix() must return by value: a reference would outlive its lock");
  std::string prefix = session.log_prefix();
  assert_false(prefix.empty(), "a session identifies itself in the log");
}

void
a_session_with_no_known_features_supports_none([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, {});
  const drain_on_exit drain{ session, io };

  assert_false(session.supports_feature(hello_feature::collections),
               "a feature that was not negotiated");
  assert_false(session.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_true(session.supported_features().empty(), "nothing was negotiated");
}

void
a_session_reports_exactly_the_features_it_was_given([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections, hello_feature::json });
  const drain_on_exit drain{ session, io };

  assert_true(session.supports_feature(hello_feature::collections), "a negotiated feature");
  assert_true(session.supports_feature(hello_feature::json), "a negotiated feature");
  assert_false(session.supports_feature(hello_feature::duplex),
               "a feature that was not negotiated");

  const auto features = session.supported_features();
  assert_eq(features.size(), std::size_t{ 2 }, "the negotiated features, and no others");
  assert_true(std::find(features.begin(), features.end(), hello_feature::collections) !=
                features.end(),
              "collections is listed");
  assert_true(std::find(features.begin(), features.end(), hello_feature::json) != features.end(),
              "json is listed");
}

void
the_mcbp_context_reflects_the_known_features([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });
  const drain_on_exit drain{ session, io };

  auto mcbp_ctx = session.context();
  assert_true(mcbp_ctx.supports_feature(hello_feature::collections), "a negotiated feature");
  assert_false(mcbp_ctx.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_false(mcbp_ctx.config.has_value(), "no configuration arrives before bootstrap");
}

void
session_info_accessors_are_safe_under_concurrent_reads([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session =
    make_session(io, { hello_feature::collections, hello_feature::json, hello_feature::duplex });

  constexpr int num_threads = 8;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  std::promise<void> start_gate;
  auto start_future = start_gate.get_future().share();

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&session, start_future]() -> void {
      start_future.wait();
      constexpr int iterations = 1000;
      for (int i = 0; i < iterations; ++i) {
        // log_prefix() and supported_features() take session_info_mutex_; supports_feature() and
        // context() read the atomic mask. A lock taken twice on one path deadlocks here, and the
        // case's timeout is what reports it.
        static_cast<void>(session.log_prefix());
        static_cast<void>(session.supported_features());
        static_cast<void>(session.supports_feature(hello_feature::collections));
        static_cast<void>(session.context());
      }
    });
  }

  start_gate.set_value(); // release all threads at once
  for (auto& th : threads) {
    th.join();
  }
  stop_and_drain(session, io);
}

void
a_freshly_constructed_session_is_neither_stopped_nor_bootstrapped([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io);
  const drain_on_exit drain{ session, io };
  assert_false(session.is_stopped(), "a session that has not been stopped");
  assert_false(session.is_bootstrapped(), "a session that has not bootstrapped");
}

void
feature_bitmask_supports_high_ordinal_features_including_subdoc_binary_xattr(
  [[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io,
                              { hello_feature::tls,
                                hello_feature::snappy,
                                hello_feature::collections,
                                hello_feature::preserve_ttl,
                                hello_feature::subdoc_replica_read,
                                hello_feature::subdoc_binary_xattr });
  const drain_on_exit drain{ session, io };

  assert_true(session.supports_feature(hello_feature::tls), "bit 0x02");
  assert_true(session.supports_feature(hello_feature::snappy), "bit 0x0a");
  assert_true(session.supports_feature(hello_feature::collections), "bit 0x12");
  assert_true(session.supports_feature(hello_feature::preserve_ttl), "bit 0x14");
  assert_true(session.supports_feature(hello_feature::subdoc_replica_read), "bit 0x1c");
  assert_true(session.supports_feature(hello_feature::subdoc_binary_xattr),
              "bit 0x21 (> 31 requires 64-bit bitmask)");
  assert_false(session.supports_feature(hello_feature::alt_request_support), "not negotiated");
  assert_false(session.supports_feature(hello_feature::sync_replication), "not negotiated");

  const auto mcbp_ctx = session.context();
  assert_true(mcbp_ctx.supports_feature(hello_feature::subdoc_binary_xattr),
              "mcbp_context bitmask preserves bit 0x21");
  assert_true(mcbp_ctx.supports_feature(hello_feature::collections),
              "mcbp_context bitmask preserves collections");
  assert_false(mcbp_ctx.supports_feature(hello_feature::sync_replication),
               "mcbp_context bitmask rejects non-negotiated feature");
}

void
a_feature_ordinal_beyond_the_mask_width_is_never_supported([[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> config{};
  const couchbase::core::mcbp_context all_bits{ config, ~std::uint64_t{ 0 } };
  const auto beyond = static_cast<hello_feature>(couchbase::core::feature_mask_width);

  assert_eq(couchbase::core::feature_mask_bit(beyond),
            std::uint64_t{ 0 },
            "an ordinal at the mask width has no bit");
  assert_false(all_bits.supports_feature(beyond),
               "an ordinal at the mask width is not supported even when every bit is set");
}

void
the_default_collection_always_resolves_to_uid_zero([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });

  const auto default_uid = session.get_collection_uid("_default._default");
  stop_and_drain(session, io);

  assert_true(default_uid.has_value(), "_default._default is always resolved");
  assert_eq(default_uid.value_or(999U), std::uint32_t{ 0 }, "_default._default has UID 0");
}

void
a_custom_collection_resolves_once_its_uid_is_cached([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });

  const auto before = session.get_collection_uid("inventory.airline");
  session.update_collection_uid("inventory.airline", 42U);
  const auto after = session.get_collection_uid("inventory.airline");
  const auto default_after = session.get_collection_uid("_default._default");
  stop_and_drain(session, io);

  assert_false(before.has_value(), "a custom collection is absent before it is cached");
  assert_true(after.has_value(), "a custom collection resolves after it is cached");
  assert_eq(after.value_or(0U), std::uint32_t{ 42 }, "the cached UID is returned");
  assert_eq(default_after.value_or(999U),
            std::uint32_t{ 0 },
            "caching a custom collection leaves _default._default at UID 0");
}

void
bootstrap_hello_updates_supported_features_bitmask([[maybe_unused]] context& ctx)
{
  asio::io_context server_io{};
  asio::ip::tcp::acceptor acceptor(server_io,
                                   asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
  const auto server_port = acceptor.local_endpoint().port();

  std::thread server_thread([&acceptor]() {
    std::error_code ec;
    asio::ip::tcp::socket sock(acceptor.get_executor());
    acceptor.accept(sock, ec);
    if (ec) {
      return;
    }

    auto send_response = [&sock](std::uint8_t opcode,
                                 std::uint16_t status,
                                 std::uint32_t opaque,
                                 const std::vector<std::uint8_t>& body) -> bool {
      std::array<std::uint8_t, 24> hdr{};
      hdr[0] = 0x81; // client_response magic
      hdr[1] = opcode;
      const auto status_be = couchbase::core::utils::host_to_network(status);
      std::memcpy(hdr.data() + 6, &status_be, sizeof(status_be));
      const auto body_len_be =
        couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(body.size()));
      std::memcpy(hdr.data() + 8, &body_len_be, sizeof(body_len_be));
      std::memcpy(hdr.data() + 12, &opaque, sizeof(opaque));

      std::error_code write_ec;
      asio::write(sock, asio::buffer(hdr), write_ec);
      if (!write_ec && !body.empty()) {
        asio::write(sock, asio::buffer(body), write_ec);
      }
      return !write_ec;
    };

    for (int req_idx = 0; req_idx < 4; ++req_idx) {
      std::array<std::uint8_t, 24> req_hdr{};
      asio::read(sock, asio::buffer(req_hdr), ec);
      if (ec) {
        break;
      }
      const std::uint8_t opcode = req_hdr[1];
      std::uint32_t body_len_be = 0;
      std::memcpy(&body_len_be, req_hdr.data() + 8, sizeof(body_len_be));
      const auto body_len = couchbase::core::utils::network_to_host(body_len_be);
      std::uint32_t opaque = 0;
      std::memcpy(&opaque, req_hdr.data() + 12, sizeof(opaque));

      if (body_len > 0) {
        std::vector<std::uint8_t> req_body(body_len);
        asio::read(sock, asio::buffer(req_body), ec);
        if (ec) {
          break;
        }
      }

      if (opcode == 0x1f) { // hello
        // Negotiate collections (0x0012) and subdoc_binary_xattr (0x0021), dropping snappy (0x000a)
        if (!send_response(opcode, 0x0000, opaque, { 0x00, 0x12, 0x00, 0x21 })) {
          break;
        }
      } else if (opcode == 0x20) { // sasl_list_mechs
        if (!send_response(opcode, 0x0000, opaque, { 'P', 'L', 'A', 'I', 'N' })) {
          break;
        }
      } else if (opcode == 0x21) { // sasl_auth
        if (!send_response(opcode, 0x0000, opaque, {})) {
          break;
        }
      } else if (opcode == 0xb5) { // get_cluster_config
        // 0x0008 == key_value_status_code::no_bucket (completes bucket-less bootstrap)
        send_response(opcode, 0x0008, opaque, {});
        break;
      }
    }
  });

  asio::io_context io{};
  auto origin = couchbase::core::origin(couchbase::core::cluster_credentials{ "user", "pass" },
                                        couchbase::core::utils::parse_connection_string(
                                          "couchbase://127.0.0.1:" + std::to_string(server_port)));
  // Under the case budget, so a mock that stalls the handshake fails the bootstrap assertion
  // rather than the runner's timeout.
  origin.options().bootstrap_timeout = scaled_budget(std::chrono::seconds(2));
  // Construct with initial known_features = { snappy } so we verify that the HELLO response
  // overwrites supported_features_mask_ (clearing snappy and setting collections +
  // subdoc_binary_xattr).
  mcbp_session session{ "test-client-id",
                        "test-node-uuid",
                        io,
                        std::move(origin),
                        std::make_shared<stub_bootstrap_listener>(),
                        std::nullopt,
                        { hello_feature::snappy } };

  const bool initial_snappy = session.supports_feature(hello_feature::snappy);
  const bool initial_collections = session.supports_feature(hello_feature::collections);

  std::error_code bootstrap_ec = asio::error::would_block;
  session.bootstrap([&bootstrap_ec, &io, &session](
                      std::error_code ec, const couchbase::core::topology::configuration&) {
    bootstrap_ec = ec;
    asio::post(io, [&session]() {
      session.stop(couchbase::retry_reason::do_not_retry);
    });
  });

  io.run();
  stop_and_drain(session, io);
  server_thread.join();

  assert_true(initial_snappy, "initial feature before HELLO");
  assert_false(initial_collections, "collections not set before HELLO");
  assert_eq(bootstrap_ec, std::error_code{}, "bootstrap succeeds against mock MCBP server");
  assert_false(session.supports_feature(hello_feature::snappy),
               "HELLO response cleared initial snappy bit from supported_features_mask_");
  assert_true(session.supports_feature(hello_feature::collections),
              "HELLO response set collections bit in supported_features_mask_");
  assert_true(
    session.supports_feature(hello_feature::subdoc_binary_xattr),
    "HELLO response set high-ordinal subdoc_binary_xattr bit in supported_features_mask_");

  const auto mcbp_ctx = session.context();
  assert_false(mcbp_ctx.supports_feature(hello_feature::snappy),
               "context() reflects cleared snappy bit after HELLO");
  assert_true(mcbp_ctx.supports_feature(hello_feature::collections),
              "context() reflects negotiated collections bit after HELLO");
  assert_true(mcbp_ctx.supports_feature(hello_feature::subdoc_binary_xattr),
              "context() reflects negotiated subdoc_binary_xattr bit after HELLO");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(log_prefix_is_returned_by_value) },
      { CASE(a_session_with_no_known_features_supports_none) },
      { CASE(a_session_reports_exactly_the_features_it_was_given) },
      { CASE(the_mcbp_context_reflects_the_known_features) },
      { CASE(session_info_accessors_are_safe_under_concurrent_reads) },
      { CASE(a_freshly_constructed_session_is_neither_stopped_nor_bootstrapped) },
      { CASE(feature_bitmask_supports_high_ordinal_features_including_subdoc_binary_xattr) },
      { CASE(a_feature_ordinal_beyond_the_mask_width_is_never_supported) },
      { CASE(the_default_collection_always_resolves_to_uid_zero) },
      { CASE(a_custom_collection_resolves_once_its_uid_is_cached) },
      { CASE(bootstrap_hello_updates_supported_features_bitmask) },
    },
  };
}

} // namespace couchbase::test
