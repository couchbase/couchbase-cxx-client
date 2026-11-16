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

// The session accessors that read state guarded by session_info_mutex_: log_prefix() reads
// log_prefix_, supports_feature() and supported_features() read supported_features_, and context()
// reads config_ together with supported_features_. What the cases hold is that each returns by
// value, so no caller is handed a reference that outlives the lock.
//
// Every case here constructs an mcbp_session and never bootstraps it, so the socket is never
// opened and no case touches the network.

#include "framework/test_registry.hxx"

#include "core/crypto/cbcrypto.h"
#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/origin.hxx"
#include "core/platform/base64.h"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/cmd_sasl_auth.hxx"
#include "core/protocol/cmd_sasl_list_mechs.hxx"
#include "core/protocol/cmd_sasl_step.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/protocol/magic.hxx"
#include "core/sasl/client.h"
#include "core/sasl/error.h"
#include "core/utils/connection_string.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>

#include <type_traits>

#include <algorithm>
#include <array>
#include <cstdint>
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

// Bootstrap is never invoked here, so none of these is ever called.
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

void
log_prefix_is_returned_by_value([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io);

  // The signature is the subject here, not the value: the accessor reads log_prefix_ under
  // session_info_mutex_, so returning a reference would hand the caller state the lock no longer
  // protects. Initialising a std::string does not catch that -- it compiles and copies from a
  // const& return just as well -- so the type is asserted directly.
  static_assert(std::is_same_v<decltype(session.log_prefix()), std::string>,
                "log_prefix() must return by value: a reference would outlive its lock");
  std::string prefix = session.log_prefix();
  assert_false(prefix.empty(), "a session identifies itself in the log");
  stop_and_drain(session, io);
}

void
a_session_with_no_known_features_supports_none([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, {});

  assert_false(session.supports_feature(hello_feature::collections),
               "a feature that was not negotiated");
  assert_false(session.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_true(session.supported_features().empty(), "nothing was negotiated");
  stop_and_drain(session, io);
}

void
a_session_reports_exactly_the_features_it_was_given([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections, hello_feature::json });

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
  stop_and_drain(session, io);
}

void
the_mcbp_context_reflects_the_known_features([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });

  auto mcbp_ctx = session.context();
  assert_true(mcbp_ctx.supports_feature(hello_feature::collections), "a negotiated feature");
  assert_false(mcbp_ctx.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_false(mcbp_ctx.config.has_value(), "no configuration arrives before bootstrap");
  stop_and_drain(session, io);
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
        // All four accessors take session_info_mutex_ to read the state they report. A lock
        // taken twice on one path deadlocks here; the case's timeout is what reports it.
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
  assert_false(session.is_stopped(), "a session that has not been stopped");
  assert_false(session.is_bootstrapped(), "a session that has not bootstrapped");
  stop_and_drain(session, io);
}

auto
extract_client_nonce(std::string_view client_first) -> std::string
{
  const auto pos = client_first.rfind(",r=");
  return pos == std::string_view::npos ? std::string{}
                                       : std::string{ client_first.substr(pos + 3) };
}

void
scram_step_rejects_a_server_nonce_without_the_client_nonce_prefix([[maybe_unused]] context& ctx)
{
  {
    couchbase::core::sasl::ClientContext sasl(
      [] {
        return std::string{ "user" };
      },
      [] {
        return std::string{ "pass" };
      },
      { "SCRAM-SHA256" });
    auto [start_err, client_first] = sasl.start();
    assert_eq(start_err, couchbase::core::sasl::error::OK, "SCRAM start succeeds");

    auto [step_err, payload] = sasl.step("r=unrelated_server_nonce,s=c2FsdA==,i=4096");
    assert_eq(step_err,
              couchbase::core::sasl::error::BAD_PARAM,
              "server nonce not starting with client nonce is rejected");
  }

  {
    couchbase::core::sasl::ClientContext sasl(
      [] {
        return std::string{ "user" };
      },
      [] {
        return std::string{ "pass" };
      },
      { "SCRAM-SHA256" });
    auto [start_err, client_first] = sasl.start();
    assert_eq(start_err, couchbase::core::sasl::error::OK, "SCRAM start succeeds");
    const auto client_nonce = extract_client_nonce(client_first);

    const auto bare_nonce_msg = "r=" + client_nonce + ",s=c2FsdA==,i=4096";
    auto [step_err, payload] = sasl.step(bare_nonce_msg);
    assert_eq(step_err,
              couchbase::core::sasl::error::BAD_PARAM,
              "server nonce that adds no server portion is rejected");
  }
}

void
scram_step_rejects_non_printable_nonce_or_invalid_salt_without_throwing(
  [[maybe_unused]] context& ctx)
{
  {
    couchbase::core::sasl::ClientContext sasl(
      [] {
        return std::string{ "user" };
      },
      [] {
        return std::string{ "pass" };
      },
      { "SCRAM-SHA256" });
    auto [start_err, client_first] = sasl.start();
    assert_eq(start_err, couchbase::core::sasl::error::OK, "SCRAM start succeeds");
    const auto client_nonce = extract_client_nonce(client_first);

    const std::string bad_nonce_msg =
      "r=" + client_nonce + std::string{ "srv\x01", 4 } + ",s=c2FsdA==,i=4096";
    auto [step_err, payload] = sasl.step(bad_nonce_msg);
    assert_eq(step_err,
              couchbase::core::sasl::error::BAD_PARAM,
              "non-printable character in combined nonce returns BAD_PARAM without throwing");
  }

  {
    couchbase::core::sasl::ClientContext sasl(
      [] {
        return std::string{ "user" };
      },
      [] {
        return std::string{ "pass" };
      },
      { "SCRAM-SHA256" });
    auto [start_err, client_first] = sasl.start();
    assert_eq(start_err, couchbase::core::sasl::error::OK, "SCRAM start succeeds");
    const auto client_nonce = extract_client_nonce(client_first);

    const std::string bad_salt_msg = "r=" + client_nonce + "srv,s=!!!not-base64!!!,i=4096";
    auto [step_err, payload] = sasl.step(bad_salt_msg);
    assert_eq(step_err,
              couchbase::core::sasl::error::BAD_PARAM,
              "invalid base64 salt returns BAD_PARAM without throwing");
  }
}

void
sasl_response_parsers_reject_header_prefixes_larger_than_the_body([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::sasl_auth_response_body;
  using couchbase::core::protocol::sasl_step_response_body;

  couchbase::core::protocol::header_buffer auth_hdr{};
  auth_hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  auth_hdr[1] = static_cast<std::byte>(sasl_auth_response_body::opcode);

  sasl_auth_response_body auth_resp{};
  assert_false(auth_resp.parse(key_value_status_code::success,
                               auth_hdr,
                               0,
                               50,
                               0,
                               { std::byte{ 'o' }, std::byte{ 'k' } },
                               {}),
               "sasl_auth rejects a header prefix larger than the body");

  couchbase::core::protocol::header_buffer step_hdr{};
  step_hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  step_hdr[1] = static_cast<std::byte>(sasl_step_response_body::opcode);

  sasl_step_response_body step_resp{};
  assert_false(step_resp.parse(key_value_status_code::success,
                               step_hdr,
                               0,
                               50,
                               0,
                               { std::byte{ 'o' }, std::byte{ 'k' } },
                               {}),
               "sasl_step rejects a header prefix larger than the body");
}

void
scram_step_rejects_invalid_iteration_counts_and_repeated_or_plain_steps(
  [[maybe_unused]] context& ctx)
{
  const auto check_bad_first_step = [](std::string_view suffix, const char* what) {
    couchbase::core::sasl::ClientContext sasl(
      [] {
        return std::string{ "user" };
      },
      [] {
        return std::string{ "pass" };
      },
      { "SCRAM-SHA256" });
    auto [start_err, client_first] = sasl.start();
    assert_eq(start_err, couchbase::core::sasl::error::OK, "SCRAM start succeeds");
    const auto client_nonce = extract_client_nonce(client_first);
    const std::string challenge = "r=" + client_nonce + std::string{ suffix };
    auto [step_err, payload] = sasl.step(challenge);
    assert_eq(step_err, couchbase::core::sasl::error::BAD_PARAM, what);

    // A subsequent step() call after a failed first step must also return BAD_PARAM without
    // throwing std::logic_error.
    auto [step2_err, payload2] = sasl.step("v=dGVzdA==");
    assert_eq(step2_err,
              couchbase::core::sasl::error::BAD_PARAM,
              "step after failed first step returns BAD_PARAM without throwing");
  };

  check_bad_first_step("srv,s=c2FsdA==,i=-5", "negative iteration count is rejected");
  check_bad_first_step("srv,s=c2FsdA==,i=4294967297",
                       "64-bit overflowing iteration count is rejected");
  check_bad_first_step("srv,s=c2FsdA==,i=4096abc",
                       "iteration count with trailing non-digits is rejected");
  check_bad_first_step("srv,s=c2FsdA==,i=04096", "iteration count with leading zero is rejected");
  check_bad_first_step("srv,s=c2FsdA==,i=2000000",
                       "excessive iteration count above limit is rejected");
  check_bad_first_step("srv with space,s=c2FsdA==,i=4096",
                       "ASCII space in server nonce is rejected per RFC 5802");
  check_bad_first_step("srv,s=c2FsdA==,i=4096,", "trailing comma in attribute list is rejected");

  // PLAIN mechanism step() returns BAD_PARAM instead of throwing std::logic_error.
  couchbase::core::sasl::ClientContext plain_sasl(
    [] {
      return std::string{ "user" };
    },
    [] {
      return std::string{ "pass" };
    },
    { "PLAIN" });
  auto [pstart_err, pstart_payload] = plain_sasl.start();
  assert_eq(pstart_err, couchbase::core::sasl::error::OK, "PLAIN start succeeds");
  auto [pstep_err, pstep_payload] = plain_sasl.step("unexpected");
  assert_eq(pstep_err,
            couchbase::core::sasl::error::BAD_PARAM,
            "PLAIN step returns BAD_PARAM without throwing");
}

void
sasl_list_mechs_validates_prefix_bounds_and_skips_prefix_bytes([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::sasl_list_mechs_response_body;

  couchbase::core::protocol::header_buffer hdr{};
  hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  hdr[1] = static_cast<std::byte>(sasl_list_mechs_response_body::opcode);

  sasl_list_mechs_response_body oob{};
  assert_false(
    oob.parse(
      key_value_status_code::success, hdr, 0, 50, 0, { std::byte{ 'P' }, std::byte{ 'L' } }, {}),
    "sasl_list_mechs rejects prefix larger than body");

  sasl_list_mechs_response_body with_prefix{};
  const std::vector<std::byte> body{
    std::byte{ 0xff }, std::byte{ 0xff }, std::byte{ 'P' }, std::byte{ 'L' },
    std::byte{ 'A' },  std::byte{ 'I' },  std::byte{ 'N' },
  };
  assert_true(with_prefix.parse(key_value_status_code::success, hdr, 2, 0, 0, body, {}),
              "sasl_list_mechs with framing extras parses");
  assert_eq(with_prefix.supported_mechs(),
            std::vector<std::string>{ "PLAIN" },
            "framing extras are excluded from the first mechanism name");

  sasl_list_mechs_response_body empty_body{};
  assert_true(empty_body.parse(key_value_status_code::success, hdr, 0, 0, 0, {}, {}),
              "empty sasl_list_mechs body parses");
  assert_true(empty_body.supported_mechs().empty(),
              "empty sasl_list_mechs body produces no empty mechanism name");
}

void
scram_start_generates_a_fresh_client_nonce_on_restart([[maybe_unused]] context& ctx)
{
  couchbase::core::sasl::ClientContext sasl(
    [] {
      return std::string{ "user" };
    },
    [] {
      return std::string{ "pass" };
    },
    { "SCRAM-SHA256" });

  auto [start1_err, client_first1_view] = sasl.start();
  assert_eq(start1_err, couchbase::core::sasl::error::OK, "first SCRAM start succeeds");
  const std::string client_first1{ client_first1_view };
  const auto nonce1 = extract_client_nonce(client_first1);
  assert_false(nonce1.empty(), "first client nonce is non-empty");
  assert_eq(nonce1.find(' '), std::string::npos, "client nonce contains no spaces");

  const std::string server_first1 = "r=" + nonce1 + "srv,s=c2FsdA==,i=1";
  auto [step1_err, client_final1] = sasl.step(server_first1);
  assert_eq(step1_err, couchbase::core::sasl::error::CONTINUE, "first server-first step succeeds");

  auto [start2_err, client_first2_view] = sasl.start();
  assert_eq(start2_err, couchbase::core::sasl::error::OK, "restarted SCRAM start succeeds");
  const std::string client_first2{ client_first2_view };
  const auto nonce2 = extract_client_nonce(client_first2);
  assert_false(nonce2.empty(), "second client nonce is non-empty");
  assert_true(nonce2 != nonce1, "restarting start() generates a fresh distinct client nonce");

  auto [replay_err, replay_payload] = sasl.step(server_first1);
  assert_eq(replay_err,
            couchbase::core::sasl::error::BAD_PARAM,
            "replaying a server-first message from the prior exchange is rejected");
}

struct raw_mcbp_frame {
  std::uint8_t opcode{};
  std::array<std::uint8_t, 4> opaque{};
  std::string key{};
  std::string value{};
};

auto
read_mcbp_frame(asio::ip::tcp::socket& sock) -> std::optional<raw_mcbp_frame>
{
  std::array<std::uint8_t, 24> hdr{};
  asio::error_code ec;
  asio::read(sock, asio::buffer(hdr), ec);
  if (ec) {
    return std::nullopt;
  }
  const bool alt = hdr[0] == 0x08U;
  const std::size_t framing_len = alt ? hdr[2] : 0U;
  const std::size_t key_len =
    alt ? hdr[3] : ((static_cast<std::size_t>(hdr[2]) << 8U) | static_cast<std::size_t>(hdr[3]));
  const std::size_t ext_len = hdr[4];
  const std::size_t body_len =
    (static_cast<std::size_t>(hdr[8]) << 24U) | (static_cast<std::size_t>(hdr[9]) << 16U) |
    (static_cast<std::size_t>(hdr[10]) << 8U) | static_cast<std::size_t>(hdr[11]);

  std::string body(body_len, '\0');
  asio::read(sock, asio::buffer(body), ec);
  if (ec || framing_len + ext_len + key_len > body_len) {
    return std::nullopt;
  }

  raw_mcbp_frame frame{};
  frame.opcode = hdr[1];
  frame.opaque = { hdr[12], hdr[13], hdr[14], hdr[15] };
  frame.key = body.substr(framing_len + ext_len, key_len);
  frame.value = body.substr(framing_len + ext_len + key_len);
  return frame;
}

void
write_mcbp_response(asio::ip::tcp::socket& sock,
                    const raw_mcbp_frame& req,
                    std::uint16_t status,
                    std::string_view value = {})
{
  std::string out(24, '\0');
  out[0] = static_cast<char>(0x81);
  out[1] = static_cast<char>(req.opcode);
  out[6] = static_cast<char>((status >> 8U) & 0xffU);
  out[7] = static_cast<char>(status & 0xffU);
  const auto len = static_cast<std::uint32_t>(value.size());
  out[8] = static_cast<char>((len >> 24U) & 0xffU);
  out[9] = static_cast<char>((len >> 16U) & 0xffU);
  out[10] = static_cast<char>((len >> 8U) & 0xffU);
  out[11] = static_cast<char>(len & 0xffU);
  out[12] = static_cast<char>(req.opaque[0]);
  out[13] = static_cast<char>(req.opaque[1]);
  out[14] = static_cast<char>(req.opaque[2]);
  out[15] = static_cast<char>(req.opaque[3]);
  out.append(value);
  asio::error_code ec;
  asio::write(sock, asio::buffer(out), ec);
}

enum class scram_bootstrap_scenario {
  premature_auth_success,
  step_success_missing_verifier,
  step_success_wrong_verifier,
  step_success_valid_verifier,
};

auto
run_scram_bootstrap_scenario(scram_bootstrap_scenario scenario) -> std::pair<std::error_code, bool>
{
  asio::io_context server_io{};
  asio::ip::tcp::acceptor acceptor(server_io,
                                   asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
  const auto port = acceptor.local_endpoint().port();

  std::thread server_thread([&acceptor, port, scenario]() {
    asio::ip::tcp::socket sock(acceptor.get_executor());
    asio::error_code ec;
    acceptor.accept(sock, ec);
    if (ec) {
      return;
    }

    std::string client_first_bare;
    std::string server_first;
    while (auto frame = read_mcbp_frame(sock)) {
      using couchbase::core::protocol::client_opcode;
      switch (static_cast<client_opcode>(frame->opcode)) {
        case client_opcode::hello:
          write_mcbp_response(sock, *frame, 0x00);
          break;
        case client_opcode::sasl_list_mechs:
          write_mcbp_response(sock, *frame, 0x00, "SCRAM-SHA512");
          break;
        case client_opcode::sasl_auth: {
          if (scenario == scram_bootstrap_scenario::premature_auth_success) {
            write_mcbp_response(sock, *frame, 0x00);
            break;
          }
          client_first_bare = frame->value.substr(3);
          const auto nonce = extract_client_nonce(frame->value);
          server_first = "r=" + nonce + "srv,s=c2FsdA==,i=1";
          write_mcbp_response(sock, *frame, 0x21, server_first);
          break;
        }
        case client_opcode::sasl_step: {
          if (scenario == scram_bootstrap_scenario::step_success_missing_verifier) {
            write_mcbp_response(sock, *frame, 0x00, "");
            break;
          }
          if (scenario == scram_bootstrap_scenario::step_success_wrong_verifier) {
            write_mcbp_response(sock, *frame, 0x00, "v=dGVzdA==");
            break;
          }
          const auto p_pos = frame->value.rfind(",p=");
          const auto client_final_no_proof = frame->value.substr(0, p_pos);
          const std::string auth_msg =
            client_first_bare + "," + server_first + "," + client_final_no_proof;
          const auto salted_pw = couchbase::core::crypto::PBKDF2_HMAC(
            couchbase::core::crypto::Algorithm::ALG_SHA512, "pass", "salt", 1);
          const auto server_key = couchbase::core::crypto::CBC_HMAC(
            couchbase::core::crypto::Algorithm::ALG_SHA512, salted_pw, "Server Key");
          const auto server_sig = couchbase::core::crypto::CBC_HMAC(
            couchbase::core::crypto::Algorithm::ALG_SHA512, server_key, auth_msg);
          write_mcbp_response(
            sock, *frame, 0x00, "v=" + couchbase::core::base64::encode(server_sig));
          break;
        }
        case client_opcode::get_cluster_config: {
          const std::string cfg =
            R"({"rev":1,"nodesExt":[{"thisNode":true,"hostname":"127.0.0.1","services":{"kv":)" +
            std::to_string(port) + "}}]}";
          write_mcbp_response(sock, *frame, 0x00, cfg);
          return;
        }
        default:
          write_mcbp_response(sock, *frame, 0x81);
          return;
      }
    }
  });

  asio::io_context client_io{};
  auto origin = couchbase::core::origin(couchbase::core::cluster_credentials{ "user", "pass" },
                                        couchbase::core::utils::parse_connection_string(
                                          "couchbase://127.0.0.1:" + std::to_string(port)));
  mcbp_session session("test-client-id",
                       "test-node-uuid",
                       client_io,
                       std::move(origin),
                       std::make_shared<stub_bootstrap_listener>());

  std::error_code bootstrap_ec{};
  session.bootstrap(
    [&bootstrap_ec](std::error_code ec, const couchbase::core::topology::configuration&) {
      bootstrap_ec = ec;
    });
  client_io.run();

  const bool bootstrapped = session.is_bootstrapped();
  session.stop(couchbase::retry_reason::do_not_retry);
  client_io.restart();
  client_io.run();

  server_thread.join();
  return { bootstrap_ec, bootstrapped };
}

void
mcbp_session_bootstrap_validates_scram_step_and_server_verifier([[maybe_unused]] context& ctx)
{
  {
    const auto [ec, bootstrapped] =
      run_scram_bootstrap_scenario(scram_bootstrap_scenario::premature_auth_success);
    assert_eq(ec,
              couchbase::errc::common::authentication_failure,
              "premature SASL_AUTH success for SCRAM is rejected");
    assert_false(bootstrapped, "session is not marked bootstrapped on premature SASL_AUTH success");
  }

  {
    const auto [ec, bootstrapped] =
      run_scram_bootstrap_scenario(scram_bootstrap_scenario::step_success_missing_verifier);
    assert_eq(ec,
              couchbase::errc::common::authentication_failure,
              "SASL_STEP success with missing verifier is rejected");
    assert_false(bootstrapped, "session is not marked bootstrapped when verifier is missing");
  }

  {
    const auto [ec, bootstrapped] =
      run_scram_bootstrap_scenario(scram_bootstrap_scenario::step_success_wrong_verifier);
    assert_eq(ec,
              couchbase::errc::common::authentication_failure,
              "SASL_STEP success with wrong verifier is rejected");
    assert_false(bootstrapped, "session is not marked bootstrapped when verifier is wrong");
  }

  {
    const auto [ec, bootstrapped] =
      run_scram_bootstrap_scenario(scram_bootstrap_scenario::step_success_valid_verifier);
    assert_true(!ec, "complete SCRAM exchange with valid verifier succeeds");
    assert_true(bootstrapped, "session is bootstrapped after valid SCRAM exchange");
  }
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
      { CASE(scram_step_rejects_a_server_nonce_without_the_client_nonce_prefix) },
      { CASE(scram_step_rejects_non_printable_nonce_or_invalid_salt_without_throwing) },
      { CASE(sasl_response_parsers_reject_header_prefixes_larger_than_the_body) },
      { CASE(scram_step_rejects_invalid_iteration_counts_and_repeated_or_plain_steps) },
      { CASE(sasl_list_mechs_validates_prefix_bounds_and_skips_prefix_bytes) },
      { CASE(scram_start_generates_a_fresh_client_nonce_on_restart) },
      { CASE(mcbp_session_bootstrap_validates_scram_step_and_server_verifier) },
    },
  };
}

} // namespace couchbase::test
