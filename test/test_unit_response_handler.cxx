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

// Tests for core/response_handler.hxx
//
// The response_handler interface is the callback boundary between io::mcbp_session and
// higher-level components (bucket_impl, and in CXXCBC-798, crud_component).
//
// Key design point tested here: handle_response() receives an optional<mcbp_session>
// snapshot so that implementors can extract connection metadata (remote/local address
// and port) without retaining a long-lived reference to the live session.  This is
// particularly important for crud_component (CXXCBC-798), which builds
// key_value_error_context from that metadata after the response has been decoded.
//
// Two call sites exist in io::mcbp_session:
//   1. Normal response path — session is present (non-nullopt).
//   2. Session shutdown path — session is present for address tracking but the error
//      code signals cancellation (e.g. socket_closed_while_in_flight).
//
// Because io::mcbp_session requires an Asio io_context and a live network connection
// to construct fully, unit tests use std::nullopt for the session parameter and focus
// on verifying the interface contract and argument forwarding.

#include "core/error_context/key_value_error_map_info.hxx"
#include "core/io/mcbp_message.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/magic.hxx"
#include "core/response_handler.hxx"

// mcbp_session is forward-declared in response_handler.hxx; include the full
// definition so that std::optional<io::mcbp_session> is a complete type in tests.
#include "core/io/mcbp_session.hxx"

#include <catch2/catch_test_macros.hpp>
#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace
{

// ---------------------------------------------------------------------------
// Captures every argument passed to handle_response for later inspection
// ---------------------------------------------------------------------------
struct captured_call {
  std::shared_ptr<couchbase::core::mcbp::queue_request> request{};
  std::optional<couchbase::core::io::mcbp_session> session{};
  std::error_code error{};
  couchbase::retry_reason reason{ couchbase::retry_reason::do_not_retry };
  couchbase::core::io::mcbp_message msg{};
  std::optional<couchbase::core::key_value_error_map_info> error_info{};
  bool called{ false };
};

class recording_handler : public couchbase::core::response_handler
{
public:
  explicit recording_handler(captured_call& out)
    : out_{ out }
  {
  }

  void handle_response(std::shared_ptr<couchbase::core::mcbp::queue_request> request,
                       std::optional<couchbase::core::io::mcbp_session> session,
                       std::error_code error,
                       couchbase::retry_reason reason,
                       couchbase::core::io::mcbp_message msg,
                       std::optional<couchbase::core::key_value_error_map_info> error_info) override
  {
    out_.request = std::move(request);
    out_.session = std::move(session);
    out_.error = error;
    out_.reason = reason;
    out_.msg = std::move(msg);
    out_.error_info = std::move(error_info);
    out_.called = true;
  }

private:
  captured_call& out_;
};

auto
make_request() -> std::shared_ptr<couchbase::core::mcbp::queue_request>
{
  return std::make_shared<couchbase::core::mcbp::queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [](const std::shared_ptr<couchbase::core::mcbp::queue_response>&,
       const std::shared_ptr<couchbase::core::mcbp::queue_request>&,
       std::error_code) -> void {
    });
}

} // namespace

// ---------------------------------------------------------------------------
// Interface contract: default constructibility via derived type
// ---------------------------------------------------------------------------

TEST_CASE("unit: response_handler is default-constructible via derived type", "[unit]")
{
  // response_handler must be default-constructible so that derived classes
  // (e.g. bucket_impl, crud_component's inner handler) can be constructed
  // without supplying an explicit base-class argument.
  captured_call out;
  REQUIRE_NOTHROW(recording_handler{ out });
}

// ---------------------------------------------------------------------------
// Interface contract: copy construction and assignment are allowed
// ---------------------------------------------------------------------------

TEST_CASE("unit: response_handler supports shared_ptr copy semantics", "[unit]")
{
  // Copying a shared_ptr<response_handler> is the standard pattern used by
  // io::mcbp_session, which holds and passes the handler around.
  captured_call out;
  auto h1 = std::make_shared<recording_handler>(out);
  std::shared_ptr<couchbase::core::response_handler> h2 = h1;
  REQUIRE(h2 != nullptr);
}

// ---------------------------------------------------------------------------
// Session shutdown path: error signals cancellation, no live session
// ---------------------------------------------------------------------------

TEST_CASE("unit: handle_response session-shutdown path: nullopt session with cancellation error",
          "[unit]")
{
  // When the session is shutting down it cancels all in-flight operations by
  // calling handle_response with std::nullopt for session AND a non-empty error
  // code (e.g. errc::common::request_canceled).  The handler receives both so
  // it can still propagate the error even without connection metadata.
  captured_call out;
  recording_handler handler{ out };

  auto req = make_request();
  handler.handle_response(req,
                          std::nullopt,
                          couchbase::errc::common::request_canceled,
                          couchbase::retry_reason::socket_closed_while_in_flight,
                          {},
                          std::nullopt);

  REQUIRE(out.called);
  CHECK(out.request == req);
  CHECK_FALSE(out.session.has_value());
  CHECK(out.error == couchbase::errc::common::request_canceled);
  CHECK(out.reason == couchbase::retry_reason::socket_closed_while_in_flight);
}

// ---------------------------------------------------------------------------
// Normal response path: success, no error, no session
// ---------------------------------------------------------------------------

TEST_CASE("unit: handle_response success path with nullopt session", "[unit]")
{
  // Successful response with no session (e.g. synthetic completion in tests or
  // implementations that do not yet supply a session snapshot).
  captured_call out;
  recording_handler handler{ out };

  auto req = make_request();
  handler.handle_response(
    req, std::nullopt, {}, couchbase::retry_reason::do_not_retry, {}, std::nullopt);

  REQUIRE(out.called);
  CHECK(out.request == req);
  CHECK_FALSE(out.session.has_value());
  CHECK_FALSE(out.error);
  CHECK(out.reason == couchbase::retry_reason::do_not_retry);
}

// ---------------------------------------------------------------------------
// Null request: session drain may pass empty request during shutdown
// ---------------------------------------------------------------------------

TEST_CASE("unit: handle_response tolerates null request pointer", "[unit]")
{
  // The shutdown drain path may produce a null request if the operation slot
  // is already empty.  Handlers must cope gracefully.
  captured_call out;
  recording_handler handler{ out };

  handler.handle_response(nullptr,
                          std::nullopt,
                          couchbase::errc::network::cluster_closed,
                          couchbase::retry_reason::socket_closed_while_in_flight,
                          {},
                          std::nullopt);

  REQUIRE(out.called);
  CHECK(out.request == nullptr);
  CHECK(out.error == couchbase::errc::network::cluster_closed);
}

// ---------------------------------------------------------------------------
// Extended error info path
// ---------------------------------------------------------------------------

TEST_CASE("unit: handle_response forwards optional error_map_info to the handler", "[unit]")
{
  // The server may attach extended error information from its error map.
  // The handler must receive it intact so it can be stored in the error context.
  captured_call out;
  recording_handler handler{ out };

  couchbase::core::key_value_error_map_info info{ 0x0001, "KEY_ENOENT", "key does not exist", {} };

  auto req = make_request();
  handler.handle_response(req,
                          std::nullopt,
                          couchbase::errc::key_value::document_not_found,
                          couchbase::retry_reason::do_not_retry,
                          {},
                          info);

  REQUIRE(out.called);
  REQUIRE(out.error_info.has_value());
  CHECK(out.error_info->code() == 0x0001);
  CHECK(out.error_info->name() == "KEY_ENOENT");
}

// ---------------------------------------------------------------------------
// Polymorphic dispatch via base pointer (the real caller pattern)
// ---------------------------------------------------------------------------

TEST_CASE("unit: handle_response dispatches correctly through base pointer", "[unit]")
{
  // io::mcbp_session and collections_component hold a shared_ptr<response_handler>
  // and call through the virtual interface.  Verify vtable dispatch works correctly.
  captured_call out;
  std::shared_ptr<couchbase::core::response_handler> handler =
    std::make_shared<recording_handler>(out);

  auto req = make_request();
  handler->handle_response(
    req, std::nullopt, {}, couchbase::retry_reason::do_not_retry, {}, std::nullopt);

  REQUIRE(out.called);
  CHECK(out.request == req);
}
