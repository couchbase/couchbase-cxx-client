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

// The one query-index-admin property that needs a component rather than the converter
// (CXXCBC-901): where the refusal of a conditional secondary index is delivered.
//
// A completion arriving inline out of execute() re-enters the caller before it holds the
// pending_call the call is about to return, so a caller that cancels from its own handler cancels
// a call it has not been given yet. The error code alone cannot see this -- it is the same code
// either way -- so the case runs execute() against an io_context it has not started and asserts
// that nothing has completed yet.
//
// No server is needed: the refusal is decided before anything is dispatched, so the channel below
// is never connected.

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/protostellar/component.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <optional>
#include <string>
#include <utility>

namespace couchbase::test
{
namespace
{
namespace om = ::couchbase::core::operations::management;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using namespace std::chrono_literals;

[[nodiscard]] auto
conditional_secondary_index() -> om::query_index_create_request
{
  om::query_index_create_request request{};
  request.bucket_name = "travel";
  request.index_name = "ix";
  request.keys = { "country" };
  request.condition = R"(country = "US")";
  request.client_context_id = "ctx-42";
  return request;
}

void
a_refused_index_completes_on_the_io_context()
{
  asio::io_context io;
  component comp{
    io,
    component_config{
      grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials()),
      cluster_credentials{},
      { 5000ms },
    },
  };

  int completions = 0;
  std::optional<om::query_index_create_response> outcome{};
  const auto call =
    comp.execute(conditional_secondary_index(), [&completions, &outcome](auto response) {
      ++completions;
      outcome = std::move(response);
    });

  assert_false(outcome.has_value(), "the refusal is not delivered inline out of execute()");

  // A call refused before dispatch has no gRPC context to cancel, and the queued completion is
  // owed to the caller either way.
  call.cancel();

  assert_true(io.poll() > 0, "the refusal is queued on the io context");
  assert_eq(completions, 1, "and delivered exactly once, cancelled or not");
  assert_true(outcome->ctx.ec == errc::common::feature_not_available,
              "a conditional secondary index has no couchbase2 encoding");
  assert_eq(outcome->ctx.client_context_id,
            std::string{ "ctx-42" },
            "a response that never reached the server still correlates to the request");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_query_index_admin_component",
    {
      { "a_refused_index_completes_on_the_io_context",
        a_refused_index_completes_on_the_io_context },
    },
  };
}

} // namespace couchbase::test
