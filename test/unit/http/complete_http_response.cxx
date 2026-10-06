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

#include "framework/test_registry.hxx"

#include "core/error_context/http.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
struct stub_response {
  couchbase::core::error_context::http ctx{};
};

// make_response throws `first` on the first call and, when `throws_again` is set, on every call
// after it.
template<typename Exception>
struct stub_request {
  using response_type = stub_response;

  bool throws_again{ false };
  int* calls{ nullptr };

  [[nodiscard]] auto make_response(couchbase::core::error_context::http&& ctx,
                                   const couchbase::core::io::http_response& /* encoded */) const
    -> stub_response
  {
    if (++*calls == 1 || throws_again) {
      throw Exception{};
    }
    return stub_response{ std::move(ctx) };
  }
};

struct decode_error : std::runtime_error {
  decode_error()
    : std::runtime_error{ "not decodable" }
  {
  }
};

template<typename Request>
auto
complete(const Request& request, std::uint32_t status) -> stub_response
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append("not json");
  return couchbase::core::io::complete_http_response(
    request,
    [] {
      return couchbase::core::error_context::http{};
    },
    encoded);
}

void
a_retry_request_propagates_to_the_caller([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<couchbase::core::priv::retry_http_request> request{ false, &calls };
  assert_throws<couchbase::core::priv::retry_http_request>(
    [&] {
      static_cast<void>(complete(request, 200));
    },
    "the command retries the request, so the exception is not swallowed");
  assert_eq(calls, 1, "make_response is not called again");
}

void
an_undecodable_response_other_than_400_is_a_parsing_failure([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<decode_error> request{ false, &calls };
  assert_eq(complete(request, 500).ctx.ec,
            couchbase::errc::common::parsing_failure,
            "only a 400 is the caller's error");
  assert_eq(calls, 2, "the response is built again from the error context");
}

void
a_second_throw_completes_from_the_error_context([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<decode_error> request{ true, &calls };
  assert_eq(complete(request, 400).ctx.ec,
            couchbase::errc::common::invalid_argument,
            "a 400 still completes, with invalid_argument");
  calls = 0;
  assert_eq(complete(request, 500).ctx.ec,
            couchbase::errc::common::parsing_failure,
            "any other status still completes, with parsing_failure");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_retry_request_propagates_to_the_caller) },
      { CASE(an_undecodable_response_other_than_400_is_a_parsing_failure) },
      { CASE(a_second_throw_completes_from_the_error_context) },
    },
  };
}

} // namespace couchbase::test
