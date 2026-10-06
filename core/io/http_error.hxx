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

#pragma once

#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

#include <cstdint>
#include <exception>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace couchbase::core::io
{
/**
 * RFC-58: an HTTP 400 is the caller's error. A 400 whose body could not be decoded names no more
 * specific error, so it is reported as invalid_argument rather than as the decoding failure.
 *
 * A decoding failure is the streaming lexer's own code, for a body read with
 * http_response_body::use_json_streaming, or parsing_failure when body is not JSON. A
 * parsing_failure for a body that is JSON is a mapped service error, such as a query syntax error,
 * and is kept. Every other error, and any error for another status, is returned unchanged.
 */
[[nodiscard]] inline auto
invalid_argument_for_undecodable_400(std::error_code ec,
                                     std::uint32_t status_code,
                                     std::string_view body) -> std::error_code
{
  if (status_code != 400) {
    return ec;
  }
  if (ec.category() == impl::streaming_json_lexer_category()) {
    return errc::common::invalid_argument;
  }
  if (ec == errc::common::parsing_failure) {
    try {
      static_cast<void>(utils::json::parse(body));
    } catch (const std::exception&) {
      return errc::common::invalid_argument;
    }
  }
  return ec;
}

/**
 * Completes a buffered HTTP request from its response. make_context builds a fresh error context.
 *
 * A body that make_response cannot decode, including one without the members it reads, completes
 * the request instead of escaping as an exception: invalid_argument for an HTTP 400 (RFC-58),
 * parsing_failure otherwise. priv::retry_http_request still propagates to the caller.
 */
template<typename Request, typename MakeContext>
[[nodiscard]] auto
complete_http_response(Request& request, MakeContext&& make_context, const http_response& encoded)
  -> typename std::remove_const_t<Request>::response_type
{
  try {
    auto response = request.make_response(make_context(), encoded);
    response.ctx.ec = invalid_argument_for_undecodable_400(
      response.ctx.ec, encoded.status_code, encoded.body.data());
    return response;
  } catch (const priv::retry_http_request&) {
    throw;
  } catch (const std::exception&) {
    auto ctx = make_context();
    ctx.ec =
      encoded.status_code == 400 ? errc::common::invalid_argument : errc::common::parsing_failure;
    // make_response decodes nothing once ctx.ec is set, so this cannot throw again.
    return request.make_response(std::move(ctx), encoded);
  }
}
} // namespace couchbase::core::io
