/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/error_context/key_value_error_map_info.hxx"

#include <couchbase/retry_reason.hxx>

#include <memory>
#include <optional>
#include <system_error>

namespace couchbase::core::mcbp
{
class queue_request;
} // namespace couchbase::core::mcbp

namespace couchbase::core
{
namespace io
{
struct mcbp_message;
class mcbp_session;
} // namespace io

/**
 * Interface for handling MCBP (Memcached Binary Protocol) responses dispatched by
 * @ref io::mcbp_session.
 *
 * Implementors receive the decoded response together with enough context to populate
 * dispatch tracing spans and key-value error contexts without holding a long-lived
 * reference to the session.
 *
 * @note Adding, removing, or reordering parameters of @ref handle_response is an ABI
 *       breaking change for any out-of-tree implementors of this interface.
 */
class response_handler
{
public:
  response_handler() = default;
  response_handler(const response_handler&) = default;
  response_handler(response_handler&&) = delete;
  auto operator=(const response_handler&) -> response_handler& = default;
  auto operator=(response_handler&&) -> response_handler& = delete;
  virtual ~response_handler() = default;

  /**
   * Called by @ref io::mcbp_session when a queued request completes (successfully or with
   * an error).
   *
   * @param request  The originating request.  May be empty when the session is shutting
   *                 down and synthetically cancels in-flight operations.
   * @param session  A snapshot of the session that delivered the response, carrying
   *                 connection metadata (remote/local address and port) needed to populate
   *                 dispatch tracing spans and error contexts.  Used by
   *                 @ref crud_component (CXXCBC-798) to build
   *                 @ref key_value_error_context without retaining a live session reference.
   * @param error    Transport-level or protocol-level error code; empty on success.
   * @param reason   Retry classification hint supplied by the session.
   * @param msg      The raw decoded MCBP response message.
   * @param error_info  Optional extended error information from the server's error map.
   */
  virtual void handle_response(std::shared_ptr<mcbp::queue_request> request,
                               std::optional<io::mcbp_session> session,
                               std::error_code error,
                               retry_reason reason,
                               io::mcbp_message msg,
                               std::optional<key_value_error_map_info> error_info) = 0;
};

} // namespace couchbase::core
