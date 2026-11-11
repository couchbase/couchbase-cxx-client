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

#pragma once

#include "core/protocol/client_opcode.hxx"

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace couchbase::core::io
{
class mcbp_session;
class mcbp_session_impl;

class mcbp_session_unit_test_api
{
public:
  // The opcodes of the requests written before the session bootstrapped, in the order written. A
  // request stays here whatever happens to its subscription, so a test can count writes that a
  // cancellation has already withdrawn.
  [[nodiscard]] auto pending_opcodes() const -> std::vector<protocol::client_opcode>;

  // Routes a response frame with this opcode, status and opaque to its subscription, as the read
  // loop does for a frame from the socket. Returns false when nothing is subscribed under opaque.
  [[nodiscard]] auto deliver_response(protocol::client_opcode opcode,
                                      std::uint16_t status,
                                      std::uint32_t opaque) const -> bool;

  // As deliver_response(), as an alternative-format frame whose framing extras carry this encoded
  // server duration.
  [[nodiscard]] auto deliver_response_with_server_duration(protocol::client_opcode opcode,
                                                           std::uint16_t status,
                                                           std::uint32_t opaque,
                                                           std::uint16_t encoded_duration) const
    -> bool;

  // Runs before a non-bootstrapped session inserts a subscription in write_and_subscribe(). Set
  // before the session is used across threads.
  void on_before_subscribe(std::function<void(std::uint32_t)> hook) const;

private:
  friend class mcbp_session;

  explicit mcbp_session_unit_test_api(std::shared_ptr<mcbp_session_impl> impl);

  std::shared_ptr<mcbp_session_impl> impl_;
};
} // namespace couchbase::core::io
