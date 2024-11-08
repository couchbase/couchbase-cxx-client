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

#include "core/error_context/key_value.hxx"
#include "core/impl/subdoc/command.hxx"
#include "core/io/mcbp_context.hxx"
#include "core/io/retry_context.hxx"
#include "core/protocol/client_request.hxx"
#include "core/protocol/cmd_lookup_in_replica.hxx"
#include "core/public_fwd.hxx"
#include "core/timeout_defaults.hxx"

#include "core/error_context/subdocument_error_context.hxx"
#include <couchbase/lookup_in_result.hxx>

namespace couchbase::core::impl
{
struct lookup_in_replica_response {
  struct entry {
    std::string path;
    couchbase::codec::binary value;
    std::size_t original_index;
    bool exists;
    protocol::subdoc_opcode opcode;
    key_value_status_code status;
    std::error_code ec{};
  };
  subdocument_error_context ctx{};
  couchbase::cas cas{};
  std::vector<entry> fields{};
  bool deleted{ false };
};

struct lookup_in_replica_request {
  using response_type = lookup_in_replica_response;
  using encoded_request_type = protocol::client_request<protocol::lookup_in_replica_request_body>;
  using encoded_response_type =
    protocol::client_response<protocol::lookup_in_replica_response_body>;

  static const inline std::string observability_identifier = "lookup_in_replica";

  document_id id;
  std::vector<couchbase::core::impl::subdoc::command> specs{};
  std::optional<std::chrono::milliseconds> timeout{};
  std::shared_ptr<couchbase::tracing::request_span> parent_span{ nullptr };
  std::uint16_t partition{};
  std::uint32_t opaque{};
  io::retry_context<false> retries{};

  [[nodiscard]] auto encode_to(encoded_request_type& encoded,
                               mcbp_context&& context) -> std::error_code;

  [[nodiscard]] auto make_response(key_value_error_context&& ctx,
                                   const encoded_response_type& encoded) const
    -> lookup_in_replica_response;
};
} // namespace couchbase::core::impl
