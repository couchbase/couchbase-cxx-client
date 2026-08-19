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

#include "error_context/key_value_error_map_info.hxx"

#include <couchbase/retry_reason.hxx>

#include <memory>
#include <optional>
#include <system_error>

namespace couchbase::core
{
class bucket_impl;

namespace mcbp
{
class queue_request;
class queue_response;
} // namespace mcbp

class bucket_unit_test_api
{
public:
  // The response path a session drives once a request has been written. Reachable in a test only
  // through here: it decides between completing a request and handing it to a retry, and both
  // outcomes have to be checkable without a server.
  void resolve_response(const std::shared_ptr<mcbp::queue_request>& request,
                        const std::shared_ptr<mcbp::queue_response>& response,
                        std::error_code ec,
                        retry_reason reason,
                        std::optional<key_value_error_map_info> error_info);

private:
  friend class bucket;

  explicit bucket_unit_test_api(std::shared_ptr<bucket_impl> impl);

  std::shared_ptr<bucket_impl> impl_;
};
} // namespace couchbase::core
