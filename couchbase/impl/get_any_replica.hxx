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

#include <couchbase/api/get_any_replica.hxx>
#include <couchbase/api/get_replica_result.hxx>

#include <couchbase/document_id.hxx>
#include <couchbase/error_context/key_value.hxx>

namespace couchbase::impl
{

/**
 * Request structure, that contains everything related to get_any_replica request.
 *
 * @see couchbase::api::make_any_replica_request
 * @see couchbase::api::get_any_replica_options
 *
 * @since 1.0.0
 * @internal
 */
class get_any_replica_request
{
  public:
    using context_type = couchbase::error_context::key_value;
    using response_type = api::get_replica_result;

    explicit get_any_replica_request(document_id id, std::optional<std::chrono::milliseconds> timeout)
      : id_{ std::move(id) }
      , timeout_{ timeout }
    {
    }

    [[nodiscard]] const auto& id() const
    {
        return id_;
    }

    [[nodiscard]] const auto& timeout() const
    {
        return timeout_;
    }

  private:
    document_id id_;
    std::optional<std::chrono::milliseconds> timeout_{};
};

} // namespace couchbase::impl
