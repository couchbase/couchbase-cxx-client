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

#include "get_all_replicas.hxx"

namespace couchbase::api
{
[[nodiscard]] std::shared_ptr<couchbase::impl::get_all_replicas_request>
make_get_all_replicas_request(document_id id, const get_all_replicas_options& options)
{
    return std::make_shared<couchbase::impl::get_all_replicas_request>(couchbase::document_id(std::move(id)), options.timeout);
}
} // namespace couchbase::api
