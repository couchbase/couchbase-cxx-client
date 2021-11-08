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

#include <couchbase/operations/document_exists.hxx>

#include <couchbase/errors.hxx>

#include <couchbase/logger/logger.hxx>

#include <couchbase/document_id_fmt.hxx>

namespace couchbase::operations
{
std::error_code
exists_request::encode_to(exists_request::encoded_request_type& encoded, mcbp_context&& /* context */) const
{
    encoded.opaque(opaque);
    encoded.body().id(partition, id);
    return {};
}

exists_response
exists_request::make_response(error_context::key_value&& ctx, const encoded_response_type& encoded) const
{
    exists_response response{ std::move(ctx), partition };
    if (!response.ctx.ec) {
        response.cas = encoded.body().cas();
        response.partition_id = encoded.body().partition_id();
        switch (encoded.body().status()) {
            case 0x00:
                response.status = exists_response::observe_status::found;
                break;
            case 0x01:
                response.status = exists_response::observe_status::persisted;
                break;
            case 0x80:
                response.status = exists_response::observe_status::not_found;
                break;
            case 0x81:
                response.status = exists_response::observe_status::logically_deleted;
                break;
            default:
                LOG_WARNING("invalid observe status for \"{}\": {:x}", id, encoded.body().status());
                response.status = exists_response::observe_status::invalid;
                break;
        }
    }
    return response;
}
} // namespace couchbase::operations
