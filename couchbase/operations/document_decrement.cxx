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

#include <couchbase/operations/document_decrement.hxx>

#include <couchbase/errors.hxx>

namespace couchbase::operations
{
std::error_code
decrement_request::encode_to(decrement_request::encoded_request_type& encoded, mcbp_context&& /* context */) const
{
    encoded.opaque(opaque);
    encoded.partition(partition);
    encoded.body().id(id);
    encoded.body().delta(delta);
    if (initial_value) {
        encoded.body().initial_value(initial_value.value());
        encoded.body().expiry(expiry);
    } else {
        encoded.body().initial_value(0);
        encoded.body().expiry(0xffff'ffff);
    }
    if (preserve_expiry) {
        encoded.body().preserve_expiry();
    }
    return {};
}

decrement_response
decrement_request::make_response(error_context::key_value&& ctx, const encoded_response_type& encoded) const
{
    decrement_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        response.cas = encoded.cas();
        response.content = encoded.body().content();
        response.token = encoded.body().token();
        response.token.partition_id = partition;
        response.token.bucket_name = response.ctx.id.bucket();
    }
    return response;
}
} // namespace couchbase::operations
