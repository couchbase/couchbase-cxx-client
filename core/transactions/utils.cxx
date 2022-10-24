/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include "core/operations.hxx"
#include "core/operations/management/bucket_get_all.hxx"

#include "internal/utils.hxx"

namespace couchbase::core::transactions
{
std::string
collection_spec_from_id(const core::document_id& id)
{
    std::string retval = id.scope();
    return retval.append(".").append(id.collection());
}

bool
document_ids_equal(const core::document_id& id1, const core::document_id& id2)
{
    return id1.key() == id2.key() && id1.bucket() == id2.bucket() && id1.scope() == id2.scope() && id1.collection() == id2.collection();
}

std::string
jsonify(const tao::json::value& obj)
{
    return core::utils::json::generate(obj);
}

std::uint64_t
now_ns_from_vbucket(const tao::json::value& vbucket)
{
    std::string now_str = vbucket.at("HLC").at("now").get_string();
    return stoull(now_str, nullptr, 10) * 1000000000;
}

void
wrap_collection_call(result& res, std::function<void(result&)> call)
{
    call(res);
    if (!res.is_success()) {
        throw client_error(res);
    }
    if (!res.values.empty() && !res.ignore_subdoc_errors) {
        for (const auto& v : res.values) {
            if (v.status != subdoc_result::status_type::success) {
                throw client_error(res);
            }
        }
    }
}

result
wrap_operation_future(std::future<result>& fut, bool ignore_subdoc_errors)
{
    auto res = fut.get();
    if (!res.is_success()) {
        throw client_error(res);
    }
    // we should raise here, as we are doing a non-subdoc request and can't specify
    // access_deleted.  TODO: consider changing client to return document_not_found
    if (res.is_deleted && res.values.empty()) {
        res.ec = couchbase::errc::key_value::document_not_found;
        throw client_error(res);
    }
    if (!res.values.empty() && !ignore_subdoc_errors) {
        for (const auto& v : res.values) {
            if (v.status != subdoc_result::status_type::success) {
                throw client_error(res);
            }
        }
    }
    return res;
}

template<>
bool
is_error(const core::operations::mutate_in_response& resp)
{
    return resp.ctx.ec() || resp.ctx.first_error_index();
}

template<>
std::optional<error_class>
error_class_from_response_extras(const core::operations::mutate_in_response& resp)
{
    if (!resp.ctx.first_error_index()) {
        return {};
    }
    auto status = resp.fields.at(*resp.ctx.first_error_index()).status;
    if (status == key_value_status_code::subdoc_path_not_found) {
        return FAIL_PATH_NOT_FOUND;
    }
    if (status == key_value_status_code::subdoc_path_exists) {
        return FAIL_PATH_ALREADY_EXISTS;
    }
    return FAIL_OTHER;
}

core::document_id
atr_id_from_bucket_and_key(const couchbase::transactions::transactions_config::built& cfg,
                           const std::string& bucket,
                           const std::string& key)
{
    if (cfg.metadata_collection) {
        return { cfg.metadata_collection->bucket, cfg.metadata_collection->scope, cfg.metadata_collection->collection, key };
    }
    return { bucket, couchbase::scope::default_name, couchbase::collection::default_name, key };
}
} // namespace couchbase::core::transactions
