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

#include "transaction_get_result.hxx"
#include "result.hxx"

namespace couchbase::core::transactions
{
transaction_get_result
transaction_get_result::create_from(const core::operations::lookup_in_response& resp)
{
    std::optional<std::string> atr_id;
    std::optional<std::string> transaction_id;
    std::optional<std::string> attempt_id;
    std::optional<std::string> operation_id;
    std::optional<std::vector<std::byte>> staged_content;
    std::optional<std::string> atr_bucket_name;
    std::optional<std::string> atr_scope_name;
    std::optional<std::string> atr_collection_name;
    std::optional<tao::json::value> forward_compat;

    // read from xattrs.txn.restore
    std::optional<std::string> cas_pre_txn;
    std::optional<std::string> revid_pre_txn;
    std::optional<std::uint32_t> exptime_pre_txn;
    std::optional<std::string> crc32_of_staging;

    // read from $document
    std::optional<std::string> cas_from_doc;
    std::optional<std::string> revid_from_doc;
    std::optional<std::uint32_t> exptime_from_doc;
    std::optional<std::string> crc32_from_doc;

    std::optional<std::string> op;
    std::vector<std::byte> content;

    if (resp.fields[0].status == key_value_status_code::success) {
        atr_id = codec::tao_json_serializer::deserialize<std::string>(resp.fields[0].value);
    }
    if (resp.fields[1].status == key_value_status_code::success) {
        transaction_id = codec::tao_json_serializer::deserialize<std::string>(resp.fields[1].value);
    }
    if (resp.fields[2].status == key_value_status_code::success) {
        attempt_id = codec::tao_json_serializer::deserialize<std::string>(resp.fields[2].value);
    }
    if (resp.fields[3].status == key_value_status_code::success) {
        operation_id = codec::tao_json_serializer::deserialize<std::string>(resp.fields[3].value);
    }
    if (resp.fields[4].status == key_value_status_code::success) {
        staged_content = resp.fields[4].value;
    }
    if (resp.fields[5].status == key_value_status_code::success) {
        atr_bucket_name = codec::tao_json_serializer::deserialize<std::string>(resp.fields[5].value);
    }
    if (resp.fields[6].status == key_value_status_code::success) {
        atr_scope_name = codec::tao_json_serializer::deserialize<std::string>(resp.fields[6].value);
    }
    if (resp.fields[7].status == key_value_status_code::success) {
        atr_collection_name = codec::tao_json_serializer::deserialize<std::string>(resp.fields[7].value);
    }

    if (resp.fields[8].status == key_value_status_code::success) {
        auto restore = core::utils::json::parse_binary(resp.fields[8].value);
        cas_pre_txn = restore["CAS"].as<std::string>();
        // only present in 6.5+
        revid_pre_txn = restore["revid"].as<std::string>();
        exptime_pre_txn = restore["exptime"].as<std::uint32_t>();
    }
    if (resp.fields[9].status == key_value_status_code::success) {
        op = codec::tao_json_serializer::deserialize<std::string>(resp.fields[9].value);
    }
    if (resp.fields[10].status == key_value_status_code::success) {
        auto doc = core::utils::json::parse_binary(resp.fields[10].value);
        cas_from_doc = doc["CAS"].as<std::string>();
        // only present in 6.5+
        revid_from_doc = doc["revid"].as<std::string>();
        exptime_from_doc = doc["exptime"].as<std::uint32_t>();
        crc32_from_doc = doc["value_crc32c"].as<std::string>();
    }
    if (resp.fields[11].status == key_value_status_code::success) {
        crc32_of_staging = codec::tao_json_serializer::deserialize<std::string>(resp.fields[11].value);
    }
    if (resp.fields[12].status == key_value_status_code::success) {
        forward_compat = core::utils::json::parse_binary(resp.fields[12].value);
    } else {
        forward_compat = tao::json::empty_object;
    }
    if (resp.fields[13].status == key_value_status_code::success) {
        content = resp.fields[13].value;
    }

    transaction_links links(atr_id,
                            atr_bucket_name,
                            atr_scope_name,
                            atr_collection_name,
                            transaction_id,
                            attempt_id,
                            operation_id,
                            staged_content,
                            cas_pre_txn,
                            revid_pre_txn,
                            exptime_pre_txn,
                            crc32_of_staging,
                            op,
                            forward_compat,
                            resp.deleted);
    document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc, crc32_from_doc);
    return { { resp.ctx.bucket(), resp.ctx.scope(), resp.ctx.collection(), resp.ctx.id() },
             content,
             resp.cas.value(),
             links,
             std::make_optional(md) };
}

transaction_get_result
transaction_get_result::create_from(const core::document_id& id, const result& res)
{
    std::optional<std::string> atr_id;
    std::optional<std::string> transaction_id;
    std::optional<std::string> attempt_id;
    std::optional<std::string> operation_id;
    std::optional<std::vector<std::byte>> staged_content;
    std::optional<std::string> atr_bucket_name;
    std::optional<std::string> atr_scope_name;
    std::optional<std::string> atr_collection_name;
    std::optional<tao::json::value> forward_compat;

    // read from xattrs.txn.restore
    std::optional<std::string> cas_pre_txn;
    std::optional<std::string> revid_pre_txn;
    std::optional<std::uint32_t> exptime_pre_txn;
    std::optional<std::string> crc32_of_staging;

    // read from $document
    std::optional<std::string> cas_from_doc;
    std::optional<std::string> revid_from_doc;
    std::optional<std::uint32_t> exptime_from_doc;
    std::optional<std::string> crc32_from_doc;

    std::optional<std::string> op;
    std::vector<std::byte> content;

    if (res.values[0].has_value()) {
        atr_id = res.values[0].content_as<std::string>();
    }
    if (res.values[1].has_value()) {
        transaction_id = res.values[1].content_as<std::string>();
    }
    if (res.values[2].has_value()) {
        attempt_id = res.values[2].content_as<std::string>();
    }
    if (res.values[3].has_value()) {
        operation_id = res.values[3].content_as<std::string>();
    }
    if (res.values[4].has_value()) {
        staged_content = res.values[4].raw_value;
    }
    if (res.values[5].has_value()) {
        atr_bucket_name = res.values[5].content_as<std::string>();
    }
    if (res.values[6].has_value()) {
        atr_scope_name = res.values[6].content_as<std::string>();
    }
    if (res.values[7].has_value()) {
        atr_collection_name = res.values[7].content_as<std::string>();
    }
    if (res.values[8].has_value()) {
        auto restore = res.values[8].content_as();
        cas_pre_txn = restore["CAS"].as<std::string>();
        // only present in 6.5+
        revid_pre_txn = restore["revid"].as<std::string>();
        exptime_pre_txn = restore["exptime"].as<std::uint32_t>();
    }
    if (res.values[9].has_value()) {
        op = res.values[9].content_as<std::string>();
    }
    if (res.values[10].has_value()) {
        auto doc = res.values[10].content_as();
        cas_from_doc = doc["CAS"].as<std::string>();
        // only present in 6.5+
        revid_from_doc = doc["revid"].as<std::string>();
        exptime_from_doc = doc["exptime"].as<std::uint32_t>();
        crc32_from_doc = doc["value_crc32c"].as<std::string>();
    }
    if (res.values[11].has_value()) {
        crc32_of_staging = res.values[11].content_as<std::string>();
    }
    if (res.values[12].has_value()) {
        forward_compat = res.values[12].content_as();
    } else {
        forward_compat = tao::json::empty_object;
    }
    if (res.values[13].has_value()) {
        content = res.values[13].raw_value;
    }

    transaction_links links(atr_id,
                            atr_bucket_name,
                            atr_scope_name,
                            atr_collection_name,
                            transaction_id,
                            attempt_id,
                            operation_id,
                            staged_content,
                            cas_pre_txn,
                            revid_pre_txn,
                            exptime_pre_txn,
                            crc32_of_staging,
                            op,
                            forward_compat,
                            res.is_deleted);
    document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc, crc32_from_doc);
    return { id, content, res.cas, links, std::make_optional(md) };
}
} // namespace couchbase::core::transactions
