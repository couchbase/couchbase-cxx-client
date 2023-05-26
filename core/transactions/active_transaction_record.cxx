/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "active_transaction_record.hxx"

namespace couchbase::core::transactions
{
active_transaction_record
active_transaction_record::map_to_atr(const operations::lookup_in_response& resp)
{
    std::vector<atr_entry> entries;
    if (resp.fields[0].status == key_value_status_code::success) {
        auto attempts = core::utils::json::parse_binary(resp.fields[0].value);
        auto vbucket = core::utils::json::parse_binary(resp.fields[1].value);
        auto now_ns = now_ns_from_vbucket(vbucket);
        entries.reserve(attempts.get_object().size());
        for (const auto& [key, val] : attempts.get_object()) {
            std::optional<tao::json::value> forward_compat;
            if (const auto* compat = val.find(ATR_FIELD_FORWARD_COMPAT); compat != nullptr) {
                forward_compat = *compat;
            }
            std::optional<uint32_t> expires_after_msec = std::max(val.optional<int32_t>(ATR_FIELD_EXPIRES_AFTER_MSECS).value_or(0), 0);
            entries.emplace_back(resp.ctx.bucket(),
                                 resp.ctx.id(),
                                 key,
                                 attempt_state_value(val.at(ATR_FIELD_STATUS).get_string()),
                                 parse_mutation_cas(val.optional<std::string>(ATR_FIELD_START_TIMESTAMP).value_or("")),
                                 parse_mutation_cas(val.optional<std::string>(ATR_FIELD_START_COMMIT).value_or("")),
                                 parse_mutation_cas(val.optional<std::string>(ATR_FIELD_TIMESTAMP_COMPLETE).value_or("")),
                                 parse_mutation_cas(val.optional<std::string>(ATR_FIELD_TIMESTAMP_ROLLBACK_START).value_or("")),
                                 parse_mutation_cas(val.optional<std::string>(ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE).value_or("")),
                                 expires_after_msec,
                                 process_document_ids(val, ATR_FIELD_DOCS_INSERTED),
                                 process_document_ids(val, ATR_FIELD_DOCS_REPLACED),
                                 process_document_ids(val, ATR_FIELD_DOCS_REMOVED),
                                 forward_compat,
                                 now_ns,
                                 val.optional<std::string>(ATR_FIELD_DURABILITY_LEVEL));
        }
    }
    return active_transaction_record(
      { resp.ctx.bucket(), resp.ctx.scope(), resp.ctx.collection(), resp.ctx.id() }, resp.cas.value(), std::move(entries));
}
} // namespace couchbase::core::transactions
