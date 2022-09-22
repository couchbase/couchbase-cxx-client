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

#include "core/cluster.hxx"
#include "core/operations.hxx"

#include <couchbase/transactions/internal/exceptions_internal.hxx>
#include <couchbase/transactions/internal/utils.hxx>

#include <future>
#include <memory>
#include <optional>
#include <vector>

namespace couchbase::transactions
{

/*
    inline active_transaction_record active_transaction_record::map_to_atr(const couchbase::document_id& atr_id,
                                                                           result& res,
                                                                           nlohmann::json& attempts)
    {
        auto vbucket = res.values[1].content_as<nlohmann::json>();
        auto now_ns = now_ns_from_vbucket(vbucket);
        std::vector<atr_entry> entries;
        entries.reserve(attempts.size());
        for (auto& element : attempts.items()) {
            auto& val = element.value();
            entries.emplace_back(
              atr_id.bucket(),
              atr_id.key(),
              element.key(),
              attempt_state_value(val[ATR_FIELD_STATUS].get<std::string>()),
              parse_mutation_cas(val.value(ATR_FIELD_START_TIMESTAMP, "")),
              parse_mutation_cas(val.value(ATR_FIELD_START_COMMIT, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_COMPLETE, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_START, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE, "")),
              val.count(ATR_FIELD_EXPIRES_AFTER_MSECS) ? std::make_optional(val[ATR_FIELD_EXPIRES_AFTER_MSECS].get<std::uint32_t>())
                                                       : std::optional<std::uint32_t>(),
              process_document_ids(val, ATR_FIELD_DOCS_INSERTED),
              process_document_ids(val, ATR_FIELD_DOCS_REPLACED),
              process_document_ids(val, ATR_FIELD_DOCS_REMOVED),
              val.contains(ATR_FIELD_FORWARD_COMPAT) ? std::make_optional(val[ATR_FIELD_FORWARD_COMPAT].get<nlohmann::json>())
                                                     : std::nullopt,
              now_ns,
              val.contains(ATR_FIELD_DURABILITY_LEVEL) ? std::make_optional(val[ATR_FIELD_DURABILITY_LEVEL].get<nlohmann::json>())
                 : std::nullopt);
        }
        return active_transaction_record(atr_id, res.cas, std::move(entries));
    }

*/
} // namespace couchbase::transactions
