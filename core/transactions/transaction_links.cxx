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

#include "transaction_links.hxx"

std::ostream&
couchbase::core::transactions::operator<<(std::ostream& os, const transaction_links& links)
{
    os << "transaction_links{atr: " << links.atr_id_.value_or("none") << ", atr_bkt: " << links.atr_bucket_name_.value_or("none")
       << ", atr_coll: " << links.atr_collection_name_.value_or("none") << ", atr_scope: " << links.atr_scope_name_.value_or("none")
       << ", txn_id: " << links.staged_transaction_id_.value_or("none") << ", attempt_id: " << links.staged_attempt_id_.value_or("none")
       << ", crc32_of_staging:" << links.crc32_of_staging_.value_or("none") << "}";
    return os;
}
