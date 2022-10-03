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

#include "core/transactions.hxx"
#include <couchbase/cluster.hxx>

namespace couchbase
{
auto
cluster::transactions() -> std::shared_ptr<couchbase::transactions::transactions>
{
    // TODO: add mutex for thread safety.
    if (!transactions_) {
        // TODO: fill in the cluster config, add an optional transactions_config, use it here.
        transactions_ = std::make_shared<couchbase::core::transactions::transactions>(core_, couchbase::transactions::transaction_config());
    }
    return transactions_;
}
} // namespace couchbase