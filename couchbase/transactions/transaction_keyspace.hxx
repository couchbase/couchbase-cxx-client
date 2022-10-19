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
#pragma once

#include "couchbase/collection.hxx"
#include "couchbase/scope.hxx"

#include <string>
#include <utility>

namespace couchbase::transactions
{
/**
 * @brief  Offline, serializable representation of a bucket, scope, and collection
 */
struct transaction_keyspace {

    std::string bucket;
    std::string scope{ couchbase::scope::default_name };
    std::string collection{ couchbase::collection::default_name };

    transaction_keyspace(const transaction_keyspace&) = default;
    transaction_keyspace(transaction_keyspace&&) = default;
    transaction_keyspace& operator=(const transaction_keyspace& keyspace)
    {
        if (this != &keyspace) {
            bucket = keyspace.bucket;
            scope = keyspace.scope;
            collection = keyspace.collection;
        }
        return *this;
    }
    bool operator==(const transaction_keyspace& keyspace)
    {
        return bucket == keyspace.bucket && scope == keyspace.scope && collection == keyspace.collection;
    }

    transaction_keyspace(std::string bucket_name, std::string scope_name, std::string collection_name)
      : bucket(std::move(bucket_name))
      , scope(std::move(scope_name))
      , collection(std::move(collection_name))
    {
        if (scope.empty()) {
            scope = couchbase::scope::default_name;
        }
        if (collection.empty()) {
            collection = couchbase::collection::default_name;
        }
    }

    explicit transaction_keyspace(const std::string& bucket_name)
      : transaction_keyspace{ bucket_name, couchbase::scope::default_name, couchbase::collection::default_name }
    {
    }

    template<typename OStream>
    friend OStream& operator<<(OStream& os, const transaction_keyspace& keyspace)
    {
        os << "transaction_keyspace{";
        os << "bucket: " << keyspace.bucket;
        os << ", scope: " << keyspace.scope;
        os << ", collection: " << keyspace.collection;
        os << "}";
        return os;
    }
};
} // namespace couchbase::transactions