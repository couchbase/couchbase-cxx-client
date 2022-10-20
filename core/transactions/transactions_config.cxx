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

#include "attempt_context_testing_hooks.hxx"
#include "cleanup_testing_hooks.hxx"

#include "couchbase/transactions/transactions_config.hxx"

#include <memory>

namespace couchbase::transactions
{

transactions_config::transactions_config()
  : level_(couchbase::durability_level::majority)
  , expiration_time_(std::chrono::seconds(15))
  , attempt_context_hooks_(new core::transactions::attempt_context_testing_hooks())
  , cleanup_hooks_(new core::transactions::cleanup_testing_hooks())
{
}

transactions_config::~transactions_config()
{
}

transactions_config::transactions_config(transactions_config&& c)
  : level_(std::move(c.level_))
  , expiration_time_(std::move(c.expiration_time_))
  , attempt_context_hooks_(std::move(c.attempt_context_hooks_))
  , cleanup_hooks_(std::move(c.cleanup_hooks_))
  , metadata_collection_(std::move(c.metadata_collection_))
  , query_config_(std::move(c.query_config_))
  , cleanup_config_(std::move(c.cleanup_config_))
{
}

transactions_config::transactions_config(const transactions_config& config)
  : level_(config.durability_level())
  , expiration_time_(config.expiration_time())
  , attempt_context_hooks_(new core::transactions::attempt_context_testing_hooks(config.attempt_context_hooks()))
  , cleanup_hooks_(new core::transactions::cleanup_testing_hooks(config.cleanup_hooks()))
  , metadata_collection_(config.metadata_collection())
  , query_config_(config.query_config())
  , cleanup_config_(config.cleanup_config())
{
}

transactions_config&
transactions_config::operator=(const transactions_config& c)
{
    level_ = c.durability_level();
    expiration_time_ = c.expiration_time();
    attempt_context_hooks_ = std::make_unique<core::transactions::attempt_context_testing_hooks>(c.attempt_context_hooks());
    cleanup_hooks_ = std::make_unique<core::transactions::cleanup_testing_hooks>(c.cleanup_hooks());
    query_config_ = c.query_config();
    metadata_collection_ = c.metadata_collection();
    cleanup_config_ = c.cleanup_config_;
    return *this;
}

void
transactions_config::test_factories(core::transactions::attempt_context_testing_hooks& hooks,
                                    core::transactions::cleanup_testing_hooks& cleanup_hooks)
{
    attempt_context_hooks_ = std::make_unique<core::transactions::attempt_context_testing_hooks>(hooks);
    cleanup_hooks_ = std::make_unique<core::transactions::cleanup_testing_hooks>(cleanup_hooks);
}

transactions_config::built
transactions_config::build() const
{
    return { level_,         expiration_time_,     kv_timeout_,           attempt_context_hooks_,
             cleanup_hooks_, metadata_collection_, query_config_.build(), cleanup_config_.build() };
}

// These are needed since we have a unique_ptr

transactions_config::built::built()
  : level(durability_level::majority)
  , expiration_time(std::chrono::seconds(15))
  , attempt_context_hooks(std::make_unique<core::transactions::attempt_context_testing_hooks>())
  , cleanup_hooks(std::make_unique<core::transactions::cleanup_testing_hooks>())
{
}

transactions_config::built::built(const transactions_config::built& c)
  : level(c.level)
  , expiration_time(c.expiration_time)
  , kv_timeout(c.kv_timeout)
  , metadata_collection(c.metadata_collection)
  , query_config(c.query_config)
  , cleanup_config(c.cleanup_config)
{
    if (c.attempt_context_hooks) {
        attempt_context_hooks = std::make_unique<core::transactions::attempt_context_testing_hooks>(*c.attempt_context_hooks);
    }
    if (c.cleanup_hooks) {
        cleanup_hooks = std::make_unique<core::transactions::cleanup_testing_hooks>(*c.cleanup_hooks);
    }
}

transactions_config::built::~built()
{
}

transactions_config::built::built(const couchbase::durability_level level,
                                  const std::chrono::nanoseconds expiry,
                                  const std::optional<std::chrono::milliseconds> kv_timeout,
                                  const std::unique_ptr<core::transactions::attempt_context_testing_hooks>& attempt_test_hooks,
                                  const std::unique_ptr<core::transactions::cleanup_testing_hooks>& cleanup_test_hooks,
                                  const std::optional<transaction_keyspace>& metadata_collection,
                                  const transactions_query_config::built& query,
                                  const transactions_cleanup_config::built& cleanup)
  : level(level)
  , expiration_time(expiry)
  , kv_timeout(kv_timeout)
  , metadata_collection(metadata_collection)
  , query_config(query)
  , cleanup_config(cleanup)
{
    if (attempt_test_hooks) {
        attempt_context_hooks = std::make_unique<core::transactions::attempt_context_testing_hooks>(*attempt_test_hooks);
    }
    if (cleanup_test_hooks) {
        cleanup_hooks = std::make_unique<core::transactions::cleanup_testing_hooks>(*cleanup_test_hooks);
    }
}

transactions_config::built&
transactions_config::built::operator=(const couchbase::transactions::transactions_config::built& b)
{
    level = b.level;
    expiration_time = b.expiration_time;
    kv_timeout = b.kv_timeout;
    if (b.attempt_context_hooks) {
        attempt_context_hooks = std::make_unique<core::transactions::attempt_context_testing_hooks>(*b.attempt_context_hooks);
    } else {
        attempt_context_hooks = std::make_unique<core::transactions::attempt_context_testing_hooks>();
    }
    if (b.cleanup_hooks) {
        cleanup_hooks = std::make_unique<core::transactions::cleanup_testing_hooks>(*b.cleanup_hooks);
    } else {
        cleanup_hooks = std::make_unique<core::transactions::cleanup_testing_hooks>();
    }
    metadata_collection = b.metadata_collection;
    query_config = b.query_config;
    cleanup_config = b.cleanup_config;
    return *this;
}
} // namespace couchbase::transactions
