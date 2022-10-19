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

transactions_config::~transactions_config() = default;

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

} // namespace couchbase::transactions
