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

#include "couchbase/transactions/transaction_config.hxx"

namespace couchbase::transactions
{

transaction_config::transaction_config()
  : level_(couchbase::durability_level::majority)
  , cleanup_window_(std::chrono::seconds(120))
  , expiration_time_(std::chrono::seconds(15))
  , cleanup_lost_attempts_(true)
  , cleanup_client_attempts_(true)
  , attempt_context_hooks_(new core::transactions::attempt_context_testing_hooks())
  , cleanup_hooks_(new core::transactions::cleanup_testing_hooks())
  , scan_consistency_(core::query_scan_consistency::request_plus)
{
}

transaction_config::~transaction_config() = default;

transaction_config::transaction_config(const transaction_config& config)
  : level_(config.durability_level())
  , cleanup_window_(config.cleanup_window())
  , expiration_time_(config.expiration_time())
  , cleanup_lost_attempts_(config.cleanup_lost_attempts())
  , cleanup_client_attempts_(config.cleanup_client_attempts())
  , attempt_context_hooks_(new core::transactions::attempt_context_testing_hooks(config.attempt_context_hooks()))
  , cleanup_hooks_(new core::transactions::cleanup_testing_hooks(config.cleanup_hooks()))
  , scan_consistency_(config.scan_consistency())
  , custom_metadata_collection_(config.custom_metadata_collection())

{
}

transaction_config&
transaction_config::operator=(const transaction_config& c)
{
    level_ = c.durability_level();
    cleanup_window_ = c.cleanup_window();
    expiration_time_ = c.expiration_time();
    cleanup_lost_attempts_ = c.cleanup_lost_attempts();
    cleanup_client_attempts_ = c.cleanup_client_attempts();
    attempt_context_hooks_.reset(new core::transactions::attempt_context_testing_hooks(c.attempt_context_hooks()));
    cleanup_hooks_.reset(new core::transactions::cleanup_testing_hooks(c.cleanup_hooks()));
    scan_consistency_ = c.scan_consistency();
    custom_metadata_collection_ = c.custom_metadata_collection();
    return *this;
}

void
transaction_config::test_factories(core::transactions::attempt_context_testing_hooks& hooks,
                                   core::transactions::cleanup_testing_hooks& cleanup_hooks)
{
    attempt_context_hooks_.reset(new core::transactions::attempt_context_testing_hooks(hooks));
    cleanup_hooks_.reset(new core::transactions::cleanup_testing_hooks(cleanup_hooks));
}

} // namespace couchbase::core::transactions
