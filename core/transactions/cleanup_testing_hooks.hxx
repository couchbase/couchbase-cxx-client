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

#include "core/transactions/error_class.hxx"

#include <functional>
#include <string>

namespace couchbase::core::transactions
{
using error_func3 = std::function<std::optional<error_class>(const std::string&)>;
using error_func4 = std::function<std::optional<error_class>(void)>;
namespace
{
std::optional<couchbase::core::transactions::error_class>
noop1(const std::string&)
{
    return {};
}
std::optional<couchbase::core::transactions::error_class>
noop2()
{
    return {};
}

} // namespace

/**
 * Hooks purely for testing purposes.  If you're an end-user looking at these for any reason, then please contact us first
 * about your use-case: we are always open to adding good ideas into the transactions library.
 */
struct cleanup_testing_hooks {
    error_func3 before_commit_doc = noop1;
    error_func3 before_doc_get = noop1;
    error_func3 before_remove_doc_staged_for_removal = noop1;
    error_func3 before_remove_doc = noop1;
    error_func3 before_atr_get = noop1;
    error_func3 before_remove_links = noop1;

    error_func4 before_atr_remove = noop2;

    error_func4 on_cleanup_docs_completed = noop2;
    error_func4 on_cleanup_completed = noop2;

    error_func3 client_record_before_create = noop1;
    error_func3 client_record_before_get = noop1;
    error_func3 client_record_before_update = noop1;
    error_func3 client_record_before_remove_client = noop1;

    // needed for unique_ptr<cleanup_testing_hooks> in transaction_config, with a forward declaration.
    ~cleanup_testing_hooks()
    {
    }
};
} // namespace couchbase::core::transactions