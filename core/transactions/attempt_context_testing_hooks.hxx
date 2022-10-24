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
#include <optional>
#include <string>

namespace couchbase::core::transactions
{
class attempt_context;

using error_func1 = std::function<std::optional<error_class>(attempt_context*)>;
using error_func2 = std::function<std::optional<error_class>(attempt_context*, const std::string&)>;

namespace
{
std::optional<error_class>
noop_1(attempt_context*)
{
    return {};
}

std::optional<error_class>
noop_2(attempt_context*, const std::string&)
{
    return {};
}

std::optional<const std::string>
noop_3(attempt_context*)
{
    return {};
}

bool
noop_4(attempt_context*, const std::string&, std::optional<const std::string>)
{
    return false;
}
} // namespace

static const std::string STAGE_ROLLBACK = "rollback";
static const std::string STAGE_GET = "get";
static const std::string STAGE_INSERT = "insert";
static const std::string STAGE_REPLACE = "replace";
static const std::string STAGE_REMOVE = "remove";
static const std::string STAGE_BEFORE_COMMIT = "commit";
static const std::string STAGE_ABORT_GET_ATR = "abortGetAtr";
static const std::string STAGE_ROLLBACK_DOC = "rollbackDoc";
static const std::string STAGE_DELETE_INSERTED = "deleteInserted";
static const std::string STAGE_CREATE_STAGED_INSERT = "createdStagedInsert";
static const std::string STAGE_REMOVE_DOC = "removeDoc";
static const std::string STAGE_COMMIT_DOC = "commitDoc";
static const std::string STAGE_BEFORE_RETRY = "beforeRetry";
static const std::string STAGE_REMOVE_STAGED_INSERT = "removeStagedInsert";

static const std::string STAGE_ATR_COMMIT = "atrCommit";
static const std::string STAGE_ATR_COMMIT_AMBIGUITY_RESOLUTION = "atrCommitAmbiguityResolution";
static const std::string STAGE_ATR_ABORT = "atrAbort";
static const std::string STAGE_ATR_ROLLBACK_COMPLETE = "atrRollbackComplete";
static const std::string STAGE_ATR_PENDING = "atrPending";
static const std::string STAGE_ATR_COMPLETE = "atrComplete";

static const std::string STAGE_QUERY = "query";
static const std::string STAGE_QUERY_BEGIN_WORK = "queryBeginWork";
static const std::string STAGE_QUERY_COMMIT = "queryCommit";
static const std::string STAGE_QUERY_ROLLBACK = "queryRollback";
static const std::string STAGE_QUERY_KV_GET = "queryKvGet";
static const std::string STAGE_QUERY_KV_REPLACE = "queryKvReplace";
static const std::string STAGE_QUERY_KV_REMOVE = "queryKvRemove";
static const std::string STAGE_QUERY_KV_INSERT = "queryKvInsert";

/**
 * Hooks purely for testing purposes.  If you're an end-user looking at these for any reason, then please contact us first
 * about your use-case: we are always open to adding good ideas into the transactions library.
 */
struct attempt_context_testing_hooks {
    error_func1 before_atr_commit = noop_1;
    error_func1 before_atr_commit_ambiguity_resolution = noop_1;
    error_func1 after_atr_commit = noop_1;
    error_func2 before_doc_committed = noop_2;
    error_func2 before_removing_doc_during_staged_insert = noop_2;
    error_func2 before_rollback_delete_inserted = noop_2;
    error_func2 after_doc_committed_before_saving_cas = noop_2;
    error_func2 after_doc_committed = noop_2;
    error_func2 before_staged_insert = noop_2;
    error_func2 before_staged_remove = noop_2;
    error_func2 before_staged_replace = noop_2;
    error_func2 before_doc_removed = noop_2;
    error_func2 before_doc_rolled_back = noop_2;
    error_func2 after_doc_removed_pre_retry = noop_2;
    error_func2 after_doc_removed_post_retry = noop_2;
    error_func2 after_get_complete = noop_2;
    error_func2 after_staged_replace_complete_before_cas_saved = noop_2;
    error_func2 after_staged_replace_complete = noop_2;
    error_func2 after_staged_remove_complete = noop_2;
    error_func2 after_staged_insert_complete = noop_2;
    error_func2 after_rollback_replace_or_remove = noop_2;
    error_func2 after_rollback_delete_inserted = noop_2;
    error_func2 before_check_atr_entry_for_blocking_doc = noop_2;
    error_func2 before_doc_get = noop_2;
    error_func2 before_get_doc_in_exists_during_staged_insert = noop_2;
    error_func2 before_query = noop_2;
    error_func2 after_query = noop_2;
    error_func2 before_remove_staged_insert = noop_2;
    error_func2 after_remove_staged_insert = noop_2;

    error_func1 after_docs_committed = noop_1;
    error_func1 after_docs_removed = noop_1;
    error_func1 after_atr_pending = noop_1;
    error_func1 before_atr_pending = noop_1;
    error_func1 before_atr_complete = noop_1;
    error_func1 before_atr_rolled_back = noop_1;
    error_func1 after_atr_complete = noop_1;
    error_func1 before_get_atr_for_abort = noop_1;
    error_func1 before_atr_aborted = noop_1;
    error_func1 after_atr_aborted = noop_1;
    error_func1 after_atr_rolled_back = noop_1;

    std::function<std::optional<const std::string>(attempt_context*)> random_atr_id_for_vbucket = noop_3;

    std::function<bool(attempt_context*, const std::string&, std::optional<const std::string>)> has_expired_client_side = noop_4;

    // needed for unique_ptr<attempt_context_testing_hooks> in transaction_config, with a forward declaration.
    ~attempt_context_testing_hooks()
    {
    }
};
} // namespace couchbase::core::transactions
