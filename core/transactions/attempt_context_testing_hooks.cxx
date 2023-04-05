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

namespace couchbase::core::transactions
{
namespace
{
inline std::optional<error_class>
noop_1(attempt_context*)
{
    return {};
}

inline std::optional<error_class>
noop_2(attempt_context*, const std::string&)
{
    return {};
}

inline std::optional<const std::string>
noop_3(attempt_context*)
{
    return {};
}

inline bool
noop_4(attempt_context*, const std::string&, std::optional<const std::string>)
{
    return false;
}
} // namespace

attempt_context_testing_hooks::attempt_context_testing_hooks()
  : before_atr_commit{ noop_1 }
  , before_atr_commit_ambiguity_resolution{ noop_1 }
  , after_atr_commit{ noop_1 }
  , before_doc_committed{ noop_2 }
  , before_removing_doc_during_staged_insert{ noop_2 }
  , before_rollback_delete_inserted{ noop_2 }
  , after_doc_committed_before_saving_cas{ noop_2 }
  , after_doc_committed{ noop_2 }
  , before_staged_insert{ noop_2 }
  , before_staged_remove{ noop_2 }
  , before_staged_replace{ noop_2 }
  , before_doc_removed{ noop_2 }
  , before_doc_rolled_back{ noop_2 }
  , after_doc_removed_pre_retry{ noop_2 }
  , after_doc_removed_post_retry{ noop_2 }
  , after_get_complete{ noop_2 }
  , after_staged_replace_complete_before_cas_saved{ noop_2 }
  , after_staged_replace_complete{ noop_2 }
  , after_staged_remove_complete{ noop_2 }
  , after_staged_insert_complete{ noop_2 }
  , after_rollback_replace_or_remove{ noop_2 }
  , after_rollback_delete_inserted{ noop_2 }
  , before_check_atr_entry_for_blocking_doc{ noop_2 }
  , before_doc_get{ noop_2 }
  , before_get_doc_in_exists_during_staged_insert{ noop_2 }
  , before_query{ noop_2 }
  , after_query{ noop_2 }
  , before_remove_staged_insert{ noop_2 }
  , after_remove_staged_insert{ noop_2 }
  , after_docs_committed{ noop_1 }
  , after_docs_removed{ noop_1 }
  , after_atr_pending{ noop_1 }
  , before_atr_pending{ noop_1 }
  , before_atr_complete{ noop_1 }
  , before_atr_rolled_back{ noop_1 }
  , after_atr_complete{ noop_1 }
  , before_get_atr_for_abort{ noop_1 }
  , before_atr_aborted{ noop_1 }
  , after_atr_aborted{ noop_1 }
  , after_atr_rolled_back{ noop_1 }
  , random_atr_id_for_vbucket{ noop_3 }
  , has_expired_client_side{ noop_4 }
{
}
} // namespace couchbase::core::transactions
