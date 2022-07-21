/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

namespace couchbase::core
{
enum class bucket_capability {
    couchapi,
    xattr,
    dcp,
    cbhello,
    touch,
    cccp,
    xdcr_checkpointing,
    nodes_ext,
    collections,
    durable_write,
    tombstoned_user_xattrs,
};

enum class cluster_capability {
    n1ql_cost_based_optimizer,
    n1ql_index_advisor,
    n1ql_javascript_functions,
    n1ql_inline_functions,
    n1ql_enhanced_prepared_statements,
};
} // namespace couchbase::core
