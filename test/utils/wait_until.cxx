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

#include "wait_until.hxx"

namespace test::utils
{
bool
wait_until_bucket_healthy(couchbase::cluster& cluster, const std::string& bucket_name)
{
    return wait_until([&cluster, bucket_name]() {
        couchbase::operations::management::bucket_get_request req{ bucket_name };
        auto resp = test::utils::execute(cluster, req);
        if (resp.ctx.ec) {
            return false;
        }
        if (resp.bucket.nodes.empty()) {
            return false;
        }
        for (const auto& node : resp.bucket.nodes) {
            if (node.status != "healthy") {
                return false;
            }
        }
        return true;
    });
}
} // namespace test::utils