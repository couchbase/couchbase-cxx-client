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

#include "internal/get_and_open_buckets.hxx"

#include "core/cluster.hxx"
#include "core/operations/management/bucket_get_all.hxx"

#include <condition_variable>

namespace couchbase::core::transactions
{
std::list<std::string>
get_and_open_buckets(std::shared_ptr<core::cluster> c)
{
    core::operations::management::bucket_get_all_request req{};
    // don't wrap this one, as the kv timeout isn't appropriate here.
    auto mtx = std::make_shared<std::mutex>();
    auto cv = std::make_shared<std::condition_variable>();
    std::size_t count = 1; // non-zero so we know not to stop waiting immediately
    std::list<std::string> bucket_names;
    c->execute(std::move(req), [cv, &bucket_names, &c, mtx, &count](core::operations::management::bucket_get_all_response resp) {
        {
            std::unique_lock lock(*mtx);
            // now set count to correct # of buckets to try to open
            count = resp.buckets.size();
        }
        for (const auto& b : resp.buckets) {
            // open the bucket
            c->open_bucket(b.name, [cv, name = b.name, &bucket_names, mtx, &count](std::error_code ec) {
                std::unique_lock lock(*mtx);
                if (!ec) {
                    // push bucket name into list only if we successfully opened it
                    bucket_names.push_back(name);
                }
                if (--count == 0) {
                    cv->notify_all();
                }
            });
        }
    });
    std::unique_lock<std::mutex> lock(*mtx);
    cv->wait(lock);
    return bucket_names;
}
} // namespace couchbase::core::transactions
