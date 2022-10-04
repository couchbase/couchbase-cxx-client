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

#include <string>
#include <vector>

namespace couchbase::core::transactions
{
class atr_ids
{
  public:
    static const std::string& atr_id_for_vbucket(std::size_t vbucket_id);
    static std::size_t vbucket_for_key(const std::string& key);
    static const std::vector<std::string>& all();
};

} // namespace couchbase::core::transactions
