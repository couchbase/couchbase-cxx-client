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

#include <couchbase/operations/management/error_utils.hxx>

namespace couchbase::operations::management
{

std::error_code
extract_common_error_code(std::uint32_t status_code, const std::string& response_body)
{
    if (status_code == 429) {
        if (response_body.find("Limit(s) exceeded") != std::string::npos) {
            return error::common_errc::rate_limited;
        }
        if (response_body.find("Maximum number of collections has been reached for scope") != std::string::npos) {
            return error::common_errc::quota_limited;
        }
    }
    return error::common_errc::internal_server_failure;
}

} // namespace couchbase::operations::management
