/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/error_codes.hxx>

#include <string>

namespace couchbase::core::impl
{
struct analytics_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.analytics";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (static_cast<errc::analytics>(ev)) {
            case errc::analytics::compilation_failure:
                return "compilation_failure (301)";
            case errc::analytics::job_queue_full:
                return "job_queue_full (302)";
            case errc::analytics::dataset_not_found:
                return "dataset_not_found (303)";
            case errc::analytics::dataverse_not_found:
                return "dataverse_not_found (304)";
            case errc::analytics::dataset_exists:
                return "dataset_exists (305)";
            case errc::analytics::dataverse_exists:
                return "dataverse_exists (306)";
            case errc::analytics::link_not_found:
                return "link_not_found (307)";
            case errc::analytics::link_exists:
                return "link_exists (308)";
        }
        return "FIXME: unknown error code (recompile with newer library): couchbase.analytics." + std::to_string(ev);
    }
};

const inline static analytics_error_category category_instance;

const std::error_category&
analytics_category() noexcept
{
    return category_instance;
}

} // namespace couchbase::core::impl
