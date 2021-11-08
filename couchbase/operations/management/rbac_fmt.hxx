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

#include <couchbase/operations/management/rbac.hxx>

#include <spdlog/fmt/fmt.h>

template<>
struct fmt::formatter<couchbase::operations::management::rbac::auth_domain> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::operations::management::rbac::auth_domain domain, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (domain) {
            case couchbase::operations::management::rbac::auth_domain::unknown:
                name = "unknown";
                break;
            case couchbase::operations::management::rbac::auth_domain::local:
                name = "local";
                break;
            case couchbase::operations::management::rbac::auth_domain::external:
                name = "external";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
