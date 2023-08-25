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

#pragma once

#include <couchbase/management/scope_spec.hxx>

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace couchbase
{
struct get_all_scopes_options : public common_options<get_all_scopes_options> {
  public:
    struct built : public common_options<get_all_scopes_options>::built {
    };

    [[nodiscard]] auto build() const -> built
    {
        return { build_common_options() };
    }

  private:
};

using get_all_scopes_handler =
  std::function<void(couchbase::manager_error_context, std::vector<couchbase::management::bucket::scope_spec>)>;
} // namespace couchbase
