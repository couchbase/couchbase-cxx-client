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

#include <couchbase/configuration_profile.hxx>

#include <memory>
#include <string>

namespace couchbase
{
class cluster_options;

class configuration_profiles_registry
{
  public:
    static void register_profile(const std::string& name, std::shared_ptr<configuration_profile> profile);
    static void apply_profile(const std::string& name, couchbase::cluster_options& options);
};
} // namespace couchbase
