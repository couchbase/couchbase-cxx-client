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

#include <couchbase/cluster_options.hxx>
#include <couchbase/configuration_profiles_registry.hxx>
#include <couchbase/wan_development_configuration_profile.hxx>

#include <map>
#include <mutex>

namespace couchbase
{

struct registry {
    static inline std::map<std::string, std::shared_ptr<configuration_profile>, std::less<>> store{
        { "wan_development", std::make_shared<wan_development_configuration_profile>() }
    };
    static inline std::mutex store_mutex{};
};

void
configuration_profiles_registry::register_profile(const std::string& name, std::shared_ptr<configuration_profile> profile)
{
    if (name.empty()) {
        return;
    }
    std::scoped_lock lock(registry::store_mutex);
    registry::store[name] = std::move(profile);
}

void
configuration_profiles_registry::apply_profile(const std::string& name, couchbase::cluster_options& options)
{
    std::shared_ptr<configuration_profile> profile;
    if (name.empty()) {
        return;
    } else {
        std::scoped_lock lock(registry::store_mutex);
        if (auto it = registry::store.find(name); it == registry::store.end()) {
            return;
        } else if (it->second != nullptr) {
            profile = it->second;
        }
    }

    profile->apply(options);
}

auto
configuration_profiles_registry::available_profiles() -> std::vector<std::string>
{
    std::vector<std::string> profile_names;
    std::scoped_lock lock(registry::store_mutex);
    for (const auto& [name, _] : registry::store) {
        profile_names.push_back(name);
    }
    return profile_names;
}
} // namespace couchbase
