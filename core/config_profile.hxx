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

#include "cluster_options.hxx"
#include <chrono>
#include <fmt/core.h>
#include <map>
#include <stdexcept>
#include <string>

namespace couchbase::core
{
class config_profile
{
  public:
    virtual ~config_profile() = default;
    virtual void apply(cluster_options&) = 0;
};

class development_profile : public config_profile
{
  public:
    void apply(couchbase::core::cluster_options& opts) override
    {
        opts.key_value_timeout = std::chrono::milliseconds(20000); // 20 sec kv timeout.
        opts.connect_timeout = std::chrono::milliseconds(5000);    // 5 sec connect timeout.
    }
};

// this class just registers the known profiles defined above, and allows access to them.
class config_profiles
{
  private:
    std::map<std::string, std::shared_ptr<couchbase::core::config_profile>> profiles_;

  public:
    config_profiles() noexcept
    {
        // add all known profiles (above) to the map
        profiles_.emplace(std::make_pair(std::string("wan_development"), std::make_shared<development_profile>()));
    }

    void apply(const std::string& profile_name, couchbase::core::cluster_options& opts)
    {
        auto it = profiles_.find(profile_name);
        if (it != profiles_.end()) {
            it->second->apply(opts);
        } else {
            throw std::invalid_argument(fmt::format("unknown profile '{}'", profile_name));
        }
    }

    template<typename T>
    void register_profile(const std::string& name)
    {
        // This will just add it, doesn't look to see if it is overwriting an existing profile.
        // TODO: perhaps add a template Args param?
        // TODO: should we make this thread-safe?   Easy enough here, but we'd need to make the
        //   singleton thread-safe too.
        profiles_.emplace(std::make_pair(name, std::make_shared<T>()));
    }
};

config_profiles&
known_profiles();

} // namespace couchbase::core