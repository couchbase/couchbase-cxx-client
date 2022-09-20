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
#include <mutex>
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
        opts.key_value_durable_timeout = std::chrono::milliseconds(20000); // 20 sec durable kv timeout.
        opts.connect_timeout = std::chrono::milliseconds(20000);           // 20 sec connect timeout.
        opts.view_timeout = std::chrono::milliseconds(120000);             // 2 minute view timeout
        opts.query_timeout = std::chrono::milliseconds(120000);            // 2 minute query timeout
        opts.analytics_timeout = std::chrono::milliseconds(120000);        // 2 minute analytics timeout
        opts.search_timeout = std::chrono::milliseconds(120000);           // 2 minute search timeout
        opts.management_timeout = std::chrono::milliseconds(120000);       // 2 minute management timeout
    }
};

// this class just registers the known profiles defined above, and allows access to them.
class config_profiles
{
  private:
    std::map<std::string, std::shared_ptr<couchbase::core::config_profile>> profiles_;
    std::mutex mut_;

  public:
    config_profiles() noexcept
    {
        // add all known profiles (above) to the map
        register_profile<development_profile>("wan_development");
    }

    void apply(const std::string& profile_name, couchbase::core::cluster_options& opts)
    {
        std::lock_guard<std::mutex> lock(mut_);
        auto it = profiles_.find(profile_name);
        if (it != profiles_.end()) {
            it->second->apply(opts);
        } else {
            throw std::invalid_argument(fmt::format("unknown profile '{}'", profile_name));
        }
    }

    template<typename T, typename... Args>
    void register_profile(const std::string& name, Args... args)
    {
        // This will just add it, doesn't look to see if it is overwriting an existing profile.
        // TODO: perhaps add a template Args param?
        // TODO: should we make this thread-safe?   Easy enough here, but we'd need to make the
        //   singleton thread-safe too.
        std::lock_guard<std::mutex> lock(mut_);
        profiles_.emplace(std::make_pair(name, std::make_shared<T>(args...)));
    }
};

config_profiles&
known_profiles();

} // namespace couchbase::core