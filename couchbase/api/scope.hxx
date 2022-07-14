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

#include <memory>

namespace couchbase
{
class cluster;
} // namespace couchbase

namespace couchbase::api
{
class scope
{
  public:
    /**
     * Constant for the name of the default scope in the bucket.
     */
    static constexpr auto default_name{ "_default" };

    /**
     * Returns name of the bucket where the scope is defined.
     *
     * @return name of the bucket
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& bucket_name() const
    {
        return bucket_name_;
    }

    /**
     * Returns name of the scope.
     *
     * @return name of the scope
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    [[nodiscard]] api::collection collection(std::string_view collection_name) const
    {
        return { core_, bucket_name_, name_, collection_name };
    }

  private:
    friend class bucket;

    scope(std::shared_ptr<couchbase::cluster> core, std::string_view bucket_name, std::string_view name)
      : core_(std::move(core))
      , bucket_name_(bucket_name)
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::cluster> core_;
    std::string bucket_name_;
    std::string name_;
};
} // namespace couchbase::api
