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

#include <couchbase/api/collection.hxx>
#include <couchbase/api/scope.hxx>

#include <memory>

namespace couchbase
{
class cluster;
} // namespace couchbase

namespace couchbase::api
{
class cluster;

class bucket
{
  public:
    [[nodiscard]] api::scope default_scope() const
    {
        return { core_, name_, scope::default_name };
    }

    [[nodiscard]] api::collection default_collection() const
    {
        return { core_, name_, scope::default_name, collection::default_name };
    }

    [[nodiscard]] api::scope scope(std::string_view scope_name) const
    {
        return { core_, name_, scope_name };
    }

  private:
    friend class cluster;

    bucket(std::shared_ptr<couchbase::cluster> core, std::string_view name)
      : core_(std::move(core))
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::cluster> core_;
    std::string name_;
};
} // namespace couchbase::api
