/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/cluster.hxx"

#include <couchbase/bucket.hxx>

#include <memory>

namespace couchbase
{
class bucket_impl : public std::enable_shared_from_this<bucket_impl>
{
  public:
    bucket_impl(core::cluster core, std::string_view name)
      : core_{ std::move(core) }
      , name_{ name }
    {
    }

    [[nodiscard]] auto name() const -> const std::string&
    {
        return name_;
    }

    [[nodiscard]] auto core() const -> const core::cluster&
    {
        return core_;
    }

  private:
    core::cluster core_;
    std::string name_;
};

bucket::bucket(core::cluster core, std::string_view name)
  : impl_(std::make_shared<bucket_impl>(std::move(core), name))
{
}

auto
bucket::default_scope() const -> couchbase::scope
{
    return { impl_->core(), impl_->name(), scope::default_name };
}

auto
bucket::default_collection() const -> couchbase::collection
{
    return { impl_->core(), impl_->name(), scope::default_name, collection::default_name };
}

auto
bucket::scope(std::string_view scope_name) const -> couchbase::scope
{
    return { impl_->core(), impl_->name(), scope_name };
}

auto
bucket::collections() const -> collection_manager
{
    return { impl_->core(), impl_->name() };
}
} // namespace couchbase
