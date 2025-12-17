/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Present Couchbase, Inc.
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

#include "cluster_label_listener.hxx"

#include <mutex>
#include <shared_mutex>

namespace couchbase::core
{
class cluster_label_listener_impl
{
public:
  cluster_label_listener_impl() = default;

  void update_config(topology::configuration config)
  {
    const std::scoped_lock lock(mutex_);
    if (config.cluster_name.has_value() && (cluster_name_ != config.cluster_name)) {
      cluster_name_ = std::move(config.cluster_name);
    }
    if (config.cluster_uuid.has_value() && (cluster_uuid_ != config.cluster_uuid)) {
      cluster_uuid_ = std::move(config.cluster_uuid);
    }
  }

  auto cluster_labels() -> cluster_label_listener::labels
  {
    const std::shared_lock lock(mutex_);
    return cluster_label_listener::labels{
      cluster_name_,
      cluster_uuid_,
    };
  }

private:
  std::shared_mutex mutex_{};
  std::optional<std::string> cluster_name_{};
  std::optional<std::string> cluster_uuid_{};
};

cluster_label_listener::cluster_label_listener()
  : impl_{ std::make_shared<cluster_label_listener_impl>() }
{
}

void
cluster_label_listener::update_config(topology::configuration config)
{
  impl_->update_config(std::move(config));
}

auto
cluster_label_listener::cluster_labels() const -> labels
{
  return impl_->cluster_labels();
}
} // namespace couchbase::core
