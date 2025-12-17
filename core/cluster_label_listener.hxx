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

#pragma once

#include "config_listener.hxx"

#include <memory>
#include <optional>
#include <string>

namespace couchbase::core
{
class cluster_label_listener_impl;

class cluster_label_listener : public config_listener
{
public:
  cluster_label_listener();
  void update_config(topology::configuration config) override;

  struct labels {
    std::optional<std::string> cluster_name;
    std::optional<std::string> cluster_uuid;
  };

  [[nodiscard]] auto cluster_labels() const -> labels;

private:
  std::shared_ptr<cluster_label_listener_impl> impl_;
};
} // namespace couchbase::core
