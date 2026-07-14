/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "core/config_listener.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/metrics/meter.hxx>

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>

#include "core/cluster_label_listener.hxx"

namespace couchbase::core::metrics
{
struct metric_attributes {
  std::string service;
  std::string operation;
  // The standardized error type, precomputed once from the operation's error code (see
  // standardized_error_type()). Stored rather than the raw error code so that both the encoded tag
  // set and the recorder-cache key (operator<) use it without recomputing a string per comparison.
  // Empty denotes "no error".
  std::string error_type{};
  std::optional<std::string> bucket_name{};
  std::optional<std::string> scope_name{};
  std::optional<std::string> collection_name{};

  struct {
    std::optional<std::string> cluster_name{};
    std::optional<std::string> cluster_uuid{};
  } internal{};

  [[nodiscard]] auto encode() const -> std::map<std::string, std::string>;
};

// Map an error code to the standardized error-type string used as the metric's error_type tag (see
// metric_attributes::encode()). The empty string denotes "no error". Exposed so the recorder cache
// can key on the same bounded set of error types that actually reach the tags.
[[nodiscard]] auto
standardized_error_type(std::error_code ec) -> std::string;

inline auto
operator<(const metric_attributes& lhs, const metric_attributes& rhs) -> bool
{
  // Order by the precomputed standardized error type, not the raw error code. encode() only ever
  // emits the standardized type in the tag set, so keying on it collapses the many distinct codes
  // that share a type onto one recorder and keeps the cache bounded by the tag cardinality (rather
  // than one entry per raw code, unbounded for high-cardinality system/asio codes). Because the
  // type is stored on the attributes, every comparison is a plain string compare -- no per-lookup
  // string construction or allocation, which is what keeps the cache lookup cheap on the hot path.
  return std::tie(lhs.service,
                  lhs.operation,
                  lhs.error_type,
                  lhs.bucket_name,
                  lhs.scope_name,
                  lhs.collection_name,
                  lhs.internal.cluster_name,
                  lhs.internal.cluster_uuid) < std::tie(rhs.service,
                                                        rhs.operation,
                                                        rhs.error_type,
                                                        rhs.bucket_name,
                                                        rhs.scope_name,
                                                        rhs.collection_name,
                                                        rhs.internal.cluster_name,
                                                        rhs.internal.cluster_uuid);
}

class meter_wrapper
{
public:
  explicit meter_wrapper(std::shared_ptr<couchbase::metrics::meter> meter,
                         std::shared_ptr<cluster_label_listener> label_listener);

  void start();
  void stop();

  void record_value(metric_attributes attrs, std::chrono::steady_clock::time_point start_time);

  // Resolve (and cache) the value recorder for a set of attributes. The underlying meter returns a
  // stable recorder for a given name/tag set, so resolving once per distinct attribute set keeps
  // the per-operation tag-map construction and recorder lookup off the hot path.
  [[nodiscard]] auto value_recorder_for(const metric_attributes& attrs)
    -> std::shared_ptr<couchbase::metrics::value_recorder>;

  [[nodiscard]] auto wrapped() -> std::shared_ptr<couchbase::metrics::meter>;

  [[nodiscard]] static auto create(std::shared_ptr<couchbase::metrics::meter> meter,
                                   std::shared_ptr<cluster_label_listener> label_listener)
    -> std::shared_ptr<meter_wrapper>;

private:
  std::shared_ptr<couchbase::metrics::meter> meter_;
  std::shared_ptr<cluster_label_listener> cluster_label_listener_;
  std::mutex recorder_cache_mutex_{};
  // Keyed by the full attribute set (not just service/operation): the meter is pluggable, and a
  // custom meter may hand out a distinct recorder per tag combination, so the cache must ask it
  // once for each distinct attribute set rather than assume any particular dedup granularity. The
  // key mirrors what encode() emits -- in particular it orders by the standardized error type (see
  // operator<), not the raw error code -- so the key space is finite (services x operations x
  // scope/collection x standardized error type x cluster) and the cache stays bounded.
  std::map<metric_attributes, std::shared_ptr<couchbase::metrics::value_recorder>>
    recorder_cache_{};
};
} // namespace couchbase::core::metrics
