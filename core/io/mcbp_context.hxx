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

#include "core/protocol/hello_feature.hxx"
#include "core/topology/configuration.hxx"

#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace couchbase::core
{

inline constexpr auto feature_mask_width = std::numeric_limits<std::uint64_t>::digits;

// The bit for a feature in a 64-bit feature mask, or zero for a feature whose ordinal does not fit:
// such a feature is never reported as supported, and shifting by the width is undefined.
[[nodiscard]] constexpr auto
feature_mask_bit(protocol::hello_feature feature) noexcept -> std::uint64_t
{
  const auto bit = static_cast<std::uint16_t>(feature);
  return bit < feature_mask_width ? (std::uint64_t{ 1 } << bit) : 0;
}

[[nodiscard]] inline auto
compute_features_mask(const std::vector<protocol::hello_feature>& features) noexcept
  -> std::uint64_t
{
  std::uint64_t mask = 0;
  for (const auto f : features) {
    mask |= feature_mask_bit(f);
  }
  return mask;
}

struct mcbp_context {
  const std::optional<topology::configuration>& config;
  std::uint64_t supported_features_mask{ 0 };

  mcbp_context(const std::optional<topology::configuration>& cfg, std::uint64_t mask) noexcept
    : config{ cfg }
    , supported_features_mask{ mask }
  {
  }

  [[nodiscard]] auto supports_feature(protocol::hello_feature feature) const noexcept -> bool
  {
    return (supported_features_mask & feature_mask_bit(feature)) != 0;
  }
};

} // namespace couchbase::core
