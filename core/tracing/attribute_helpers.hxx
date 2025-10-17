/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2025 Couchbase, Inc.
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

#include <couchbase/durability_level.hxx>
#include <couchbase/tracing/request_span.hxx>

#include "constants.hxx"

namespace couchbase::core::tracing
{
void inline set_durability_level_attribute(
  const std::shared_ptr<couchbase::tracing::request_span>& span,
  const durability_level durability)
{
  switch (durability) {
    case durability_level::none:
      break;
    case durability_level::majority:
      span->add_tag(attributes::op::durability_level, "majority");
      break;
    case durability_level::majority_and_persist_to_active:
      span->add_tag(attributes::op::durability_level, "majority_and_persist_active");
      break;
    case durability_level::persist_to_majority:
      span->add_tag(attributes::op::durability_level, "persist_majority");
      break;
  }
}
} // namespace couchbase::core::tracing
