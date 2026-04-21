/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include <couchbase/common_options.hxx>
#include <couchbase/error.hxx>
#include <couchbase/node_id.hxx>

#include <functional>

namespace couchbase
{

/**
 * Options for @ref collection#node_id_for().
 *
 * @since 1.3.2
 * @uncommitted
 */
struct node_id_for_options : public common_options<node_id_for_options> {
  /**
   * Immutable value object representing consistent options.
   *
   * @since 1.3.2
   * @internal
   */
  struct built : public common_options<node_id_for_options>::built {
  };

  /**
   * Validates options and returns them as an immutable value.
   *
   * @return consistent options as an immutable value
   *
   * @since 1.3.2
   * @internal
   */
  [[nodiscard]] auto build() const -> built
  {
    return { build_common_options() };
  }
};

using node_id_for_handler = std::function<void(error, node_id)>;

} // namespace couchbase
