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

#include <couchbase/cas.hxx>
#include <couchbase/node_id.hxx>

#include <utility>

namespace couchbase
{

/**
 * Base class for operations of data service.
 *
 * @since 1.0.0
 * @committed
 */
class result
{
public:
  /**
   * @since 1.0.0
   * @internal
   */
  result() = default;

  /**
   * @param cas
   *
   * @since 1.0.0
   * @committed
   */
  explicit result(couchbase::cas cas)
    : cas_(cas)
  {
  }

  /**
   * @param cas
   * @param node_id identity of the node that served the request
   *
   * @since 1.3.2
   * @uncommitted
   */
  result(couchbase::cas cas, couchbase::node_id node_id)
    : cas_(cas)
    , node_id_(std::move(node_id))
  {
  }

  /**
   * @return
   *
   * @since 1.0.0
   * @committed
   */
  [[nodiscard]] auto cas() const -> couchbase::cas
  {
    return cas_;
  }

  /**
   * Returns the identity of the cluster node that served this request.
   *
   * The returned node_id is default-constructed (falsy) when the node
   * could not be determined.
   *
   * @return identity of the serving node
   *
   * @since 1.3.2
   * @uncommitted
   */
  [[nodiscard]] auto node_id() const -> const couchbase::node_id&
  {
    return node_id_;
  }

  /**
   * @since 1.3.2
   * @internal
   */
  void node_id(couchbase::node_id id)
  {
    node_id_ = std::move(id);
  }

private:
  couchbase::cas cas_{ 0U };
  couchbase::node_id node_id_{};
};

} // namespace couchbase
