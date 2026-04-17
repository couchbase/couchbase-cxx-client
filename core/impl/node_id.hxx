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

#include <couchbase/node_id.hxx>

#include <cstdint>
#include <string>

namespace couchbase
{

/**
 * Internal factory for constructing node_id instances from core-level data.
 *
 * This class is a friend of node_id and provides the only way to construct
 * a non-default node_id outside the couchbase namespace.
 */
class internal_node_id
{
public:
  static auto build(std::string node_uuid, std::string hostname, std::uint16_t port) -> node_id;
};

} // namespace couchbase
