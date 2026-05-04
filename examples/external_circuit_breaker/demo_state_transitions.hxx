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

#include "circuit_breaker.hxx"

#include <couchbase/node_id.hxx>

namespace example::demo
{

/**
 * Drive a real node_id through the full state machine using synthetic
 * failure/success records.  We do not depend on the cluster actually
 * being sick — the breaker is a passive observer of whatever the caller
 * reports.
 */
auto
demo_state_transitions(example::cb::circuit_breaker& breaker, const couchbase::node_id& victim)
  -> void;

} // namespace example::demo
