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

#include "../observability/span_owner.hxx"
#include "run.top_level.pb.h"
#include "sdk.bucket.collection_manager.pb.h"

#include <couchbase/bucket.hxx>

namespace fit_cxx::commands::collection_management
{
struct command_args {
  couchbase::bucket bucket;
  observability::span_owner* spans;
  bool return_result{ true };
};

protocol::run::Result
execute_command(const protocol::sdk::bucket::collection_manager::Command& cmd,
                const command_args& args);
} // namespace fit_cxx::commands::collection_management
