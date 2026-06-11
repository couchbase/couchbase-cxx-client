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

#include "run.top_level.pb.h"
#include "sdk.kv.binary.commands.pb.h"
#include "sdk.kv.commands.pb.h"

#include "../counters.hxx"
#include "../observability/span_owner.hxx"
#include "../service.hxx"
#include "../stream.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/tracing/request_span.hxx>

#include <memory>
#include <string>

namespace fit_cxx::commands::key_value
{
struct command_args {
  couchbase::collection collection;
  std::string key;
  observability::span_owner* spans;
  bool return_result{ true };
};

protocol::run::Result
execute_command(const protocol::sdk::kv::Insert& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Upsert& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Replace& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Remove& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Get& cmd, const command_args& args);

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::rangescan::Scan& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::lookup_in::LookupIn& cmd, const command_args& args);

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::lookup_in::LookupInAllReplicas& cmd,
                          const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::lookup_in::LookupInAnyReplica& cmd,
                const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::mutate_in::MutateIn& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAndLock& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Unlock& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAndTouch& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Exists& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Touch& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAnyReplica& cmd, const command_args& args);

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::GetAllReplicas& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Append& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Prepend& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Increment& cmd, const command_args& args);

protocol::run::Result
execute_command(const protocol::sdk::kv::Decrement& cmd, const command_args& args);
} // namespace fit_cxx::commands::key_value
