/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025 Couchbase, Inc.
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

#include "core/protocol/client_opcode.hxx"

#include <asio/io_context.hpp>
#include <tao/json/value.hpp>

#include <chrono>
#include <memory>

namespace couchbase::core
{
struct orphan_reporter_options {
  std::chrono::milliseconds emit_interval{ std::chrono::seconds{ 10 } };
  std::size_t sample_size{ 64 };
};

struct orphan_attributes {
  std::string connection_id;
  std::string operation_id;
  std::string last_remote_socket;
  std::string last_local_socket;
  std::chrono::microseconds total_duration;
  std::chrono::microseconds last_server_duration{ 0 };
  std::chrono::microseconds total_server_duration{ 0 };
  std::string operation_name;

  auto operator<(const orphan_attributes& other) const -> bool;
  auto to_json() const -> tao::json::value;
};

class orphan_reporter_impl;

class orphan_reporter
{
public:
  orphan_reporter(asio::io_context& ctx, const orphan_reporter_options& options);

  void add_orphan(orphan_attributes&& orphan);
  void start();
  void stop();
  auto flush_and_create_output() -> std::optional<std::string>;

private:
  std::shared_ptr<orphan_reporter_impl> impl_;
};
} // namespace couchbase::core
