/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include <cstddef>
#include <memory>
#include <string>

namespace couchbase::test::bench
{
// An in-process MCBP server on an ephemeral loopback port, served by one io thread. It answers
// just enough for the SDK to bootstrap bucket "default" and run GET, UPSERT and REMOVE against an
// in-memory map. It handles these opcodes:
//
// - HELLO, accepting xerror, select_bucket, snappy, json, collections and mutation_seqno
// - SASL (any mechanism, any credentials)
// - GET_ERROR_MAP
// - SELECT_BUCKET
// - GET_CLUSTER_CONFIG
// - GET, SET, DELETE and NOOP
//
// SET stores the value and its datatype as sent, compressed or not, and GET returns both. DELETE
// removes the document; GET and DELETE answer not_found for a missing key, which the KV cases'
// cleanup relies on. Every other opcode is answered with unknown_command. The cluster config names
// one node at this server's own port, maps every vbucket to it and never changes its rev.
//
// Frames are encoded by hand and nothing is shared with core/io or core/protocol, so the server's
// own cost does not move with the SDK under measurement.
class fake_server
{
public:
  fake_server();
  ~fake_server();
  fake_server(const fake_server&) = delete;
  fake_server(fake_server&&) = delete;
  auto operator=(const fake_server&) -> fake_server& = delete;
  auto operator=(fake_server&&) -> fake_server& = delete;

  // couchbase://127.0.0.1:<port>=kv
  [[nodiscard]] auto connection_string() const -> std::string;
  [[nodiscard]] auto username() const -> std::string;
  [[nodiscard]] auto password() const -> std::string;

  // The stored documents whose datatype has the Snappy bit. Read on the server's io thread.
  [[nodiscard]] auto snappy_documents() const -> std::size_t;

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};
} // namespace couchbase::test::bench
