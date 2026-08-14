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

// Codegen smoke test for CXXCBC-886. It proves the Protostellar schema was vendored and the
// generated stubs compile and link: a protobuf message from kv.v1, the vendored google/rpc/status
// message, and a gRPC service type. It does not talk to any server (env-agnostic).

#include "framework/test_runner.hxx"

#include <string>

#include <couchbase/kv/v1/kv.grpc.pb.h>
#include <couchbase/kv/v1/kv.pb.h>
#include <google/rpc/status.pb.h>

namespace couchbase::test
{
namespace
{
void
kv_message_roundtrips()
{
  ::couchbase::kv::v1::GetRequest req;
  req.set_bucket_name("travel-sample");
  req.set_scope_name("inventory");
  req.set_collection_name("airline");
  req.set_key("airline_10");
  assert_eq(req.bucket_name(), std::string{ "travel-sample" }, "bucket_name round-trips");
  assert_eq(req.key(), std::string{ "airline_10" }, "key round-trips");

  ::couchbase::kv::v1::GetResponse resp;
  resp.set_cas(0x1234'5678'9abc'def0ULL);
  resp.set_content_uncompressed("{\"type\":\"airline\"}");
  assert_eq(resp.cas(), 0x1234'5678'9abc'def0ULL, "cas round-trips");
  assert_eq(resp.content_uncompressed(),
            std::string{ "{\"type\":\"airline\"}" },
            "content_uncompressed round-trips");
}

void
googleapis_status_message_is_available()
{
  // Proves google/rpc/status.proto (the sole googleapis import) was fetched and compiled.
  ::google::rpc::Status status;
  status.set_code(5); // NOT_FOUND
  status.set_message("document not found");
  assert_eq(status.code(), 5, "status code round-trips");
}

void
grpc_service_stub_is_generated()
{
  // Proves the gRPC plugin ran and its output linked: the generated service exposes its full
  // name, and the Stub type is a complete type.
  assert_eq(std::string{ ::couchbase::kv::v1::KvService::service_full_name() },
            std::string{ "couchbase.kv.v1.KvService" },
            "KvService full name");
  // Completeness of Stub is a compile-time property, so assert it at compile time: an incomplete
  // type fails the sizeof here regardless of whether this case is ever executed or filtered out.
  static_assert(sizeof(::couchbase::kv::v1::KvService::Stub) > 0, "Stub is a complete type");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_codegen_smoke",
    {
      // All three are in-memory message/stub checks with no I/O, so they get the tightest preset
      // rather than the 5 s network default.
      { "kv_message_roundtrips", kv_message_roundtrips, timeout::instant },
      { "googleapis_status_message_is_available",
        googleapis_status_message_is_available,
        timeout::instant },
      { "grpc_service_stub_is_generated", grpc_service_stub_is_generated, timeout::instant },
    },
  };
}

} // namespace couchbase::test
