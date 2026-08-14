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

// KV timeout resolution over couchbase2 (CXXCBC-904): a durable mutation falls back to the durable
// timeout (RFC 77 KvDurableTimeout), a non-durable operation to the standard KV timeout, and an
// explicit request timeout wins over both.
//
// Two layers, because they fail independently. The first cases pin the rule, which is pure. The
// last two pin the wiring -- that the value a caller configured is the one a real operation is
// dispatched with -- by reading the gRPC deadline arriving at a localhost KvService. Nothing
// between the cluster options and the wire reports which budget was chosen, so the deadline the
// server observes is the only place that choice becomes visible.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/test_runner.hxx"

#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/document_id.hxx"
#include "core/origin.hxx"
#include "core/protostellar/component.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/durability_level.hxx>

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace ps = ::couchbase::core::protostellar;
namespace v1 = ::couchbase::kv::v1;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::document_id;
using ::couchbase::core::origin;
using ::couchbase::core::utils::parse_connection_string;
using namespace std::chrono_literals;

auto
sample_timeouts() -> ps::component_timeouts
{
  ps::component_timeouts t{};
  t.key_value = 2'500ms;
  t.key_value_durable = 10'000ms;
  return t;
}

void
non_durable_uses_the_standard_kv_timeout()
{
  const auto t = sample_timeouts();
  assert_true(ps::resolve_kv_timeout(std::nullopt, couchbase::durability_level::none, t) == 2'500ms,
              "no durability -> standard key_value timeout");
}

void
durable_uses_the_durable_timeout()
{
  const auto t = sample_timeouts();
  assert_true(ps::resolve_kv_timeout(std::nullopt, couchbase::durability_level::majority, t) ==
                10'000ms,
              "majority durability -> durable timeout");
  assert_true(ps::resolve_kv_timeout(
                std::nullopt, couchbase::durability_level::persist_to_majority, t) == 10'000ms,
              "persist_to_majority -> durable timeout");
  assert_true(ps::resolve_kv_timeout(std::nullopt,
                                     couchbase::durability_level::majority_and_persist_to_active,
                                     t) == 10'000ms,
              "majority_and_persist_to_active -> durable timeout");
}

void
explicit_request_timeout_always_wins()
{
  const auto t = sample_timeouts();
  assert_true(ps::resolve_kv_timeout(1'234ms, couchbase::durability_level::none, t) == 1'234ms,
              "request timeout wins for non-durable");
  assert_true(ps::resolve_kv_timeout(1'234ms, couchbase::durability_level::majority, t) == 1'234ms,
              "request timeout wins for durable");
}

// No floor is applied to an explicit timeout, at any durability level.
//
// The rows come from gocb's own table for this rule (kvopmanager_core_test.go), which pairs each
// durability level with a timeout below 1500ms; gocb expects each to be coerced up to that floor,
// on its couchbase2 path as well as its classic one. This transport expects the opposite, so the
// levels are enumerated rather than sampled: a floor introduced later would most likely be written
// as a single guard covering every level, and one level asserted would be enough to catch it, but
// a guard applied to only the persistence levels -- which is the shape of the disagreement between
// SDKs here -- would slip past a majority-only case.
void
an_explicit_timeout_is_never_raised_to_a_floor()
{
  const auto t = sample_timeouts();
  const std::array<couchbase::durability_level, 3> durable_levels{
    couchbase::durability_level::majority,
    couchbase::durability_level::majority_and_persist_to_active,
    couchbase::durability_level::persist_to_majority,
  };
  for (const auto level : durable_levels) {
    assert_true(ps::resolve_kv_timeout(1'000ms, level, t) == 1'000ms,
                "a sub-floor explicit timeout is dispatched as given");
    assert_true(ps::resolve_kv_timeout(100ms, level, t) == 100ms,
                "and so is one far below the floor");
  }
}

// The rule is keyed on the level a request asks for, and a request type that cannot carry one asks
// for none. That is what keeps a read out of the durable budget: get_request has no
// durability_level member at all, so there is no value it could supply that selects it.
void
a_request_that_cannot_be_durable_asks_for_no_durability()
{
  const ops::get_request read;
  assert_true(ps::requested_durability(read) == couchbase::durability_level::none,
              "a read asks for no durability");

  ops::upsert_request mutation;
  mutation.durability_level = couchbase::durability_level::majority;
  assert_true(ps::requested_durability(mutation) == couchbase::durability_level::majority,
              "a mutation asks for the level it was given");

  const ops::upsert_request plain_mutation;
  assert_true(ps::requested_durability(plain_mutation) == couchbase::durability_level::none,
              "a mutation that requested no durability asks for none");
}

// Records the deadline each call arrives with, which is the only observable that names which budget
// the client resolved. Answers immediately, so what the case measures is the deadline and not the
// clock.
class deadline_recording_kv_service final : public v1::KvService::Service
{
public:
  std::atomic<int> calls{ 0 };
  std::atomic<std::int64_t> observed_budget_ms{ -1 };

  auto Get(grpc::ServerContext* context,
           const v1::GetRequest* /* request */,
           v1::GetResponse* response) -> grpc::Status override
  {
    record(context);
    response->set_content_uncompressed("{}");
    response->set_cas(42);
    return grpc::Status::OK;
  }

  auto Upsert(grpc::ServerContext* context,
              const v1::UpsertRequest* /* request */,
              v1::UpsertResponse* response) -> grpc::Status override
  {
    record(context);
    response->set_cas(42);
    return grpc::Status::OK;
  }

private:
  void record(const grpc::ServerContext* context)
  {
    ++calls;
    observed_budget_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                               context->deadline() - std::chrono::system_clock::now())
                               .count());
  }
};

// Drives one operation against a localhost KvService and reports the budget its call carried. The
// two timeouts are set far apart and both away from their stock defaults, so the value that comes
// back names which one was used and whether it came from the caller's configuration at all.
template<class Request>
auto
observed_budget_for(Request request) -> std::int64_t
{
  int port = 0;
  deadline_recording_kv_service service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  auto server = builder.BuildAndStart();

  auto parsed = parse_connection_string("couchbase2://127.0.0.1:" + std::to_string(port));
  origin cluster_origin{ cluster_credentials{}, parsed }; // no credentials -> plaintext allowed
  cluster_origin.options().enable_tls = false;
  cluster_origin.options().key_value_timeout = 400ms;
  cluster_origin.options().key_value_durable_timeout = 3'000ms;

  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  std::thread io_thread([&io]() {
    io.run();
  });
  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  const auto open_ec = opened.get_future().get();

  std::promise<void> answered;
  auto completion = answered.get_future();
  cluster.execute(std::move(request), [&answered](auto /* response */) {
    answered.set_value();
  });
  const auto arrived = completion.wait_for(10s) == std::future_status::ready;

  // Tear down before asserting: a failed assertion throws, and unwinding past a joinable thread
  // calls std::terminate, which would replace the message with a bare "terminate called".
  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
  guard.reset();
  io_thread.join();
  server->Shutdown(std::chrono::system_clock::now());
  server->Wait();

  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds");
  assert_true(arrived, "the operation completes");
  assert_true(service.calls.load() >= 1, "the operation reached the service");
  return service.observed_budget_ms.load();
}

// A durable mutation issued with no timeout of its own is dispatched with the cluster's configured
// KvDurableTimeout.
//
// Each bound catches a distinct failure. Below the lower one the operation was bounded by the
// standard KV budget, which is what happens when the durable default is resolved only where the
// per-attempt deadline is stamped: the retry loop hands the component an already-resolved timeout
// on every attempt including the first, so the component's own fallback never selects. Above the
// upper one the durable budget came from the built-in default rather than the configured value,
// which is what happens when key_value_durable is never populated from the cluster options.
void
a_durable_mutation_without_a_timeout_gets_the_configured_durable_budget()
{
  ops::upsert_request mutation;
  mutation.id = document_id{ "b", "s", "c", "k" };
  mutation.value = cu::to_binary("{}");
  mutation.flags = 0x06U;
  mutation.durability_level = couchbase::durability_level::majority;

  const auto budget = observed_budget_for(std::move(mutation));

  assert_true(budget > 1'500,
              "a durable mutation is not bounded by the standard key_value timeout");
  assert_true(budget <= 3'000,
              "and its durable budget is the configured key_value_durable_timeout rather than the "
              "built-in default");
}

// The negative half: resolving a durable default must not widen an operation that did not ask for
// durability. A read cannot carry a level at all, so it keeps the standard budget.
void
a_read_is_never_given_the_durable_budget()
{
  ops::get_request read;
  read.id = document_id{ "b", "s", "c", "k" };

  const auto budget = observed_budget_for(std::move(read));

  assert_true(budget <= 400, "a read keeps the standard key_value timeout");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_component_timeouts",
    {
      { "non_durable_uses_the_standard_kv_timeout", non_durable_uses_the_standard_kv_timeout },
      { "durable_uses_the_durable_timeout", durable_uses_the_durable_timeout },
      { "explicit_request_timeout_always_wins", explicit_request_timeout_always_wins },
      { "an_explicit_timeout_is_never_raised_to_a_floor",
        an_explicit_timeout_is_never_raised_to_a_floor },
      { "a_request_that_cannot_be_durable_asks_for_no_durability",
        a_request_that_cannot_be_durable_asks_for_no_durability },
      { "a_durable_mutation_without_a_timeout_gets_the_configured_durable_budget",
        a_durable_mutation_without_a_timeout_gets_the_configured_durable_budget,
        timeout::slow },
      { "a_read_is_never_given_the_durable_budget",
        a_read_is_never_given_the_durable_budget,
        timeout::slow },
    },
  };
}

} // namespace couchbase::test
