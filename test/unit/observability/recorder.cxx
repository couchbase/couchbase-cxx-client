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

#include "framework/test_registry.hxx"

#include "core/impl/observability_recorder.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
class noop_meter : public couchbase::metrics::meter
{
public:
  auto get_value_recorder(const std::string& /* name */,
                          const std::map<std::string, std::string>& /* tags */)
    -> std::shared_ptr<couchbase::metrics::value_recorder> override
  {
    return nullptr;
  }
};

class recording_value_recorder : public couchbase::metrics::value_recorder
{
public:
  explicit recording_value_recorder(std::vector<std::map<std::string, std::string>>& recorded,
                                    std::map<std::string, std::string> tags)
    : recorded_{ recorded }
    , tags_{ std::move(tags) }
  {
  }

  void record_value(std::int64_t /* value */) override
  {
    recorded_.push_back(tags_);
  }

private:
  std::vector<std::map<std::string, std::string>>& recorded_;
  std::map<std::string, std::string> tags_;
};

class recording_meter : public couchbase::metrics::meter
{
public:
  std::vector<std::map<std::string, std::string>> recorded{};

  auto get_value_recorder(const std::string& /* name */,
                          const std::map<std::string, std::string>& tags)
    -> std::shared_ptr<couchbase::metrics::value_recorder> override
  {
    return std::make_shared<recording_value_recorder>(recorded, tags);
  }
};

class recording_span : public couchbase::tracing::request_span
{
public:
  using couchbase::tracing::request_span::request_span;

  bool ended{ false };

  void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
  {
  }

  void add_tag(const std::string& /* name */, const std::string& /* value */) override
  {
  }

  void end() override
  {
    ended = true;
  }
};

class recording_tracer : public couchbase::tracing::request_tracer
{
public:
  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    return std::make_shared<recording_span>(std::move(name), std::move(parent));
  }
};

void
finish_tolerates_an_expired_meter([[maybe_unused]] context& ctx)
{
  // The streaming path defers finish() to a user-controlled terminal (a stream handle's
  // destructor/cancel()), which can run after the owning cluster — and the meter it owns — is gone.
  auto tracer = couchbase::core::tracing::tracer_wrapper::create(
    std::make_shared<couchbase::core::tracing::noop_tracer>(), nullptr);
  auto meter =
    couchbase::core::metrics::meter_wrapper::create(std::make_shared<noop_meter>(), nullptr);

  auto rec = couchbase::core::impl::observability_recorder::create(
    "query", /* parent_span */ nullptr, tracer, meter);
  assert_true(rec != nullptr, "a recorder is created for an operation with no parent span");

  // Expire the meter's weak_ptr before finish() runs, as a cluster teardown would.
  meter.reset();
  assert_no_throw(
    [&]() {
      rec->finish(couchbase::errc::common::request_canceled);
    },
    "finish() records nothing rather than dereferencing the expired meter");
}

void
tracing_suboperation_creates_child_span_without_recording_metrics([[maybe_unused]] context& ctx)
{
  auto raw_meter = std::make_shared<recording_meter>();
  auto tracer =
    couchbase::core::tracing::tracer_wrapper::create(std::make_shared<recording_tracer>(), nullptr);
  auto meter = couchbase::core::metrics::meter_wrapper::create(raw_meter, nullptr);

  auto parent = couchbase::core::impl::observability_recorder::create(
    "manager_query_get_all_indexes", nullptr, tracer, meter);
  auto child = parent->record_tracing_suboperation("query");

  assert_true(child->operation_span()->parent() == parent->operation_span(),
              "the sub-operation span is parented to the outer operation span");

  child->finish(0, {});
  parent->finish(0, {});

  assert_eq(raw_meter->recorded.size(), std::size_t{ 1 }, "only the outer operation is metered");
  assert_eq(raw_meter->recorded.front().at("db.operation.name"),
            "manager_query_get_all_indexes",
            "the metered operation is the outer management call");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(finish_tolerates_an_expired_meter) },
      { CASE(tracing_suboperation_creates_child_span_without_recording_metrics) },
    },
  };
}

} // namespace couchbase::test
