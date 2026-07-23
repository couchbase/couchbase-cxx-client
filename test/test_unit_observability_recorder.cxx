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

#include "core/impl/observability_recorder.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/metrics/meter.hxx>

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <memory>
#include <string>

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
} // namespace

TEST_CASE("unit: observability_recorder::finish tolerates an expired meter", "[unit]")
{
  // The streaming path defers finish() to a user-controlled terminal (a stream handle's
  // destructor/cancel()), which can run after the owning cluster — and the meter it owns — is gone.
  // finish() must record nothing rather than dereferencing the expired meter weak_ptr.
  auto tracer = couchbase::core::tracing::tracer_wrapper::create(
    std::make_shared<couchbase::core::tracing::noop_tracer>(), nullptr);
  auto meter =
    couchbase::core::metrics::meter_wrapper::create(std::make_shared<noop_meter>(), nullptr);

  auto rec = couchbase::core::impl::observability_recorder::create(
    "query", /* parent_span */ nullptr, tracer, meter);
  REQUIRE(rec != nullptr);

  // Expire the meter's weak_ptr before finish() runs, as a cluster teardown would.
  meter.reset();
  rec->finish(couchbase::errc::common::request_canceled);

  SUCCEED("finish() did not dereference the expired meter weak_ptr");
}
