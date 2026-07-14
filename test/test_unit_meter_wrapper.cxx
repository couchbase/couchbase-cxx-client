/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "test_helper.hxx"

#include "core/cluster_label_listener.hxx"
#include "core/metrics/meter_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/metrics/meter.hxx>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

namespace
{
class counting_value_recorder : public couchbase::metrics::value_recorder
{
public:
  std::vector<std::int64_t> values{};

  void record_value(std::int64_t value) override
  {
    values.push_back(value);
  }
};

// A meter that returns a stable recorder per distinct tag set and counts how many times the
// wrapper asks it to resolve a recorder. Memoization must make that count drop to one per
// distinct attribute set regardless of how many values are recorded.
class counting_meter : public couchbase::metrics::meter
{
public:
  int get_value_recorder_calls{ 0 };
  std::map<std::map<std::string, std::string>, std::shared_ptr<counting_value_recorder>>
    recorders{};

  auto get_value_recorder(const std::string& /* name */,
                          const std::map<std::string, std::string>& tags)
    -> std::shared_ptr<couchbase::metrics::value_recorder> override
  {
    ++get_value_recorder_calls;
    auto& recorder = recorders[tags];
    if (!recorder) {
      recorder = std::make_shared<counting_value_recorder>();
    }
    return recorder;
  }
};

auto
kv_attributes(std::string operation) -> couchbase::core::metrics::metric_attributes
{
  couchbase::core::metrics::metric_attributes attrs{};
  attrs.service = "kv";
  attrs.operation = std::move(operation);
  attrs.bucket_name = "default";
  attrs.scope_name = "_default";
  attrs.collection_name = "_default";
  return attrs;
}

// A non-Couchbase error category whose codes reuse the same numeric range as the SDK's network
// errors (>= 1000), to prove classification keys off the category, not the raw value.
class foreign_category : public std::error_category
{
public:
  [[nodiscard]] auto name() const noexcept -> const char* override
  {
    return "foreign";
  }
  [[nodiscard]] auto message(int /* value */) const -> std::string override
  {
    return "foreign error";
  }
};
const foreign_category g_foreign_category{};
} // namespace

TEST_CASE("unit: meter_wrapper resolves a recorder once per attribute set", "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  couchbase::core::metrics::meter_wrapper wrapper{ meter, nullptr };

  const auto attrs = kv_attributes("get");
  for (int i = 0; i < 5; ++i) {
    wrapper.value_recorder_for(attrs)->record_value(i);
  }

  REQUIRE(meter->get_value_recorder_calls == 1);
  REQUIRE(meter->recorders.size() == 1);
  REQUIRE(meter->recorders.begin()->second->values == std::vector<std::int64_t>{ 0, 1, 2, 3, 4 });
}

TEST_CASE("unit: meter_wrapper returns the same recorder instance for equal attributes", "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  couchbase::core::metrics::meter_wrapper wrapper{ meter, nullptr };

  const auto attrs = kv_attributes("get");
  REQUIRE(wrapper.value_recorder_for(attrs).get() == wrapper.value_recorder_for(attrs).get());
}

TEST_CASE("unit: meter_wrapper resolves distinct recorders for distinct attributes", "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  couchbase::core::metrics::meter_wrapper wrapper{ meter, nullptr };

  wrapper.value_recorder_for(kv_attributes("get"))->record_value(1);
  wrapper.value_recorder_for(kv_attributes("upsert"))->record_value(2);
  wrapper.value_recorder_for(kv_attributes("get"))->record_value(3);

  REQUIRE(meter->get_value_recorder_calls == 2);
  REQUIRE(meter->recorders.size() == 2);
}

TEST_CASE("unit: meter_wrapper keys the cache by standardized error type, not raw error code",
          "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  couchbase::core::metrics::meter_wrapper wrapper{ meter, nullptr };

  // Two distinct raw error codes that map to the same standardized error type (both fall outside
  // the Couchbase categories, so standardized_error_type() yields "_OTHER" for each). The key
  // therefore matches, so the wrapper must resolve one recorder, not one per raw code.
  using couchbase::core::metrics::standardized_error_type;
  auto attrs_timed_out = kv_attributes("get");
  attrs_timed_out.error_type = standardized_error_type(std::make_error_code(std::errc::timed_out));
  auto attrs_conn_refused = kv_attributes("get");
  attrs_conn_refused.error_type =
    standardized_error_type(std::make_error_code(std::errc::connection_refused));

  REQUIRE(wrapper.value_recorder_for(attrs_timed_out).get() ==
          wrapper.value_recorder_for(attrs_conn_refused).get());
  REQUIRE(meter->get_value_recorder_calls == 1);
  REQUIRE(meter->recorders.size() == 1);
}

TEST_CASE("unit: meter_wrapper record_value routes every value through one cached recorder",
          "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  // A real (unconfigured) listener yields empty cluster labels, which is all this path needs.
  auto listener = std::make_shared<couchbase::core::cluster_label_listener>();
  couchbase::core::metrics::meter_wrapper wrapper{ meter, listener };

  const auto attrs = kv_attributes("get");
  for (int i = 0; i < 4; ++i) {
    wrapper.record_value(attrs, std::chrono::steady_clock::now());
  }

  // The recorder is resolved once and reused; every recorded value lands on that one recorder.
  REQUIRE(meter->get_value_recorder_calls == 1);
  REQUIRE(meter->recorders.size() == 1);
  REQUIRE(meter->recorders.begin()->second->values.size() == 4);
}

TEST_CASE("unit: standardized_error_type classifies by category, not raw value", "[unit]")
{
  using couchbase::core::metrics::standardized_error_type;

  // No error yields no error_type tag at all.
  REQUIRE(standardized_error_type({}).empty());

  // The SDK's network errors (values >= 1000, network category) are bucketed generically.
  REQUIRE(standardized_error_type(couchbase::errc::network::resolve_failure) == "CouchbaseError");

  // A foreign error whose value is also >= 1000 must not be misclassified as a Couchbase error just
  // because of the numeric range; it belongs to no Couchbase category.
  REQUIRE(standardized_error_type(std::error_code(1001, g_foreign_category)) == "_OTHER");
}

TEST_CASE("unit: meter_wrapper record_value tolerates a null cluster-label listener", "[unit]")
{
  auto meter = std::make_shared<counting_meter>();
  // The listener is optional; record_value() must not dereference it when absent.
  couchbase::core::metrics::meter_wrapper wrapper{ meter, nullptr };

  wrapper.record_value(kv_attributes("get"), std::chrono::steady_clock::now());

  REQUIRE(meter->get_value_recorder_calls == 1);
  REQUIRE(meter->recorders.size() == 1);
  REQUIRE(meter->recorders.begin()->second->values.size() == 1);
}
