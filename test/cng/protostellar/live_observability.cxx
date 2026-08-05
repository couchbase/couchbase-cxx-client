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

// Observability over couchbase2 (CXXCBC-902). Operation spans and the operation-latency metric
// are emitted at the public layer (observability_recorder) BEFORE transport selection, so a
// couchbase2 operation gets them through the existing tracer/meter with no transport-specific code.
// This test proves that end to end via the public couchbase::cluster API, and it asserts the SHAPE
// of what was emitted rather than merely that something was: the exact span sequence, the absence
// of a dispatch_to_server span (RFC 77 leaves dispatch to the gRPC layer's own span), the operation
// and service attributes, and the metric those land on. A count of spans is satisfied by any span
// with any name carrying any attributes, so it cannot see a lost request_encoding child, a classic
// operation identifier leaking through, or latency filed under the wrong service.
//
// It is also the only case in the suite that drives the public API, which makes it the guard for
// everything below observability -- it is what caught setup_observability() going missing from
// open_protostellar(). cluster_only, and the CI `cng` job runs it on every pull request.

#include "framework/test_runner.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace couchbase::cng::test
{
namespace
{
// Records what observability actually emitted -- the name of every span, its parent, the tags put
// on it, and the name and tags of every metric recorded -- rather than only how many there were.
//
// A count is satisfied by any span with any name carrying any attributes, so it cannot see the
// failures that matter here: a KV operation losing its request_encoding child, a span carrying the
// classic operation identifier instead of the Protostellar one, or a latency metric filed under the
// wrong service. Those are the regressions this test exists to catch, and each of them keeps the
// counters happily non-zero.
struct emitted {
  mutable std::mutex mutex;
  std::vector<std::string> span_names;
  std::map<std::string, std::map<std::string, std::string>> span_tags;
  std::vector<std::string> metric_names;
  std::vector<std::map<std::string, std::string>> metric_tags;

  void note_span(const std::string& name)
  {
    const std::scoped_lock lock{ mutex };
    span_names.push_back(name);
  }

  void note_tag(const std::string& span, const std::string& key, const std::string& value)
  {
    const std::scoped_lock lock{ mutex };
    span_tags[span][key] = value;
  }

  void note_metric(const std::string& name, const std::map<std::string, std::string>& tags)
  {
    const std::scoped_lock lock{ mutex };
    metric_names.push_back(name);
    metric_tags.push_back(tags);
  }

  [[nodiscard]] auto has_span(const std::string& name) const -> bool
  {
    const std::scoped_lock lock{ mutex };
    return std::find(span_names.begin(), span_names.end(), name) != span_names.end();
  }

  [[nodiscard]] auto tag(const std::string& span, const std::string& key) const -> std::string
  {
    const std::scoped_lock lock{ mutex };
    const auto s = span_tags.find(span);
    if (s == span_tags.end()) {
      return {};
    }
    const auto t = s->second.find(key);
    return t == s->second.end() ? std::string{} : t->second;
  }

  [[nodiscard]] auto metric_tag(std::size_t index, const std::string& key) const -> std::string
  {
    const std::scoped_lock lock{ mutex };
    if (index >= metric_tags.size()) {
      return {};
    }
    const auto t = metric_tags[index].find(key);
    return t == metric_tags[index].end() ? std::string{} : t->second;
  }

  [[nodiscard]] auto describe() const -> std::string
  {
    const std::scoped_lock lock{ mutex };
    std::string out{ "spans:" };
    for (const auto& name : span_names) {
      out += " " + name;
    }
    for (const auto& [s, tags] : span_tags) {
      out += " [" + s + ":";
      for (const auto& [k, v] : tags) {
        out += " " + k + "=" + v;
      }
      out += "]";
    }
    out += " | metrics:";
    for (std::size_t i = 0; i < metric_names.size(); ++i) {
      out += " " + metric_names[i] + "{";
      for (const auto& [k, v] : metric_tags[i]) {
        out += k + "=" + v + ",";
      }
      out += "}";
    }
    return out;
  }
};

class recording_span : public couchbase::tracing::request_span
{
public:
  recording_span(std::string name,
                 std::shared_ptr<couchbase::tracing::request_span> parent,
                 std::shared_ptr<emitted> sink)
    : request_span(std::move(name), std::move(parent))
    , sink_{ std::move(sink) }
  {
    sink_->note_span(this->name());
  }
  void add_tag(const std::string& tag_name, std::uint64_t value) override
  {
    sink_->note_tag(name(), tag_name, std::to_string(value));
  }
  void add_tag(const std::string& tag_name, const std::string& value) override
  {
    sink_->note_tag(name(), tag_name, value);
  }
  void end() override
  {
  }

private:
  std::shared_ptr<emitted> sink_;
};

class recording_tracer : public couchbase::tracing::request_tracer
{
public:
  explicit recording_tracer(std::shared_ptr<emitted> sink)
    : sink_{ std::move(sink) }
  {
  }
  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent = {})
    -> std::shared_ptr<couchbase::tracing::request_span> override
  {
    return std::make_shared<recording_span>(std::move(name), std::move(parent), sink_);
  }

private:
  std::shared_ptr<emitted> sink_;
};

class recording_recorder : public couchbase::metrics::value_recorder
{
public:
  recording_recorder(std::string name,
                     std::map<std::string, std::string> tags,
                     std::shared_ptr<emitted> sink)
    : name_{ std::move(name) }
    , tags_{ std::move(tags) }
    , sink_{ std::move(sink) }
  {
  }
  void record_value(std::int64_t /* value */) override
  {
    sink_->note_metric(name_, tags_);
  }

private:
  std::string name_;
  std::map<std::string, std::string> tags_;
  std::shared_ptr<emitted> sink_;
};

class recording_meter : public couchbase::metrics::meter
{
public:
  explicit recording_meter(std::shared_ptr<emitted> sink)
    : sink_{ std::move(sink) }
  {
  }
  auto get_value_recorder(const std::string& name, const std::map<std::string, std::string>& tags)
    -> std::shared_ptr<couchbase::metrics::value_recorder> override
  {
    return std::make_shared<recording_recorder>(name, tags, sink_);
  }

private:
  std::shared_ptr<emitted> sink_;
};

// safe_getenv rather than std::getenv: MSVC deprecates getenv (C4996) and these builds treat
// warnings as errors, so a raw call fails the Windows legs outright (C2220) while leaving the
// Linux and macOS legs green.
auto
env_or(const char* name, const char* fallback) -> std::string
{
  return safe_getenv(name).value_or(fallback);
}

void
kv_op_emits_span_and_metric_over_couchbase2()
{
  const auto connstr = safe_getenv("TEST_CONNECTION_STRING");
  if (!connstr.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  const auto& connection_string = connstr.value();
  if (connection_string.rfind("couchbase2://", 0) != 0) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }

  auto observed = std::make_shared<emitted>();

  couchbase::cluster_options options{ env_or("TEST_CB2_USERNAME", "Administrator"),
                                      env_or("TEST_CB2_PASSWORD", "password") };
  options.security().tls_verify(couchbase::tls_verify_mode::none);
  options.tracing().tracer(std::make_shared<recording_tracer>(observed));
  options.metrics().meter(std::make_shared<recording_meter>(observed));

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds");

  auto collection =
    cluster.bucket(env_or("TEST_CB2_BUCKET", "default")).scope("_default").collection("_default");
  auto [upsert_err, upsert_result] = collection.upsert("cng-observability-key", true, {}).get();
  assert_false(upsert_err.ec().operator bool(), "upsert over couchbase2 succeeds");
  static_cast<void>(upsert_result); // only the error and the emitted spans/metrics matter here

  cluster.close().get();

  const auto bucket = env_or("TEST_CB2_BUCKET", "default");
  const auto seen = observed->describe();

  // The shape, not the count. Each assertion below names a regression it would catch.

  // A KV operation keeps the classic top-level span and its request_encoding child. Losing the
  // child -- the likeliest casualty of transport-specific code creeping into the public layer --
  // leaves the counters non-zero and would pass a "something was emitted" check.
  assert_true(observed->span_names == std::vector<std::string>{ "upsert", "request_encoding" },
              "KV emits exactly the upsert span and its request_encoding child; " + seen);

  // RFC 77: couchbase2 must NOT emit dispatch_to_server -- the gRPC library's own span replaces it.
  // Reintroducing it would double-count dispatch in every trace.
  assert_false(observed->has_span("dispatch_to_server"),
               "couchbase2 leaves dispatch_to_server to the gRPC layer; " + seen);

  // db.operation.name matches the span name on both transports; compared against it rather than a
  // literal so the two cannot drift apart.
  assert_eq(observed->tag("upsert", "db.operation.name"),
            observed->span_names.at(0),
            "db.operation.name matches the span name");
  assert_eq(observed->tag("upsert", "couchbase.service"),
            std::string{ "kv" },
            "the span is attributed to the kv service, not a transport-specific one");
  assert_eq(observed->tag("upsert", "db.system.name"), std::string{ "couchbase" }, "system tag");
  assert_eq(observed->tag("upsert", "db.namespace"), bucket, "the span names the bucket");

  // The keyspace is three parts, not one: a path that stopped at the bucket would satisfy every
  // assertion above.
  assert_eq(observed->tag("upsert", "couchbase.scope.name"),
            std::string{ "_default" },
            "the span names the scope");
  assert_eq(observed->tag("upsert", "couchbase.collection.name"),
            std::string{ "_default" },
            "the span names the collection");

  // Latency lands on the transport-agnostic metric, tagged for the right service and operation --
  // metrics filed under the wrong service are invisible until someone reads a dashboard.
  assert_true(observed->metric_names == std::vector<std::string>{ "db.client.operation.duration" },
              "exactly one operation-duration metric is recorded; " + seen);
  assert_eq(observed->metric_tag(0, "couchbase.service"),
            std::string{ "kv" },
            "the metric is tagged with the kv service");
  assert_eq(observed->metric_tag(0, "db.operation.name"),
            std::string{ "upsert" },
            "the metric is tagged with the operation");
  assert_eq(observed->metric_tag(0, "couchbase.scope.name"),
            std::string{ "_default" },
            "the metric is tagged with the scope");
  assert_eq(observed->metric_tag(0, "couchbase.collection.name"),
            std::string{ "_default" },
            "the metric is tagged with the collection");

  // couchbase.retries is deliberately NOT asserted. It is emitted, but make_error_context pins
  // retry_attempts at 0 and the retry loops never write the accumulated state back, so its value is
  // always "0" regardless of what happened. Asserting "0" would lock that defect in as expected
  // behaviour; it is tracked separately (CXXCBC-921).
  assert_false(observed->tag("upsert", "couchbase.retries").empty(),
               "the retry attribute is present (its value is tracked by CXXCBC-921)");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_observability",
    {
      { "kv_op_emits_span_and_metric_over_couchbase2",
        kv_op_emits_span_and_metric_over_couchbase2,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::cng::test
