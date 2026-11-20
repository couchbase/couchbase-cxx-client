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

#include "core/tracing/attribute_helpers.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"
#include "core/tracing/wrapper_sdk_tracer.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/tracing/request_span.hxx>

#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
#include <couchbase/tracing/otel_tracer.hxx>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/trace/span.h>
#endif

#include <asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace couchbase::test
{
namespace
{
class legacy_only_span : public couchbase::tracing::request_span
{
public:
  using couchbase::tracing::request_span::request_span;

  std::map<std::string, std::uint64_t> uint_tags{};
  std::map<std::string, std::string> string_tags{};

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    uint_tags[name] = value;
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    string_tags[name] = value;
  }

  void end() override
  {
  }
};

class native_boolean_span
  : public couchbase::tracing::request_span
  , public couchbase::tracing::boolean_tag_request_span
{
public:
  using couchbase::tracing::request_span::add_tag;
  using couchbase::tracing::request_span::request_span;

  std::map<std::string, std::uint64_t> uint_tags{};
  std::map<std::string, std::string> string_tags{};
  std::map<std::string, bool> bool_tags{};

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    uint_tags[name] = value;
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    string_tags[name] = value;
  }

  void add_bool_tag(const std::string& name, bool value) override
  {
    bool_tags[name] = value;
  }

  void end() override
  {
  }
};

void
request_span_forwards_boolean_tags_to_boolean_tag_request_span([[maybe_unused]] context& ctx)
{
  auto impl = std::make_shared<native_boolean_span>("op");
  const std::shared_ptr<couchbase::tracing::request_span> span = impl;

  const bool const_false = false;
  span->add_tag("enabled", true);
  span->add_tag("disabled", const_false);
  couchbase::core::tracing::add_bool_tag(span, "helper_true", true);

  assert_eq(impl->bool_tags.size(), std::size_t{ 3 }, "three native boolean tags recorded");
  assert_true(impl->bool_tags.at("enabled"), "enabled recorded as true");
  assert_false(impl->bool_tags.at("disabled"), "disabled recorded as false");
  assert_true(impl->bool_tags.at("helper_true"), "helper recorded true");
  assert_true(impl->uint_tags.empty(), "no integer tags recorded");
  assert_true(impl->string_tags.empty(), "no string tags recorded");
}

void
legacy_request_span_falls_back_to_integer_tags([[maybe_unused]] context& ctx)
{
  auto impl = std::make_shared<legacy_only_span>("op");
  const std::shared_ptr<couchbase::tracing::request_span> span = impl;

  span->add_tag("enabled", true);
  span->add_tag("disabled", false);
  couchbase::core::tracing::add_bool_tag(span, "helper_false", false);

  assert_eq(impl->uint_tags.size(), std::size_t{ 3 }, "integer fallback recorded three tags");
  assert_eq(impl->uint_tags.at("enabled"), std::uint64_t{ 1 }, "true maps to 1");
  assert_eq(impl->uint_tags.at("disabled"), std::uint64_t{ 0 }, "false maps to 0");
  assert_eq(impl->uint_tags.at("helper_false"), std::uint64_t{ 0 }, "helper false maps to 0");
  assert_true(impl->string_tags.empty(), "no string tags recorded");
}

void
string_literals_and_integers_do_not_convert_to_bool([[maybe_unused]] context& ctx)
{
  auto impl = std::make_shared<native_boolean_span>("op");
  const std::shared_ptr<couchbase::tracing::request_span> span = impl;

  const char* cstr = "cstr_value";
  span->add_tag("literal", "literal_value");
  span->add_tag("cstr", cstr);
  span->add_tag("std_string", std::string{ "string_value" });
  span->add_tag("zero_int", 0);
  span->add_tag("one_uint", 1U);
  span->add_tag("u64", std::uint64_t{ 42 });

  impl->add_tag("derived_literal", "value");
  impl->add_tag("derived_bool", true);
  impl->add_tag("derived_int", 7);

  assert_eq(impl->bool_tags.size(), std::size_t{ 1 }, "only explicit bool routed to bool_tags");
  assert_true(impl->bool_tags.at("derived_bool"), "derived bool tag recorded");
  assert_eq(impl->string_tags.at("literal"), std::string{ "literal_value" }, "literal kept string");
  assert_eq(impl->string_tags.at("cstr"), std::string{ "cstr_value" }, "const char* kept string");
  assert_eq(
    impl->string_tags.at("std_string"), std::string{ "string_value" }, "std::string kept string");
  assert_eq(
    impl->string_tags.at("derived_literal"), std::string{ "value" }, "derived literal kept string");
  assert_eq(impl->uint_tags.at("zero_int"), std::uint64_t{ 0 }, "int 0 routed to uint64_t");
  assert_eq(impl->uint_tags.at("one_uint"), std::uint64_t{ 1 }, "unsigned 1 routed to uint64_t");
  assert_eq(impl->uint_tags.at("u64"), std::uint64_t{ 42 }, "uint64_t routed to uint64_t");
  assert_eq(impl->uint_tags.at("derived_int"), std::uint64_t{ 7 }, "derived int routed to uint64");
}

void
wrapper_sdk_span_stores_boolean_tags([[maybe_unused]] context& ctx)
{
  couchbase::core::tracing::wrapper_sdk_tracer tracer{};
  auto root = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>("root");
  auto child_base = tracer.start_span("child", root);

  child_base->add_tag("flag_true", true);
  child_base->add_tag("flag_false", false);
  child_base->add_tag("flag_overwrite", false);
  child_base->add_tag("flag_overwrite", true);
  child_base->add_tag("label", "value");
  child_base->add_tag("count", std::uint64_t{ 9 });

  const auto children = root->children();
  assert_eq(children.size(), std::size_t{ 1 }, "one wrapper child span created");
  const auto& child = children.front();
  assert_eq(child->bool_tags().size(), std::size_t{ 3 }, "three boolean keys stored");
  assert_true(child->bool_tags().at("flag_true"), "flag_true stored as true");
  assert_false(child->bool_tags().at("flag_false"), "flag_false stored as false");
  assert_true(child->bool_tags().at("flag_overwrite"), "second assignment overwrites boolean tag");
  assert_eq(
    child->string_tags().at("label"), std::string{ "value" }, "string tag stored separately");
  assert_eq(child->uint_tags().at("count"), std::uint64_t{ 9 }, "uint tag stored separately");
}

void
noop_and_threshold_spans_ignore_boolean_tags([[maybe_unused]] context& ctx)
{
  couchbase::core::tracing::noop_tracer noop{};
  auto noop_span = noop.start_span("noop", nullptr);
  assert_false(noop_span->uses_tags(), "noop span reports uses_tags false");
  noop_span->add_tag("ignored", true);
  noop_span->end();

  asio::io_context io{};
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds::zero();
  auto threshold = std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);
  auto op_span = threshold->start_span("get", nullptr);
  op_span->add_tag(couchbase::core::tracing::attributes::op::service,
                   couchbase::core::tracing::service::key_value);
  op_span->add_tag(couchbase::core::tracing::attributes::dispatch::server_duration, true);
  std::this_thread::sleep_for(std::chrono::microseconds{ 5 });
  op_span->end();

  const auto report = threshold->flush_and_create_output().value_or(std::string{});
  assert_contains(report,
                  R"("last_server_duration_us":0)",
                  "boolean server_duration tag does not fall back to integer 1");
  assert_contains(report,
                  R"("total_server_duration_us":0)",
                  "boolean server_duration tag leaves total_server_duration_us at 0");
}

#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
class recording_otel_span final : public opentelemetry::trace::Span
{
public:
  enum class kind {
    boolean,
    integer,
    string,
    other,
  };

  struct entry {
    kind value_kind{ kind::other };
    bool bool_val{ false };
    std::uint64_t uint_val{ 0 };
    std::string str_val{};
  };

  std::map<std::string, entry> attributes{};

  void SetAttribute(opentelemetry::nostd::string_view key,
                    const opentelemetry::common::AttributeValue& value) noexcept override
  {
    entry e{};
    if (opentelemetry::nostd::holds_alternative<bool>(value)) {
      e.value_kind = kind::boolean;
      e.bool_val = opentelemetry::nostd::get<bool>(value);
    } else if (opentelemetry::nostd::holds_alternative<std::uint64_t>(value)) {
      e.value_kind = kind::integer;
      e.uint_val = opentelemetry::nostd::get<std::uint64_t>(value);
    } else if (opentelemetry::nostd::holds_alternative<opentelemetry::nostd::string_view>(value)) {
      e.value_kind = kind::string;
      const auto sv = opentelemetry::nostd::get<opentelemetry::nostd::string_view>(value);
      e.str_val.assign(sv.data(), sv.size());
    } else if (opentelemetry::nostd::holds_alternative<const char*>(value)) {
      e.value_kind = kind::string;
      e.str_val = opentelemetry::nostd::get<const char*>(value);
    }
    attributes[std::string{ key.data(), key.size() }] = std::move(e);
  }

  void AddEvent(opentelemetry::nostd::string_view /* name */) noexcept override
  {
  }

  void AddEvent(opentelemetry::nostd::string_view /* name */,
                opentelemetry::common::SystemTimestamp /* timestamp */) noexcept override
  {
  }

  void AddEvent(opentelemetry::nostd::string_view /* name */,
                opentelemetry::common::SystemTimestamp /* timestamp */,
                const opentelemetry::common::KeyValueIterable& /* attributes */) noexcept override
  {
  }

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  void AddLink(const opentelemetry::trace::SpanContext& /* target */,
               const opentelemetry::common::KeyValueIterable& /* attrs */) noexcept override
  {
  }

  void AddLinks(
    const opentelemetry::trace::SpanContextKeyValueIterable& /* links */) noexcept override
  {
  }
#endif

  void SetStatus(opentelemetry::trace::StatusCode /* code */,
                 opentelemetry::nostd::string_view /* description */) noexcept override
  {
  }

  void UpdateName(opentelemetry::nostd::string_view /* name */) noexcept override
  {
  }

  void End(const opentelemetry::trace::EndSpanOptions& /* options */) noexcept override
  {
  }

  [[nodiscard]] auto IsRecording() const noexcept -> bool override
  {
    return true;
  }

  [[nodiscard]] auto GetContext() const noexcept -> opentelemetry::trace::SpanContext override
  {
    return opentelemetry::trace::SpanContext::GetInvalid();
  }
};

void
otel_request_span_records_native_boolean_attributes([[maybe_unused]] context& ctx)
{
  auto raw = std::make_shared<recording_otel_span>();
  const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> otel_ptr{ raw };
  auto wrapped = std::make_shared<couchbase::tracing::otel_request_span>(otel_ptr);
  const std::shared_ptr<couchbase::tracing::request_span> base = wrapped;

  base->add_tag("without_content", true);
  wrapped->add_tag("from_exclusive", false);
  base->add_tag("scan_type", "range");
  base->add_tag("num_partitions", std::uint64_t{ 1024 });

  assert_eq(raw->attributes.size(), std::size_t{ 4 }, "four OTel attributes recorded");
  assert_true(raw->attributes.at("without_content").value_kind ==
                recording_otel_span::kind::boolean,
              "without_content passed as native bool");
  assert_true(raw->attributes.at("without_content").bool_val, "without_content is true");
  assert_true(raw->attributes.at("from_exclusive").value_kind == recording_otel_span::kind::boolean,
              "from_exclusive passed as native bool");
  assert_false(raw->attributes.at("from_exclusive").bool_val, "from_exclusive is false");
  assert_true(raw->attributes.at("scan_type").value_kind == recording_otel_span::kind::string,
              "string literal passed as string");
  assert_eq(raw->attributes.at("scan_type").str_val, std::string{ "range" }, "scan_type matches");
  assert_true(raw->attributes.at("num_partitions").value_kind == recording_otel_span::kind::integer,
              "uint64_t passed as integer");
  assert_eq(
    raw->attributes.at("num_partitions").uint_val, std::uint64_t{ 1024 }, "num_partitions matches");
}
#endif
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(request_span_forwards_boolean_tags_to_boolean_tag_request_span) },
      { CASE(legacy_request_span_falls_back_to_integer_tags) },
      { CASE(string_literals_and_integers_do_not_convert_to_bool) },
      { CASE(wrapper_sdk_span_stores_boolean_tags) },
      { CASE(noop_and_threshold_spans_ignore_boolean_tags) },
#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
      { CASE(otel_request_span_records_native_boolean_attributes) },
#endif
    },
  };
}

} // namespace couchbase::test
