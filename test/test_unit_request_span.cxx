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

#include "core/tracing/threshold_logging_tracer.hxx"

#include <couchbase/tracing/request_span.hxx>

#include <cstdint>
#include <optional>
#include <string>

namespace
{
// A span that implements only the required interface — it does not opt into typed dispatch capture,
// so it must behave like any external tracer's span.
class minimal_span : public couchbase::tracing::request_span
{
public:
  void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
  {
  }
  void add_tag(const std::string& /* name */, const std::string& /* value */) override
  {
  }
  void end() override
  {
  }
};

// A span that opts into typed dispatch capture.
class capturing_span : public minimal_span
{
public:
  std::optional<std::uint32_t> captured_operation_id{};

  auto try_set_dispatch_operation_id(std::uint32_t opaque) -> bool override
  {
    captured_operation_id = opaque;
    return true;
  }
};
} // namespace

TEST_CASE("unit: a span does not capture the typed dispatch operation id by default", "[unit]")
{
  minimal_span span;
  // External tracers rely on this default being false so the SDK records the operation id as a
  // string tag instead.
  REQUIRE_FALSE(span.try_set_dispatch_operation_id(0x1a));
}

TEST_CASE("unit: a span may opt into capturing the typed dispatch operation id", "[unit]")
{
  capturing_span span;
  REQUIRE(span.try_set_dispatch_operation_id(0x1a));
  REQUIRE(span.captured_operation_id == 0x1a);
}

TEST_CASE("unit: a captured dispatch operation id formats to the reported string", "[unit]")
{
  using couchbase::core::tracing::format_dispatch_operation_id;
  // The reporting path turns the captured raw opaque into exactly the "0x<hex>" string that used to
  // be built eagerly as a tag. Cover the boundaries, including the widest value (10 chars) that
  // must still fit the fixed formatting buffer.
  REQUIRE(format_dispatch_operation_id(0x1a) == "0x1a");
  REQUIRE(format_dispatch_operation_id(0) == "0x0");
  REQUIRE(format_dispatch_operation_id(0xffffffff) == "0xffffffff");
}
