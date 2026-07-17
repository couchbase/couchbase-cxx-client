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

#include "core/tracing/constants.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"

#include <couchbase/tracing/request_span.hxx>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <optional>
#include <string>

namespace
{
// Sanitizer builds intercept allocation and provide their own operator new/delete, so this counter
// and the overrides below are compiled out under sanitizers (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED),
// along with the two allocation-count tests; the functional capture behaviour is covered elsewhere.
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
std::atomic<long> g_alloc_count{ 0 };
#endif

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

// A span that opts into typed dispatch capture and records the captured values so the test can
// assert the SDK routed them through the typed path.
class capturing_span : public minimal_span
{
public:
  std::optional<std::uint32_t> captured_operation_id{};
  std::optional<std::string> captured_local_id{};
  std::optional<std::uint64_t> captured_server_duration_us{};
  std::optional<std::string> captured_peer_address{};
  std::optional<std::uint16_t> captured_peer_port{};

  auto try_set_dispatch_operation_id(std::uint32_t opaque) -> bool override
  {
    captured_operation_id = opaque;
    return true;
  }

  auto try_set_dispatch_local_id(const std::string& local_id) -> bool override
  {
    captured_local_id = local_id;
    return true;
  }

  auto try_set_dispatch_result(std::uint64_t server_duration_us,
                               const std::string& peer_address,
                               std::uint16_t peer_port) -> bool override
  {
    captured_server_duration_us = server_duration_us;
    captured_peer_address = peer_address;
    captured_peer_port = peer_port;
    return true;
  }
};

// A span whose typed setters record nothing but a flag — used to isolate the cost of the call
// itself (the tag-name temporary) from any value copy the capture would otherwise perform.
class flag_only_span : public minimal_span
{
public:
  bool touched{ false };

  auto try_set_dispatch_local_id(const std::string& /* local_id */) -> bool override
  {
    touched = true;
    return true;
  }

  auto try_set_dispatch_result(std::uint64_t /* server_duration_us */,
                               const std::string& /* peer_address */,
                               std::uint16_t /* peer_port */) -> bool override
  {
    touched = true;
    return true;
  }
};
} // namespace

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void*
operator new(std::size_t n)
{
  g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  void* p = std::malloc(n != 0 ? n : 1);
  if (p == nullptr) {
    throw std::bad_alloc{};
  }
  return p;
}

void
operator delete(void* p) noexcept
{
  std::free(p);
}

void
operator delete(void* p, std::size_t) noexcept
{
  std::free(p);
}
#endif

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

TEST_CASE("unit: a span does not capture typed dispatch metadata by default", "[unit]")
{
  minimal_span span;
  // External tracers rely on these defaults being false so the SDK falls back to string tags.
  REQUIRE_FALSE(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"));
  REQUIRE_FALSE(span.try_set_dispatch_result(120, "192.168.1.5", 11210));
}

TEST_CASE("unit: a span may opt into capturing typed dispatch metadata", "[unit]")
{
  capturing_span span;

  REQUIRE(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"));
  REQUIRE(span.captured_local_id == "66388CF5BFCF7522/18CC8791579B567C");

  REQUIRE(span.try_set_dispatch_result(120, "192.168.1.5", 11210));
  REQUIRE(span.captured_server_duration_us == 120);
  REQUIRE(span.captured_peer_address == "192.168.1.5");
  REQUIRE(span.captured_peer_port == 11210);
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
TEST_CASE("unit: the typed local_id setter avoids the tag-name allocation on the hot path",
          "[unit]")
{
  flag_only_span span;

  // The string tag path materializes a std::string for the tag name from the const char* constant,
  // which may heap-allocate; the typed setter takes the value directly and never constructs a
  // tag-name temporary. Assert the invariant -- the typed path allocates nothing and is never worse
  // than the tag path -- rather than an absolute tag-path count: whether "couchbase.local_id"
  // heap-allocates depends on the standard library's SSO capacity (libc++'s larger SSO keeps it
  // inline) and on allocation instrumentation being active, so a fixed ">= 1" is not portable.
  const long before_tag = g_alloc_count.load(std::memory_order_relaxed);
  span.add_tag(couchbase::core::tracing::attributes::dispatch::local_id, "x");
  const long tag_allocs = g_alloc_count.load(std::memory_order_relaxed) - before_tag;

  const long before_typed = g_alloc_count.load(std::memory_order_relaxed);
  const bool captured = span.try_set_dispatch_local_id("x");
  const long typed_allocs = g_alloc_count.load(std::memory_order_relaxed) - before_typed;

  REQUIRE(captured);
  REQUIRE(typed_allocs == 0);
  REQUIRE(typed_allocs <= tag_allocs);
}

TEST_CASE("unit: the typed result setter avoids the tag-name allocations on the hot path", "[unit]")
{
  flag_only_span span;

  // Each of the three string-tag names (server_duration/peer_address/peer_port) may allocate a
  // temporary std::string for the name; the single typed call captures all three with no tag-name
  // temporaries. As above, assert the invariant (typed path allocates nothing and is never worse
  // than the tag path) rather than an absolute count, since how many of the three names exceed the
  // SSO limit depends on the standard library and instrumentation.
  const long before_tags = g_alloc_count.load(std::memory_order_relaxed);
  span.add_tag(couchbase::core::tracing::attributes::dispatch::server_duration,
               static_cast<std::uint64_t>(120));
  span.add_tag(couchbase::core::tracing::attributes::dispatch::peer_address, "p");
  span.add_tag(couchbase::core::tracing::attributes::dispatch::peer_port,
               static_cast<std::uint64_t>(11210));
  const long tag_allocs = g_alloc_count.load(std::memory_order_relaxed) - before_tags;

  const long before_typed = g_alloc_count.load(std::memory_order_relaxed);
  const bool captured = span.try_set_dispatch_result(120, "p", 11210);
  const long typed_allocs = g_alloc_count.load(std::memory_order_relaxed) - before_typed;

  REQUIRE(captured);
  REQUIRE(typed_allocs == 0);
  REQUIRE(typed_allocs <= tag_allocs);
}
#endif
