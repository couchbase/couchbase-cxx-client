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

#include "framework/test_registry.hxx"

#include "core/tracing/constants.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"

#include <couchbase/tracing/request_span.hxx>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <optional>
#include <string>

namespace
{
// Sanitizer builds intercept allocation and provide their own operator new/delete, so this counter
// and the overrides below are compiled out under sanitizers (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED),
// along with the two allocation-count cases; the functional capture behaviour is covered elsewhere.
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
std::atomic<long> g_alloc_count{ 0 };
#endif
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

namespace couchbase::test
{
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

// A span that opts into typed dispatch capture and records the captured values so the case can
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

void
a_span_does_not_capture_the_typed_dispatch_operation_id_by_default([[maybe_unused]] context& ctx)
{
  minimal_span span;
  // External tracers rely on this default being false so the SDK records the operation id as a
  // string tag instead.
  assert_false(span.try_set_dispatch_operation_id(0x1a), "the default declines the typed capture");
}

void
a_span_may_opt_into_capturing_the_typed_dispatch_operation_id([[maybe_unused]] context& ctx)
{
  capturing_span span;
  assert_true(span.try_set_dispatch_operation_id(0x1a), "an opted-in span accepts the capture");
  assert_true(span.captured_operation_id == 0x1a, "the raw opaque reaches the span");
}

void
a_captured_dispatch_operation_id_formats_to_the_reported_string([[maybe_unused]] context& ctx)
{
  using couchbase::core::tracing::format_dispatch_operation_id;
  // The reporting path turns the captured raw opaque into exactly the "0x<hex>" string that used to
  // be built eagerly as a tag. Cover the boundaries, including the widest value (10 chars) that
  // must still fit the fixed formatting buffer.
  assert_eq(format_dispatch_operation_id(0x1a), "0x1a", "a typical opaque");
  assert_eq(format_dispatch_operation_id(0), "0x0", "the smallest opaque");
  assert_eq(format_dispatch_operation_id(0xffffffff), "0xffffffff", "the widest opaque");
}

void
a_span_does_not_capture_typed_dispatch_metadata_by_default([[maybe_unused]] context& ctx)
{
  minimal_span span;
  // External tracers rely on these defaults being false so the SDK falls back to string tags.
  assert_false(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"),
               "the default declines the local id");
  assert_false(span.try_set_dispatch_result(120, "192.168.1.5", 11210),
               "the default declines the dispatch result");
}

void
a_span_may_opt_into_capturing_typed_dispatch_metadata([[maybe_unused]] context& ctx)
{
  capturing_span span;

  assert_true(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"),
              "an opted-in span accepts the local id");
  assert_true(span.captured_local_id == "66388CF5BFCF7522/18CC8791579B567C",
              "the local id reaches the span");

  assert_true(span.try_set_dispatch_result(120, "192.168.1.5", 11210),
              "an opted-in span accepts the dispatch result");
  assert_true(span.captured_server_duration_us == 120, "the server duration reaches the span");
  assert_true(span.captured_peer_address == "192.168.1.5", "the peer address reaches the span");
  assert_true(span.captured_peer_port == 11210, "the peer port reaches the span");
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void
the_typed_local_id_setter_avoids_the_tag_name_allocation([[maybe_unused]] context& ctx)
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

  assert_true(captured, "the typed setter is the path taken");
  assert_eq(typed_allocs, 0L, "the typed local id setter allocates nothing");
  assert_true(typed_allocs <= tag_allocs, "the typed path is never worse than the tag path");
}

void
the_typed_result_setter_avoids_the_tag_name_allocations([[maybe_unused]] context& ctx)
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

  assert_true(captured, "the typed setter is the path taken");
  assert_eq(typed_allocs, 0L, "the typed result setter allocates nothing");
  assert_true(typed_allocs <= tag_allocs, "the typed path is never worse than the tag path");
}
#endif
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_span_does_not_capture_the_typed_dispatch_operation_id_by_default) },
      { CASE(a_span_may_opt_into_capturing_the_typed_dispatch_operation_id) },
      { CASE(a_captured_dispatch_operation_id_formats_to_the_reported_string) },
      { CASE(a_span_does_not_capture_typed_dispatch_metadata_by_default) },
      { CASE(a_span_may_opt_into_capturing_typed_dispatch_metadata) },
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
      { CASE(the_typed_local_id_setter_avoids_the_tag_name_allocation) },
      { CASE(the_typed_result_setter_avoids_the_tag_name_allocations) },
#endif
    },
  };
}

} // namespace couchbase::test
